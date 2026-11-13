// ----------------------------------------------------------------------
// File: Logging.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "common/Namespace.hh"
#include "common/Logging.hh"
#include "common/Path.hh"
#include <grpc/support/log.h>
#if defined(EOS_HAVE_ZSTD) && EOS_HAVE_ZSTD
#include <zstd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif
#include <XrdSys/XrdSysPthread.hh>
#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <new>
#include <type_traits>

//------------------------------------------------------------------------------
// Anonymous namespace for gRPC logging helpers
//------------------------------------------------------------------------------
namespace
{
//------------------------------------------------------------------------------
// Disable gRPC logging
//------------------------------------------------------------------------------
void
DisableGrpcLogging()
{
#ifdef HAVE_GRPC_LOGGING
  auto GrpcLogFunction = [](gpr_log_func_args*) {
    // This function intentionally does nothing to suppress gRPC logs
  };
  gpr_set_log_function(GrpcLogFunction);
#else

#endif
}

//------------------------------------------------------------------------------
// Enable gRPC logging
//------------------------------------------------------------------------------
void
EnableGrpcLogging()
{
#ifdef HAVE_GRPC_LOGGING
  gpr_set_log_function(nullptr);
#else

#endif
}
}

EOSCOMMONNAMESPACE_BEGIN

static std::atomic<int> sCounter {0};
static typename std::aligned_storage<sizeof(Logging), alignof(Logging)>::type
logging_buf; ///< Memory for the global logging object
Logging& gLogging = reinterpret_cast<Logging&>(logging_buf);

#if defined(EOS_HAVE_ZSTD) && EOS_HAVE_ZSTD
struct ZstdLogState {
  int fd = -1;
  ZSTD_CStream* cstream = nullptr;
  time_t segmentStart = 0;
  std::string currentPath;
  std::string symlinkPath;
};
#endif
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
LoggingInitializer::LoggingInitializer()
{
  if (sCounter++ == 0) {
    new (&gLogging) Logging(); // placement new
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
LoggingInitializer::~LoggingInitializer()
{
  if (--sCounter == 0) {
    gLogging.LB->shutDown();
    (&gLogging)->~Logging();
  }
}

//------------------------------------------------------------------------------
// Get singleton instance
//------------------------------------------------------------------------------
Logging&
Logging::GetInstance()
{
  return gLogging;
}

//------------------------------------------------------------------------------
/// Destructor
//------------------------------------------------------------------------------
// Logging::~Logging(){
// 	if (LB)
// 		delete LB;
// }

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Logging::Logging()
    : gLogMask(0)
    , gPriorityLevel(0)
    , gToSysLog(false)
    , gToStderr(true)
    , gUnit("none")
    , gShortFormat(0)
    , gRateLimiter(false)
{
  LB = new LogBuffer;
  // Initialize the log array and sets the log circular size
  gLogCircularIndex.resize(LOG_DEBUG + 1);
  gLogMemory.resize(LOG_DEBUG + 1);
  gCircularIndexSize = EOSCOMMONLOGGING_CIRCULARINDEXSIZE;

  for (int i = 0; i <= LOG_DEBUG; i++) {
    gLogCircularIndex[i] = 0;
    gLogMemory[i].resize(gCircularIndexSize);
  }

  gZeroVid.name = "-";
  XrdOucString tosyslog;

  if (getenv("EOS_LOG_SYSLOG")) {
    tosyslog = getenv("EOS_LOG_SYSLOG");

    if ((tosyslog == "1" ||
         (tosyslog == "true"))) {
      gToSysLog = true;
    }
  }

#if defined(EOS_HAVE_ZSTD) && EOS_HAVE_ZSTD
  // ZSTD logging configuration from environment
  const char* zenv = getenv("EOS_ZSTD_LOGGING");

  if (zenv && (!strcmp(zenv, "1") || !strcasecmp(zenv, "true") ||
               !strcasecmp(zenv, "yes") || !strcasecmp(zenv, "on"))) {
    gZstdEnable = true;
  }

  // Prefer renamed variables; fallback to legacy for compatibility
  const char* zrot = getenv("EOS_ZSTD_LOGGING_ROTATION");

  if ((!zrot || !*zrot)) {
    zrot = getenv("EOS_ZSTD_ROTATION");
  }

  if (zrot && *zrot) {
    int v = atoi(zrot);

    if (v > 0) {
      gZstdRotationSeconds = v;
    }
  }

  const char* lvl = getenv("EOS_ZSTD_LOGGING_LEVEL");

  if ((!lvl || !*lvl)) {
    lvl = getenv("EOS_ZSTD_LEVEL");
  }

  if (lvl && *lvl) {
    int v = atoi(lvl);

    if (v >= 1 && v <= 19) {
      gZstdLevel = v;
    }
  }

  const char* xrd = getenv("XRDLOGDIR");
  gZstdBaseDir = (xrd && *xrd) ? xrd : "/var/log/eos";

  if (gZstdEnable) {
    // Redirect STDERR into our pipe to capture messages from other components
    int fds[2];

    if (::pipe(fds) == 0) {
      gStderrPipeRead = fds[0];
      gStderrPipeWrite = fds[1];
      fflush(stderr);
      ::dup2(gStderrPipeWrite, STDERR_FILENO);
      gStderrThread = std::thread([this]() {
        this->stderrReaderLoop();
      });
      gStderrThread.detach();
    }

    // Initialize zstd logging dir and migrate any existing plain main log
    zstdMaybeInit();
    zstdMigratePlainMain();
  }

#endif
}

//------------------------------------------------------------------------------
// Should log function
//------------------------------------------------------------------------------
bool
Logging::shouldlog(const char* func, int priority)
{
  if (priority == LOG_SILENT) {
    return true;
  }

  // short cut if log messages are masked
  if (!((LOG_MASK(priority) & gLogMask))) {
    return false;
  }

  // apply filter to avoid message flooding for debug messages
  if (priority >= LOG_INFO) {
    if (gDenyFilter.Num()) {
      // this is a normal filter by function name
      if (gDenyFilter.Find(func)) {
        return false;
      }
    }
  }

  return true;
}


LogBuffer::log_buffer*
LogBuffer::log_alloc_buffer()
{
  static thread_local std::unique_ptr<log_buffer> tl_buffer;

  if (shuttingDown) {
    return nullptr;
  }

  if (!tl_buffer) {
    tl_buffer.reset(new log_buffer());
  }

  tl_buffer->h.fanOutBuffer = nullptr;
  tl_buffer->h.fanOutTag[0] = '\0';
  return tl_buffer.get();
}

//------------------------------------------------------------------------------
// The queue lock is held only for one short append: spin a little on
// try_lock before the mutex puts the thread to sleep
//------------------------------------------------------------------------------
static void
lock_queue(std::unique_lock<std::mutex>& guard)
{
  for (int i = 0; i < 100; ++i) {
    if (guard.try_lock()) {
      return;
    }

#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("yield");
#endif
  }

  guard.lock();
}

void
LogBuffer::log_queue_buffer(LogBuffer::log_buffer* buff)
{
  // Build the record outside the lock, the lock only covers one append
  static thread_local std::string tl_record;
  log_record rec{};
  rec.lineLen = strlen(buff->buffer);
  rec.msgOffset = buff->h.ptr - buff->buffer;
  rec.priority = buff->h.priority;
  // fanOutS/fanOut are only set when fanOutBuffer is set
  rec.fanOutS = buff->h.fanOutBuffer ? buff->h.fanOutS : nullptr;
  rec.hasFanOut = buff->h.fanOutBuffer && buff->h.fanOut;

  if (rec.hasFanOut) {
    rec.fanOut = buff->h.fanOut;
    rec.fanOutLen = strlen(buff->h.fanOutBuffer);
  }

  memcpy(rec.fanOutTag, buff->h.fanOutTag, sizeof(rec.fanOutTag));
  memcpy(rec.sourceTag, buff->h.sourceTag, sizeof(rec.sourceTag));
  tl_record.assign(reinterpret_cast<const char*>(&rec), sizeof(rec));
  tl_record.append(buff->buffer, rec.lineLen + 1);

  if (rec.hasFanOut) {
    tl_record.append(buff->h.fanOutBuffer, rec.fanOutLen + 1);
  }

  std::unique_lock<std::mutex> guard(log_buffer_mutex, std::defer_lock);
  lock_queue(guard);

  if (shuttingDown) {
    return;
  }

  /* this starts the log thread */
  if ((not log_thread_started) and (not log_suspended)) {
    resume_int();
  }

  const size_t limit = (size_t)max_log_buffers * 8 * 1024;

  while (!pending.empty() && (pending.size() + tl_record.size() > limit)) {
    if ((log_buffer_num_waits & 0xfff) == 0) {
      fprintf(stderr, "log_buffer_shortage #%u with %u waiters, in_q %d bytes %zu\n",
              log_buffer_num_waits, log_buffer_waiters, log_buffer_in_q, pending.size());
    }

    log_buffer_num_waits++;
    log_buffer_waiters++;
    log_buffer_shortage.wait(guard);
    log_buffer_waiters--;

    if (shuttingDown) {
      return;
    }
  }

  // The log thread only sleeps on an empty queue
  const bool wake = pending.empty();
  pending += tl_record;
  log_buffer_in_q++;

  if (wake) {
    log_buffer_cond.notify_one();
  }
}

void
LogBuffer::log_thread()
{
  std::string batch, out;
  std::vector<FILE*> toFlush;
  std::unique_lock<std::mutex> guard(log_buffer_mutex);

  while (true) {
    log_buffer_cond.wait(guard, [this] { return !pending.empty() || shuttingDown; });

    if ((shuttingDown == kShutdownDrop) || pending.empty()) {
      return;
    }

    batch.swap(pending);
    log_buffer_in_q = 0;

    if (log_buffer_waiters > 0) {
      log_buffer_shortage.notify_all();
    }

    guard.unlock();
    log_write_batch(batch, out, toFlush);
    batch.clear();
    guard.lock();
  }
}

void
LogBuffer::log_write_batch(const std::string& batch, std::string& out,
                           std::vector<FILE*>& toFlush)
{
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  out.clear();
  toFlush.clear();

  for (size_t off = 0; off < batch.size();) {
    log_record rec;
    memcpy(&rec, batch.data() + off, sizeof(rec));
    const char* line = batch.data() + off + sizeof(rec);
    const char* fanOutText = line + rec.lineLen + 1;
    off += sizeof(rec) + rec.lineLen + 1 + (rec.hasFanOut ? rec.fanOutLen + 1 : 0);

    if (g_logging.gToStderr && !g_logging.IsZstdEnabled()) {
      out.append(line, rec.lineLen);
      out += '\n';

      if (out.size() >= (1 << 20)) {
        fwrite(out.data(), 1, out.size(), stderr);
        out.clear();
      }
    }

    if (g_logging.gToSysLog) {
      syslog(rec.priority, "%s", line + rec.msgOffset);
    }

    if (g_logging.IsZstdEnabled()) {
      // Resolve canonical per-tag name from source/fanout and write compressed
      std::string tag = g_logging.ResolveZstdTag(rec.sourceTag, rec.fanOutTag);

      if (!tag.empty()) {
        g_logging.WriteZstd(tag.c_str(), line);
      }

      // Also write the main line into the compressed xrdlog.<service> stream
      std::string mainTag = g_logging.GetMainZstdTag();
      g_logging.WriteZstd(mainTag.c_str(), line);
    } else {
      auto flushLater = [&toFlush](FILE* fp) {
        if (std::find(toFlush.begin(), toFlush.end(), fp) == toFlush.end()) {
          toFlush.push_back(fp);
        }
      };

      if (rec.fanOutS) {
        fwrite(line, 1, rec.lineLen, rec.fanOutS);
        fputc('\n', rec.fanOutS);
        flushLater(rec.fanOutS);
      }

      if (rec.hasFanOut) {
        fputs(fanOutText, rec.fanOut);
        flushLater(rec.fanOut);
      }
    }
  }

  if (!out.empty()) {
    fwrite(out.data(), 1, out.size(), stderr);
  }

  fflush(stderr);

  for (FILE* fp : toFlush) {
    fflush(fp);
  }
}

//------------------------------------------------------------------------------
// Stripe of the calling thread
//------------------------------------------------------------------------------
std::mutex&
LogMemoryMutex::ThreadStripe()
{
  static std::atomic<size_t> sNext{0};
  static thread_local const size_t tl_stripe = sNext++ % kStripes;
  return mStripes[tl_stripe].mtx;
}

//------------------------------------------------------------------------------
// localtime_r takes the glibc time zone lock: convert once per second and thread
//------------------------------------------------------------------------------
static void
cached_localtime(time_t t, struct tm& out)
{
  static thread_local time_t tl_sec = -1;
  static thread_local struct tm tl_tm;

  if (t != tl_sec) {
    localtime_r(&t, &tl_tm);
    tl_sec = t;
  }

  out = tl_tm;
}

//------------------------------------------------------------------------------
// Logging function
//------------------------------------------------------------------------------
const char*
Logging::log(const char* func, const char* file, int line, const char* logid,
             const VirtualIdentity& vid, const char* cident, int priority,
             const char* msg, ...)
{
  bool silent = (priority == LOG_SILENT);

  // short cut if log messages are masked
  if (!silent && !((LOG_MASK(priority) & gLogMask))) {
    return "";
  }

  // apply filter to avoid message flooding for debug messages
  if (!silent && priority >= LOG_INFO) {
    if (gAllowFilter.Num()) {
      // if this is a pass-through filter e.g. we want to see exactly this messages
      if (!gAllowFilter.Find(func)) {
        return "";
      }
    } else if (gDenyFilter.Num()) {
      // this is a normal filter by function name
      if (gDenyFilter.Find(func)) {
        return "";
      }
    }
  }

  struct LogBuffer::log_buffer* logBuffer = LB->log_alloc_buffer();

  if (logBuffer == NULL) {
    return "";  /* log object being destroyed */
  }

  char* buffer = logBuffer->buffer;
  XrdOucString File = file;
  // we show only one hierarchy directory like Acl (assuming that we have only
  // file names like *.cc and *.hh
  File.erase(0, File.rfind("/") + 1);
  File.erase(File.length() - 3);
  // Capture source tag (basename without extension) for later zstd tag selection
  strncpy(logBuffer->h.sourceTag, File.c_str(),
          sizeof(logBuffer->h.sourceTag) - 1);
  logBuffer->h.sourceTag[sizeof(logBuffer->h.sourceTag) - 1] = '\0';
  time_t current_time;
  struct timeval tv;
  struct timezone tz;
  tm tm;
  va_list args;
  va_start(args, msg);
  gettimeofday(&tv, &tz);
  current_time = tv.tv_sec;
  char linen[16];
  sprintf(linen, "%d", line);
  char fcident[1024];
  XrdOucString truncname = vid.name;

  // we show only the last 16 bytes of the name
  if (truncname.length() > 16) {
    truncname.insert("..", 0);
    truncname.erase(0, truncname.length() - 16);
  }

  char sourceline[64];

  if (gShortFormat) {
    cached_localtime(current_time, tm);
    snprintf(sourceline, sizeof(sourceline) - 1, "%s:%s", File.c_str(), linen);
    XrdOucString slog = logid;

    if (slog.beginswith("logid:")) {
      slog.erase(0, 6);
      sprintf(buffer,
              "%02d%02d%02d %02d:%02d:%02d t=%lu.%06lu f=%-16s l=%s %s s=%-24s ",
              tm.tm_year - 100, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
              tm.tm_min, tm.tm_sec, current_time, (unsigned long) tv.tv_usec,
              func, GetPriorityString(priority), slog.c_str(), sourceline);
    } else {
      sprintf(buffer,
              "%02d%02d%02d %02d:%02d:%02d t=%lu.%06lu f=%-16s l=%s tid=%016lx s=%-24s ",
              tm.tm_year - 100, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
              tm.tm_min, tm.tm_sec, current_time, (unsigned long) tv.tv_usec,
              func, GetPriorityString(priority), (unsigned long) XrdSysThread::ID(),
              sourceline);
    }
  } else {
    sprintf(fcident,
            "tident=%s sec=%-5s uid=%d gid=%d name=%s geo=\"%s\" xt=\"%s\" ob=\"%s\"",
            cident,
            vid.prot.c_str(), vid.uid, vid.gid, truncname.c_str(), vid.geolocation.c_str(),
            vid.trace.c_str(), vid.onbehalf.c_str());
    cached_localtime(current_time, tm);
    snprintf(sourceline, sizeof(sourceline) - 1, "%s:%s", File.c_str(), linen);
    sprintf(buffer,
            "%02d%02d%02d %02d:%02d:%02d time=%lu.%06lu func=%-24s level=%s logid=%s unit=%s tid=%016lx source=%-30s %s ",
            tm.tm_year - 100, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
            tm.tm_sec, current_time, (unsigned long) tv.tv_usec, func,
            GetPriorityString(priority), logid, gUnit.c_str(),
            (unsigned long) XrdSysThread::ID(), sourceline, fcident);
  }

  char* ptr = buffer + strlen(buffer);
  // limit the length of the output to buffer-1 length
  vsnprintf(ptr, sizeof(logBuffer->buffer) - (ptr - buffer + 1), msg, args);

  if (!silent && gRateLimiter) {
    std::lock_guard<std::mutex> lock(gRateLimitMutex);

    if (rate_limit(tv, priority, file, line)) {
      va_end(args);
      return "";
    }
  }

  logBuffer->h.ptr = ptr;

  if (!silent) {
    if (gLogFanOut.size()) {
      logBuffer->h.fanOutS = NULL;
      logBuffer->h.fanOut = NULL;
      logBuffer->h.fanOutBuffer = ptr + strlen(ptr) + 1;
      logBuffer->h.fanOutBufLen = sizeof(logBuffer->buffer) -
                                  (logBuffer->h.fanOutBuffer - logBuffer->buffer);
      logBuffer->h.fanOutTag[0] = '\0';

      // we do log-message fanout
      // The "*" fan-out gets the main line, the log thread adds the newline
      if (gLogFanOut.count("*")) {
        logBuffer->h.fanOutS = gLogFanOut["*"];
        snprintf(logBuffer->h.fanOutTag, sizeof(logBuffer->h.fanOutTag), "*");
      }

      if (gLogFanOut.count(File.c_str())) {
        logBuffer->buffer[15] = 0;
        logBuffer->h.fanOut = gLogFanOut[File.c_str()];
        snprintf(logBuffer->h.fanOutBuffer, logBuffer->h.fanOutBufLen,
                 "%s %s%s%s %-30s %s \n",
                 logBuffer->buffer,
                 GetLogColour(GetPriorityString(priority)),
                 GetPriorityString(priority),
                 EOS_TEXTNORMAL,
                 sourceline,
                 logBuffer->h.ptr); /* truncation not an issue */
        logBuffer->buffer[15] = ' ';
        snprintf(logBuffer->h.fanOutTag, sizeof(logBuffer->h.fanOutTag), "%s",
                 File.c_str());
      } else {
        if (gLogFanOut.count("#")) {
          logBuffer->buffer[15] = 0;
          logBuffer->h.fanOut = gLogFanOut["#"];
          snprintf(logBuffer->h.fanOutBuffer, logBuffer->h.fanOutBufLen,
                   "%s %s%s%s [%05d/%05d] %16s ::%-16s %s \n",
                   logBuffer->buffer,
                   GetLogColour(GetPriorityString(priority)),
                   GetPriorityString(priority),
                   EOS_TEXTNORMAL,
                   vid.uid,
                   vid.gid,
                   truncname.c_str(),
                   func,
                   logBuffer->h.ptr
                  );
          logBuffer->buffer[15] = ' ';
          snprintf(logBuffer->h.fanOutTag, sizeof(logBuffer->h.fanOutTag), "#");
        }
      }
    }
  }

  va_end(args);
  const char* rptr;

  if (silent) {
    priority = LOG_DEBUG;
  }

  // store into global log memory: reserve the slot and copy the line under
  // the stripe of this thread, so a reader holding all stripes sees no gap
  {
    std::lock_guard<std::mutex> lock(gMutex.ThreadStripe());
    const unsigned long idx =
        __atomic_fetch_add(&gLogCircularIndex[priority], 1, __ATOMIC_RELAXED);
    XrdOucString& slot = gLogMemory[priority][idx % gCircularIndexSize];
    /* the following copies the buffer, hence it can be queued and vanish anytime after */
    slot = buffer;
    rptr = slot.c_str();
  }

  if (!silent) {
    logBuffer->h.priority = priority;
    LB->log_queue_buffer(logBuffer);
  }

  return rptr;
}

bool
Logging::rate_limit(struct timeval& tv, int priority, const char* file,
                    int line)
{
  static bool do_limit = false;
  static std::string last_file = "";
  static int last_line = 0;
  static int last_priority = priority;
  static struct timeval last_tv;

  if (!gRateLimiter) {
    return false;
  }

  if ((line == last_line) &&
      (priority == last_priority) &&
      (last_file == file) &&
      (priority < LOG_WARNING)) {
    float elapsed = (1.0 * (tv.tv_sec - last_tv.tv_sec)) - ((
                      tv.tv_usec - last_tv.tv_usec) / 1000000.0);

    if (elapsed < 5.0) {
      if (!do_limit) {
        fprintf(stderr,
                "                 ---- high rate error messages suppressed ----\n");
      }

      do_limit = true;
    } else {
      do_limit = false;
    }
  } else {
    do_limit = false;
  }

  if (!do_limit) {
    last_tv = tv;
    last_line = line;
    last_file = file;
    last_priority = priority;
  }

  return do_limit;
}

//------------------------------------------------------------------------------
// Set the log priority
//------------------------------------------------------------------------------
void
Logging::SetLogPriority(int pri)
{
  gLogMask = LOG_UPTO(pri);
  gPriorityLevel = pri;

  if (pri == LOG_DEBUG) {
    EnableGrpcLogging();
  } else {
    DisableGrpcLogging();
  }
}

#if defined(EOS_HAVE_ZSTD) && EOS_HAVE_ZSTD
std::string
Logging::GetMainZstdTag() const
{
  const std::string& s = gZstdUnitDir.empty() ? gZstdBaseDir : gZstdUnitDir;
  auto pos = s.find_last_of('/');
  std::string base = (pos == std::string::npos) ? s : s.substr(pos + 1);

  if (base.empty()) {
    base = "mgm";
  }

  return std::string("xrdlog.") + base;
}

std::string
Logging::resolveZstdTag(const char* sourceTag, const char* fanOutTag) const
{
  auto isAllowed = [&](const std::string & t) -> bool {
    for (const auto& a : gZstdAllowedTags) if (a == t)
      {
        return true;
      }

    return false;
  };

  // Prefer explicit fan-out tag if it exists and is allowed
  if (fanOutTag && *fanOutTag) {
    std::string t = fanOutTag;

    if (isAllowed(t)) {
      return t;
    }
  }

  // Map source tag via alias table
  std::string st = (sourceTag ? sourceTag : "");

  for (const auto& pr : gZstdAliasPairs) {
    if (st == pr.first) {
      if (isAllowed(pr.second)) {
        return pr.second;
      }
    }
  }

  // If source tag is directly in allowed list, keep it
  if (isAllowed(st)) {
    return st;
  }

  // Otherwise, do not create a separate per-tag stream
  return std::string();
}
#endif

#if defined(EOS_HAVE_ZSTD) && EOS_HAVE_ZSTD
static void write_all(int fd, const void* buf, size_t len)
{
  const char* p = static_cast<const char*>(buf);

  while (len > 0) {
    ssize_t n = ::write(fd, p, len);

    if (n <= 0) {
      if (errno == EINTR) {
        continue;
      }

      break;
    }

    p += n;
    len -= (size_t)n;
  }
}
#endif

void
Logging::stderrReaderLoop()
{
#if defined(EOS_HAVE_ZSTD) && EOS_HAVE_ZSTD
  // Read from gStderrPipeRead and forward to main compressed log
  std::string mainTag = GetMainZstdTag();
  std::string buf;
  buf.reserve(8 << 10);
  char tmp[4096];

  while (true) {
    ssize_t n = ::read(gStderrPipeRead, tmp, sizeof(tmp));

    if (n <= 0) {
      if (n < 0 && errno == EINTR) {
        continue;
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }

    for (ssize_t i = 0; i < n; ++i) {
      char c = tmp[i];

      if (c == '\n') {
        WriteZstd(mainTag.c_str(), buf.c_str());
        buf.clear();
      } else {
        buf.push_back(c);
      }
    }
  }

#endif
}

void
Logging::WriteZstd(const char* tag, const char* line)
{
#if defined(EOS_HAVE_ZSTD) && EOS_HAVE_ZSTD

  if (!gZstdEnable) {
    return;
  }

  if (!line || !tag || !*tag) {
    return;
  }

  std::lock_guard<std::mutex> g(gZstdMutex);
  zstdMaybeInit();
  time_t now = ::time(nullptr);
  zstdRotateIfNeededLocked(tag, now);
  ZstdLogState* st = zstdGetStateLocked(tag);

  if (!st || st->fd < 0 || !st->cstream) {
    return;
  }

  // Build a line with newline terminator if missing
  size_t len = ::strlen(line);
  char nl = '\n';
  // First write the line
  ZSTD_inBuffer in1 = { line, len, 0 };
  char outBuf[1 << 14];

  do {
    ZSTD_outBuffer out = { outBuf, sizeof(outBuf), 0 };
    size_t r = ZSTD_compressStream2(st->cstream, &out, &in1, ZSTD_e_flush);
    write_all(st->fd, outBuf, out.pos);

    if (ZSTD_isError(r)) {
      break;
    }

    if (in1.pos == in1.size) {
      break;
    }
  } while (true);

  // Then a newline if not already present
  if (len == 0 || line[len - 1] != '\n') {
    ZSTD_inBuffer in2 = { &nl, 1, 0 };
    char outBuf2[256];

    do {
      ZSTD_outBuffer out = { outBuf2, sizeof(outBuf2), 0 };
      size_t r = ZSTD_compressStream2(st->cstream, &out, &in2, ZSTD_e_flush);
      write_all(st->fd, outBuf2, out.pos);

      if (ZSTD_isError(r)) {
        break;
      }

      if (in2.pos == in2.size) {
        break;
      }
    } while (true);
  }

#else
  (void)tag;
  (void)line;
#endif
}

#if defined(EOS_HAVE_ZSTD) && EOS_HAVE_ZSTD
void Logging::zstdMaybeInit()
{
  if (!gZstdEnable) {
    return;
  }

  if (gZstdUnitDir.empty()) {
    // Place compressed logs directly into the base log directory (no extra subdir)
    gZstdUnitDir = gZstdBaseDir;

    // Normalize: strip trailing slashes
    while (gZstdUnitDir.size() > 1 && gZstdUnitDir.back() == '/') {
      gZstdUnitDir.pop_back();
    }
  }
}

void Logging::zstdRotateIfNeededLocked(const std::string& tag, time_t now)
{
  ZstdLogState* st = zstdGetStateLocked(tag);

  if (!st || st->fd < 0 || !st->cstream) {
    zstdOpenLocked(tag, now);
    return;
  }

  if ((now - st->segmentStart) >= gZstdRotationSeconds) {
    zstdCloseLocked(tag);
    zstdOpenLocked(tag, now);
  }
}

std::string Logging::zstdMakeSegmentPath(const std::string& tag,
    time_t ts) const
{
  char tbuf[64];
  struct tm tm;
  localtime_r(&ts, &tm);
  strftime(tbuf, sizeof(tbuf), "%Y%m%d-%H%M%S", &tm);
  std::string path = gZstdUnitDir;
  path += "/logs/";
  path += tag;
  path += "-";
  path += tbuf;
  path += ".zst";
  return path;
}

void Logging::zstdEnsureDir()
{
  eos::common::Path p((gZstdUnitDir + "/.keep").c_str());
  (void)p.MakeParentPath(S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
  (void)::chmod(gZstdUnitDir.c_str(),
                S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
  // Ensure logs/ subdirectory exists for real files
  eos::common::Path lp((gZstdUnitDir + "/logs/.keep").c_str());
  (void)lp.MakeParentPath(S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
  (void)::chmod((gZstdUnitDir + "/logs").c_str(),
                S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
}

ZstdLogState* Logging::zstdGetStateLocked(const std::string& tag)
{
  auto it = gZstdStates.find(tag);

  if (it != gZstdStates.end()) {
    return it->second;
  }

  ZstdLogState* st = new ZstdLogState();
  st->symlinkPath = gZstdUnitDir + "/" + tag + ".zstd";
  gZstdStates[tag] = st;
  return st;
}

void Logging::zstdOpenLocked(const std::string& tag, time_t now)
{
  zstdEnsureDir();
  ZstdLogState* st = zstdGetStateLocked(tag);
  std::string path = zstdMakeSegmentPath(tag, now);
  int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC, 0644);

  if (fd < 0) {
    return;
  }

  st->fd = fd;
  st->segmentStart = now;
  st->currentPath = path;

  if (!st->cstream) {
    st->cstream = ZSTD_createCStream();
  }

  ZSTD_CCtx_setParameter(st->cstream, ZSTD_c_compressionLevel, gZstdLevel);
  (void)ZSTD_initCStream(st->cstream, gZstdLevel);
  // Emit a header by flushing an empty input
  char dummy = 0;
  ZSTD_inBuffer in = { &dummy, 0, 0 };
  char outBuf[64];
  ZSTD_outBuffer out = { outBuf, sizeof(outBuf), 0 };
  (void)ZSTD_compressStream2(st->cstream, &out, &in, ZSTD_e_flush);
  write_all(st->fd, outBuf, out.pos);
  // Update symlink
  ::unlink(st->symlinkPath.c_str());
  // Create relative symlink pointing into logs/
  auto slash = path.find_last_of('/');
  std::string base = (slash == std::string::npos) ? path : path.substr(slash + 1);
  std::string relTarget = "logs/";
  relTarget += base;
  ::symlink(relTarget.c_str(), st->symlinkPath.c_str());
}

void Logging::zstdCloseLocked(const std::string& tag)
{
  ZstdLogState* st = zstdGetStateLocked(tag);

  if (!st) {
    return;
  }

  if (st->cstream) {
    // End frame gracefully
    char outBuf[256];
    ZSTD_inBuffer in = { nullptr, 0, 0 };
    size_t remaining = 0;

    do {
      ZSTD_outBuffer out = { outBuf, sizeof(outBuf), 0 };
      remaining = ZSTD_compressStream2(st->cstream, &out, &in, ZSTD_e_end);
      write_all(st->fd, outBuf, out.pos);
    } while (remaining != 0);

    ZSTD_freeCStream(st->cstream);
    st->cstream = nullptr;
  }

  if (st->fd >= 0) {
    ::close(st->fd);
    st->fd = -1;
  }

  st->currentPath.clear();
}
#endif

#if defined(EOS_HAVE_ZSTD) && EOS_HAVE_ZSTD
void Logging::zstdMigratePlainMain()
{
  // Move contents of existing plain main log (e.g. xrdlog.mgm) into the compressed stream, then unlink it
  std::string tag = GetMainZstdTag(); // e.g., "xrdlog.mgm"
  std::string plainPath = gZstdUnitDir + "/" + tag; // direct file in base dir
  struct stat st {};

  if (::stat(plainPath.c_str(), &st) != 0 || !S_ISREG(st.st_mode) ||
      st.st_size <= 0) {
    return;
  }

  int fd = ::open(plainPath.c_str(), O_RDONLY | O_CLOEXEC);

  if (fd < 0) {
    return;
  }

  std::string buf;
  buf.reserve(1 << 16);
  char tmp[8192];
  ssize_t n;

  while ((n = ::read(fd, tmp, sizeof(tmp))) > 0) {
    for (ssize_t i = 0; i < n; ++i) {
      char c = tmp[i];

      if (c == '\n') {
        if (!buf.empty()) {
          WriteZstd(tag.c_str(), buf.c_str());
          buf.clear();
        } else {
          // empty line: still log as newline
          WriteZstd(tag.c_str(), "");
        }
      } else {
        buf.push_back(c);
      }
    }
  }

  if (!buf.empty()) {
    WriteZstd(tag.c_str(), buf.c_str());
    buf.clear();
  }

  ::close(fd);
  ::unlink(plainPath.c_str());
}
#endif

EOSCOMMONNAMESPACE_END
