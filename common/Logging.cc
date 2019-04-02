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
#include "XrdSys/XrdSysPthread.hh"
#include <new>
#include <type_traits>

EOSCOMMONNAMESPACE_BEGIN

static std::atomic<int> sCounter {0};
static typename std::aligned_storage<sizeof(Logging), alignof(Logging)>::type
logging_buf; ///< Memory for the global logging object
Logging& gLogging = reinterpret_cast<Logging&>(logging_buf);

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
LoggingInitializer::LoggingInitializer()
{
  if (sCounter++ == 0) {
    new(&gLogging) Logging();  // placement new
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
LoggingInitializer::~LoggingInitializer()
{
  if (--sCounter == 0) {
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
// Constructor
//------------------------------------------------------------------------------
Logging::Logging():
  gLogMask(0), gPriorityLevel(0), gToSysLog(false),  gUnit("none"),
  gShortFormat(0), gRateLimiter(false)
{
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

//------------------------------------------------------------------------------
// Logging function
//------------------------------------------------------------------------------
const char*
Logging::log(const char* func, const char* file, int line, const char* logid,
             const VirtualIdentity& vid, const char* cident, int priority,
             const char* msg, ...)
{
  static int logmsgbuffersize = 1024 * 1024;
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

  static char* buffer = 0;

  if (!buffer) {
    // 1 M print buffer
    buffer = (char*) malloc(logmsgbuffersize);
  }

  XrdOucString File = file;
  // we show only one hierarchy directory like Acl (assuming that we have only
  // file names like *.cc and *.hh
  File.erase(0, File.rfind("/") + 1);
  File.erase(File.length() - 3);
  static time_t current_time;
  static struct timeval tv;
  static struct timezone tz;
  struct tm tm;
  XrdSysMutexHelper scope_lock(gMutex);
  va_list args;
  va_start(args, msg);
  gettimeofday(&tv, &tz);
  current_time = tv.tv_sec;
  static char linen[16];
  sprintf(linen, "%d", line);
  static char fcident[1024];
  XrdOucString truncname = vid.name;

  // we show only the last 16 bytes of the name
  if (truncname.length() > 16) {
    truncname.insert("..", 0);
    truncname.erase(0, truncname.length() - 16);
  }

  char sourceline[64];

  if (gShortFormat) {
    localtime_r(&current_time, &tm);
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
    sprintf(fcident, "tident=%s sec=%-5s uid=%d gid=%d name=%s geo=\"%s\"", cident,
            vid.prot.c_str(), vid.uid, vid.gid, truncname.c_str(), vid.geolocation.c_str());
    localtime_r(&current_time, &tm);
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
  vsnprintf(ptr, logmsgbuffersize - (ptr - buffer + 1), msg, args);

  if (!silent && rate_limit(tv, priority, file, line)) {
    return "";
  }

  if (!silent && gToSysLog) {
    syslog(priority, "%s", ptr);
  }

  if (!silent) {
    if (gLogFanOut.size()) {
      // we do log-message fanout
      if (gLogFanOut.count("*")) {
        fprintf(gLogFanOut["*"], "%s\n", buffer);
        fflush(gLogFanOut["*"]);
      }

      if (gLogFanOut.count(File.c_str())) {
        buffer[15] = 0;
        fprintf(gLogFanOut[File.c_str()], "%s %s%s%s %-30s %s \n",
                buffer,
                GetLogColour(GetPriorityString(priority)),
                GetPriorityString(priority),
                EOS_TEXTNORMAL,
                sourceline,
                ptr);
        fflush(gLogFanOut[File.c_str()]);
        buffer[15] = ' ';
      } else {
        if (gLogFanOut.count("#")) {
          buffer[15] = 0;
          fprintf(gLogFanOut["#"], "%s %s%s%s [%05d/%05d] %16s ::%-16s %s \n",
                  buffer,
                  GetLogColour(GetPriorityString(priority)),
                  GetPriorityString(priority),
                  EOS_TEXTNORMAL,
                  vid.uid,
                  vid.gid,
                  truncname.c_str(),
                  func,
                  ptr
                 );
          fflush(gLogFanOut["#"]);
          buffer[15] = ' ';
        }
      }

      fprintf(stderr, "%s\n", buffer);
      fflush(stderr);
    } else {
      fprintf(stderr, "%s\n", buffer);
      fflush(stderr);
    }
  }

  va_end(args);
  const char* rptr;

  if (silent) {
    priority = LOG_DEBUG;
  }

  // store into global log memory
  gLogMemory[priority][(gLogCircularIndex[priority]) % gCircularIndexSize] =
    buffer;
  rptr = gLogMemory[priority][(gLogCircularIndex[priority]) %
                              gCircularIndexSize].c_str();
  gLogCircularIndex[priority]++;
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

EOSCOMMONNAMESPACE_END
