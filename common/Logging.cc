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
#include <atomic>

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
// Constructor
//------------------------------------------------------------------------------
Logging::Logging():
  gLogMask(0), gPriorityLevel(0), gToSysLog(false),  gUnit("none"),
  gShortFormat(0), gRateLimiter(false)
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


#if LOG_BUFFER_DBG 

static int cmpPtr(const void * a, const void * b) {
    return ( *(intptr_t*)a - *(intptr_t*)b );
}

static void
check_log_buffer_chain(LogBuffer::log_buffer *chain, int maxSize, const char *name, const char *_FILE, const int _LINE) {
    LogBuffer::log_buffer *buff2, *buff3;
    LogBuffer::log_buffer **arrAct = (LogBuffer::log_buffer **) malloc(maxSize * sizeof(void *));

    int m;
    for (m = 0, buff2 = chain; buff2 != NULL; buff2 = buff2->h.next, m++) {
        if (m > 0) {
            if (buff3 == buff2) {
                fprintf(stderr,
    "%s:%d log_buffer_prb buffer twice (in succesion) on %s chain after %u items, cut\n",
                _FILE, _LINE, name, m);
                fflush(stderr);
                buff3->h.next = NULL;
            }
        };

        buff3 = buff2;

        if (m >= maxSize) {
            fprintf(stderr,
    "%s:%d log_buffer_prb more (%u) buffers on %s list than total (%u), cut\n",
                _FILE, _LINE, m, name, maxSize);
            fflush(stderr);
            buff3->h.next = NULL;
            break;
        }

        arrAct[m] = buff2;
    }

    if (m > 1) {
        qsort(arrAct, m, sizeof(void *), cmpPtr); 
        int i;
        for (i=1; i < m; i++)
            if (arrAct[i-1] == arrAct[i]) {
                buff2 = arrAct[i-1];
                fprintf(stderr,
                "%s:%d log_buffer_prb buffer %p twice on chain %s, cut\n",  _FILE, _LINE,
                        buff2, name);
                fflush(stderr);
                buff2->h.next = NULL;
            }
    }
    free(arrAct);
    fprintf(stderr, "%s:%d log_buffer chain %s verified %u elements\n",
            _FILE, _LINE, name, m);
}
#endif


LogBuffer::log_buffer *
LogBuffer::log_alloc_buffer() {

    LogBuffer::log_buffer *buff = NULL;

    std::unique_lock<std::mutex> guard(log_buffer_mutex);
    if (shuttingDown) return NULL;

    /* log_buffer_balance is incorrect until we really allocated a buffer! */
    LogBuffer::log_buffer_balance++;

    while (true) {
        buff = free_buffers;
        if (buff != NULL) {
            free_buffers = buff->h.next;
            log_buffer_free--;
            break;
        }

        if (shuttingDown) return NULL;

        /* no free buffer, alloc new one if below budget, or wait */
        if ( LogBuffer::log_buffer_total < LogBuffer::max_log_buffers ) {
            buff = (struct log_buffer *) malloc(sizeof(struct log_buffer));

            log_buffer_total++;
            
#if LOG_BUFFER_DBG > 2
            buff->h.debug1 = 7;
            {
                /* consistency checks, to be removed*/
                int num_in_queue;
                LogBuffer::log_buffer *bx = active_head, *bbx;
                for (num_in_queue=0; bx != NULL; num_in_queue++) {
                    bbx = bx->h.next;
                    if (bx == bbx) {
                        fprintf(stderr, "%s:%d active log_buffer_prb!\n",
                                __FILE__, __LINE__);
                        bx->h.next = NULL;
                        break;
                    }
                    bx = bbx;
                }
                if (num_in_queue != log_buffer_in_q)
                    fprintf(stderr, "%s:%d wrong log_buffer_in_q: %d != %d\n",
                            __FILE__, __LINE__,
                            num_in_queue, log_buffer_in_q);
            
            
                fprintf(stderr,
    "\ntotal_log_buffers: %d balance %d in_q %d free %d waiters %d\n",
                        log_buffer_total, log_buffer_balance,
                        log_buffer_in_q,
                        log_buffer_free,
                        log_buffer_waiters);
            }
#else
            if ((log_buffer_total & 0x1ff) == 0)
                fprintf(stderr,
    "\ntotal_log_buffers: %d balance %d in_q %d free %d waiters %d\n",
                        log_buffer_total, log_buffer_balance,
                        log_buffer_in_q, log_buffer_free, log_buffer_waiters);
#endif
            break;
        }

        /* wait for a free buffer */
#if LOG_BUFFER_DBG > 2
        check_log_buffer_chain(active_head, log_buffer_total,
                "active", __FILE__, __LINE__);
#endif

        if ((log_buffer_num_waits & 0xfff) == 0)
            fprintf(stderr,
    "log_buffer_shortage #%u with %u waiters, total_log_buffers %u balance %d in_q %u free %u\n",
                log_buffer_num_waits, log_buffer_waiters,
                LogBuffer::log_buffer_total, LogBuffer::log_buffer_balance,
                log_buffer_in_q, log_buffer_free);

        log_buffer_num_waits++;
        LogBuffer::log_buffer_waiters++;  /* this asks for a wake-up call when a buffer is freed */
        log_buffer_shortage.wait(guard);
        LogBuffer::log_buffer_waiters--;

        /* retry... */
        continue;

    }

    buff->h.next = NULL;
    buff->h.fanOutBuffer = NULL;
    
#if LOG_BUFFER_DBG 
    buff->h.debug1 = 42;;
#endif

    return buff;
}

void
LogBuffer::log_return_buffers(LogBuffer::log_buffer *buff) {
    LogBuffer::log_buffer *buff2, *buff3;

    /* count number of buffers returned, find last one (buff2 (='previous')) */
    int n = 1;
    for (buff2 = buff; (buff3 = buff2->h.next) != NULL; buff2 = buff3) {
#if LOG_BUFFER_DBG > 1
        if (buff3->h.debug1 != 42) {    /* check the next buffer */
            fprintf(stderr,
        "%s:%d log_buffer_prb returning circular buffer list %p->%p code %d, cut\n",
        __FILE__, __LINE__, buff2, buff3, buff3->h.debug1);
            buff2->h.next = NULL;
            break;
        } else          /*debug*/
#endif
#if LOG_BUFFER_DBG > 0
            buff2->h.debug1 = 52;           /* flag buffer as seen */
#endif
        n++;
    }

    std::unique_lock<std::mutex> guard(log_buffer_mutex);

#if LOG_BUFFER_DBG > 0
    buff2->h.debug1 = 52;                   /* flags the last buffer as well, it has been checked */
    if (log_buffer_free + n > log_buffer_total) {   /* Something's wrong, check all chains thoroughly */
        fprintf(stderr, "%s:%d log_buffer_prb log_buffer_free %d > log_buffer_total %d, %d buffers returned\n",
                __FILE__, __LINE__, log_buffer_free, log_buffer_total, n);
        fflush(stderr);

        if (n > 1)
            check_log_buffer_chain(buff, n+1, "2Bfreed", __FILE__, __LINE__);
        
        check_log_buffer_chain(active_head, log_buffer_total, "active", __FILE__, __LINE__);
        return;         /* drop all these buffers */
    }
#endif


    buff2->h.next = free_buffers;
    free_buffers = buff;

    log_buffer_free += n;

    if (log_buffer_waiters > 0) {   /* This is the condition the CV protects */
        if (n == 1)
            log_buffer_shortage.notify_one();
        else
            log_buffer_shortage.notify_all();
    }

}

void
LogBuffer::log_queue_buffer(LogBuffer::log_buffer *buff) {
    std::unique_lock<std::mutex> guard(log_buffer_mutex);

    if (shuttingDown) { /* get out quickly, the queues are no longer valid */
        // free(buff);
        return;
    }

    /* this starts the log thread */
    if ((not log_thread_started) and (not log_suspended))
        resume_int();

    LogBuffer::log_buffer *prev;

    if (active_tail == NULL) {
        /* the following works because offset(next) == 0 */
        prev = (LogBuffer::log_buffer *) &active_tail;
    } else 
        prev = active_tail;

    buff->h.next = NULL;
    prev->h.next = buff;
    active_tail = buff;
    if (!active_head) active_head = buff;

    log_buffer_in_q++;

    /* log_buffer_balance designates buffers between intended allocation and print queueing */
    log_buffer_balance--;

    log_buffer_cond.notify_all();
}


void
LogBuffer::log_thread() {
    LogBuffer::log_buffer *buff = NULL, *buff_2b_returned=NULL;
    unsigned int num_buff_2b_returned = 0;

#if LOG_BUFFER_DBG
    unsigned int notify_counter = 0;
    unsigned int old_waits = 0;
#endif


    std::unique_lock<std::mutex> guard(log_buffer_mutex);
    while(1) {
        if ( shuttingDown > 0 or active_head == NULL or num_buff_2b_returned > 15 or log_buffer_waiters > 0 ) {
            if (buff_2b_returned != NULL) {
                guard.unlock();
                log_return_buffers(buff_2b_returned);
                guard.lock();

                num_buff_2b_returned = 0;
                buff_2b_returned = NULL;
                continue;
            }

            if (shuttingDown > 0 and (active_head == NULL or shuttingDown > 3)) {                                 
                /* there is no safe way to dispatch what's still in the queue: the stream pointers
                 * may no longer be valid unless this is a graceful shutdown */
                shuttingDown = 42;
                guard.unlock();                      /* Time to get out */
                return;
            }

            if (active_head == NULL) {
#if LOG_BUFFER_DBG
                if ((log_buffer_num_waits > old_waits) and ++notify_counter > 1000) {
                    notify_counter = 0;
                    old_waits = log_buffer_num_waits;

                    fprintf(stderr, "\nlog_buffer queue empty, log_buffer_total: %d balance %d free %d waits %u waiters %d\n",
                        log_buffer_total, log_buffer_balance, log_buffer_free,
                        log_buffer_num_waits, log_buffer_waiters);
                }
#endif
                fflush(stderr);

                log_buffer_cond.wait(guard);
                if (shuttingDown) {
                    shuttingDown = 41;
                    continue;
                }
            }

        }

        if (active_head) {

            /* unchain */
            buff = active_head;
            active_head = active_head->h.next;
            if (active_head == NULL) active_tail = NULL;
            log_buffer_in_q--;

            guard.unlock();                                 /* drop while buffer is printed */

            fprintf(stderr, "%s\n", buff->buffer);
            if (active_head == NULL) fflush(stderr);        /* only flush if there's no other */

            if (eos::common::Logging::GetInstance().gToSysLog) {
              syslog(buff->h.priority, "%s", buff->h.ptr);
            }

            if (buff->h.fanOutBuffer != NULL) {
                if (buff->h.fanOutS != NULL) {
                    fputs(buff->h.fanOutBuffer, buff->h.fanOutS);
                    fflush(buff->h.fanOutS);
                }
                if (buff->h.fanOut != NULL) {
                    fputs(buff->h.fanOutBuffer, buff->h.fanOut);
                    fflush(buff->h.fanOut);
                }
            }

            if (buff_2b_returned != buff) {
                buff->h.next = buff_2b_returned;
                buff_2b_returned = buff;
                num_buff_2b_returned++;
            } else 
                fprintf(stderr, "%s.%d log_buffer_prb returning returned log_buffer\n",
                        __FILE__, __LINE__);
            
            guard.lock();
        }
    }
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

  struct LogBuffer::log_buffer *logBuffer = LB->log_alloc_buffer();
  if (logBuffer == NULL) return "";                     /* log object being destroyed */

  char* buffer = logBuffer->buffer;

  XrdOucString File = file;
  // we show only one hierarchy directory like Acl (assuming that we have only
  // file names like *.cc and *.hh
  File.erase(0, File.rfind("/") + 1);
  File.erase(File.length() - 3);
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
  vsnprintf(ptr, sizeof(logBuffer->buffer) - (ptr - buffer + 1), msg, args);

  if (!silent) {
    XrdSysMutexHelper scope_lock(gMutex);         
    if (rate_limit(tv, priority, file, line)) {
        LB->log_return_buffers(logBuffer);
        return "";
      }
  }

  logBuffer->h.ptr = ptr;

  if (!silent) {
    if (gLogFanOut.size()) {
      logBuffer->h.fanOutS = NULL;
      logBuffer->h.fanOut = NULL;

      logBuffer->h.fanOutBuffer = ptr + strlen(ptr) + 1;
      logBuffer->h.fanOutBufLen = sizeof(logBuffer->buffer) - (logBuffer->h.fanOutBuffer-logBuffer->buffer);
      
      // we do log-message fanout
      if (gLogFanOut.count("*")) {
        logBuffer->h.fanOutS = gLogFanOut["*"];
        snprintf(logBuffer->h.fanOutBuffer, logBuffer->h.fanOutBufLen, "%s\n", logBuffer->buffer);
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
        }
      }

    }

  }

  va_end(args);
  const char* rptr;

  if (silent) {
    priority = LOG_DEBUG;
  }

  // store into global log memory
  { 
    XrdSysMutexHelper scope_lock(gMutex);         

    /* the following copies the buffer, hence it can be queued and vanish anytime after */
    gLogMemory[priority][(gLogCircularIndex[priority]) % gCircularIndexSize] =
      buffer;
    rptr = gLogMemory[priority][(gLogCircularIndex[priority]) %
                              gCircularIndexSize].c_str();
    gLogCircularIndex[priority]++;
  }

  logBuffer->h.priority = priority;
  LB->log_queue_buffer(logBuffer);

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
