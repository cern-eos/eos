// ----------------------------------------------------------------------
// File: Logging.hh
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

/**
 * @file   Logging.hh
 *
 * @brief  Class for message logging.
 *
 * You can use this class without creating an instance object (it provides a
 * global singleton). All the 'eos_<state>' functions require that the logging
 * class inherits from the 'LogId' class. As an alternative as set of static
 * 'eos_static_<state>' logging functinos are provided. To define the log level
 * one uses the static 'SetLogPriority' function. 'SetFilter' allows to filter
 * out log messages which are identified by their function/method name
 * (__FUNCTION__). If you prefix this comma seperated list with 'PASS:' it is
 * used as an acceptance filter. By default all logging is printed to 'stderr'.
 * You can arrange a log stream filter fan-out using 'AddFanOut'. The fan-out of
 * messages is defined by the source filename the message comes from and mappes
 * to a FILE* where the message is written. If you add a '*' fan-out you can
 * write all messages into this file. If you add a '#' fan-out you can write
 * all messages which are not in any other fan-out (besides '*') into that file.
 * The fan-out functionality assumes that
 * source filenames follow the pattern <fan-out-name>.xx !!!!
 */

#ifndef __EOSCOMMON_LOGGING_HH__
#define __EOSCOMMON_LOGGING_HH__

#include "common/Namespace.hh"
#include "common/Mapping.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSec/XrdSecEntity.hh"
#include <string.h>
#include <sys/syslog.h>
#include <sys/time.h>
#include <uuid/uuid.h>
#include <string>
#include <vector>
#include <sstream>

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

EOSCOMMONNAMESPACE_BEGIN

#define EOS_TEXTNORMAL "\033[0m"
#define EOS_TEXTBLACK  "\033[49;30m"
#define EOS_TEXTRED    "\033[49;31m"
#define EOS_TEXTREDERROR "\033[47;31m\e[5m"
#define EOS_TEXTBLUEERROR "\033[47;34m\e[5m"
#define EOS_TEXTGREEN  "\033[49;32m"
#define EOS_TEXTYELLOW "\033[49;33m"
#define EOS_TEXTBLUE   "\033[49;34m"
#define EOS_TEXTBOLD   "\033[1m"
#define EOS_TEXTUNBOLD "\033[0m"
#define LOG_SILENT 0xffff

#define eos_debug_lite(...)                                                  \
  if ((LOG_MASK(LOG_DEBUG) & eos::common::Logging::GetInstance().GetLogMask()) != 0) { \
    eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, \
                                            this->logId, vid, this->cident,  \
                                            (LOG_DEBUG), __VA_ARGS__);       \
  }

#define eos_info_lite(...)                                                   \
  if ((LOG_MASK(LOG_INFO) & eos::common::Logging::GetInstance().GetLogMask()) != 0) { \
    eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, \
                                            this->logId, vid, this->cident,  \
                                            (LOG_INFO), __VA_ARGS__);        \
      }

#define eos_err_lite(...)                                                   \
  if ((LOG_MASK(LOG_ERR) & eos::common::Logging::GetInstance().GetLogMask()) != 0) { \
    eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, \
                                            this->logId, vid, this->cident,  \
                                            (LOG_ERR), __VA_ARGS__);        \
      }

//------------------------------------------------------------------------------
//! Log Macros usable in objects inheriting from the logId Class
//------------------------------------------------------------------------------
#define eos_log(__EOSCOMMON_LOG_PRIORITY__ , ...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, this->logId, \
                                          vid, this->cident, __EOSCOMMON_LOG_PRIORITY__, __VA_ARGS__)
#define eos_debug(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, this->logId, \
                                          vid, this->cident, (LOG_DEBUG), __VA_ARGS__)
#define eos_info(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, this->logId, \
                                          vid, this->cident, (LOG_INFO), __VA_ARGS__)
#define eos_notice(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, this->logId, \
                                          vid, this->cident, (LOG_NOTICE), __VA_ARGS__)
#define eos_warning(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, this->logId, \
            vid, this->cident, (LOG_WARNING), __VA_ARGS__)
#define eos_err(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, this->logId, \
            vid, this->cident, (LOG_ERR) , __VA_ARGS__)
#define eos_crit(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, this->logId, \
            vid, this->cident, (LOG_CRIT), __VA_ARGS__)
#define eos_alert(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, this->logId, \
            vid, this->cident, (LOG_ALERT)  , __VA_ARGS__)
#define eos_emerg(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, this->logId, \
                                          vid, this->cident, (LOG_EMERG)  , __VA_ARGS__)
#define eos_silent(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, this->logId, \
                                          vid, this->cident, (LOG_SILENT)  , __VA_ARGS__)



//------------------------------------------------------------------------------
//! Log Macros usable in singleton objects used by individual threads
//! You should define locally LodId tlLogId in the thread function
//------------------------------------------------------------------------------
#define eos_thread_debug(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, tlLogId.logId, \
                                          vid, tlLogId.cident, (LOG_DEBUG)  , __VA_ARGS__)
#define eos_thread_info(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, tlLogId.logId, \
                                          vid, tlLogId.cident, (LOG_INFO)   , __VA_ARGS__)
#define eos_thread_notice(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, tlLogId.logId, \
                                          vid, tlLogId.cident, (LOG_NOTICE) , __VA_ARGS__)
#define eos_thread_warning(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, tlLogId.logId, \
                                          vid, tlLogId.cident, (LOG_WARNING), __VA_ARGS__)
#define eos_thread_err(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, tlLogId.logId, \
                                          vid, tlLogId.cident, (LOG_ERR)    , __VA_ARGS__)
#define eos_thread_crit(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, tlLogId.logId, \
                                          vid, tlLogId.cident, (LOG_CRIT)   , __VA_ARGS__)
#define eos_thread_alert(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, tlLogId.logId, \
                                          vid, tlLogId.cident, (LOG_ALERT)  , __VA_ARGS__)
#define eos_thread_emerg(...) \
   eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, tlLogId.logId, \
                                           vid, tlLogId.cident, (LOG_EMERG)  , __VA_ARGS__)

//------------------------------------------------------------------------------
//! Log Macros usable from static member functions without LogId object
//------------------------------------------------------------------------------
#define eos_static_log(__EOSCOMMON_LOG_PRIORITY__ , ...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, "static", \
    0,0,0,0, "",  (__EOSCOMMON_LOG_PRIORITY__) , __VA_ARGS__
#define eos_static_debug(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, "static..............................", \
                                          eos::common::gLogging.gZeroVid, "", (LOG_DEBUG), __VA_ARGS__)
#define eos_static_info(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, "static..............................", \
                             eos::common::gLogging.gZeroVid, "", (LOG_INFO), __VA_ARGS__)
#define eos_static_notice(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, "static..............................", \
                                          eos::common::gLogging.gZeroVid, "", (LOG_NOTICE), __VA_ARGS__)
#define eos_static_warning(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, "static..............................", \
                                          eos::common::gLogging.gZeroVid, "", (LOG_WARNING), __VA_ARGS__)
#define eos_static_err(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, "static..............................", \
                                          eos::common::gLogging.gZeroVid, "", (LOG_ERR), __VA_ARGS__)
#define eos_static_crit(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, "static..............................", \
                                          eos::common::gLogging.gZeroVid, "", (LOG_CRIT), __VA_ARGS__)
#define eos_static_alert(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, "static..............................", \
                                          eos::common::gLogging.gZeroVid, "", (LOG_ALERT)  , __VA_ARGS__)
#define eos_static_emerg(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, "static..............................", \
                                          eos::common::gLogging.gZeroVid,"", (LOG_EMERG)  , __VA_ARGS__)
#define eos_static_silent(...) \
  eos::common::Logging::GetInstance().log(__FUNCTION__,__FILE__, __LINE__, "static..............................", \
                                          eos::common::gLogging.gZeroVid,"", (LOG_SILENT)  , __VA_ARGS__)

//------------------------------------------------------------------------------
//! Log Macros to check if a function would log in a certain log level
//------------------------------------------------------------------------------
#define EOS_LOGS_DEBUG   eos::common::Logging::GetInstance().shouldlog(__FUNCTION__,(LOG_DEBUG)  )
#define EOS_LOGS_INFO    eos::common::Logging::GetInstance().shouldlog(__FUNCTION__,(LOG_INFO)   )
#define EOS_LOGS_NOTICE  eos::common::Logging::GetInstance().shouldlog(__FUNCTION__,(LOG_NOTICE) )
#define EOS_LOGS_WARNING eos::common::Logging::GetInstance().shouldlog(__FUNCTION__,(LOG_WARNING))
#define EOS_LOGS_ERR     eos::common::Logging::GetInstance().shouldlog(__FUNCTION__,(LOG_ERR)    )
#define EOS_LOGS_CRIT    eos::common::Logging::GetInstance().shouldlog(__FUNCTION__,(LOG_CRIT)   )
#define EOS_LOGS_ALERT   eos::common::Logging::GetInstance().shouldlog(__FUNCTION__,(LOG_ALERT)  )
#define EOS_LOGS_EMERG   eos::common::Logging::GetInstance().shouldlog(__FUNCTION__,(LOG_EMERG)  )
#define EOS_LOGS_SILENT   eos::common::Logging::GetInstance().shouldlog(__FUNCTION__,(LOG_SILENT)  )

#define EOSCOMMONLOGGING_CIRCULARINDEXSIZE 10000

//------------------------------------------------------------------------------
//! Class implementing EOS logging
//------------------------------------------------------------------------------
class LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  LogId()
  {
    uuid_t uuid;
    uuid_generate_time(uuid);
    uuid_unparse(uuid, logId);
    sprintf(cident, "<service>");
    vid.uid = getuid();
    vid.gid = getgid();
    vid.name = "";
    vid.tident = "";
    vid.prot = "";
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~LogId() = default;

  //----------------------------------------------------------------------------
  //! Generate log id value
  //----------------------------------------------------------------------------
  static std::string GenerateLogId()
  {
    char log_id[40];
    uuid_t uuid;
    uuid_generate_time(uuid);
    uuid_unparse(uuid, log_id);
    return log_id;
  }

  //----------------------------------------------------------------------------
  //! For calls which are not client initiated this function set's a unique
  //! dummy log id
  //----------------------------------------------------------------------------
  void
  SetSingleShotLogId(const char* td = "<single-exec>")
  {
    snprintf(logId, sizeof(logId) - 1, "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx");
    snprintf(cident, sizeof(cident) - 1, "%s", td);
  }

  //----------------------------------------------------------------------------
  //! Set's the logid
  //----------------------------------------------------------------------------
  void
  SetLogId(const char* newlogid)
  {
    if (newlogid && (strncmp(newlogid, logId, sizeof(newlogid) - 1) != 0)) {
      snprintf(logId, sizeof(logId) - 1, "%s", newlogid);
    }
  }

  //----------------------------------------------------------------------------
  //! Set's the logid and trace identifier
  //----------------------------------------------------------------------------
  void
  SetLogId(const char* newlogid, const char* td)
  {
    if (newlogid && (newlogid != logId)) {
      snprintf(logId, sizeof(logId) , "%s", newlogid);
    }

    if (td) {
      snprintf(cident, sizeof(cident) , "%s", td);
    }
  }

  //----------------------------------------------------------------------------
  //! Set's the logid, vid and trace identifier
  //----------------------------------------------------------------------------
  void
  SetLogId(const char* newlogid,
           const XrdSecEntity* client,
           const char* td = "<service>")
  {
    if (newlogid) {
      SetLogId(newlogid, td);
    }

    if (client) {
      vid.name = client->name;
      vid.host = (client->host) ? client->host : "?";
      vid.prot = client->prot;
    }
  }

  //----------------------------------------------------------------------------
  //! Set's the logid, vid and trace identifier
  //----------------------------------------------------------------------------
  void
  SetLogId(const char* newlogid, const VirtualIdentity& vid_in,
           const char* td = "")
  {
    vid = vid_in;
    snprintf(cident, sizeof(cident), "%s", td);

    if (vid.token && vid.token->Valid()) {
      // use the voucher as logging ID
      snprintf(logId, sizeof(logId), "%s", vid.token->Voucher().c_str());
    } else {
      if (newlogid != logId) {
	snprintf(logId, sizeof(logId), "%s", newlogid);
      }
    }
  }

  char logId[40]; //< the log Id for message printout
  char cident[256]; //< the client identifier
  VirtualIdentity vid; //< the client identity
};

//------------------------------------------------------------------------------
//! Class wrapping global singleton objects for logging
//------------------------------------------------------------------------------
class Logging
{
public:
  //! Typedef for circular index pointing to the next message position int he log array
  typedef std::vector< unsigned long > LogCircularIndex;
  //! Typdef for log message array
  typedef std::vector< std::vector <XrdOucString> > LogArray;
  VirtualIdentity gZeroVid; ///< Root vid
  LogCircularIndex gLogCircularIndex; //< global circular index
  LogArray gLogMemory; //< global logging memory
  unsigned long gCircularIndexSize; //< global circular index size
  int gLogMask; //< log mask
  int gPriorityLevel; //< log priority
  bool gToSysLog; //< duplicate into syslog
  XrdSysMutex gMutex; //< global mutex
  XrdOucString gUnit; //< global unit name
  //! Global list of function names allowed to log
  XrdOucHash<const char*> gAllowFilter;
  //! Global list of function names denied to log
  XrdOucHash<const char*> gDenyFilter;
  int gShortFormat; //< indiciating if the log-output is in short format
  //! Here one can define log fan-out to different file descriptors than stderr
  std::map<std::string, FILE*> gLogFanOut;

  bool gRateLimiter; //< indicating to apply message rate limiting

  //----------------------------------------------------------------------------
  //! Get singleton instance
  //----------------------------------------------------------------------------
  static Logging& GetInstance();

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Logging();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~Logging() = default;


  //----------------------------------------------------------------------------
  //! Get current loglevel
  //----------------------------------------------------------------------------
  void
  EnableRateLimiter()
  {
    gRateLimiter = true;
  }

  //----------------------------------------------------------------------------
  //! Get current loglevel
  //----------------------------------------------------------------------------
  int
  GetLogMask() const
  {
    return gLogMask;
  }

  //----------------------------------------------------------------------------
  //! Set the log priority (like syslog)
  //----------------------------------------------------------------------------
  void
  SetLogPriority(int pri)
  {
    gLogMask = LOG_UPTO(pri);
    gPriorityLevel = pri;
  }

  //----------------------------------------------------------------------------
  //! Set the log unit name
  //----------------------------------------------------------------------------
  void
  SetUnit(const char* unit)
  {
    gUnit = unit;
  }

  void
  SetSysLog(bool onoff)
  {
    gToSysLog = onoff;
  }

  //----------------------------------------------------------------------------
  //! Set index size
  //----------------------------------------------------------------------------
  void
  SetIndexSize(size_t size)
  {
    gCircularIndexSize = size;

    for (int i = 0; i <= LOG_DEBUG; i++) {
      gLogCircularIndex[i] = 0;
      gLogMemory[i].resize(size);
      gLogMemory[i].shrink_to_fit();
    }
  }


  //----------------------------------------------------------------------------
  //! Set the log filter
  //----------------------------------------------------------------------------
  void
  SetFilter(const char* filter)
  {
    int pos = 0;
    char del = ',';
    XrdOucString token;
    XrdOucString pass_tag = "PASS:";
    XrdOucString sfilter = filter;
    // Clear both maps
    gDenyFilter.Purge();
    gAllowFilter.Purge();

    if ((pos = sfilter.find(pass_tag)) != STR_NPOS) {
      // Extract the function names which are allowed to log
      pos += pass_tag.length();

      while ((pos = sfilter.tokenize(token, pos, del)) != -1) {
        gAllowFilter.Add(token.c_str(), NULL, 0, Hash_data_is_key);
      }
    } else {
      // Extract the function names which are denied to log
      pos = 0;

      try {
        while ((pos = sfilter.tokenize(token, pos, del)) != -1) {
          gDenyFilter.Add(token.c_str(), NULL, 0, Hash_data_is_key);
        }
      } catch (int& err) {}
    }
  }

  //----------------------------------------------------------------------------
  //! Return priority as string
  //----------------------------------------------------------------------------
  const char*
  GetPriorityString(int pri)
  {
    if (pri == (LOG_INFO)) {
      return "INFO ";
    }

    if (pri == (LOG_DEBUG)) {
      return "DEBUG";
    }

    if (pri == (LOG_ERR)) {
      return "ERROR";
    }

    if (pri == (LOG_EMERG)) {
      return "EMERG";
    }

    if (pri == (LOG_ALERT)) {
      return "ALERT";
    }

    if (pri == (LOG_CRIT)) {
      return "CRIT ";
    }

    if (pri == (LOG_WARNING)) {
      return "WARN ";
    }

    if (pri == (LOG_NOTICE)) {
      return "NOTE ";
    }

    if (pri == (LOG_SILENT)) {
      return "";
    }

    return "NONE ";
  }

  //----------------------------------------------------------------------------
  //! Return priority int from string
  //----------------------------------------------------------------------------
  int
  GetPriorityByString(const char* pri)
  {
    if (!strcmp(pri, "info")) {
      return LOG_INFO;
    }

    if (!strcmp(pri, "debug")) {
      return LOG_DEBUG;
    }

    if (!strcmp(pri, "err")) {
      return LOG_ERR;
    }

    if (!strcmp(pri, "emerg")) {
      return LOG_EMERG;
    }

    if (!strcmp(pri, "alert")) {
      return LOG_ALERT;
    }

    if (!strcmp(pri, "crit")) {
      return LOG_CRIT;
    }

    if (!strcmp(pri, "warning")) {
      return LOG_WARNING;
    }

    if (!strcmp(pri, "notice")) {
      return LOG_NOTICE;
    }

    if (!strcmp(pri, "silent")) {
      return LOG_SILENT;
    }

    return -1;
  }

  //----------------------------------------------------------------------------
  //! Add a tag fanout filedescriptor to the logging module
  //----------------------------------------------------------------------------
  void
  AddFanOut(const char* tag, FILE* fd)
  {
    gLogFanOut[tag] = fd;
  }

  //----------------------------------------------------------------------------
  //! Add a tag fanout alias to the logging module
  //----------------------------------------------------------------------------
  void
  AddFanOutAlias(const char* alias, const char* tag)
  {
    if (gLogFanOut.count(tag)) {
      gLogFanOut[alias] = gLogFanOut[tag];
    }
  }

  //----------------------------------------------------------------------------
  //! Get a color for a given logging level
  //----------------------------------------------------------------------------
  const char*
  GetLogColour(const char* loglevel)
  {
    if (!strcmp(loglevel, "INFO ")) {
      return EOS_TEXTGREEN;
    }

    if (!strcmp(loglevel, "ERROR")) {
      return EOS_TEXTRED;
    }

    if (!strcmp(loglevel, "WARN ")) {
      return EOS_TEXTYELLOW;
    }

    if (!strcmp(loglevel, "NOTE ")) {
      return EOS_TEXTBLUE;
    }

    if (!strcmp(loglevel, "CRIT ")) {
      return EOS_TEXTREDERROR;
    }

    if (!strcmp(loglevel, "EMERG")) {
      return EOS_TEXTBLUEERROR;
    }

    if (!strcmp(loglevel, "ALERT")) {
      return EOS_TEXTREDERROR;
    }

    if (!strcmp(loglevel, "DEBUG")) {
      return "";
    }

    return "";
  }

  //----------------------------------------------------------------------------
  //! Check if we should log in the defined level/filter
  //!
  //! @param func name of the calling function
  //! @param priority priority level of the message
  //!
  //----------------------------------------------------------------------------
  bool shouldlog(const char* func, int priority);

  //----------------------------------------------------------------------------
  //! Log a message into the global buffer
  //!
  //! @param func name of the calling function
  //! @param file name of the source file calling
  //! @param line line in the source file
  //! @param logid log message identifier
  //! @param vid virtual id of the caller
  //! @param cident client identifier
  //! @param priority priority level of the message
  //! @param msg the actual log message
  //!
  //! @return pointer to the log message
  //----------------------------------------------------------------------------
  const char* log(const char* func, const char* file, int line,
                  const char* logid, const VirtualIdentity& vid,
                  const char* cident, int priority, const char* msg, ...);


  //----------------------------------------------------------------------------
  //! estimates log message distance and similiary to suppress log messages
  //!
  //! @param time of the message
  //! @param priority of the message
  //! @param source file name
  //! @param line in source file
  //!
  //! @return true if it should be suppressed, otherwise false
  //---------------------------------------------------------------------------

  bool rate_limit(struct timeval& tv, int priority, const char* file, int line);
};

extern Logging& gLogging; ///< Global logging object

//------------------------------------------------------------------------------
//! Static Logging initializer
//------------------------------------------------------------------------------
static struct LoggingInitializer {
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  LoggingInitializer();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~LoggingInitializer();
} sLoggingInit; ///< Static initializer for every translation unit

EOSCOMMONNAMESPACE_END

#endif
