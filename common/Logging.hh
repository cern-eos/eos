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

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/Mapping.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysLogger.hh"
/*----------------------------------------------------------------------------*/
#include <string.h>
#include <sys/syslog.h>
#include <sys/time.h>
#include <uuid/uuid.h>
#include <string>
#include <vector>

/*----------------------------------------------------------------------------*/

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


/*----------------------------------------------------------------------------*/
//! Log Macros usable in objects inheriting from the logId Class
/*----------------------------------------------------------------------------*/
#define eos_log(__EOSCOMMON_LOG_PRIORITY__ , ...) eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->uid, this->gid,this->ruid, this->rgid, this->cident,  LOG_MASK(__EOSCOMMON_LOG_PRIORITY__) , __VA_ARGS__
#define eos_debug(...)   eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, vid, this->cident, (LOG_DEBUG)  , __VA_ARGS__)
#define eos_info(...)    eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, vid, this->cident, (LOG_INFO)   , __VA_ARGS__)
#define eos_notice(...)  eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, vid, this->cident, (LOG_NOTICE) , __VA_ARGS__)
#define eos_warning(...) eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, vid, this->cident, (LOG_WARNING), __VA_ARGS__)
#define eos_err(...)     eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, vid, this->cident, (LOG_ERR)    , __VA_ARGS__)
#define eos_crit(...)    eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, vid, this->cident, (LOG_CRIT)   , __VA_ARGS__)
#define eos_alert(...)   eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, vid, this->cident, (LOG_ALERT)  , __VA_ARGS__)
#define eos_emerg(...)   eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, vid, this->cident, (LOG_EMERG)  , __VA_ARGS__)

/*----------------------------------------------------------------------------*/
//! Log Macros usable in singleton objects used by individual threads
//! You should define locally LodId ThreadLogId in the thread function
/*----------------------------------------------------------------------------*/
#define eos_thread_debug(...)   eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, ThreadLogId.logId, vid, ThreadLogId.cident, (LOG_DEBUG)  , __VA_ARGS__)
#define eos_thread_info(...)    eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, ThreadLogId.logId, vid, ThreadLogId.cident, (LOG_INFO)   , __VA_ARGS__)
#define eos_thread_notice(...)  eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, ThreadLogId.logId, vid, ThreadLogId.cident, (LOG_NOTICE) , __VA_ARGS__)
#define eos_thread_warning(...) eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, ThreadLogId.logId, vid, ThreadLogId.cident, (LOG_WARNING), __VA_ARGS__)
#define eos_thread_err(...)     eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, ThreadLogId.logId, vid, ThreadLogId.cident, (LOG_ERR)    , __VA_ARGS__)
#define eos_thread_crit(...)    eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, ThreadLogId.logId, vid, ThreadLogId.cident, (LOG_CRIT)   , __VA_ARGS__)
#define eos_thread_alert(...)   eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, ThreadLogId.logId, vid, ThreadLogId.cident, (LOG_ALERT)  , __VA_ARGS__)
#define eos_thread_emerg(...)   eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, ThreadLogId.logId, vid, ThreadLogId.cident, (LOG_EMERG)  , __VA_ARGS__)

/*----------------------------------------------------------------------------*/
//! Log Macros usable from static member functions without LogId object
/*----------------------------------------------------------------------------*/
#define eos_static_log(__EOSCOMMON_LOG_PRIORITY__ , ...) eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", 0,0,0,0,"",  (__EOSCOMMON_LOG_PRIORITY__) , __VA_ARGS__
#define eos_static_debug(...)   eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static..............................", eos::common::Logging::gZeroVid,"", (LOG_DEBUG)  , __VA_ARGS__)
#define eos_static_info(...)    eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static..............................", eos::common::Logging::gZeroVid,"", (LOG_INFO)   , __VA_ARGS__)
#define eos_static_notice(...)  eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static..............................", eos::common::Logging::gZeroVid,"", (LOG_NOTICE) , __VA_ARGS__)
#define eos_static_warning(...) eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static..............................", eos::common::Logging::gZeroVid,"", (LOG_WARNING), __VA_ARGS__)
#define eos_static_err(...)     eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static..............................", eos::common::Logging::gZeroVid,"", (LOG_ERR)    , __VA_ARGS__)
#define eos_static_crit(...)    eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static..............................", eos::common::Logging::gZeroVid,"", (LOG_CRIT)   , __VA_ARGS__)
#define eos_static_alert(...)   eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static..............................", eos::common::Logging::gZeroVid,"", (LOG_ALERT)  , __VA_ARGS__)
#define eos_static_emerg(...)   eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static..............................", eos::common::Logging::gZeroVid,"", (LOG_EMERG)  , __VA_ARGS__)

/*----------------------------------------------------------------------------*/
//! Log Macros to check if a function would log in a certain log level
/*----------------------------------------------------------------------------*/
#define EOS_LOGS_DEBUG   eos::common::Logging::shouldlog(__FUNCTION__,(LOG_DEBUG)  )
#define EOS_LOGS_INFO    eos::common::Logging::shouldlog(__FUNCTION__,(LOG_INFO)   )
#define EOS_LOGS_NOTICE  eos::common::Logging::shouldlog(__FUNCTION__,(LOG_NOTICE) )
#define EOS_LOGS_WARNING eos::common::Logging::shouldlog(__FUNCTION__,(LOG_WARNING))
#define EOS_LOGS_ERR     eos::common::Logging::shouldlog(__FUNCTION__,(LOG_ERR)    )
#define EOS_LOGS_CRIT    eos::common::Logging::shouldlog(__FUNCTION__,(LOG_CRIT)   )
#define EOS_LOGS_ALERT   eos::common::Logging::shouldlog(__FUNCTION__,(LOG_ALERT)  )
#define EOS_LOGS_EMERG   eos::common::Logging::shouldlog(__FUNCTION__,(LOG_EMERG)  )


#define EOSCOMMONLOGGING_CIRCULARINDEXSIZE 10000

/*----------------------------------------------------------------------------*/
//! Class implementing EOS logging
/*----------------------------------------------------------------------------*/
class LogId
{
public:
  // ---------------------------------------------------------------------------
  //! For calls which are not client initiated this function set's a unique dummy log id
  // ---------------------------------------------------------------------------

  void
  SetSingleShotLogId (const char* td = "<single-exec>")
  {
    snprintf(logId, sizeof (logId) - 1, "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx");
    snprintf(cident, sizeof (cident) - 1, "%s", td);
  }

  // ---------------------------------------------------------------------------
  //! Set's the logid and trace identifier
  // ---------------------------------------------------------------------------

  void
  SetLogId (const char* newlogid, const char* td = "<service>")
  {
    if (newlogid != logId)
      snprintf(logId, sizeof (logId) - 1, "%s", newlogid);
    snprintf(cident, sizeof (cident) - 1, "%s", td);
  }

  // ---------------------------------------------------------------------------
  //! Set's the logid, vid and trace identifier
  // ---------------------------------------------------------------------------

  void
  SetLogId (const char* newlogid, Mapping::VirtualIdentity &vid_in, const char* td = "")
  {
    Mapping::Copy(vid_in, vid);
    snprintf(cident, sizeof (cident) - 1, "%s", td);
    if (newlogid != logId)
      snprintf(logId, sizeof (logId) - 1, "%s", newlogid);
  }


  char logId[40]; //< the log Id for message printout
  char cident[256]; //< the client identifier
  Mapping::VirtualIdentity vid; //< the client identity

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------

  LogId ()
  {
    uuid_t uuid;
    uuid_generate_time(uuid);
    uuid_unparse(uuid, logId);
    sprintf(cident, "<service>");
    vid.uid = getuid();
    vid.gid = getgid();
  }

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------

  ~LogId () { }
};

// ---------------------------------------------------------------------------
//! Class wrapping global singleton objects for logging
// ---------------------------------------------------------------------------

class Logging
{
private:
public:
  typedef std::vector< unsigned long > LogCircularIndex; //< typedef for circular index pointing to the next message position int he log array
  typedef std::vector< std::vector <XrdOucString> > LogArray; //< typdef for log message array 

  static LogCircularIndex gLogCircularIndex; //< global circular index
  static LogArray gLogMemory; //< global logging memory
  static unsigned long gCircularIndexSize; //< global circular index size
  static Mapping::VirtualIdentity gZeroVid; //< root vid
  static int gLogMask; //< log mask
  static int gPriorityLevel; //< log priority
  static XrdSysMutex gMutex; //< global mutex
  static XrdOucString gUnit; //< global unit name
  static XrdOucHash<const char*> gAllowFilter; ///< global list of function names allowed to log
  static XrdOucHash<const char*> gDenyFilter;  ///< global list of function names denied to log
  static int gShortFormat; //< indiciating if the log-output is in short format
  static std::map<std::string, FILE*> gLogFanOut; //< here one can define log fan-out to different file descriptors than stderr

  // ---------------------------------------------------------------------------
  //! Set the log priority (like syslog)
  // ---------------------------------------------------------------------------

  static void
  SetLogPriority (int pri)
  {
    gLogMask = LOG_UPTO(pri);
    gPriorityLevel = pri;
  }

  // ---------------------------------------------------------------------------
  //! Set the log unit name
  // ---------------------------------------------------------------------------

  static void
  SetUnit (const char* unit)
  {
    gUnit = unit;
  }

  // ---------------------------------------------------------------------------
  //! Set the log filter
  // ---------------------------------------------------------------------------

  static void
  SetFilter (const char* filter)
  {
    int pos = 0;
    char del = ',';
    XrdOucString token;
    XrdOucString pass_tag = "PASS:";
    XrdOucString sfilter = filter;

    // Clear both maps 
    gDenyFilter.Purge();
    gAllowFilter.Purge();
    
    if ((pos = sfilter.find(pass_tag)) != STR_NPOS)
    {
      // Extract the function names which are allowed to log       
      pos += pass_tag.length();
      
      while ((pos = sfilter.tokenize(token, pos , del)) != -1)
      {
        gAllowFilter.Add(token.c_str(), NULL, 0, Hash_data_is_key);
      }
    }
    else
    {
      // Extract the function names which are denied to log
      pos = 0;
      
      while ((pos = sfilter.tokenize(token, pos , del)) != -1)
      {
        gDenyFilter.Add(token.c_str(), NULL, 0, Hash_data_is_key);
      }
    }
  }

  // ---------------------------------------------------------------------------
  //! Return priority as string
  // ---------------------------------------------------------------------------

  static const char*
  GetPriorityString (int pri)
  {
    if (pri == (LOG_INFO)) return "INFO ";
    if (pri == (LOG_DEBUG)) return "DEBUG";
    if (pri == (LOG_ERR)) return "ERROR";
    if (pri == (LOG_EMERG)) return "EMERG";
    if (pri == (LOG_ALERT)) return "ALERT";
    if (pri == (LOG_CRIT)) return "CRIT ";
    if (pri == (LOG_WARNING)) return "WARN ";
    if (pri == (LOG_NOTICE)) return "NOTE ";

    return "NONE ";
  }

  // ---------------------------------------------------------------------------
  //! Return priority int from string
  // ---------------------------------------------------------------------------

  static int
  GetPriorityByString (const char* pri)
  {
    if (!strcmp(pri, "info")) return LOG_INFO;
    if (!strcmp(pri, "debug")) return LOG_DEBUG;
    if (!strcmp(pri, "err")) return LOG_ERR;
    if (!strcmp(pri, "emerg")) return LOG_EMERG;
    if (!strcmp(pri, "alert")) return LOG_ALERT;
    if (!strcmp(pri, "crit")) return LOG_CRIT;
    if (!strcmp(pri, "warning")) return LOG_WARNING;
    if (!strcmp(pri, "notice")) return LOG_NOTICE;
    return -1;
  }

  // ---------------------------------------------------------------------------
  //! Initialize Logger
  // ---------------------------------------------------------------------------
  static void Init ();

  // ---------------------------------------------------------------------------
  //! Add a tag fanout filedescriptor to the logging module
  // ---------------------------------------------------------------------------

  static void
  AddFanOut (const char* tag, FILE* fd)
  {
    gLogFanOut[tag] = fd;
  }

  // ---------------------------------------------------------------------------
  //! Add a tag fanout alias to the logging module
  // ---------------------------------------------------------------------------
  static void
  AddFanOutAlias (const char* alias, const char* tag)
  {
    if (gLogFanOut.count(tag))
    {
      gLogFanOut[alias] = gLogFanOut[tag];
    }
  }

  // ---------------------------------------------------------------------------
  //! Get a color for a given logging level
  // ---------------------------------------------------------------------------
  static const char* GetLogColour(const char* loglevel)
  {
    if (!strcmp(loglevel,"INFO ")) return EOS_TEXTGREEN;
    if (!strcmp(loglevel,"ERROR")) return EOS_TEXTRED;
    if (!strcmp(loglevel,"WARN ")) return EOS_TEXTYELLOW;
    if (!strcmp(loglevel,"NOTE ")) return EOS_TEXTBLUE;
    if (!strcmp(loglevel,"CRIT ")) return EOS_TEXTREDERROR;
    if (!strcmp(loglevel,"EMERG")) return EOS_TEXTBLUEERROR;
    if (!strcmp(loglevel,"ALERT")) return EOS_TEXTREDERROR;
    if (!strcmp(loglevel,"DEBUG")) return "";
    return "";
  }
  
  // ---------------------------------------------------------------------------
  //! Check if we should log in the defined level/filter
  // ---------------------------------------------------------------------------
  static bool shouldlog (const char* func, int priority);

  // ---------------------------------------------------------------------------
  //! Log a message into the global buffer
  // ---------------------------------------------------------------------------
  static const char* log (const char* func, const char* file, int line, const char* logid, const Mapping::VirtualIdentity &vid, const char* cident, int priority, const char *msg, ...);

};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif
