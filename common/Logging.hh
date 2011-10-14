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

#define eos_log(__EOSCOMMON_LOG_PRIORITY__ , ...) eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->uid, this->gid,this->ruid, this->rgid, this->cident,  LOG_MASK(__EOSCOMMON_LOG_PRIORITY__) , __VA_ARGS__
#define eos_debug(...)   eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->vid, this->cident, (LOG_DEBUG)  , __VA_ARGS__)
#define eos_info(...)    eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->vid, this->cident, (LOG_INFO)   , __VA_ARGS__)
#define eos_notice(...)  eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->vid, this->cident, (LOG_NOTICE) , __VA_ARGS__)
#define eos_warning(...) eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->vid, this->cident, (LOG_CRIT)   , __VA_ARGS__)
#define eos_err(...)     eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->vid, this->cident, (LOG_ERR)    , __VA_ARGS__)
#define eos_crit(...)    eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->vid, this->cident, (LOG_CRIT)   , __VA_ARGS__)
#define eos_alert(...)   eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->vid, this->cident, (LOG_ALERT)  , __VA_ARGS__)
#define eos_emerg(...)   eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->vid, this->cident, (LOG_EMERG)  , __VA_ARGS__)

#define eos_static_log(__EOSCOMMON_LOG_PRIORITY__ , ...) eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", 0,0,0,0,"",  (__EOSCOMMON_LOG_PRIORITY__) , __VA_ARGS__
#define eos_static_debug(...)   eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", eos::common::Logging::gZeroVid,"", (LOG_DEBUG)  , __VA_ARGS__)
#define eos_static_info(...)    eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", eos::common::Logging::gZeroVid,"", (LOG_INFO)   , __VA_ARGS__)
#define eos_static_notice(...)  eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", eos::common::Logging::gZeroVid,"", (LOG_NOTICE) , __VA_ARGS__)
#define eos_static_warning(...) eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", eos::common::Logging::gZeroVid,"", (LOG_CRIT)   , __VA_ARGS__)
#define eos_static_err(...)     eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", eos::common::Logging::gZeroVid,"", (LOG_ERR)    , __VA_ARGS__)
#define eos_static_crit(...)    eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", eos::common::Logging::gZeroVid,"", (LOG_CRIT)   , __VA_ARGS__)
#define eos_static_alert(...)   eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", eos::common::Logging::gZeroVid,"", (LOG_ALERT)  , __VA_ARGS__)
#define eos_static_emerg(...)   eos::common::Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", eos::common::Logging::gZeroVid,"", (LOG_EMERG)  , __VA_ARGS__)

#define EOSCOMMONLOGGING_CIRCULARINDEXSIZE 10000

class LogId {
public:
  
  void SetLogId(const char* newlogid, const char* td= "<service>") {
    if (newlogid != logId)
      snprintf(logId,sizeof(logId)-1,"%s",newlogid);
    snprintf(cident,sizeof(cident)-1,"%s", td);
  }
  
  void SetLogId(const char* newlogid, Mapping::VirtualIdentity &vid_in, const char* td = "") {
    Mapping::Copy(vid_in, vid);
    snprintf(cident,sizeof(cident)-1,"%s",td);
    if (newlogid != logId)
      snprintf(logId,sizeof(logId)-1,"%s",newlogid);
  }


  char logId[40];
  char cident[256];
  Mapping::VirtualIdentity vid;

  LogId() {
    uuid_t uuid;
    uuid_generate_time(uuid);
    uuid_unparse(uuid,logId);
    sprintf(cident,"<service>");
    vid.uid=getuid();
    vid.gid=getgid();
  }

  ~LogId(){}
};

class Logging {
private:
public:
  typedef std::vector< unsigned long > LogCircularIndex;
  typedef std::vector< std::vector <std::string> > LogArray;

  static LogCircularIndex gLogCircularIndex;
  static LogArray         gLogMemory;
  static unsigned long    gCircularIndexSize; 
  static Mapping::VirtualIdentity gZeroVid;
  static int gLogMask;
  static int gPriorityLevel;
  static XrdSysMutex gMutex;
  static XrdOucString gUnit;
  static XrdOucString gFilter;
  static void SetLogPriority(int pri) { gLogMask = LOG_UPTO(pri); gPriorityLevel = pri;}
  static void SetUnit(const char* unit) { gUnit = unit;}
  static void SetFilter(const char* filter) {gFilter = filter;}

  static const char* GetPriorityString(int pri) {
    if (pri==(LOG_INFO)) return "INFO ";
    if (pri==(LOG_DEBUG)) return "DEBUG";
    if (pri==(LOG_ERR)) return "ERROR";
    if (pri==(LOG_EMERG)) return "EMERG";
    if (pri==(LOG_ALERT)) return "ALERT";
    if (pri==(LOG_CRIT))  return "CRIT ";
    if (pri==(LOG_WARNING)) return "WARN ";
    if (pri==(LOG_NOTICE)) return "NOTE ";

    return "NONE ";
  }

  static int GetPriorityByString(const char* pri) {
    if (!strcmp(pri,"info"))    return LOG_INFO;
    if (!strcmp(pri,"debug"))   return LOG_DEBUG;
    if (!strcmp(pri,"err"))     return LOG_ERR;
    if (!strcmp(pri,"emerg"))   return LOG_EMERG;
    if (!strcmp(pri,"alert"))   return LOG_ALERT;
    if (!strcmp(pri,"crit"))    return LOG_CRIT;
    if (!strcmp(pri,"warning")) return LOG_WARNING;
    if (!strcmp(pri,"notice"))  return LOG_NOTICE;
    return -1;
  }


  static void Init();
  
  static void log(const char* func, const char* file, int line, const char* logid, const Mapping::VirtualIdentity &vid , const char* cident, int priority, const char *msg, ...);

};

EOSCOMMONNAMESPACE_END

#endif
