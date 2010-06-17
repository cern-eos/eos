#ifndef __XRDCOMMON_LOGGING_HH__
#define __XRDCOMMON_LOGGING_HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonMapping.hh"
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


#define eos_log(__XRDCOMMON_LOG_PRIORITY__ , ...) XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->uid, this->gid,this->ruid, this->rgid, this->cident,  LOG_MASK(__XRDCOMMON_LOG_PRIORITY__) , __VA_ARGS__
#define eos_debug(...)   XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->vid, this->cident, (LOG_DEBUG)  , __VA_ARGS__)
#define eos_info(...)    XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->vid, this->cident, (LOG_INFO)   , __VA_ARGS__)
#define eos_notice(...)  XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->vid, this->cident, (LOG_NOTICE) , __VA_ARGS__)
#define eos_warning(...) XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->vid, this->cident, (LOG_CRIT)   , __VA_ARGS__)
#define eos_err(...)     XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->vid, this->cident, (LOG_ERR)    , __VA_ARGS__)
#define eos_crit(...)    XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->vid, this->cident, (LOG_CRIT)   , __VA_ARGS__)
#define eos_alert(...)   XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->vid, this->cident, (LOG_ALERT)  , __VA_ARGS__)
#define eos_emerg(...)   XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->vid, this->cident, (LOG_EMERG)  , __VA_ARGS__)

#define eos_static_log(__XRDCOMMON_LOG_PRIORITY__ , ...) XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, "static", 0,0,0,0,"",  (__XRDCOMMON_LOG_PRIORITY__) , __VA_ARGS__
#define eos_static_debug(...)   XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, "static", XrdCommonLogging::gZeroVid,"", (LOG_DEBUG)  , __VA_ARGS__)
#define eos_static_info(...)    XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, "static", XrdCommonLogging::gZeroVid,"", (LOG_INFO)   , __VA_ARGS__)
#define eos_static_notice(...)  XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, "static", XrdCommonLogging::gZeroVid,"", (LOG_NOTICE) , __VA_ARGS__)
#define eos_static_warning(...) XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, "static", XrdCommonLogging::gZeroVid,"", (LOG_CRIT)   , __VA_ARGS__)
#define eos_static_err(...)     XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, "static", XrdCommonLogging::gZeroVid,"", (LOG_ERR)    , __VA_ARGS__)
#define eos_static_crit(...)    XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, "static", XrdCommonLogging::gZeroVid,"", (LOG_CRIT)   , __VA_ARGS__)
#define eos_static_alert(...)   XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, "static", XrdCommonLogging::gZeroVid,"", (LOG_ALERT)  , __VA_ARGS__)
#define eos_static_emerg(...)   XrdCommonLogging::log(__FUNCTION__,__FILE__, __LINE__, "static", XrdCommonLogging::gZeroVid,"", (LOG_EMERG)  , __VA_ARGS__)

#define XRDCOMMONLOGGING_CIRCULARINDEXSIZE 10000

class XrdCommonLogId {
public:
  
  void SetLogId(const char* newlogid, const char* td= "<service>") {
    sprintf(logId,newlogid);
    sprintf(cident, td);
  }
  
  void SetLogId(const char* newlogid, XrdCommonMapping::VirtualIdentity &vid_in, const char* td = "") {
    XrdCommonMapping::Copy(vid_in, vid);
    sprintf(cident,td);
    sprintf(logId,newlogid);
  }


  char logId[40];
  char cident[256];
  XrdCommonMapping::VirtualIdentity vid;

  XrdCommonLogId() {
    uuid_t uuid;
    uuid_generate_time(uuid);
    uuid_unparse(uuid,logId);
    sprintf(cident,"<service>");
    vid.uid=getuid();
    vid.gid=getgid();
  }

  ~XrdCommonLogId(){}
};

class XrdCommonLogging {
private:
public:
  typedef std::vector< unsigned long > LogCircularIndex;
  typedef std::vector< std::vector <std::string> > LogArray;

  static LogCircularIndex gLogCircularIndex;
  static LogArray         gLogMemory;
  static unsigned long    gCircularIndexSize; 
  static XrdCommonMapping::VirtualIdentity gZeroVid;
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
  
  static void log(const char* func, const char* file, int line, const char* logid, const XrdCommonMapping::VirtualIdentity &vid , const char* cident, int priority, const char *msg, ...);

};

#endif
