/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLogging.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/

int XrdCommonLogging::gLogMask=0;
int XrdCommonLogging::gPriorityLevel=0;

XrdSysMutex XrdCommonLogging::gMutex;
XrdOucString XrdCommonLogging::gUnit="none";

void
XrdCommonLogging::log(const char* func, const char* file, int line, const char* logid, uid_t uid, gid_t gid,uid_t ruid, gid_t rgid, const char* cident, int priority, const char *msg, ...) 
{
  static char buffer[16384];
  XrdOucString File = file;

  if (File.length() > 16) {
    int up = File.length() - 13;
    File.erase(3, up);
    File.insert("...",3);
  }
  if (!(priority & gLogMask)) 
    return;

  static time_t current_time;
  static struct timeval tv;
  static struct timezone tz;
  static struct tm *tm;
  gMutex.Lock();

  va_list args;
  va_start (args, msg);


  time (&current_time);
  gettimeofday(&tv, &tz);

  static char linen[16];
  sprintf(linen,"%d",line);

  static char fcident[1024];

  sprintf(fcident,"%s %d/%d [%d/%d]",cident,uid,gid,ruid,rgid);

  tm = localtime (&current_time);
  sprintf (buffer, "%s| %-36s %-24s %lu.%06d %012lu %16s:%-4s %02d%02d%02d %02d:%02d:%02d \n.....| %-50s %-10s | ", GetPriorityString(priority),logid, gUnit.c_str(), current_time, tv.tv_usec, (unsigned long)XrdSysThread::ID(), File.c_str(), linen, tm->tm_year-100, tm->tm_mon+1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, fcident, func);

  char*  ptr = buffer + strlen(buffer);
  vsprintf(ptr, msg, args);
  
  fprintf(stderr,buffer);
  fprintf(stderr,"\n\n");
  va_end(args);
  gMutex.UnLock();
}
