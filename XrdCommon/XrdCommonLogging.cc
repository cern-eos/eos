/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLogging.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/

int XrdCommonLogging::gLogMask=0;
int XrdCommonLogging::gPriorityLevel=0;

XrdCommonLogging::LogArray         XrdCommonLogging::gLogMemory;
XrdCommonLogging::LogCircularIndex XrdCommonLogging::gLogCircularIndex;
unsigned long                      XrdCommonLogging::gCircularIndexSize;
XrdSysMutex XrdCommonLogging::gMutex;
XrdOucString XrdCommonLogging::gUnit="none";
XrdOucString XrdCommonLogging::gFilter="";

XrdCommonMapping::VirtualIdentity XrdCommonLogging::gZeroVid;


/*----------------------------------------------------------------------------*/
void
XrdCommonLogging::log(const char* func, const char* file, int line, const char* logid, const XrdCommonMapping::VirtualIdentity &vid, const char* cident, int priority, const char *msg, ...) 
{
  if (!((LOG_MASK(priority) & gLogMask)))
    return;

  // apply filter to avoid message flooding for debug messages
  if (priority >= LOG_INFO)
    if ( (gFilter.find(func))!=STR_NPOS) {
      return;
    }

  static char buffer[16384];
  XrdOucString File = file;

  if (File.length() > 16) {
    int up = File.length() - 13;
    File.erase(3, up);
    File.insert("...",3);
  }


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
  
  XrdOucString truncname = vid.name;
  if (truncname.length() > 16) {
    truncname.insert("..",0);
    truncname.erase(0,truncname.length()-16);
  }
    
  sprintf(fcident,"%s %d/%d [%16s]",cident,vid.uid,vid.gid,truncname.c_str());

  tm = localtime (&current_time);
  sprintf (buffer, "%lu.%06lu %s| %-36s %-24s %014lu %16s:%-4s %02d%02d%02d %02d:%02d:%02d \t.....| %-50s %-10s | ", current_time, (unsigned long)tv.tv_usec, GetPriorityString(priority),logid, gUnit.c_str(), (unsigned long)XrdSysThread::ID(), File.c_str(), linen, tm->tm_year-100, tm->tm_mon+1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, fcident, func);

  char*  ptr = buffer + strlen(buffer);
  vsprintf(ptr, msg, args);
  
  fprintf(stderr,buffer);
  fprintf(stderr,"\n");
  fflush(stderr);
  va_end(args);
  gLogMemory[priority][(gLogCircularIndex[priority]++)%gCircularIndexSize] = buffer;
  gMutex.UnLock();
}

/*----------------------------------------------------------------------------*/
void
XrdCommonLogging::Init() 
{
  // initialize the log array and sets the log circular size
  gLogCircularIndex.resize(LOG_DEBUG+1);
  gLogMemory.resize(LOG_DEBUG+1);
  gCircularIndexSize=XRDCOMMONLOGGING_CIRCULARINDEXSIZE;
  for (int i = 0; i<= LOG_DEBUG; i++ ) {
    gLogCircularIndex[i] = 0;
    gLogMemory[i].resize(gCircularIndexSize);
  }
}
