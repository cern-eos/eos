#ifndef __EOSFST_SCANDIR_HH__
#define __EOSFST_SCANDIR_HH__
/*----------------------------------------------------------------------------*/
#include <pthread.h>
/*----------------------------------------------------------------------------*/
#include "fst/Load.hh"
#include "fst/Namespace.hh"
#include "common/Logging.hh"
#include "XrdOuc/XrdOucString.hh"
#include "fst/checksum/ChecksumPlugins.hh"
/*----------------------------------------------------------------------------*/
#include <syslog.h>
/*----------------------------------------------------------------------------*/

#include <sys/syscall.h>
#include <asm/unistd.h>

EOSFSTNAMESPACE_BEGIN

class ScanDir : eos::common::LogId {
  // ---------------------------------------------------------------------------
  //! This class scan's a directory tree and checks checksums (and blockchecksums if present)
  //! in a defined interval with limited bandwidth
  // ---------------------------------------------------------------------------
private:

  eos::fst::Load* fstLoad;
  XrdOucString dirPath;
  long int testInterval;  // in seconds

  //statistics
  long int noScanFiles;
  long int noCorruptFiles;
  float durationScan;
  long long int totalScanSize;
  long long int bufferSize;
  long int noNoChecksumFiles;
  long int noTotalFiles;
  long int SkippedFiles; 

  bool setChecksum;
  int rateBandwidth;     // MB/s
  long alignment;
  char* buffer;

  pthread_t thread;

  bool bgThread;
public:

  ScanDir(const char* dirpath, eos::fst::Load* fstload, bool bgthread=true, long int testinterval = 10, int ratebandwidth = 100, bool setchecksum=false): 
    fstLoad(fstload),dirPath(dirpath), testInterval(testinterval), rateBandwidth(ratebandwidth)
  {
    thread = 0;
    noNoChecksumFiles = 0;
    noTotalFiles     = 0;
    buffer = 0;
    bgThread = bgthread;
    alignment = pathconf(dirPath.c_str(), _PC_REC_XFER_ALIGN);
    bufferSize = 256 * alignment;
    setChecksum = setchecksum;
    size_t palignment = alignment;
    if (posix_memalign((void**)&buffer, palignment, bufferSize)){
      fprintf(stderr, "error: error calling posix_memaling on dirpath=%s. \n",dirPath.c_str());
      return;
    }
    
    if (bgthread) {
      openlog("scandir", LOG_PID | LOG_NDELAY, LOG_USER);
      XrdSysThread::Run(&thread, ScanDir::StaticThreadProc, static_cast<void *>(this),XRDSYSTHREAD_HOLD, "ScanDir Thread");
    } 
  };

  void ScanFiles();

  void CheckFile(const char*);
  eos::fst::CheckSum* GetBlockXS(const char*);
  bool ScanFileLoadAware(const char*, unsigned long long &, float &, const char*, unsigned long, const char* lfn, bool &filecxerror, bool &blockxserror); 
  
  std::string GetTimestamp();
  std::string GetTimestampSmeared();
  bool RescanFile(std::string);
  
  static void* StaticThreadProc(void*);
  void* ThreadProc();
  
  virtual ~ScanDir();

};

EOSFSTNAMESPACE_END

#endif
