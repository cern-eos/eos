#ifndef __EOSFST_SCANMOUNTPOINT_HH__
#define __EOSFST_SCANMOUNTPOINT_HH__
/*----------------------------------------------------------------------------*/
#include <pthread.h>
/*----------------------------------------------------------------------------*/
#include "fst/Load.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/Namespace.hh"
#include "XrdOuc/XrdOucString.hh"
#include "fst/checksum/ChecksumPlugins.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/


EOSFSTNAMESPACE_BEGIN

class ScanDir {
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
  long int noNoCheckumFiles;
  long int noTotalFiles;

  int rateBandwidth;     // MB/s
  int alignment;
  char* buffer;

  pthread_t thread;
  sem_t semaphore;
  
  bool bgThread;
public:

  ScanDir(const char* dirpath, eos::fst::Load* fstload, bool bgthread=true, long int testinterval = 10, int ratebandwidth = 100): 
    fstLoad(fstload),dirPath(dirpath), testInterval(testinterval), rateBandwidth(ratebandwidth)
  {
    noNoCheckumFiles = 0;
    noTotalFiles     = 0;
    bgThread = bgthread;

    alignment = pathconf(dirPath.c_str(), _PC_REC_XFER_ALIGN);
    bufferSize = 256 * alignment;
 
    if (posix_memalign((void**)&buffer, alignment, bufferSize)){
      fprintf(stderr, "error: error calling posix_memaling. \n");
      throw 0;
    }

    sem_init(&semaphore, 0, 0);   
    if (bgthread) {
      openlog("scandir", LOG_PID | LOG_NDELAY, LOG_USER);
      pthread_create(&thread, NULL, &ScanDir::StaticThreadProc, this);
    } 
  };

  void ScanFiles();

  void CheckFile(const char*);
  eos::fst::CheckSum* GetBlockXS(const char*);
  bool ScanFileLoadAware(const char*, unsigned long long &, float &, std::string, unsigned long, const char* lfn); 

  std::string GetTimestamp();
  bool RescanFile(std::string);
  
  static void* StaticThreadProc(void*);
  void* ThreadProc();
  
  virtual ~ScanDir();

};

EOSFSTNAMESPACE_END

#endif
