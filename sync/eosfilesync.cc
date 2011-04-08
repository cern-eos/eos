#define PROGNAME "eosfilesync"

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include "XrdOuc/XrdOucString.hh"
#include "XrdClient/XrdClient.hh"
#include "XrdClient/XrdClientEnv.hh"
#include "common/Logging.hh"

void usage() {
  fprintf(stderr,"usage: %s <src-path> <dst-url> [--debug]\n", PROGNAME);
  exit(-1);
}

#define TRANSFERBLOCKSIZE 1024*1024*4
#define PAGESIZE (64*1024ll)

int main (int argc, char* argv[]) {
  if (argc < 3) {
    usage();
  }

  EnvPutInt(NAME_READCACHESIZE,0);
  EnvPutInt(NAME_MAXREDIRECTCOUNT,10000);
  EnvPutInt(NAME_DATASERVERCONN_TTL,3600);
  EnvPutInt(NAME_FIRSTCONNECTMAXCNT,10000);
  
  eos::common::Logging::Init();
  eos::common::Logging::SetUnit("eosfilesync");
  eos::common::Logging::SetLogPriority(LOG_NOTICE);

  XrdOucString debugstring="";

   if (argc==4) {
    debugstring = argv[3];
  }
 
  if ( (debugstring == "--debug") || (debugstring == "-d") ) {
    eos::common::Logging::SetLogPriority(LOG_DEBUG);
  }
  
  if ( (debugstring == "--debug") || (debugstring == "-d") ) {
    eos::common::Logging::SetLogPriority(LOG_DEBUG);
  }
  
  eos_static_notice("starting %s=>%s", argv[1],argv[2]);

  off_t localoffset = 0;
  off_t remoteoffset = 0;

  struct timezone tz;
  struct timeval sync1;
  struct timeval sync2;

  gettimeofday(&sync1,&tz);

  XrdOucString sourcefile = argv[1];
  XrdOucString dsturl = argv[2];


  int fd =0; 

  do {
    fd= open (sourcefile.c_str(),O_RDONLY);
    
    if (fd<0) {
      sleep(1);
    }
  } while(fd <0 );

  XrdClient* client = new XrdClient(dsturl.c_str());

  if (!client) {
    fprintf(stderr,"Error: cannot create XrdClient object\n");
    exit(-1);
  }

  bool isopen = client->Open(kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or , kXR_mkpath | kXR_open_updt , false) || client->Open(kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or , kXR_mkpath | kXR_new, false);

  if (!isopen) {
    eos_static_err("cannot open remote file %s", dsturl.c_str());
  }

  struct stat srcstat;
  XrdClientStatInfo dststat;

  do {
    if (fstat(fd, &srcstat)) {
      eos_static_err("cannot stat source file %s - retry in 1 minute ...", sourcefile.c_str());
      sleep(60);
      continue;
      
    }

    if (!client->Stat(&dststat, true)) {
      eos_static_crit("cannot stat destination file %s", dsturl.c_str());
      exit(-1);
    }

    localoffset = srcstat.st_size;
    remoteoffset = dststat.size;

    if (remoteoffset > localoffset) {
      eos_static_err("remote file is longer than local file - force truncation\n");
      if (!client->Truncate(0)) {
	eos_static_crit("couldn't truncate remote file");
	exit(-1);
      }
      remoteoffset = 0;
    }
    
    size_t transfersize=0;
    if ( (localoffset - remoteoffset) > TRANSFERBLOCKSIZE) 
      transfersize = TRANSFERBLOCKSIZE;
    else 
      transfersize = (localoffset - remoteoffset);

    off_t mapoffset = remoteoffset - (remoteoffset %PAGESIZE);

    size_t mapsize = transfersize + (remoteoffset %PAGESIZE);

    if (!transfersize) {
      // sleep 10ms
      usleep(10000);
    } else {
      eos_static_debug("remoteoffset=%llu module=%llu mapoffset=%llu mapsize =%lu", (unsigned long long)remoteoffset,(unsigned long long)remoteoffset%PAGESIZE, (unsigned long long)mapoffset, (unsigned long)mapsize);
    
      char* copyptr = (char*) mmap(NULL, mapsize, PROT_READ, MAP_SHARED, fd,mapoffset);
      if (!copyptr) {
	eos_static_crit("cannot map source file at %llu", (unsigned long long)remoteoffset);
	exit(-1);
      }
      if (!client->Write(copyptr + (remoteoffset %PAGESIZE), remoteoffset, transfersize)) {
	eos_static_err("cannot write remote block at %llu/%lu", (unsigned long long)remoteoffset, (unsigned long)transfersize);
	sleep(60);
      }
      munmap(copyptr,mapsize);
    }
    
    gettimeofday(&sync2,&tz);
    
    float syncperiod = ( ((sync2.tv_sec - sync1.tv_sec)*1000.0) + ((sync2.tv_usec-sync1.tv_usec)/1000.0) );
    // every 1000 ms we send a sync command
    if (syncperiod > 1000) {
      if (!client->Sync()) {
	eos_static_crit("cannot sync remote file");
	exit(-1);
      }
    }
  } while (1);
}

  
