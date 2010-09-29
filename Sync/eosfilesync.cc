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

void usage() {
  fprintf(stderr,"usage: %s <src-path> <dst-url>\n", PROGNAME);
  exit(-1);
}

#define TRANSFERBLOCKSIZE 1024*1024*4
#define PAGESIZE (64*1024ll)

int main (int argc, char* argv[]) {
  if (argc != 3) {
    usage();
  }
  
  // this is done by the service script
  //  setuid(2);
  //  setgid(2);
  
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

  // don't cache!
  EnvPutInt(NAME_READCACHESIZE,0);
  EnvPutInt(NAME_MAXREDIRECTCOUNT,10000);
  EnvPutInt(NAME_DATASERVERCONN_TTL,3600);

  XrdClient* client = new XrdClient(dsturl.c_str());

  if (!client) {
    fprintf(stderr,"Error: cannot create XrdClient object\n");
    exit(-1);
  }

  bool isopen = client->Open(kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or , kXR_mkpath | kXR_open_updt , false) || client->Open(kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or , kXR_mkpath | kXR_new, false);

  if (!isopen) {
    fprintf(stderr,"Error: cannot open remote file %s\n", dsturl.c_str());
  }

  struct stat srcstat;
  XrdClientStatInfo dststat;

  do {
    if (fstat(fd, &srcstat)) {
      fprintf(stderr,"Error: cannot stat source file %s\n", sourcefile.c_str());
      exit(-1);
    }

    if (!client->Stat(&dststat, true)) {
      fprintf(stderr, "Error: cannot stat destination file %s\n", dsturl.c_str());
      exit(-1);
    }

    localoffset = srcstat.st_size;
    remoteoffset = dststat.size;

    if (remoteoffset > localoffset) {
      fprintf(stderr, "Error: remote file is longer than local file - force truncation\n");
      if (!client->Truncate(0)) {
	fprintf(stderr,"Error: couldn't truncate remote file\n");
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
      fprintf(stderr,"remoteoffset=%llu module=%llu mapoffset=%llu mapsize =%lu\n", (unsigned long long)remoteoffset,(unsigned long long)remoteoffset%PAGESIZE, (unsigned long long)mapoffset, (unsigned long)mapsize);
    
      char* copyptr = (char*) mmap(NULL, mapsize, PROT_READ, MAP_SHARED, fd,mapoffset);
      if (!copyptr) {
	fprintf(stderr,"Error: cannot map source file at %llu\n", (unsigned long long)remoteoffset);
	exit(-1);
      }
      if (!client->Write(copyptr + (remoteoffset %PAGESIZE), remoteoffset, transfersize)) {
	fprintf(stderr,"Error: cannot write remote block at %llu/%lu\n", (unsigned long long)remoteoffset, (unsigned long)transfersize);
	sleep(60);
      }
      munmap(copyptr,mapsize);
    }
    
    gettimeofday(&sync2,&tz);
    
    float syncperiod = ( ((sync2.tv_sec - sync1.tv_sec)*1000.0) + ((sync2.tv_usec-sync1.tv_usec)/1000.0) );
    // every 1000 ms we send a sync command
    if (syncperiod > 1000) {
      if (!client->Sync()) {
	fprintf(stderr,"Error: cannot sync remote file\n");
	exit(-1);
      }
    }
  } while (1);
}

  
