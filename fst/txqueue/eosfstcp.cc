// ----------------------------------------------------------------------
// File: eosfstcp.cc
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

#include <set>
#include <string>
#include <algorithm>

#include <unistd.h>
#include <errno.h>
#include <pwd.h>
#include <grp.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>
#include <iostream>
#include <openssl/md5.h>

#include <XrdPosix/XrdPosixXrootd.hh>
#include <XrdOuc/XrdOucString.hh>
#include <XrdClient/XrdClientConst.hh>

#include "fst/io/RaidDpFile.hh"
#include "fst/io/ReedSFile.hh"
#include "fst/checksum/ChecksumPlugins.hh"

#define PROGRAM "eosfstcp"
#define DEFAULTBUFFERSIZE 1*1024*1024

#define XROOTID      0x2
#define RAIDIOID     0x1
#define FSID         0x0
#define STDINOUTID   0x3
#define MAXSRCDST    16

MD5_CTX md5ctx;
const char* protocols[] = {"file","raid","xroot",NULL};
const char* xs[] = {"adler", "md5", "sha1", "crc32", "crc32c"};
std::set<std::string> xsTypeSet(xs, xs + 5);

int verbose = 0;
int debug = 0;
int trylocal = 0;
int progbar = 1;
int summary = 1;
int buffersize = DEFAULTBUFFERSIZE;
int euid = -1;
int egid = -1;
int nsrc = 1;
int ndst = 1;
int createdir = 0;
int transparentstaging = 0;
int appendmode = 0;
long long startbyte = -1;
long long stopbyte = -1;
off_t startwritebyte = 0;
off_t stopwritebyte = 0;
unsigned char md5string[MD5_DIGEST_LENGTH];
char symlinkname[4096];
int dosymlink=0;
int replicamode=0;
float bandwidth=0;
XrdOucString cpname="";

//RAID related variables
off_t offsetRaid = 0;
int nparitystripes = 0;
bool isSrcRaid = false;
bool isRaidTransfer = false;
bool storerecovery = false;
std::string replicationType ="";
eos::fst::RaidIO* redundancyObj = NULL;

//checksum variables
int kXS = 0;
off_t offsetXS = 0;
bool computeXS = false;
std::string xsString = "";
eos::fst::CheckSum* xsObj = NULL;

// To compute throughput etc
struct timeval abs_start_time;
struct timeval abs_stop_time;
struct timezone tz;

XrdPosixXrootd posixsingleton;

void usage() {
  fprintf(stderr, "Usage: %s [-5] [-X <type>] [-t <mb/s>] [-h] [-v] [-d] [-l] [-b <size>] [-n] [-s] [-u <id>] [-g <id>] [-S <#>] [-D <#>] [-N <name>]<src1> [src2...] <dst1> [dst2...]\n",PROGRAM);
  fprintf(stderr, "       -h           : help\n");
  fprintf(stderr, "       -d           : debug mode\n");
  fprintf(stderr, "       -v           : verbose mode\n");
  fprintf(stderr, "       -l           : try to force the destination to the local disk server [not supported]\n");
  fprintf(stderr, "       -a           : append to the file rather than truncate an existing file\n"); 
  fprintf(stderr, "       -b <size>    : use <size> as buffer size for copy operations\n");
  fprintf(stderr, "       -m <mode>    : set the mode for the destination file\n");
  fprintf(stderr, "       -n           : hide progress bar\n");
  fprintf(stderr, "       -N           : set name for progress printout\n");
  fprintf(stderr, "       -s           : hide summary\n");
  fprintf(stderr, "       -u <uid|name>: use <uid> as UID to execute the operation -  (user)<name> is mapped to unix UID if possible\n");
  fprintf(stderr, "       -g <gid|name>: use <gid> as GID to execute the operation - (group)<name> is mapped to unix GID if possible\n");
  fprintf(stderr, "       -t <mb/s>    : reduce the traffic to an average of <mb/s> mb/s\n");
  fprintf(stderr, "       -S <#>       : read from <#> sources in 'parallel'\n");
  fprintf(stderr, "       -D <#>       : write to <#> sources in 'parallel'\n");
  fprintf(stderr, "       -i           : enable transparent staging\n");
  fprintf(stderr, "       -p           : create all needed subdirectories for destination paths\n");
  fprintf(stderr, "       <srcN>       : path/url or - for STDIN\n");
  fprintf(stderr, "       <dstN>       : path/url or - for STDOUT\n");
  fprintf(stderr, "       -5           : compute md5\n");
  fprintf(stderr, "       -r <start>:<stop> : read only the range from <start> bytes to <stop> bytes\n");
  fprintf(stderr, "       -L <linkname>: create a symbolic link to the 1st target file with name <linkname>\n");
  fprintf(stderr, "       -R           : replication mode - avoid dir creation and stat's\n");
  fprintf(stderr, "       -e           : error correction layout: raidDP/reedS\n");
  fprintf(stderr, "       -P           : number of parity stripes\n");
  fprintf(stderr, "       -X           : checksum type: adler, crc32, crc32c, sha1, md5\n");
  fprintf(stderr, "       -f           : force the recovery of the corrupted files and store the modifications\n");
  
  exit(-1);
}

extern "C" { 
  /////////////////////////////////////////////////////////////////////
  // function + macro to allow formatted print via cout,cerr
  /////////////////////////////////////////////////////////////////////
  void cout_print(const char *format, ...)
  {
    char cout_buff[4096];
    va_list args;
    va_start(args, format);
    vsprintf(cout_buff, format, args);
    va_end(args);
    cout << cout_buff;
  }
  
  void cerr_print(const char *format, ...)
  {
    char cerr_buff[4096];
    va_list args;
    va_start(args, format);
    vsprintf(cerr_buff, format,  args);
    va_end(args);
    cerr << cerr_buff;
  }

#define COUT(s) do {                            \
    cout_print s;                               \
  } while (0)

#define CERR(s) do {                            \
    cerr_print s;                               \
  } while (0)
  
}
//////////////////////////////////////////////////////////////////////

void print_summary(char* src[MAXSRCDST], char* dst[MAXSRCDST], unsigned long long bytesread) {
  gettimeofday (&abs_stop_time, &tz);
  float abs_time = ((float)((abs_stop_time.tv_sec - abs_start_time.tv_sec) *1000 +
                          (abs_stop_time.tv_usec - abs_start_time.tv_usec) / 1000));

  XrdOucString xsrc[MAXSRCDST];
  XrdOucString xdst[MAXSRCDST];

  for (int i = 0; i < nsrc; i++) { 
    xsrc[i] = src[i];
    xsrc[i].erase(xsrc[i].rfind('?'));
  }

  for (int i = 0; i < ndst; i++) {
    xdst[i] = dst[i];
    xdst[i].erase(xdst[i].rfind('?'));
  }

  COUT(("[eosfstcp] #################################################################\n"));

  for (int i = 0; i < nsrc; i++) 
    COUT(("[eosfstcp] # Source Name [%02d]         : %s\n", i, xsrc[i].c_str()));

  for (int i = 0; i < ndst; i++)
    COUT(("[eosfstcp] # Destination Name [%02d]    : %s\n", i, xdst[i].c_str()));

  COUT(("[eosfstcp] # Data Copied [bytes]      : %lld\n", bytesread));
  if (ndst > 1) {
    COUT(("[eosfstcp] # Tot. Data Copied [bytes] : %lld\n", bytesread*ndst));
  }
  COUT(("[eosfstcp] # Realtime [s]             : %f\n", abs_time/1000.0));
  if (abs_time > 0) {
    COUT(("[eosfstcp] # Eff.Copy. Rate[MB/s]     : %f\n", bytesread/abs_time/1000.0));
  }
  if (bandwidth) {
    COUT(("[eosfstcp] # Bandwidth[MB/s]          : %d\n", (int)bandwidth));
  }
  if (computeXS) {
    COUT(("[eosfstcp] # Checksum Type %s        : ", xsString.c_str()));
    COUT(("%s", xsObj->GetHexChecksum()));
    COUT(("\n"));
  }
  COUT(("[eosfstcp] # Write Start Position     : %lld\n",startwritebyte));
  COUT(("[eosfstcp] # Write Stop  Position     : %lld\n",stopwritebyte));
  if (startbyte >= 0) {
    COUT(("[eosfstcp] # Read  Start Position     : %lld\n",startbyte));
    COUT(("[eosfstcp] # Read  Stop  Position     : %lld\n",stopbyte));
  }
}

void print_progbar(unsigned long long bytesread, unsigned long long size) {
  CERR(("[eosfstcp] %-24s Total %.02f MB\t|",cpname.c_str(), (float)size/1024/1024));
  for (int l=0; l< 20;l++) {
    if (l< ( (int)(20.0*bytesread/size)))
      CERR(("="));
    if (l==( (int)(20.0*bytesread/size)))
      CERR((">"));
    if (l> ( (int)(20.0*bytesread/size)))
      CERR(("."));
  }
  
  float abs_time=((float)((abs_stop_time.tv_sec - abs_start_time.tv_sec) *1000 +
                          (abs_stop_time.tv_usec - abs_start_time.tv_usec) / 1000));
  CERR(("| %.02f %% [%.01f MB/s]\r",100.0*bytesread/size,bytesread/abs_time/1000.0));
}


int main(int argc, char* argv[]) {
  int c;
  mode_t dest_mode[MAXSRCDST];
  int set_mode = 0;
  MD5_Init(&md5ctx);
 
  // analyse source and destination url
  extern char *optarg;
  extern int optind;


  while ( (c = getopt(argc, argv, "nshdvlipfe:P:X:b:m:u:g:t:S:D:5ar:N:L:R")) != -1) {
    switch(c) {
    case 'v':
      verbose = 1;
      break;
    case 'd':
      debug = 1;
      break;
    case 'l':
      trylocal = 1;
      break;
    case 'n':
      progbar = 0;
      break;
    case 'p':
      createdir = 1;
      break;
    case 's':
      summary = 0;
      break;
    case 'i':
      transparentstaging = 1;
      break;
    case 'a':
      appendmode = 1;
      break;
    case 'f':
      storerecovery = true;
      break;
    case 'e':
      replicationType = optarg;
      if (replicationType != "raidDP" && replicationType != "reedS") {
        fprintf(stderr, "error: no such RAID layout\n");
        exit(-1);
      }
      isRaidTransfer = true;
      break;
    case 'X':
      {
        xsString  = optarg;
        if (find(xsTypeSet.begin(), xsTypeSet.end(), xsString) == xsTypeSet.end())
        {
          fprintf(stderr, "error: no such checksum type: %s\n", optarg);
          exit(-1);
        }

        int layout = 0;
        unsigned long layoutId = 0;
        if (xsString == "adler") {
          layoutId = eos::common::LayoutId::GetId(layout, eos::common::LayoutId::kAdler);          
        }
        else if (xsString == "crc32") {
          layoutId = eos::common::LayoutId::GetId(layout, eos::common::LayoutId::kCRC32);          
        }
        else if (xsString == "md5") {
          layoutId = eos::common::LayoutId::GetId(layout, eos::common::LayoutId::kMD5);          
        }
        else if (xsString == "sha1") {
          layoutId = eos::common::LayoutId::GetId(layout, eos::common::LayoutId::kSHA1);
        }
        else if (xsString == "crc32c") {
          layoutId = eos::common::LayoutId::GetId(layout, eos::common::LayoutId::kCRC32C);          
        }

        xsObj = eos::fst::ChecksumPlugins::GetChecksumObject(layoutId);
        xsObj->Reset();
        computeXS = true;
      }
      break;
    case 'P':
      nparitystripes = atoi(optarg);
      if (nparitystripes < 2) {
        fprintf(stderr,"error: number of parity stripes >= 2\n");
        exit(-1);
      }
      break;
    case 'u':
      euid = atoi(optarg);
      char tuid[128];
      sprintf(tuid, "%d", euid);
      if (strcmp(tuid,optarg)) {
        // this is not a number, try to map it with getpwnam
        struct passwd* pwinfo = getpwnam(optarg);
        if (pwinfo) {
          euid = pwinfo->pw_uid;
          if (debug) {fprintf(stdout,"[eosfstcp]: mapping user  %s=>UID:%d\n", optarg, euid);}
        } else {
          fprintf(stderr,"error: cannot map user %s to any unix id!\n", optarg);
          exit(-ENOENT);
        }
      }
      break;
    case 'g':
      egid = atoi(optarg);
      char tgid[128];
      sprintf(tgid, "%d", egid);
      if (strcmp(tgid,optarg)) {
        // this is not a number, try to map it with getgrnam
        struct group* grinfo = getgrnam(optarg);
        if (grinfo) {
          egid = grinfo->gr_gid;
          if (debug) {fprintf(stdout,"[eosfstcp]: mapping group %s=>GID:%d\n", optarg, egid);}
        } else {
          fprintf(stderr,"error: cannot map group %s to any unix id!\n", optarg);
          exit(-ENOENT);
        }
      }
      break;
    case 't':
      bandwidth = atoi(optarg);
      if ( (bandwidth < 1) || (bandwidth > 2000) ) {
        fprintf(stderr,"error: bandwidth can only be 1 <= bandwidth <= 2000 Mb/s\n");
        exit(-1);
      }
      break;
    case 'S':
      nsrc = atoi(optarg);
      if ( (nsrc < 1) || (nsrc > MAXSRCDST) ) {
        fprintf(stderr,"error: # of sources must be 1 <= # <= %d\n",MAXSRCDST);
        exit(-1);
      }
      break;
    case 'D':
      ndst = atoi(optarg);
      if ( (ndst < 1) || (ndst > MAXSRCDST) ) {
        fprintf(stderr,"error: # of sources must be 1 <= # <= %d\n",MAXSRCDST);
        exit(-1);
      }
      break;
    case 'N':
      cpname = optarg;
      break;
    case 'b':
      buffersize = atoi(optarg);
      if ( (buffersize < 4096) || (buffersize > 100 * 1024 * 1024)) {
        fprintf(stderr,"error: buffer size can only 4k <= size <= 100 M\n");
        exit(-1);
      }
      break;
    case 'm':
      for (int i = 0; i < MAXSRCDST; i++) {
        dest_mode[i] = strtol(optarg,0,8);
      }
      set_mode=1;
      break;
    case 'r':
      char* colon;
      colon = strchr(optarg,':');
      if (colon < 0) {
        fprintf(stderr,"error: range has to be given in the format <startbyte>:<stopbyte> e.g. 0:100000\n");
        exit(-1);
      }
      *colon = 0;
      startbyte = strtoll(optarg,0,0);
      stopbyte  = strtoll(colon+1,0,0);
      if (debug) {fprintf(stdout,"[eosfstcp]: reading range start=%lld stop=%lld\n", startbyte, stopbyte);}
      break;
    case 'L':
      sprintf(symlinkname,"%s",optarg);
      dosymlink = 1;
      break;
    case 'R':
      replicamode = 1;
      break;
    case 'h':
    default:
      usage();
      ;
    }
  }

  if (optind - 1 + nsrc + ndst >= argc) {
    usage();
  }

  // we need a buffer
  unsigned char* buffer = (unsigned char*) malloc(buffersize);

  if (!buffer) {
    fprintf(stderr,"error: cannot allocate buffer of size %d!\n", buffersize);
    exit(-1);
  }
  
  if (debug) {
    fprintf(stderr, "[eosfstcp]: allocate copy buffer with %d bytes\n", buffersize);
  }
  
  char *source[MAXSRCDST];
  char *destination[MAXSRCDST];

  for (int i = 0; i < nsrc; i++) {
    source[i] = argv[optind + i];
  }

  for (int i = 0; i < ndst; i++) {
    destination[i] = argv[optind + nsrc + i];
  }

  if (verbose || debug) {
    fprintf(stdout,"[eosfstcp]: ");
    for (int i = 0; i < nsrc; i++) 
      fprintf(stdout,"src<%d>=%s ", i, source[i]); 
    for (int i = 0; i < ndst; i++) 
      fprintf(stdout,"dst<%d>=%s ", i, destination[i]);
    fprintf(stdout,"\n");
  }

  XrdOucString src[MAXSRCDST];
  XrdOucString dst[MAXSRCDST];
  for (int i = 0; i < nsrc; i++) {
    src[i] = source[i];
  }
  for (int i = 0; i < ndst; i++) {
    dst[i] = destination[i];
  }

  int sid[MAXSRCDST];
  int did[MAXSRCDST];
  
  memset(sid, 0, sizeof sid);
  memset(did, 0, sizeof did);

  for (int i = 0 ; i < nsrc; i++) {
    if (src[i].beginswith("root://")) {
      if (!isRaidTransfer) {
        sid[i] = XROOTID;
      }
      else {
        if (!nparitystripes) {
          fprintf(stderr, "error: number of parity stripes undefined\n");
          exit(-1);
        }
        if (nsrc > ndst)
          sid[i] = RAIDIOID;
        else
          sid[i] = XROOTID;               
      }
    }
    if (src[i] == "-") {
      sid[i] = STDINOUTID;
      if (i > 0) {
        fprintf(stderr,"error: you cannot read with several sources from stdin\n");
        exit(-1);
      }
    }
  }

  for (int i = 0; i < ndst; i++) {

    if (dst[i].beginswith("root://")) {
      if (!isRaidTransfer) {
        did[i] = XROOTID;
      }
      else {  
        if (!nparitystripes) {
          fprintf(stderr, "error: number of parity stripes undefined\n");
          exit(-1);
        }
        if (nsrc > ndst)
          did[i] = XROOTID;
        else
          did[i] = RAIDIOID;
      }
    }

    if (dst[i] == "-") {
      did[i]= STDINOUTID;
    }
  }

  if (verbose || debug) {
    fprintf(stdout, "[eosfstcp]: copy protocol ");
    for (int j = 0; j < nsrc; j++) {
      fprintf(stdout, "%s:", protocols[sid[j]]);
    }
    fprintf(stdout, "=>");
    for (int j = 0; j < ndst; j++) {
      fprintf(stdout, "%s:", protocols[did[j]]);
    }
    fprintf(stdout, "\n");
  }

  int srcfd[MAXSRCDST];
  int dstfd[MAXSRCDST];
  int stat_failed = 0;
  struct stat st[MAXSRCDST];
  
  if (egid >= 0) {
    if (setgid(egid)) {
      fprintf(stdout, "error: cannot change identity to gid %d\n", egid);
      exit(-EPERM);
    }
  }

  if (euid >= 0) {
    if (setuid(euid)) {
      fprintf(stdout, "error: cannot change identity to uid %d\n", euid);
      exit(-EPERM);
    }
  }

  // start the performance measurement

  gettimeofday(&abs_start_time, &tz);

  // settings specific to xrootd
  for (int i = 0; i< nsrc; i++) {
    if (sid[i] == XROOTID) {
      new XrdPosixXrootd();
      XrdPosixXrootd::setEnv(NAME_READAHEADSIZE, buffersize*3);
      XrdPosixXrootd::setEnv(NAME_READCACHESIZE, buffersize*6);
      if (debug)
        XrdPosixXrootd::setEnv(NAME_DEBUG, 10);
    }
  }

  for (int i = 0; i < ndst; i++) {
    if (did[i] == XROOTID) {
      new XrdPosixXrootd();
      XrdPosixXrootd::setEnv(NAME_READAHEADSIZE,0l);
      XrdPosixXrootd::setEnv(NAME_READCACHESIZE,0l);
      if (debug)
        XrdPosixXrootd::setEnv(NAME_DEBUG, 10);
    }
    if (did[i] == RAIDIOID) {
      new XrdPosixXrootd();
      XrdPosixXrootd::setEnv(NAME_READAHEADSIZE, buffersize*3);
      XrdPosixXrootd::setEnv(NAME_READCACHESIZE, buffersize*6);
      if (debug)
        XrdPosixXrootd::setEnv(NAME_DEBUG, 10);
    }
  }   


  if (!replicamode) {
    for (int i = 0 ; i < nsrc; i++) {
      // stat the source
      switch(sid[i]) {
      case 0:
        if (debug) {fprintf(stdout, "[eosfstcp]: doing POSIX stat on %s\n", source[i]);}
        stat_failed = lstat(source[i], &st[i]);
        break;
      case 1:
        if (debug) {fprintf(stdout, "[eosfstcp]: doing XROOT(RAIDIO) stat on %s\n", source[i]);}
        stat_failed = XrdPosixXrootd::Stat(source[i], &st[i]);
        break;
      case 2:
        if (debug) {fprintf(stdout, "[eosfstcp]: doing XROOT stat on %s\n", source[i]);}
        stat_failed = XrdPosixXrootd::Stat(source[i], &st[i]);
        break;
      case 3:
        stat_failed = 0;
        break;
      }
    
      if (!isRaidTransfer && stat_failed) {
        fprintf(stderr, "error: cannot stat source %s\n", source[i]);
        exit(-ENOENT);
      }
    }
  }

  // stat consistency check
  if (!isRaidTransfer) {
    for (int i = 0; i < nsrc; i++) {
      for (int j = 0; j < nsrc; j++) {
        if (st[i].st_size != st[j].st_size) {
          fprintf(stderr, "error: source files differe in size !\n");
          exit(-EINVAL);
        }
      }
    }
  }

  // check if this is a range link

  if (!replicamode)
    for (int i = 0; i < nsrc; i++) {  
      if (S_ISLNK(st[i].st_mode)) {
        int readlink_size = 0;

        char* readlinkbuff = (char*) malloc(4096);
        if (!readlinkbuff) {
          fprintf(stderr, "error: cannot allocate link buffer\n");
          exit(-ENOMEM);
        }
        readlinkbuff[0] = 0;

        switch(sid[i]) {
        case 0:
          if (debug) {fprintf(stdout, "[eosfstcp]: doing POSIX readlink on %s\n", source[i]);}
          readlink_size = readlink(source[i], readlinkbuff, 4096);
          break;
        case 1:
          if (debug) {fprintf(stdout, "[eosfstcp]: doing XROOT(RAIDIO) readlink on %s\n", source[i]);}
          // not implemented in xrootd posix
          readlink_size = 1;
        case 2:
          if (debug) {fprintf(stdout, "[eosfstcp]: doing XROOT readlink on %s\n", source[i]);}
          // not implemented in xrootd posix
          readlink_size = 1;
          break;
        case 3:
          readlink_size = 0;
          break;
        }
      
        if (readlink_size < 0) {
          fprintf(stderr, "error: cannot read the link of %s\n", source[i]);
          exit(-errno);
        }
      
        char* space = strchr(readlinkbuff, ' ');
        if (space) {
          *space = 0;
          char* colon = strchr(space+1, ':');
          if (colon) {
            *colon = 0;
            // yep, this is a range link
            startbyte = strtoll(space+1, 0, 0);
            stopbyte  = strtoll(colon+1, 0, 0);
            source[i] = readlinkbuff;
            if (debug) {fprintf(stdout, "[eosfstcp]: setting range to destination %s %lld:%lld\n",
                                source[0], startbyte, stopbyte);}
          }
        }
      }
    }
  
  // if we don't have transparent staging enabled, we need to check if files are online
  if (!transparentstaging) {
    for (int i = 0; i < nsrc; i++) {
      switch(sid[i]) {
      case 0:
        if (debug) {fprintf(stdout, "[eosfstcp]: POSIX is transparent for staging - nothing to check\n");}
        break;
      case 1:
        if (debug) {fprintf(stdout, "[eosfstcp]: XROOT(RAIDIO) is transparent for staging - nothing to check\n");}
        break;
      case 2:
        if (debug) {fprintf(stdout, "[eosfstcp]: XROOT is transparent for staging - nothing to check\n");}
        break;
      case 3:
        if (debug) {fprintf(stdout, "[eosfstcp]: STDIN is transparent for staging - nothing to check\n");}
        break;
      }
    }
  }
  

  // for the '-p' flag we create the needed destination directory tree
  if ((!replicamode) && createdir) {
    struct stat dstst[MAXSRCDST];
    // loop over the destination paths
    for (int i = 0; i < ndst; i++) {
      int pos = 0;
      while ( (pos = dst[i].find("/", pos+1)) != STR_NPOS ) {
        XrdOucString subpath = dst[i];
        subpath.erase(pos+1);
        switch(did[i]) {
        case 0:
          if (debug) {fprintf(stdout, "[eosfstcp]: doing POSIX stat on %s\n", subpath.c_str());}
          stat_failed = stat((char*)subpath.c_str(), &dstst[i]);
          break;
        case 1:
          if (debug) {fprintf(stdout, "[eosfstcp]: doing XROOT(RAIDIO) stat on %s\n", subpath.c_str());}
          stat_failed = XrdPosixXrootd::Stat((char*)subpath.c_str(), &dstst[i]);
          break;
        case 2:
          if (debug) {fprintf(stdout, "[eosfstcp]: doing XROOT stat on %s\n", subpath.c_str());}
          stat_failed = XrdPosixXrootd::Stat((char*)subpath.c_str(), &dstst[i]);
          break;
        case 3:
          stat_failed = 0;
          break;
          
        }

        mode_t mode;
        int mkdir_failed = 0;
        mode = dest_mode[i] | S_IXUSR | S_IXGRP | S_IXOTH;
        if (stat_failed) {
          // create the directory
          switch(did[i]) {
          case 0:
            if (debug) {fprintf(stdout,"[eosfstcp]: doing POSIX mkdir on %s\n", (char*)subpath.c_str());}
            mkdir_failed = mkdir((char*)subpath.c_str(), mode);
            break;
          case 1:
            if (debug) {fprintf(stdout,"[eosfstcp]: doing XROOT(RAIDIO) mkdir on %s\n", (char*)subpath.c_str());}
            mkdir_failed = XrdPosixXrootd::Mkdir(source[i], mode);
            break;
          case 2:
            if (debug) {fprintf(stdout,"[eosfstcp]: doing XROOT mkdir on %s\n", (char*)subpath.c_str());}
            mkdir_failed = XrdPosixXrootd::Mkdir(source[i], mode);
            break;
          case 3:
            mkdir_failed = 0;
            break;
          }
          if (mkdir_failed) {
            fprintf(stderr,"error: cannot create destination sub-directory %s\n", (char*)subpath.c_str());
            exit(-EPERM);        
          }
        }

        int chown_failed = 0;
        if (getuid() == 0) {
          // the root user can also set the user/group as in the source location
          switch(did[i]) {
          case 0:
            chown_failed = chown((char*)subpath.c_str(), st[0].st_uid, st[0].st_gid);
            break;
          case 1:
            // we don't have that here in the std. xrootd
            chown_failed = 0;
            break;
          case 2:
            // we don't have that here in the std. xrootd
            chown_failed = 0;
            break;
          case 3:
            chown_failed = 0;
            break;
          }
        }
        
        if (chown_failed) {
          fprintf(stderr, "error: cannot set owner=%d/group=%d for %s\n",
                  st[i].st_uid, st[i].st_gid, (char*)subpath.c_str());
          exit(-EPERM);
        }
      }
    }
  }
 
  if (isRaidTransfer) {
    int flags;
    std::vector<std::string> vectUrl;

    if (nsrc > ndst) {
      if (storerecovery)
        flags = O_RDWR;
      else
        flags = O_RDONLY;
      
      isSrcRaid = true;  //read operation
      for (int i = 0; i < nsrc; i++) { vectUrl.push_back(source[i]); }
    }
    else {
      flags = O_WRONLY;
      isSrcRaid = false; //write operation
      for (int i = 0; i < ndst; i++) { vectUrl.push_back(destination[i]); }
    }

    if (debug) {fprintf(stdout, "[eosfstcp]: doing XROOT(RAIDIO) open with flags: %x\n", flags);}
    
    if (replicationType == "raidDP") {
      redundancyObj = new eos::fst::RaidDpFile(vectUrl, nparitystripes, storerecovery);
    }
    else if (replicationType == "reedS") {
      redundancyObj = new eos::fst::ReedSFile(vectUrl, nparitystripes, storerecovery);
    }
    
    if (isSrcRaid && redundancyObj->open(flags)) {
      fprintf(stderr, "error: can not open RAIDIO object for read\n");
      exit(-EIO);
    }
    else if (!isSrcRaid && redundancyObj->open(flags))
    {
      fprintf(stderr, "error: can not open RAIDIO object for write\n");
      exit(-ENOENT);
    }
  }
  
  for (int i = 0; i< nsrc; i++) {
    switch(sid[i]) {
      case 0:
        if (debug) {fprintf(stdout, "[eosfstcp]: doing POSIX open to read  %s\n", source[i]);}
        srcfd[i] = open(source[i], O_RDONLY);
        break;
      case 1:
        //already taken care of
        break;
      case 2:
        if (debug) {fprintf(stdout, "[eosfstcp]: doing XROOT open to read  %s\n", source[i]);}
        srcfd[i] = XrdPosixXrootd::Open(source[i], O_RDONLY);
        break;
      case 3:
        srcfd[i] = fileno(stdin);
        break;
    }
    
    if (!isRaidTransfer && srcfd[i]<0) {
      fprintf(stderr, "error: cannot open source file %s\n", source[i]);
      exit(-ENOENT);
    }
  }


  if (startbyte > 0) {
    // seek the required start position
    for (int i = 0; i < nsrc; i++) {
      if (debug) {fprintf(stdout, "[eosfstcp]: seeking in %d to position %lld\n", srcfd[i], startbyte);}
      switch(sid[i]) {
      case 0:
        startbyte = lseek(srcfd[i], startbyte, SEEK_SET);
        offsetXS = startbyte;
        break;
      case 1:
        offsetRaid = startbyte;
        offsetXS = startbyte;
        break;
      case 2:
        startbyte = XrdPosixXrootd::Lseek(srcfd[i], startbyte, SEEK_SET);
        offsetXS = startbyte;
        break;
      }      
      if (startbyte < 0) {
        fprintf(stderr, "error: cannot seek to the required startposition of file %s %d\n", source[i], errno);
        exit(-EIO);
      }
    }
  }

  for (int i = 0; i < ndst; i++) {
    switch(did[i]) {
    case 0:
      if (debug) {fprintf(stdout, "[eosfstcp]: doing POSIX open to write  %s\n", destination[i]);}
      if (appendmode) {
        dstfd[i] = open(destination[i], O_WRONLY|O_CREAT, st[i].st_mode);
      } else {
        dstfd[i] = open(destination[i], O_WRONLY|O_TRUNC|O_CREAT, st[i].st_mode);
      }
      break;
    case 1:
      //already taken care of
      break;
    case 2:
      if (debug) {fprintf(stdout, "[eosfstcp]: doing XROOT open to write  %s\n", destination[i]);}
      if (appendmode) {
	struct stat buf;
	if (XrdPosixXrootd::Stat((char*)destination[i],&buf)) {
	  dstfd[i] = XrdPosixXrootd::Open(destination[i],O_WRONLY|O_CREAT,st[i].st_mode);
	} else {
	  dstfd[i] = XrdPosixXrootd::Open(destination[i],O_WRONLY,st[i].st_mode);
	}
      } else {
        dstfd[i] = XrdPosixXrootd::Open(destination[i], O_WRONLY|O_TRUNC|O_CREAT, st[i].st_mode);
      }
      break;
    case 3:
      dstfd[i] = fileno(stdout);
      break;
    }
    
    if (!isRaidTransfer && dstfd[i] < 0) {
      fprintf(stderr, "error: cannot open destination file %s\n", destination[i]);
      exit(-EPERM);
    }
  }

  if (appendmode) {
    // in case the file exists, seek the end and print the offset
    for (int i = 0; i < ndst; i++) {
      switch(did[i]) {
      case 0:
        startwritebyte = lseek(dstfd[i], 0, SEEK_END);
        break;
      case 1:
        //not supported
        break;
      case 2:
        startwritebyte = XrdPosixXrootd::Lseek(dstfd[i], (long long)0, SEEK_END);
        break;
      }      
      if (startwritebyte < 0) {
        fprintf(stderr, "error: cannot seek to end of file to %d of %s\n", dest_mode[i], destination[i]);
        exit(-EIO);
      }
    }
  }

  for (int i = 0; i < ndst; i++ ) {
    // let's set the source mode or a specified one
    int chmod_failed = 0;
    
    if (!set_mode) {
      // if not specified on the command line, take the source mode
      dest_mode[i] = st[0].st_mode;
    }

    switch(did[i]) {
    case 0:
      chmod_failed = chmod(destination[i], dest_mode[i]);
      break;
    case 1:
      // we don't have that here in the std. xrootd
      chmod_failed = 0;
    case 2:
      // we don't have that here in the std. xrootd
      chmod_failed = 0;
      break;
    case 3:
      chmod_failed = 0;
      break;
    }
    
    if (chmod_failed) {
      fprintf(stderr, "error: cannot set permissions to %d for file %s\n", dest_mode[i], destination[i]);
      exit(-EPERM);
    }
  }

  for (int i = 0; i < ndst; i++) {
    int chown_failed = 0;
    if (getuid() == 0) {
      // the root user can also set the user/group as in the source location
      switch(did[i]) {
      case 0:
        chown_failed = chown(destination[i], st[0].st_uid, st[0].st_gid);
        break;
      case 1:
        // we don't have that here in the std. xrootd
        chown_failed = 0;
        break;
      case 2:
        // we don't have that here in the std. xrootd
        chown_failed = 0;
        break;
      case 3:
        chown_failed = 0;
        break;
      }
    }
    
    if (chown_failed) {
      fprintf(stderr, "error: cannot set owner=%d/group=%d for %s\n", st[i].st_uid, st[i].st_gid,destination[i]);
      exit(-EPERM);
    }
  }

  
  // copy
  long long totalbytes=0;

  stopwritebyte = startwritebyte;
  while (1) {
    if (progbar) {
      gettimeofday(&abs_stop_time, &tz);
      for (int i = 0; i < nsrc; i++) {
        if (sid[i] == 3) {
          st[i].st_size = totalbytes;
        }
      }
      print_progbar(totalbytes, st[0].st_size);
    }
    
    if (bandwidth) {
      gettimeofday (&abs_stop_time, &tz);
      float abs_time=((float)((abs_stop_time.tv_sec - abs_start_time.tv_sec) *1000 +
                              (abs_stop_time.tv_usec - abs_start_time.tv_usec) / 1000));
      
      // regulate the io - sleep as desired
      float exp_time = totalbytes / bandwidth / 1000.0;
      if (abs_time < exp_time) {
        usleep((int)(1000*(exp_time - abs_time)));
      }
    }

    // for ranges we have to adjust the last buffersize
    if ( (stopbyte >= 0) && (((stopbyte - startbyte) - totalbytes) < buffersize) ) {
      buffersize = (stopbyte - startbyte) - totalbytes;
    }
    
    ssize_t nread = -1;
    switch(sid[0]) {
    case 0:
    case 3:
      nread = read(srcfd[0], (void*)(buffer), buffersize);
      break;
    case 1:
      nread = redundancyObj->read(offsetRaid, (char*)buffer, buffersize);
      offsetRaid += nread;
      break;
    case 2:
      nread = XrdPosixXrootd::Read(srcfd[0], (void*)(buffer), buffersize);
      break;
    }
   
    if (nread < 0) {
      fprintf(stderr, "error: read failed on source file %s - destination file is incomplete!\n", source[0]);
      exit(-EIO);
    }
    if (nread == 0 ) {
      // end of file
      break;
    }
    
    if (computeXS) {  
      xsObj->Add((const char*)buffer, nread, offsetXS);
      offsetXS += nread;
    }

    ssize_t nwrite = 0;
    for (int i = 0; i < ndst; i++) {
      switch(did[i]) {
      case 0:
      case 3:
        nwrite = write(dstfd[i], (void*)buffer, nread);
        break;
      case 1:
        if (i == 0) {
          nwrite = redundancyObj->write(stopwritebyte, (char*)buffer, nread);
          i = ndst - 1;
        }
        break;
      case 2:
        nwrite = XrdPosixXrootd::Write(dstfd[i], (void*)buffer, nread);
        break;
      }
      
      if (nwrite != nread) {
        fprintf(stderr, "error: write failed on destination file %s - wrote %lld/%lld bytes - destination file is incomplete!\n", destination[i], (long long)nwrite, (long long)nread);
        exit(-EIO);
      }
    }
    
    totalbytes += (long long)nwrite;
    stopwritebyte += (long long)nwrite;
    
    if (nread < buffersize) {
      // seems to be end of file, with tar this doesn't work
      //fprintf(stdout, "Seems to be the end of file.\n");
      //break;
    }
  }   //end while(1) 

  if (computeXS) {
    xsObj->Finalize();
  }

  if (progbar) {
    gettimeofday(&abs_stop_time, &tz);
    for (int i = 0; i < nsrc; i++) {
      if (sid[i] == 3) {
        st[i].st_size = totalbytes;
      }
    }
    print_progbar(totalbytes, st[0].st_size);
    cout << endl;
  }

  if (summary) {
    print_summary(source, destination, totalbytes);
  }


  for (int i = 0; i < nsrc; i++) {
    // close all files
    switch(sid[i]) {
    case 0:
      close(srcfd[i]);
      break;
    case 1:
      if (i == 0) {
        redundancyObj->close();
        i = nsrc - 1;
      }
      break;
    case 2:
      XrdPosixXrootd::Close(srcfd[i]);
      break;
    case 3:
      break;
    }
  }

  for (int i = 0; i < ndst; i++) {
    switch(did[i]) {
    case 0:
      close(dstfd[i]);
      break;
    case 1:
      if (i == 0) {
        redundancyObj->close();
        i = ndst - 1;
      }
      break;
    case 2:
      XrdPosixXrootd::Close(dstfd[i]);
      break;
    case 3:
      break;
    }
  }

  if (redundancyObj) {
    delete redundancyObj;
  }
  
  if (dosymlink) {
    int symlink_failed = 0;
    char rangedestname[4096];
    if (appendmode) {
      sprintf(rangedestname,"%s %llu:%llu", destination[0],
              (unsigned long long)startwritebyte, (unsigned long long)stopwritebyte);
    } else {
      sprintf(rangedestname, "%s", destination[0]);
    }

    if (debug) {fprintf(stdout, "[eosfstcp]: creating symlink %s->%s\n", symlinkname, rangedestname);} 
    switch(did[0]) {
    case 0:
      unlink(symlinkname);
      symlink_failed = symlink(rangedestname, symlinkname);
      break;
    case 1:
      // xrootd has no symlink support in posix 
      break;
    case 2:
      // xrootd has no symlink support in posix 
      break;
    case 3:
      break;
    }
    if (symlink_failed) {
      fprintf(stderr,"error: cannot creat symlink from %s -> %s\n", symlinkname, rangedestname);
      exit(-ESPIPE);
    }
  }
  
  return 0;
}
