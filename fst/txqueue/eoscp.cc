//------------------------------------------------------------------------------
// File: eoscp.cc
// Author: Elvin-Alin Sindrilaru / Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

/*----------------------------------------------------------------------------*/
#include <set>
#include <string>
#include <algorithm>
#include <math.h>
/*----------------------------------------------------------------------------*/
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
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdOuc/XrdOucString.hh"
#include "fst/layout/RaidDpLayout.hh"
#include "fst/layout/ReedSLayout.hh"
#include "fst/io/AsyncMetaHandler.hh"
#include "fst/io/ChunkHandler.hh"
#include "fst/checksum/ChecksumPlugins.hh"
/*----------------------------------------------------------------------------*/

#define PROGRAM "eoscp"
#define DEFAULTBUFFERSIZE 4*1024*1024
#define MAXSRCDST    16

using eos::common::LayoutId;

typedef std::vector<std::pair<int, XrdCl::File*> > VectHandlerType;
typedef std::vector<std::pair<std::string, std::string> > VectLocationType;

enum AccessType
{
  LOCAL_ACCESS, ///< local access
  RAID_ACCESS, ///< xroot protocol but with raid layout
  XRD_ACCESS, ///< xroot protocol
  CONSOLE_ACCESS ///< input/output to console
};

const char* protocols[] = {"file", "raid", "xroot", NULL};
const char* xs[] = {"adler", "md5", "sha1", "crc32", "crc32c"};
std::set<std::string> xsTypeSet (xs, xs + 5);

///! vector of source file descriptors or xrd objects
VectHandlerType src_handler;

///! vector of destination file descriptors or xrd objects
VectHandlerType dst_handler;

///! vector of source host address and path file
VectLocationType src_location;

///! vector of destination host address and path file
VectLocationType dst_location;

///! vector of async request handlers for the destination files
std::vector<eos::fst::AsyncMetaHandler*> meta_handler;

std::vector<AccessType> src_type; ///< vector of source type access
std::vector<AccessType> dst_type; ///< vector of destination type access

int verbose = 0;
int debug = 0;
int trylocal = 0;
int progbar = 1;
int summary = 1;

unsigned long long targetsize = 0;
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
char symlinkname[4096];
int dosymlink = 0;
int replicamode = 0;
float bandwidth = 0;
XrdOucString cpname = "";
XrdCl::XRootDStatus status;
uint32_t buffersize = DEFAULTBUFFERSIZE;

double read_wait = 0; ///< statistics about total read time
double write_wait = 0; ///< statistics about total write time
char* buffer = NULL; ///< used for doing the reading
bool first_time = true; ///< first time prefetch two blocks

//..............................................................................
// RAID related variables
//..............................................................................
off_t stripeWidth = 1024 * 1024;
uint64_t offsetXrd = 0;
int nparitystripes = 0;

bool isRaidTransfer = false; ///< true if we currently handle a RAID transfer
bool isSrcRaid = false; ///< meaninful only for RAID transfers 
bool isStreamFile = false; ///< the file is streamed
bool doStoreRecovery = false; ///< store recoveries if the file is corrupted
std::string opaqueInfo; ///< opaque info containing the capabilities 
///< necesssary to do a parallel IO open

std::string replicationType = "";
eos::fst::RaidMetaLayout* redundancyObj = NULL;

//..............................................................................
// Checksum variables
//..............................................................................
int kXS = 0;
off_t offsetXS = 0;
bool computeXS = false;
std::string xsString = "";
eos::fst::CheckSum* xsObj = NULL;


//..............................................................................
// To compute throughput etc
//..............................................................................
struct timeval abs_start_time;
struct timeval abs_stop_time;
struct timezone tz;

std::string progressFile = "";

char *source[MAXSRCDST];
char *destination[MAXSRCDST];

//------------------------------------------------------------------------------
// Usage command 
//------------------------------------------------------------------------------

void
usage ()
{
  fprintf(stderr, "Usage: %s [-5] [-X <type>] [-t <mb/s>] [-h] [-v] [-d] [-l] [-b <size>] [-T <size>] [-Y] [-n] [-s] [-u <id>] [-g <id>] [-S <#>] [-D <#>] [-O <filename>] [-N <name>]<src1> [src2...] <dst1> [dst2...]\n", PROGRAM);
  fprintf(stderr, "       -h           : help\n");
  fprintf(stderr, "       -d           : debug mode\n");
  fprintf(stderr, "       -v           : verbose mode\n");
  fprintf(stderr, "       -l           : try to force the destination to the local disk server [not supported]\n");
  fprintf(stderr, "       -a           : append to the file rather than truncate an existing file\n");
  fprintf(stderr, "       -b <size>    : use <size> as buffer size for copy operations\n");
  fprintf(stderr, "       -T <size>    : use <size> as target size for copies from STDIN\n");
  fprintf(stderr, "       -m <mode>    : set the mode for the destination file\n");
  fprintf(stderr, "       -n           : hide progress bar\n");
  fprintf(stderr, "       -N           : set name for progress printout\n");
  fprintf(stderr, "       -s           : hide summary\n");
  fprintf(stderr, "       -u <uid|name>: use <uid> as UID to execute the operation -  (user)<name> is mapped to unix UID if possible\n");
  fprintf(stderr, "       -g <gid|name>: use <gid> as GID to execute the operation - (group)<name> is mapped to unix GID if possible\n");
  fprintf(stderr, "       -t <mb/s>    : reduce the traffic to an average of <mb/s> mb/s\n");
  fprintf(stderr, "       -S <#>       : read from <#> sources in 'parallel'\n");
  fprintf(stderr, "       -D <#>       : write to <#> sources in 'parallel'\n");
  fprintf(stderr, "       -O <file>    : write progress file to <file> (0.00 - 100.00%%)\n");
  fprintf(stderr, "       -i           : enable transparent staging\n");
  fprintf(stderr, "       -p           : create all needed subdirectories for destination paths\n");
  fprintf(stderr, "       <srcN>       : path/url or - for STDIN\n");
  fprintf(stderr, "       <dstN>       : path/url or - for STDOUT\n");
  fprintf(stderr, "       -5           : compute md5\n");
  fprintf(stderr, "       -r <start>:<stop> : read only the range from <start> bytes to <stop> bytes\n");
  fprintf(stderr, "       -L <linkname>: create a symbolic link to the 1st target file with name <linkname>\n");
  fprintf(stderr, "       -R           : replication mode - avoid dir creation and stat's\n");
  fprintf(stderr, "       -X           : checksum type: adler, crc32, crc32c, sha1, md5\n");
  fprintf(stderr, "       -e           : RAID layouts - error correction layout: raidDP/reedS\n");
  fprintf(stderr, "       -P           : RAID layouts - number of parity stripes\n");
  fprintf(stderr, "       -f           : RAID layouts - store the modifications in case of errors\n");
  fprintf(stderr, "       -c           : RAID layouts - force check and recover any corruptions in any stripe\n");
  fprintf(stderr, "       -Y           : RAID layouts - streaming file\n");

  exit(-1);
}

extern "C"
{
  //----------------------------------------------------------------------------
  // Function + macros to allow formatted print via cout,cerr
  //----------------------------------------------------------------------------

  void
  cout_print (const char* format, ...)
  {
    char cout_buff[4096];
    va_list args;
    va_start(args, format);
    vsprintf(cout_buff, format, args);
    va_end(args);
    cout << cout_buff;
  }

  void
  cerr_print (const char* format, ...)
  {
    char cerr_buff[4096];
    va_list args;
    va_start(args, format);
    vsprintf(cerr_buff, format, args);
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

//------------------------------------------------------------------------------
// Printing summary header
//------------------------------------------------------------------------------

void
print_summary_header (VectLocationType& src,
                      VectLocationType& dst)
{
  XrdOucString xsrc[MAXSRCDST];
  XrdOucString xdst[MAXSRCDST];

  for (unsigned int i = 0; i < src.size(); i++)
  {
    xsrc[i] = src[i].first.c_str();
    xsrc[i] += src[i].second.c_str();
    xsrc[i].erase(xsrc[i].rfind('?'));
  }

  for (unsigned int i = 0; i < dst.size(); i++)
  {
    xdst[i] = dst[i].first.c_str();
    xdst[i] += dst[i].second.c_str();
    xdst[i].erase(xdst[i].rfind('?'));
  }

  time_t rawtime;
  struct tm* timeinfo;
  time(&rawtime);
  timeinfo = localtime(&rawtime);

  COUT(("[eoscp] #################################################################\n"));
  COUT(("[eoscp] # Date                     : ( %lu ) %s", (unsigned long) rawtime, asctime(timeinfo)));
  COUT(("[eoscp} # auth forced=%s krb5=%s gsi=%s\n", getenv("XrdSecPROTOCOL") ? (getenv("XrdSecPROTOCOL")) : "<none>", getenv("KRB5CCNAME") ? getenv("KRB5CCNAME") : "<none>", getenv("X509_USER_PROXY") ? getenv("X509_USER_PROXY") : "<none>"));
  for (unsigned int i = 0; i < src.size(); i++)
    COUT(("[eoscp] # Source Name [%02d]         : %s\n", i, xsrc[i].c_str()));

  for (unsigned int i = 0; i < dst.size(); i++)
    COUT(("[eoscp] # Destination Name [%02d]    : %s\n", i, xdst[i].c_str()));
}


//------------------------------------------------------------------------------
// Print summary
//------------------------------------------------------------------------------

void
print_summary (VectLocationType& src,
               VectLocationType& dst,
               unsigned long long bytesread)
{
  gettimeofday(&abs_stop_time, &tz);
  float abs_time = ((float) ((abs_stop_time.tv_sec - abs_start_time.tv_sec) * 1000 +
                             (abs_stop_time.tv_usec - abs_start_time.tv_usec) / 1000));

  time_t rawtime;
  struct tm* timeinfo;
  time(&rawtime);
  timeinfo = localtime(&rawtime);

  XrdOucString xsrc[MAXSRCDST];
  XrdOucString xdst[MAXSRCDST];

  print_summary_header(src, dst);

  for (unsigned int i = 0; i < src.size(); i++)
  {
    xsrc[i] = src[i].first.c_str();
    xsrc[i] += src[i].second.c_str();
    xsrc[i].erase(xsrc[i].rfind('?'));

    if (xsrc[i].find("//replicate:") != STR_NPOS)
    {
      // disable client redirection eoscp
      XrdCl::DefaultEnv::GetEnv()->PutInt("RedirectLimit", 1);
    }
  }

  for (unsigned int i = 0; i < dst.size(); i++)
  {
    xdst[i] = dst[i].first.c_str();
    xdst[i] += dst[i].second.c_str();
    xdst[i].erase(xdst[i].rfind('?'));
    if (xsrc[i].find("//replicate:") != STR_NPOS)
    {
      // disable client redirection eoscp
      XrdCl::DefaultEnv::GetEnv()->PutInt("RedirectLimit", 1);
    }
  }

  COUT(("[eoscp] # Data Copied [bytes]      : %lld\n", bytesread));

  if (ndst > 1)
  {
    COUT(("[eoscp] # Tot. Data Copied [bytes] : %lld\n", bytesread * ndst));
  }

  COUT(("[eoscp] # Realtime [s]             : %f\n", abs_time / 1000.0));

  if (abs_time > 0)
  {
    COUT(("[eoscp] # Eff.Copy. Rate[MB/s]     : %f\n", bytesread / abs_time / 1000.0));
  }

  if (bandwidth)
  {
    COUT(("[eoscp] # Bandwidth[MB/s]          : %d\n", (int) bandwidth));
  }

  if (computeXS)
  {
    COUT(("[eoscp] # Checksum Type %s        : ", xsString.c_str()));
    COUT(("%s", xsObj->GetHexChecksum()));
    COUT(("\n"));
  }

  COUT(("[eoscp] # Write Start Position     : %lld\n", startwritebyte));
  COUT(("[eoscp] # Write Stop  Position     : %lld\n", stopwritebyte));

  if (startbyte >= 0)
  {
    COUT(("[eoscp] # Read  Start Position     : %lld\n", startbyte));
    COUT(("[eoscp] # Read  Stop  Position     : %lld\n", stopbyte));
  }
}


//------------------------------------------------------------------------------
// Printing progress bar
//------------------------------------------------------------------------------

void
print_progbar (unsigned long long bytesread, unsigned long long size)
{
  CERR(("[eoscp] %-24s Total %.02f MB\t|", cpname.c_str(), (float) size / 1024 / 1024));

  for (int l = 0; l < 20; l++)
  {
    if (l < ((int) (20.0 * bytesread / size)))
      CERR(("="));

    if (l == ((int) (20.0 * bytesread / size)))
      CERR((">"));

    if (l > ((int) (20.0 * bytesread / size)))
      CERR(("."));
  }

  float abs_time = ((float) ((abs_stop_time.tv_sec - abs_start_time.tv_sec) * 1000 +
                             (abs_stop_time.tv_usec - abs_start_time.tv_usec) / 1000));
  CERR(("| %.02f %% [%.01f MB/s]\r", 100.0 * bytesread / size, bytesread / abs_time / 1000.0));
}


//------------------------------------------------------------------------------
// Write progress
//------------------------------------------------------------------------------

void
write_progress (unsigned long long bytesread, unsigned long long size)
{
  static double lastprogress = 0;
  double progress = 100 * bytesread / (size ? size : 1);
  if (progress > 100)progress = 100;
  if ((fabs(progress - lastprogress) <= 1.0) && (progress != 100.))
  {
    // skip this update
    return;
  }
  std::string pf = progressFile;
  pf += ".tmp";
  FILE* fd = fopen(pf.c_str(), "w+");
  if (fd)
  {
    fprintf(fd, "%.02f %llu %llu\n", progress, bytesread, size);
    fclose(fd);
    if (rename(pf.c_str(), progressFile.c_str()))
    {
      fprintf(stderr, "error: renaming of progress file failed (%s=>%s)\n", pf.c_str(), progressFile.c_str());
    }
  }
}


//------------------------------------------------------------------------------
// Abort handler
//------------------------------------------------------------------------------

void
abort_handler (int)
{
  print_summary_header(src_location, dst_location);
  fprintf(stdout, "error: [eoscp] has been aborted\n");
  exit(-1);
}



//------------------------------------------------------------------------------
// Main function
//------------------------------------------------------------------------------

int
main (int argc, char* argv[])
{
  int c;
  mode_t dest_mode[MAXSRCDST];
  int set_mode = 0;
  extern char* optarg;
  extern int optind;

  while ((c = getopt(argc, argv, "nshdvlipfce:P:X:b:m:u:g:t:S:D:5ar:N:L:RT:O:")) != -1)
  {
    switch (c)
    {
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

    case 'c':
      doStoreRecovery = true;
      offsetXrd = -1;
      break;

    case 'f':
      doStoreRecovery = true;
      break;

    case 'e':
      replicationType = optarg;

      if ((replicationType != "raidDP") && (replicationType != "reedS"))
      {
        fprintf(stderr, "error: no such RAID layout\n");
        exit(-1);
      }

      isRaidTransfer = true;
      break;

    case 'X':
    {
      xsString = optarg;

      if (find(xsTypeSet.begin(), xsTypeSet.end(), xsString) == xsTypeSet.end())
      {
        fprintf(stderr, "error: no such checksum type: %s\n", optarg);
        exit(-1);
      }

      int layout = 0;
      unsigned long layoutId = 0;

      if (xsString == "adler")
      {
        layoutId = LayoutId::GetId(layout, LayoutId::kAdler);
      }
      else if (xsString == "crc32")
      {
        layoutId = LayoutId::GetId(layout, LayoutId::kCRC32);
      }
      else if (xsString == "md5")
      {
        layoutId = LayoutId::GetId(layout, LayoutId::kMD5);
      }
      else if (xsString == "sha1")
      {
        layoutId = LayoutId::GetId(layout, LayoutId::kSHA1);
      }
      else if (xsString == "crc32c")
      {
        layoutId = LayoutId::GetId(layout, LayoutId::kCRC32C);
      }

      xsObj = eos::fst::ChecksumPlugins::GetChecksumObject(layoutId);
      if (xsObj)
      {
        xsObj->Reset();
        computeXS = true;
      }
      break;
    }
    case 'P':
      nparitystripes = atoi(optarg);
      if (nparitystripes < 2)
      {
        fprintf(stderr, "error: number of parity stripes >= 2\n");
        exit(-1);
      }
      break;
    case 'O':
      progressFile = optarg;
      break;
    case 'u':
      euid = atoi(optarg);
      char tuid[128];
      sprintf(tuid, "%d", euid);
      if (strcmp(tuid, optarg))
      {
        // this is not a number, try to map it with getpwnam
        struct passwd* pwinfo = getpwnam(optarg);
        if (pwinfo)
        {
          euid = pwinfo->pw_uid;
          if (debug)
          {
            fprintf(stdout, "[eoscp]: mapping user  %s=>UID:%d\n", optarg, euid);
          }
        }
        else
        {
          fprintf(stderr, "error: cannot map user %s to any unix id!\n", optarg);
          exit(-ENOENT);
        }
      }
      break;
    case 'g':
      egid = atoi(optarg);
      char tgid[128];
      sprintf(tgid, "%d", egid);
      if (strcmp(tgid,optarg)) 
      {
        // this is not a number, try to map it with getgrnam
        struct group* grinfo = getgrnam(optarg);
        if (grinfo) 
          {
          egid = grinfo->gr_gid;
          if (debug) 
          {
            fprintf(stdout,"[eoscp]: mapping group %s=>GID:%d\n", optarg, egid);
          }
        } 
        else 
        {
          fprintf(stderr,"error: cannot map group %s to any unix id!\n", optarg);
          exit(-ENOENT);
        }
      }
      break;
    case 't':
      bandwidth = atoi(optarg);
      if ((bandwidth < 1) || (bandwidth > 2000))
      {
        fprintf(stderr, "error: bandwidth can only be 1 <= bandwidth <= 2000 Mb/s\n");
        exit(-1);
      }
      break;
    case 'S':
      nsrc = atoi(optarg);
      if ((nsrc < 1) || (nsrc > MAXSRCDST))
      {
        fprintf(stderr, "error: # of sources must be 1 <= # <= %d\n", MAXSRCDST);
        exit(-1);
      }
      break;
    case 'D':
      ndst = atoi(optarg);
      if ((ndst < 1) || (ndst > MAXSRCDST))
      {
        fprintf(stderr, "error: # of sources must be 1 <= # <= %d\n", MAXSRCDST);
        exit(-1);
      }
      break;
    case 'N':
      cpname = optarg;
      break;
    case 'b':
      buffersize = atoi(optarg);
      if ((buffersize < 4096) || (buffersize > 100 * 1024 * 1024))
      {
        fprintf(stderr, "error: buffer size can only 4k <= size <= 100 M\n");
        exit(-1);
      }
      break;
    case 'T':
      targetsize = strtoull(optarg, 0, 10);
      break;
    case 'm':
      for (int i = 0; i < MAXSRCDST; i++)
      {
        dest_mode[i] = strtol(optarg, 0, 8);
      }
      set_mode = 1;
      break;
    case 'r':
      char* colon;
      colon = strchr(optarg, ':');
      if (colon < 0)
      {
        fprintf(stderr, "error: range has to be given in the format <startbyte>:<stopbyte> e.g. 0:100000\n");
        exit(-1);
      }
      *colon = 0;
      startbyte = strtoll(optarg, 0, 0);
      stopbyte = strtoll(colon + 1, 0, 0);
      if (debug)
      {
        fprintf(stdout, "[eoscp]: reading range start=%lld stop=%lld\n", startbyte, stopbyte);
      }
      break;
    case 'L':
      sprintf(symlinkname, "%s", optarg);
      dosymlink = 1;
      break;
    case 'R':
      replicamode = 1;
      break;
    case 'Y':
      isStreamFile = true;
      break;
    case 'h':
    default:
      usage();
      ;
    }
  }

  if (optind - 1 + nsrc + ndst >= argc)
  {
    usage();
  }

  //............................................................................
  // Allocate the buffer used for copy
  //............................................................................
  buffer = new char[2 * buffersize];

  if ((!buffer))
  {
    fprintf(stderr, "error: cannot allocate buffer of size %d\n", 2 * buffersize);
    exit(-ENOMEM);
  }

  if (debug)
  {
    fprintf(stderr, "[eoscp]: allocate copy buffer with %d bytes\n", 2 * buffersize);
  }


  //.............................................................................
  // Get the address and the file path from the input
  //.............................................................................
  std::string location;
  std::string address;
  std::string file_path;

  for (int i = 0; i < nsrc; i++)
  {
    location = argv[optind + i];
    size_t pos = location.find("://");
    pos = location.find("//",pos+3);
    if (pos == std::string::npos)
    {
      address = "";
      file_path = location;
    }
    else
    {
      address = std::string(location, 0, pos + 1);
      file_path = std::string(location, pos + 1);
    }

    src_location.push_back(std::make_pair(address, file_path));

    if (verbose || debug)
    {
      fprintf(stdout, "src<%d>=%s ", i, location.c_str());
    }
  }

  for (int i = 0; i < ndst; i++)
  {
    location = argv[optind + nsrc + i];
    size_t pos = location.find("://");
    pos = location.find("//",pos+3);

    if (pos == std::string::npos)
    {
      address = "";
      file_path = location;
    }
    else
    {
      address = std::string(location, 0, pos + 1);
      file_path = std::string(location, pos + 1);
    }

    dst_location.push_back(std::make_pair(address, file_path));

    if (verbose || debug)
    {
      fprintf(stdout, "dst<%d>=%s ", i, location.c_str());
    }
  }

  if (verbose || debug)
  {
    fprintf(stdout, "\n");
  }


  //.............................................................................
  // Get the type of access we will be doing
  //.............................................................................
  if (isRaidTransfer)
  {
    if (!nparitystripes)
    {
      fprintf(stderr, "error: number of parity stripes undefined\n");
      exit(-EINVAL);
    }

    if (nsrc > ndst)
    {
      isSrcRaid = true;
    }
    else
    {
      isSrcRaid = false;
    }
  }

  //.............................................................................
  // Get sources access type
  //.............................................................................
  for (int i = 0; i < nsrc; i++)
  {
    if (src_location[i].first.find("root://") != std::string::npos)
    {
      if (isRaidTransfer && isSrcRaid)
      {
        src_type.push_back(RAID_ACCESS);
      }
      else
      {
        //.......................................................................
        // Test if we can do parallel IO access
        //.......................................................................
        XrdCl::Buffer arg;
        XrdCl::Buffer* response = 0;
        XrdCl::XRootDStatus status;
        file_path = src_location[i].first + src_location[i].second;
        size_t spos = file_path.rfind("//");
        std::string address = file_path.substr(0, spos + 1);
        XrdCl::URL url(address);

        if (!url.IsValid())
        {
          fprintf(stderr, "URL is invalid: %s", address.c_str());
          exit(-1);
        }

        XrdCl::FileSystem fs(url);

        if (spos != std::string::npos)
        {
          file_path.erase(0, spos + 1);
        }

        std::string request = file_path;
        request += "?mgm.pcmd=open";
        arg.FromString(request);
        status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg, response);

        if (status.IsOK())
        {
          //.....................................................................
          // Parse output
          //.....................................................................
          if (verbose || debug)
          {
            fprintf(stderr, "Doing PIO_ACCESS for source location %i.\n", i);
          }

          XrdOucString tag;
          XrdOucString stripe_path;
          XrdOucString origResponse = response->GetBuffer();
          XrdOucString stringOpaque = response->GetBuffer();

          while (stringOpaque.replace("?", "&"))
          {
          }
          while (stringOpaque.replace("&&", "&"))
          {
          }

          XrdOucEnv* openOpaque = new XrdOucEnv(stringOpaque.c_str());
          char* opaque_info = (char*) strstr(origResponse.c_str(), "&&mgm.logid");
          opaqueInfo = opaque_info;

          //...................................................................
          // Now that parallel IO is possible, we add the new stripes to the
          // src_location vector, we update the number of source files and then
          // we can use the RAID-like access mode where the stripe files are
          // given as input to the command line
          //...................................................................
          if (opaque_info)
          {
            opaque_info += 2;
            LayoutId::layoutid_t layout = openOpaque->GetInt("mgm.lid");
            std::string orig_file = file_path;
            nsrc = eos::common::LayoutId::GetStripeNumber(layout) + 1;
            isRaidTransfer = true;
            isSrcRaid = true;
            if (eos::common::LayoutId::GetLayoutType(layout) == eos::common::LayoutId::kRaidDP)
            {
              src_location.clear();
              replicationType = "raidDP";
            }
            else
              if (eos::common::LayoutId::GetLayoutType(layout) == eos::common::LayoutId::kArchive)
            {
              src_location.clear();
              replicationType = "reedS";
            }
            else
            {
              nsrc = 1;
              src_type.push_back(XRD_ACCESS);
              replicationType = "replica";
            }

            if (replicationType != "replica")
            {
              for (int i = 0; i < nsrc; i++)
              {
                tag = "pio.";
                tag += i;
                stripe_path = "root://";
                stripe_path += openOpaque->Get(tag.c_str());
                stripe_path += "/";
                stripe_path += orig_file.c_str();
                int pos = stripe_path.rfind("//");

                if (pos == STR_NPOS)
                {
                  address = "";
                  file_path = stripe_path.c_str();
                }
                else
                {
                  address = std::string(stripe_path.c_str(), 0, pos + 1);
                  file_path = std::string(stripe_path.c_str(), pos + 1, stripe_path.length() - pos - 1);
                }

                src_location.push_back(std::make_pair(address, file_path));
                src_type.push_back(RAID_ACCESS);

                if (verbose || debug)
                {
                  fprintf(stdout, "src<%d>=%s \n", i, src_location.back().second.c_str());
                }
              }
            }
          }
          else
          {
            fprintf(stderr, "Error while parsing the opaque information from PIO request.\n");
            exit(-1);
          }

          delete openOpaque;
          break;
        }
        else
        {
          //.....................................................................
          // The file is not suitable for PIO access, do normal XRD access
          //.....................................................................
          src_type.push_back(XRD_ACCESS);
        }

        delete response;
      }
    }
    else if (src_location[i].second == "-")
    {
      src_type.push_back(CONSOLE_ACCESS);

      if (i > 0)
      {
        fprintf(stderr, "error: you cannot read with several sources from stdin\n");
        exit(-EPERM);
      }
    }
    else
    {
      src_type.push_back(LOCAL_ACCESS);
    }
    fprintf(stderr, "\n");
  }

  //............................................................................
  // Get destinations access type
  //............................................................................
  for (int i = 0; i < ndst; i++)
  {
    if (dst_location[i].first.find("root://") != std::string::npos)
    {
      if (isRaidTransfer && !isSrcRaid)
      {
        dst_type.push_back(RAID_ACCESS);
      }
      else
      {
        //.......................................................................
        // Here we rely on the fact that all destinations must be of the same type
        //.......................................................................
        dst_type.push_back(XRD_ACCESS);
        meta_handler.push_back(new eos::fst::AsyncMetaHandler());
      }
    }
    else if (dst_location[i].second == "-")
    {
      dst_type.push_back(CONSOLE_ACCESS);
    }
    else
    {
      dst_type.push_back(LOCAL_ACCESS);
    }

    //..........................................................................
    // Print the types of protocols involved
    //..........................................................................
    if (verbose || debug)
    {
      fprintf(stdout, "[eoscp]: copy protocol ");

      for (int j = 0; j < nsrc; j++)
      {
        fprintf(stdout, "%s:", protocols[src_type[j]]);
      }

      fprintf(stdout, "=>");

      for (int j = 0; j < ndst; j++)
      {
        fprintf(stdout, "%s:", protocols[dst_type[j]]);
      }

      fprintf(stdout, "\n");
    }
  }

  int stat_failed = 0;
  struct stat st[MAXSRCDST];

  if (egid >= 0)
  {
    if (setgid(egid))
    {
      fprintf(stdout, "error: cannot change identity to gid %d\n", egid);
      exit(-EPERM);
    }
  }

  if (euid >= 0)
  {
    if (setuid(euid))
    {
      fprintf(stdout, "error: cannot change identity to uid %d\n", euid);
      exit(-EPERM);
    }
  }

  //............................................................................
  // Start the performance measurement
  //............................................................................
  gettimeofday(&abs_start_time, &tz);

  if (!replicamode)
  {
    for (int i = 0; i < nsrc; i++)
    {
      // stat the source
      switch (src_type[i])
      {
      case LOCAL_ACCESS:
      {
        if (debug)
        {
          fprintf(stdout, "[eoscp]: doing POSIX stat on %s\n",
                  src_location[i].second.c_str());
        }

        stat_failed = lstat(src_location[i].second.c_str(), &st[i]);
      }
        break;

      case RAID_ACCESS:
        for (int j = 0; j < nsrc; j++)
        {
          st[j].st_size = 0;
          st[j].st_mode = S_IFREG | S_IRUSR | S_IWUSR | S_IRGRP;
        }
        break;
      case XRD_ACCESS:
      {
        if (debug)
        {
          fprintf(stdout, "[eoscp]: doing XROOT/RAIDIO stat on %s\n",
                  src_location[i].second.c_str());
        }

        XrdCl::URL url(src_location[i].first);

        if (!url.IsValid())
        {
          fprintf(stderr, "error: the url address is not valid\n");
          exit(-EPERM);
        }

        XrdCl::FileSystem fs(url);
        XrdCl::StatInfo* response = 0;
        status = fs.Stat(src_location[i].second, response);

        if (!status.IsOK())
        {
          stat_failed = 1;
        }
        else
        {
          stat_failed = 0;
          st[i].st_size = response->GetSize();
          st[i].st_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
          if (response->TestFlags(XrdCl::StatInfo::IsWritable))
          {
            st[i].st_mode |= S_IWGRP;
          }
        }

        delete response;
      }
        break;

      case CONSOLE_ACCESS:
        stat_failed = 0;
        break;
      }

      if (!isRaidTransfer && stat_failed)
      {
        fprintf(stderr, "error: cannot stat source %s\n", src_location[i].second.c_str());
        exit(-ENOENT);
      }
    }
  }

  //............................................................................
  // Start consistency check
  //............................................................................
  if ((!isRaidTransfer) && (nsrc > 1))
  {
    for (int i = 0; i < nsrc; i++)
    {
      for (int j = 0; j < nsrc; j++)
      {
        if (st[i].st_size != st[j].st_size)
        {
          fprintf(stderr, "error: source files differ in size !\n");
          exit(-EINVAL);
        }
      }
    }
  }

  //............................................................................
  // Check if this is a range link
  //............................................................................
  if (!replicamode)
  {
    for (int i = 0; i < nsrc; i++)
    {
      if (S_ISLNK(st[i].st_mode))
      {
        int readlink_size = 0;
        char* readlinkbuff = (char*) malloc(4096);

        if (!readlinkbuff)
        {
          fprintf(stderr, "error: cannot allocate link buffer\n");
          exit(-ENOMEM);
        }

        readlinkbuff[0] = 0;

        switch (src_type[i])
        {
        case LOCAL_ACCESS:
          if (debug)
          {
            fprintf(stdout, "[eoscp]: doing POSIX readlink on %s\n", src_location[i].second.c_str());
          }

          readlink_size = readlink(src_location[i].second.c_str(), readlinkbuff, 4096);
          break;

        case RAID_ACCESS:
        case XRD_ACCESS:
          if (debug)
          {
            fprintf(stdout, "[eoscp]: doing XROOT readlink on %s\n",
                    src_location[i].second.c_str());
          }

          //....................................................................
          // Not implemented in xrootd posix
          //....................................................................
          readlink_size = 1;

        case CONSOLE_ACCESS:
          readlink_size = 0;
          break;
        }

        if (readlink_size < 0)
        {
          fprintf(stderr, "error: cannot read the link of %s\n", src_location[i].second.c_str());
          exit(-errno);
        }

        char* space = strchr(readlinkbuff, ' ');

        if (space)
        {
          *space = 0;
          char* colon = strchr(space + 1, ':');

          if (colon)
          {
            *colon = 0;
            // yep, this is a range link
            startbyte = strtoll(space + 1, 0, 0);
            stopbyte = strtoll(colon + 1, 0, 0);
            src_location[i] = std::make_pair("", readlinkbuff);

            if (debug)
            {
              fprintf(stdout, "[eoscp]: setting range to destination %s %lld:%lld\n",
                      src_location[i].second.c_str(), startbyte, stopbyte);
            }
          }
        }
      }
    }
  }


  //............................................................................
  // If transparent staging is not enabled, we need to check if files are online
  //............................................................................
  if (!transparentstaging)
  {
    for (int i = 0; i < nsrc; i++)
    {
      switch (src_type[i])
      {
      case LOCAL_ACCESS:
        if (debug)
        {
          fprintf(stdout, "[eoscp]: POSIX is transparent for staging - nothing to check\n");
        }
        break;

      case RAID_ACCESS:
        if (debug)
        {
          fprintf(stdout, "[eoscp]: XROOT(RAIDIO) is transparent for staging - nothing to check\n");
        }
        break;

      case XRD_ACCESS:
        if (debug)
        {
          fprintf(stdout, "[eoscp]: XROOT is transparent for staging - nothing to check\n");
        }
        break;

      case CONSOLE_ACCESS:
        if (debug)
        {
          fprintf(stdout, "[eoscp]: STDIN is transparent for staging - nothing to check\n");
        }
        break;
      }
    }
  }

  //............................................................................
  // For the '-p' flag we create the needed destination directory tree
  //............................................................................
  if ((!replicamode) && createdir)
  {
    struct stat dstst[MAXSRCDST];
    mode_t mode = 0;

    //..........................................................................
    // Loop over the destination paths
    //..........................................................................
    for (int i = 0; i < ndst; i++)
    {
      int pos = 0;
      int mkdir_failed = 0;
      int chown_failed = 0;
      XrdOucString file_path = dst_location[i].second.c_str();
      XrdOucString opaque = dst_location[i].second.c_str();
      int npos;
      if ( (npos = opaque.find("?")) != STR_NPOS ) 
      {
        opaque.erase(0,npos);
      }
      while ((pos = file_path.find("/", pos + 1)) != STR_NPOS)
      {
        XrdOucString subpath = file_path;
        subpath.erase(pos + 1);

        switch (dst_type[i])
        {
        case LOCAL_ACCESS:
        {
          if (debug)
          {
            fprintf(stdout, "[eoscp]: doing POSIX stat on %s\n", subpath.c_str());
          }

          stat_failed = stat(const_cast<char*> (subpath.c_str()), &dstst[i]);

          if (stat_failed)
          {
            if (debug)
            {
              fprintf(stdout, "[eoscp]: doing POSIX mkdir on %s\n", subpath.c_str());
            }

            mkdir_failed = mkdir(const_cast<char*> (subpath.c_str()), mode);

            //..................................................................
            // The root user can also set the user/group as in the source location
            //..................................................................
            if (getuid() == 0)
            {
              chown_failed = chown(const_cast<char*> (subpath.c_str()), st[0].st_uid, st[0].st_gid);
            }
          }
        }
          break;

        case RAID_ACCESS:
        case XRD_ACCESS:
        {
          if (debug)
          {
            fprintf(stdout, "[eoscp]: doing XROOT(RAIDIO) stat on %s\n", subpath.c_str());
          }
          subpath+= opaque.c_str();
          XrdCl::URL url(dst_location[i].first.c_str());
          XrdCl::FileSystem fs(url);
          XrdCl::StatInfo* response = 0;
          status = fs.Stat(subpath.c_str(), response);

          if (!status.IsOK())
          {
            stat_failed = 1;

            if (debug)
            {
              fprintf(stdout, "[eoscp]: doing XROOT mkdir on %s\n", subpath.c_str());
            }

            status = fs.MkDir(subpath.c_str(), XrdCl::MkDirFlags::MakePath, (XrdCl::Access::Mode)mode);

            if (!status.IsOK())
            {
              mkdir_failed = 1;
            }
          }

          delete response;

          //................................................................
          // Chown not supported by the standard xroot
          //................................................................
        }
          break;

        case CONSOLE_ACCESS:
          break;
        }

        if (mkdir_failed)
        {
          fprintf(stderr, "error: cannot create destination sub-directory %s\n", subpath.c_str());
          exit(-EPERM);
        }

        if (chown_failed)
        {
          fprintf(stderr, "error: cannot set owner=%d/group=%d for %s\n",
                  st[i].st_uid, st[i].st_gid, subpath.c_str());
          exit(-EPERM);
        }
      }
    }
  }

  //............................................................................
  // Open source files
  //............................................................................
  for (int i = 0; i < nsrc; i++)
  {
    switch (src_type[i])
    {
    case LOCAL_ACCESS:
    {
      if (debug)
      {
        fprintf(stdout, "[eoscp]: doing POSIX open to read  %s\n", src_location[i].second.c_str());
      }

      src_handler.push_back(std::make_pair(open(src_location[i].second.c_str(), O_RDONLY),
                                           static_cast<XrdCl::File*> (NULL)));
    }
      break;

    case RAID_ACCESS:
    {
      if (isSrcRaid)
      {
        int flags;
        mode_t mode_sfs = 0;
        std::vector<std::string> vectUrl;

        if (doStoreRecovery) flags = O_RDWR;
        else flags = O_RDONLY;

        for (int i = 0; i < nsrc; i++)
        {
          location = src_location[i].first + src_location[i].second;
          vectUrl.push_back(location);
        }

        LayoutId::layoutid_t layout = 0;

        if (replicationType == "raidDP")
        {
          layout = LayoutId::GetId(LayoutId::kRaidDP,
                                   1, nsrc,
                                   LayoutId::BlockSizeEnum(stripeWidth),
                                   LayoutId::OssXsBlockSize,
                                   0, nparitystripes);

          redundancyObj = new eos::fst::RaidDpLayout(NULL, layout, NULL, NULL,
                                                     eos::common::LayoutId::kXrdCl,
                                                     doStoreRecovery);
        }
        else if (replicationType == "reedS")
        {
          layout = LayoutId::GetId(LayoutId::kRaid6,
                                   1, nsrc,
                                   LayoutId::BlockSizeEnum(stripeWidth),
                                   LayoutId::OssXsBlockSize,
                                   0, nparitystripes);

          redundancyObj = new eos::fst::ReedSLayout(NULL, layout, NULL, NULL,
                                                    eos::common::LayoutId::kXrdCl,
                                                    doStoreRecovery);
        }

        if (debug)
        {
          fprintf(stdout, "[eoscp]: doing XROOT(RAIDIO) open with flags: %x\n", flags);
        }

        if (redundancyObj->OpenPio(vectUrl, flags, mode_sfs, opaqueInfo.c_str()))
        {
          fprintf(stderr, "error: can not open RAID object for read/write\n");
          exit(-EIO);
        }
      }
    }
      break;

    case XRD_ACCESS:
    {
      if (debug)
      {
        fprintf(stdout, "[eoscp]: doing XROOT open to read  %s\n",
                src_location[i].second.c_str());
      }

      location = src_location[i].first + src_location[i].second;
      XrdCl::File* file = new XrdCl::File();
      status = file->Open(location, XrdCl::OpenFlags::Read);
      if (!status.IsOK())
      {
        fprintf(stderr, "error: can not open XROOT object for read\n");
        exit(-EIO);
      }

      src_handler.push_back(std::make_pair(0, file));
    }
      break;

    case CONSOLE_ACCESS:
      src_handler.push_back(std::make_pair(fileno(stdin), static_cast<XrdCl::File*> (NULL)));
      break;
    }

    if ((!isRaidTransfer) &&
        (src_handler[i].first <= 0) &&
        (src_handler[i].second == NULL))
    {
      fprintf(stderr, "error: cannot open source file %s\n",
              src_location[i].second.c_str());
      exit(-ENOENT);
    }

    if (isRaidTransfer && isSrcRaid)
    {
      break;
    }
  }

  //............................................................................
  // Seek the required start position
  //............................................................................
  if (startbyte > 0)
  {
    for (int i = 0; i < nsrc; i++)
    {
      if (debug)
      {
        fprintf(stdout, "[eoscp]: seeking in %s to position %lld\n",
                src_location[i].second.c_str(), startbyte);
      }

      switch (src_type[i])
      {
      case LOCAL_ACCESS:
      {
        startbyte = lseek(src_handler[i].first, startbyte, SEEK_SET);
        offsetXS = startbyte;
      }
        break;

      case RAID_ACCESS:
      {
        offsetXrd = startbyte;
        offsetXS = startbyte;
      }
        break;

      case XRD_ACCESS:
      {
        //TODO::
        //startbyte = XrdPosixXrootd::Lseek( srcfd[i], startbyte, SEEK_SET );
        offsetXS = startbyte;
      }
        break;

      case CONSOLE_ACCESS:
        break;
      }

      if (startbyte < 0)
      {
        fprintf(stderr, "error: cannot seek start position of file %s %d\n",
                src_location[i].second.c_str(), errno);
        exit(-EIO);
      }
    }
  }

  //............................................................................
  // Open destination files
  //............................................................................
  for (int i = 0; i < ndst; i++)
  {
    switch (dst_type[i])
    {
    case LOCAL_ACCESS:
    {
      if (debug)
      {
        fprintf(stdout, "[eoscp]: doing POSIX open to write  %s\n",
                dst_location[i].second.c_str());
      }

      if (appendmode)
      {
        dst_handler.push_back(std::make_pair(open(dst_location[i].second.c_str(),
                                                  O_WRONLY | O_CREAT, st[i].st_mode),
                                             static_cast<XrdCl::File*> (NULL)));
      }
      else
      {
        dst_handler.push_back(std::make_pair(open(dst_location[i].second.c_str(),
                                                  O_WRONLY | O_TRUNC | O_CREAT, st[i].st_mode),
                                             static_cast<XrdCl::File*> (NULL)));
      }
    }
      break;

    case RAID_ACCESS:
    {
      if (!isSrcRaid)
      {
        int flags;
        std::vector<std::string> vectUrl;

        flags = SFS_O_CREAT | SFS_O_WRONLY;
        for (int i = 0; i < ndst; i++)
        {
          location = dst_location[i].first + dst_location[i].second;
          vectUrl.push_back(location);
        }

        LayoutId::layoutid_t layout = 0;

        if (replicationType == "raidDP")
        {
          layout = LayoutId::GetId(LayoutId::kRaidDP,
                                   1, ndst,
                                   LayoutId::BlockSizeEnum(stripeWidth),
                                   LayoutId::OssXsBlockSize,
                                   0, nparitystripes);

          redundancyObj = new eos::fst::RaidDpLayout(NULL, layout, NULL, NULL,
                                                     eos::common::LayoutId::kXrdCl,
                                                     doStoreRecovery,
                                                     isStreamFile);
        }
        else if (replicationType == "reedS")
        {
          layout = LayoutId::GetId(LayoutId::kRaid6,
                                   1, ndst,
                                   stripeWidth,
                                   LayoutId::OssXsBlockSize,
                                   0, nparitystripes);

          redundancyObj = new eos::fst::ReedSLayout(NULL, layout, NULL, NULL,
                                                    eos::common::LayoutId::kXrdCl,
                                                    doStoreRecovery,
                                                    isStreamFile);
        }

        if (debug)
        {
          fprintf(stdout, "[eoscp]: doing XROOT(RAIDIO) open with flags: %x\n", flags);
        }

        if (redundancyObj->OpenPio(vectUrl, flags))
        {
          fprintf(stderr, "error: can not open RAID object for write\n");
          exit(-EIO);
        }
      }
    }
      break;

    case XRD_ACCESS:
    {
      if (debug)
      {
        fprintf(stdout, "[eoscp]: doing XROOT open to write  %s\n",
                dst_location[i].second.c_str());
      }

      XrdCl::File* file = new XrdCl::File();
      location = dst_location[i].first + dst_location[i].second;

      if (appendmode)
      {
        XrdCl::URL url(dst_location[i].first);

        if (!url.IsValid())
        {
          fprintf(stderr, "error: the destination url address is not valid\n");
          exit(-EPERM);
        }

        XrdCl::FileSystem fs(url);
        XrdCl::StatInfo* response = 0;
        status = fs.Stat(dst_location[i].second, response);

        if (status.IsOK())
        {
          status = file->Open(location,
                              XrdCl::OpenFlags::Append,
                              (XrdCl::Access::Mode)st[i].st_mode);

        }
        else
        {
          status = file->Open(location,
                              XrdCl::OpenFlags::Delete | XrdCl::OpenFlags::Update,
                              (XrdCl::Access::Mode)st[i].st_mode);
        }

        delete response;
      }
      else
      {
        status = file->Open(location,
                            XrdCl::OpenFlags::Delete | XrdCl::OpenFlags::Update,
                            XrdCl::Access::UR | XrdCl::Access::UW);
      }

      if (!status.IsOK())
      {
        fprintf(stderr, "error: could not open the XROOT file, err=%s. \n",
                status.ToString().c_str());
        exit(-EPERM);
      }

      dst_handler.push_back(std::make_pair(0, file));
    }
      break;

    case CONSOLE_ACCESS:
      dst_handler.push_back(std::make_pair(fileno(stdout),
                                           static_cast<XrdCl::File*> (NULL)));
      break;
    }

    if ((!isRaidTransfer) &&
        (dst_handler[i].first <= 0) &&
        (dst_handler[i].second == NULL))
    {
      fprintf(stderr, "error: cannot open destination file %s\n",
              dst_location[i].second.c_str());
      exit(-EPERM);
    }

    if (isRaidTransfer && !isSrcRaid)
    {
      break;
    }
  }

  //............................................................................
  // In case the file exists, seek the end and print the offset
  //............................................................................
  if (appendmode)
  {
    for (int i = 0; i < ndst; i++)
    {
      switch (dst_type[i])
      {
      case LOCAL_ACCESS:
        startwritebyte = lseek(dst_handler[i].first, 0, SEEK_END);
        break;

      case RAID_ACCESS:
        // Not supported
        break;

      case XRD_ACCESS:
        //TODO::
        //startwritebyte = XrdPosixXrootd::Lseek( dstfd[i], ( long long )0, SEEK_END );
        break;

      case CONSOLE_ACCESS:
        // Not supported
        break;
      }

      if (startwritebyte < 0)
      {
        fprintf(stderr, "error: cannot seek to end of file to %d of %s\n",
                dest_mode[i], dst_location[i].second.c_str());
        exit(-EIO);
      }
    }
  }

  //............................................................................
  // Set the source mode or a specified one for the destination 
  //............................................................................
  for (int i = 0; i < ndst; i++)
  {
    int chmod_failed = 0;
    int chown_failed = 0;

    if (!set_mode)
    {
      //........................................................................
      // If not specified on the command line, take the source mode
      //........................................................................
      dest_mode[i] = st[0].st_mode;
    }

    switch (dst_type[i])
    {
    case LOCAL_ACCESS:
    {
      chmod_failed = chmod(dst_location[i].second.c_str(), dest_mode[i]);

      if (getuid() == 0)
      {
        chown_failed = chown(dst_location[i].second.c_str(), st[0].st_uid, st[0].st_gid);
      }
    }
      break;

    case RAID_ACCESS:
    case XRD_ACCESS:
    case CONSOLE_ACCESS:
      //........................................................................
      // Not supported, no such functionality in the standard xroot or console
      //........................................................................
      break;
    }

    if (chmod_failed)
    {
      fprintf(stderr, "error: cannot set permissions to %d for file %s\n",
              dest_mode[i], dst_location[i].second.c_str());
      exit(-EPERM);
    }

    if (chown_failed)
    {
      fprintf(stderr, "error: cannot set owner=%d/group=%d for %s\n",
              st[i].st_uid, st[i].st_gid, dst_location[i].second.c_str());
      exit(-EPERM);
    }
  }

  //............................................................................
  // Do the actual copy operation
  //............................................................................
  char* ptr_buffer = buffer;
  long long totalbytes = 0;
  double wait_time = 0;
  struct timespec start, end;

  stopwritebyte = startwritebyte;
  while (1)
  {
    if (progressFile.length())
    {
      write_progress(totalbytes, st[0].st_size);
    }

    if (progbar)
    {
      gettimeofday(&abs_stop_time, &tz);
      for (int i = 0; i < nsrc; i++)
      {
        if ((src_type[i] == XRD_ACCESS) && (!targetsize))
        {
          st[i].st_size = totalbytes;
        }
      }

      print_progbar(totalbytes, st[0].st_size);
    }

    if (bandwidth)
    {
      gettimeofday(&abs_stop_time, &tz);
      float abs_time = static_cast<float> ((abs_stop_time.tv_sec - abs_start_time.tv_sec) * 1000 +
                                           (abs_stop_time.tv_usec - abs_start_time.tv_usec) / 1000);

      //........................................................................
      // Regulate the io - sleep as desired
      //........................................................................
      float exp_time = totalbytes / bandwidth / 1000.0;

      if (abs_time < exp_time)
      {
        usleep((int) (1000 * (exp_time - abs_time)));
      }
    }

    //..........................................................................
    // For ranges we have to adjust the last buffersize
    //..........................................................................
    if ((stopbyte >= 0) &&
        (((stopbyte - startbyte) - totalbytes) < buffersize))
    {
      buffersize = (stopbyte - startbyte) - totalbytes;
    }

    int nread = -1;

    switch (src_type[0])
    {
    case LOCAL_ACCESS:
    case CONSOLE_ACCESS:
      nread = read(src_handler[0].first,
                   static_cast<void *> (ptr_buffer),
                   buffersize);
      break;

    case RAID_ACCESS:
    {
      nread = redundancyObj->Read(offsetXrd, ptr_buffer, buffersize);
      offsetXrd += nread;
    }
      break;

    case XRD_ACCESS:
    {
      eos::common::Timing::GetTimeSpec(start);
      uint32_t xnread = 0;
      status = src_handler[0].second->Read(offsetXrd, buffersize, ptr_buffer, xnread);
      nread = xnread;
      if (!status.IsOK())
      {
        fprintf(stderr, "Error while doing reading. \n");
        exit(-1);
      }

      eos::common::Timing::GetTimeSpec(end);
      wait_time = static_cast<double> ((end.tv_sec * 1000 + end.tv_nsec / 1000000)-
                                       (start.tv_sec * 1000 + start.tv_nsec / 1000000));
      read_wait += wait_time;
      offsetXrd += nread;
    }
      break;
    }

    if (nread < 0)
    {
      fprintf(stderr, "error: read failed on file %s - destination file "
              "is incomplete!\n", src_location[0].second.c_str());
      exit(-EIO);
    }

    if (nread == 0)
    {
      // end of file
      break;
    }

    if (computeXS)
    {
      xsObj->Add(static_cast<const char*> (ptr_buffer), nread, offsetXS);
      offsetXS += nread;
    }

    int nwrite = 0;

    for (int i = 0; i < ndst; i++)
    {
      switch (dst_type[i])
      {
      case LOCAL_ACCESS:
      case CONSOLE_ACCESS:
        nwrite = write(dst_handler[i].first, ptr_buffer, nread);
        break;

      case RAID_ACCESS:
      {
        if (i == 0)
        {
          nwrite = redundancyObj->Write(stopwritebyte, ptr_buffer, nread);
          i = ndst;
        }
      }
        break;

      case XRD_ACCESS:
      {
        //......................................................................
        // Do writes in async mode
        //......................................................................
        eos::common::Timing::GetTimeSpec(start);
        eos::fst::ChunkHandler* chunk_handler;
        chunk_handler = meta_handler[i]->Register(stopwritebyte, nread, NULL, true);
        status = dst_handler[i].second->Write(stopwritebyte,
                                              nread,
                                              ptr_buffer,
                                              chunk_handler);
        nwrite = nread;
        eos::common::Timing::GetTimeSpec(end);
        wait_time = static_cast<double> ((end.tv_sec * 1000 + end.tv_nsec / 1000000)-
                                         (start.tv_sec * 1000 + start.tv_nsec / 1000000));
        write_wait += wait_time;
      }
        break;
      }

      if (nwrite != nread)
      {
        fprintf(stderr, "error: write failed on destination file %s - "
                "wrote %lld/%lld bytes - destination file is incomplete!\n",
                dst_location[i].second.c_str(), (long long) nwrite, (long long) nread);
        exit(-EIO);
      }
    }

    totalbytes += nwrite;
    stopwritebyte += nwrite;
  } // end while(1)

  //.............................................................................
  // Wait for all async write requests before moving on
  //.............................................................................
  eos::common::Timing::GetTimeSpec(start);

  for (int i = 0; i < ndst; i++)
  {
    if (dst_type[i] == XRD_ACCESS)
    {
      if (!meta_handler[i]->WaitOK())
      {
        fprintf(stderr, "Error while doing the asyn writing.\n");
      }
      delete meta_handler[i];
    }
  }

  eos::common::Timing::GetTimeSpec(end);
  wait_time = static_cast<double> ((end.tv_sec * 1000 + end.tv_nsec / 1000000)-
                                   (start.tv_sec * 1000 + start.tv_nsec / 1000000));
  write_wait += wait_time;

  if (computeXS && xsObj)
  {
    xsObj->Finalize();
  }

  if (progbar)
  {
    gettimeofday(&abs_stop_time, &tz);

    for (int i = 0; i < nsrc; i++)
    {
      if (src_type[i] == XRD_ACCESS)
      {
        st[i].st_size = totalbytes;
      }
    }

    print_progbar(totalbytes, st[0].st_size);
    cout << endl;
  }

  if (summary)
  {
    print_summary(src_location, dst_location, totalbytes);
  }

  if (computeXS && xsObj)
  {
    delete xsObj;
  }

  //............................................................................
  // Close all files
  //............................................................................
  for (int i = 0; i < nsrc; i++)
  {
    switch (src_type[i])
    {
    case LOCAL_ACCESS:
      close(src_handler[i].first);
      break;

    case RAID_ACCESS:
      if (i == 0)
      {
        redundancyObj->Close();
        i = nsrc;
      }
      break;

    case XRD_ACCESS:
      status = src_handler[i].second->Close();
      delete src_handler[i].second;
      break;

    case CONSOLE_ACCESS:
      break;
    }
  }

  for (int i = 0; i < ndst; i++)
  {
    switch (dst_type[i])
    {
    case LOCAL_ACCESS:
      close(dst_handler[i].first);
      break;

    case RAID_ACCESS:
      if (i == 0)
      {
        redundancyObj->Close();
        i = ndst;
      }
      break;

    case XRD_ACCESS:
      status = dst_handler[i].second->Close();
      delete dst_handler[i].second;
      break;

    case CONSOLE_ACCESS:
      //........................................................................
      // Nothing to do
      //........................................................................
      break;
    }
  }

  if (redundancyObj)
  {
    delete redundancyObj;
  }

  if (dosymlink)
  {
    int symlink_failed = 0;
    char rangedestname[4096];

    if (appendmode)
    {
      sprintf(rangedestname, "%s %llu:%llu",
              dst_location[0].second.c_str(),
              static_cast<unsigned long long> (startwritebyte),
              static_cast<unsigned long long> (stopwritebyte));
    }
    else
    {
      sprintf(rangedestname, "%s", dst_location[0].second.c_str());
    }

    if (debug)
    {
      fprintf(stdout, "[eoscp]: creating symlink %s->%s\n", symlinkname, rangedestname);
    }

    switch (dst_type[0])
    {
    case LOCAL_ACCESS:
    {
      unlink(symlinkname);
      symlink_failed = symlink(rangedestname, symlinkname);
    }
      break;

    case RAID_ACCESS:
    case XRD_ACCESS:
    case CONSOLE_ACCESS:
      //........................................................................
      // Noting to do, xrootd has no symlink support in posix
      //........................................................................
      break;
    }

    if (symlink_failed)
    {
      fprintf(stderr, "error: cannot creat symlink from %s -> %s\n",
              symlinkname, rangedestname);
      exit(-ESPIPE);
    }
  }

  // fprintf(stderr, "Total read wait time is: %f miliseconds. \n", read_wait);
  // fprintf(stderr, "Total write wait time is: %f miliseconds. \n", write_wait);

  // Free memory
  delete[] buffer;

  return 0;
}
