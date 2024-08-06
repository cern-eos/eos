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

#include <set>
#include <string>
#include <algorithm>
#include <math.h>
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
#include <chrono>
#include <openssl/md5.h>
#include <optional>
#include <getopt.h>
#include <XrdCl/XrdClFile.hh>
#include <XrdCl/XrdClDefaultEnv.hh>
#include <XrdCl/XrdClPostMaster.hh>
#include <XrdOuc/XrdOucString.hh>
#include "common/XrdErrorMap.hh"
#include "common/Timing.hh"
#include "common/SymKeys.hh"
#include "common/StringSplit.hh"
#include "common/json/Jsonifiable.hh"
#include "common/json/JsonCppJsonifier.hh"
#include "fst/layout/RaidDpLayout.hh"
#include "fst/layout/ReedSLayout.hh"
#include "fst/io/AsyncMetaHandler.hh"
#include "fst/io/ChunkHandler.hh"
#include "fst/io/xrd/XrdIo.hh"
#include "fst/io/FileIo.hh"
#include "fst/io/FileIoPluginCommon.hh"
#include "fst/checksum/ChecksumPlugins.hh"

#define PROGRAM "eoscp"
#define DEFAULTBUFFERSIZE 4*1024*1024
#define MAXSRCDST    32

using eos::common::LayoutId;

typedef std::vector<std::pair<std::string, std::string> > VectLocationType;

enum AccessType {
  LOCAL_ACCESS, ///< local access
  RAID_ACCESS, ///< xroot protocol but with raid layout
  XRD_ACCESS, ///< xroot protocol
  RIO_ACCESS, ///< any File IO plug-in remote protocol
  CONSOLE_ACCESS ///< input/output to console
};

class XferSummary : public eos::common::Jsonifiable<XferSummary>
{
public:
  std::vector<std::string> sources;
  std::vector<std::string> destinations;
  time_t rawtime;
  std::string astime;
  std::optional<std::string> xrdsecprotocol;
  std::optional<std::string> krb5ccname;
  std::optional<std::string> x509userproxy;
  std::string src_clientinfo;
  std::string dst_clientinfo;
  uint64_t bytescopied;
  uint64_t totalbytescopied;
  float abs_time;
  float realtime;
  float copyrate;
  double ingress_rate;
  double egress_rate;
  double ingress_microseconds;
  double egress_microseconds;
  float bandwidth;
  std::optional<std::string> checksum_type;
  std::optional<std::string> checksum_value;
  off_t write_start;
  off_t write_stop;
  long long read_start;
  long long read_stop;
  int ndst;
  virtual ~XferSummary() = default;
};

class XferSummaryJson : public eos::common::JsonCppJsonifier<XferSummary>
{
public:
  virtual ~XferSummaryJson() = default;
  void jsonify(const XferSummary* obj, std::stringstream& oss) override
  {
    Json::Value root;
    root["unixtime"] = Json::UInt64(obj->rawtime);
    root["date"] = obj->astime;
    root["auth"] = obj->xrdsecprotocol ? Json::Value(*obj->xrdsecprotocol) :
                   Json::nullValue;
    root["krb5"] = obj->krb5ccname ? Json::Value(*obj->krb5ccname) :
                   Json::nullValue;
    root["x509userproxy"] = obj->x509userproxy ? Json::Value(
                              *obj->x509userproxy) : Json::nullValue;
    initializeArray(root["sources"]);

    for (size_t i = 0; i < obj->sources.size(); ++i) {
      root["sources"].append(obj->sources[i]);
    }

    for (size_t i = 0; i < obj->destinations.size(); ++i) {
      root["destinations"].append(obj->destinations[i]);
    }

    root["bytes_copied"] = Json::UInt64(obj->bytescopied);

    if (obj->ndst > 1) {
      root["totalbytes_copied"] = Json::UInt64(obj->totalbytescopied);
    }

    root["realtime"] = obj->realtime;
    root["copy_rate"] = obj->copyrate;
    root["ingress_rate"] = obj->ingress_rate;
    root["egress_rate"] = obj->egress_rate;
    root["ingress_server_info"] = !obj->src_clientinfo.empty() ? Json::Value(
                                    obj->src_clientinfo) : Json::nullValue;
    root["egress_server_info"] = !obj->dst_clientinfo.empty() ? Json::Value(
                                   obj->dst_clientinfo) : Json::nullValue;
    root["bandwidth"] = obj->bandwidth ? Json::Value(obj->bandwidth) :
                        Json::nullValue;
    root["checksum_type"] = obj->checksum_type ? Json::Value(
                              *obj->checksum_type) : Json::nullValue;
    root["checksum_value"] = obj->checksum_value ? Json::Value(
                               *obj->checksum_value) : Json::nullValue;
    root["write_start"] = Json::UInt64(obj->write_start);
    root["write_stop"] = Json::UInt64(obj->write_stop);
    root["read_start"] = obj->read_start >= 0 ? Json::UInt64(
                           obj->read_start) : Json::nullValue;
    root["read_stop"] = obj->read_start >= 0 ? Json::UInt64(
                          obj->read_stop) : Json::nullValue;
    oss << root;
  }
};

const char* protocols[] = {"file", "raid", "xroot", "rio", NULL};
const char* xs[] = {"adler", "md5", "sha1", "crc32", "crc32c"};
std::set<std::string> xsTypeSet(xs, xs + 5);

///! vector of source file descriptors or IO objects
std::vector<std::pair<int, void*> > src_handler;

///! vector of destination file descriptors or IO objects
std::vector<std::pair<int, void*> > dst_handler;

///! vector of source host address and path file
VectLocationType src_location;

///! vector of destination host address and path file
VectLocationType dst_location;

std::vector<AccessType> src_type; ///< vector of source type access
std::vector<AccessType> dst_type; ///< vector of destination type access

int verbose = 0;
int debug = 0;
int monitoring = 0;
int jsonoutput = 0;
int trylocal = 0;
int progbar = 1;
int summary = 1;
int nopio = 0;

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
int retc = 0;
uint32_t buffersize = DEFAULTBUFFERSIZE;

double read_wait = 0; ///< statistics about total read time
double write_wait = 0; ///< statistics about total write time
char* buffer = NULL; ///< used for doing the reading
bool first_time = true; ///< first time prefetch two blocks
bool nooverwrite = false; ///< buy default we overwrite the target files
int gtimeout = 0; ///< copy process timeout in seconds
int cksumcomparison =
  0; ///< performs a checksum comparison between the source and the destination, returns an error to the user in the case it happens
int cksummismatchdelete =
  0; ///< performs a deletion of the destination file if the checksum of the source and the destination mismatch

//..............................................................................
// RAID related variables
//..............................................................................
off_t stripeWidth = 1024 * 1024;
uint64_t offsetXrd = 0;
int nparitystripes = 0;

bool isRaidTransfer = false; ///< true if we currently handle a RAID transfer
bool isSrcRaid = false; ///< meaningful only for RAID transfers
bool isStreamFile = false; ///< the file is streamed
bool doStoreRecovery = false; ///< store recoveries if the file is corrupted
std::string opaqueInfo; ///< opaque info containing the capabilities
///< necessary to do a parallel IO open

std::string replicationType = "";
//TODO: deal with the case when both the source and the destination are RAIN files
eos::fst::RainMetaLayout* redundancyObj = NULL;

std::string dst_lasturl;
std::string src_lasturl;

//..............................................................................
// Checksum variables
//..............................................................................
off_t offsetXS = 0;
bool computeXS = false;
std::string xsString = "";
std::string xsValue = "";
std::unique_ptr<eos::fst::CheckSum> xsObj;


//..............................................................................
// To compute throughput etc
//..............................................................................
struct timeval abs_start_time;
struct timeval abs_stop_time;
struct timezone tz;
double ingress_microseconds = 0;
double egress_microseconds = 0;

std::string progressFile = "";

char* source[MAXSRCDST];
char* destination[MAXSRCDST];

//------------------------------------------------------------------------------
// Usage command
//------------------------------------------------------------------------------

void
usage()
{
  fprintf(stderr,
          "Usage: %s [--version] [-5] [-0] [-X <type>] [-t <mb/s>] [-h] [-x] [-v] [-V] [-d] [-l] [-j] [-b <size>] [-T <size>] [-Y] [-n] [-s] [-u <id>] [-g <id>] [-S <#>] [-D <#>] [-O <filename>] [-N <name>]<src1> [src2...] <dst1> [dst2...]\n",
          PROGRAM);
  fprintf(stderr, "       -h           : help\n");
  fprintf(stderr, "       --version    : eoscp software version\n");
  fprintf(stderr, "       -d           : debug mode\n");
  fprintf(stderr, "       -v           : verbose mode\n");
  fprintf(stderr, "       -V           : write summary as key value pairs\n");
  fprintf(stderr, "       -l           : try to force the destination to the "
          "local disk server [not supported]\n");
  fprintf(stderr, "       -a           : append to the file rather than truncate"
          " an existing file\n");
  fprintf(stderr, "       -A <offset>  : append/overwrite at offset\n");
  fprintf(stderr,
          "       -b <size>    : use <size> as buffer size for copy operations\n");
  fprintf(stderr,
          "       -T <size>    : use <size> as target size for copies from STDIN\n");
  fprintf(stderr,
          "       -m <mode>    : set the mode for the destination file\n");
  fprintf(stderr, "       -n           : hide progress bar\n");
  fprintf(stderr, "       -N           : set name for progress printout\n");
  fprintf(stderr, "       -s           : hide summary\n");
  fprintf(stderr,
          "       -j           : JSON output (flags -V -d -v -s are ignored)\n");
  fprintf(stderr,
          "       -u <uid|name>: use <uid> as UID to execute the operation -  (user)<name> is mapped to unix UID if possible\n");
  fprintf(stderr,
          "       -g <gid|name>: use <gid> as GID to execute the operation - (group)<name> is mapped to unix GID if possible\n");
  fprintf(stderr,
          "       -t <mb/s>    : reduce the traffic to an average of <mb/s> mb/s\n");
  fprintf(stderr, "       -S <#>       : read from <#> sources in 'parallel'\n");
  fprintf(stderr, "       -D <#>       : write to <#> sources in 'parallel'\n");
  fprintf(stderr, "       -q <s>               : quit copy after <s> seconds\n");
  fprintf(stderr,
          "       -O <file>    : write progress file to <file> (0.00 - 100.00%%)\n");
  fprintf(stderr, "       -i           : enable transparent staging\n");
  fprintf(stderr,
          "       -p           : create all needed subdirectories for destination paths\n");
  fprintf(stderr, "       <srcN>       : path/url or - for STDIN\n");
  fprintf(stderr, "       <dstN>       : path/url or - for STDOUT\n");
  fprintf(stderr, "       -5           : compute md5\n");
  fprintf(stderr,
          "       -r <start>:<stop> : read only the range from <start> bytes to <stop> bytes\n");
  fprintf(stderr,
          "       -L <linkname>: create a symbolic link to the 1st target file with name <linkname>\n");
  fprintf(stderr,
          "       -R           : replication mode - avoid dir creation and stat's\n");
  fprintf(stderr,
          "       -X           : checksum type: adler, crc32, crc32c, sha1, md5\n");
  fprintf(stderr,
          "       -e           : RAID layouts - error correction layout: raiddp/reeds\n");
  fprintf(stderr,
          "       -P           : RAID layouts - number of parity stripes\n");
  fprintf(stderr,
          "       -f           : RAID layouts - store the modifications in case of errors\n");
  fprintf(stderr,
          "       -c           : RAID layouts - force check and recover any corruptions in any stripe\n");
  fprintf(stderr, "       -Y           : RAID layouts - streaming file\n");
  fprintf(stderr,
          "       -0           : RAID layouts - don't use parallel IO mode\n");
  fprintf(stderr, "       -x           : don't overwrite an existing file\n");
  fprintf(stderr,
          "       -C           : fail if checksum comparison between source and destination fails (XRootD destination only)\n");
  fprintf(stderr,
          "       -E           : automatically delete the destination file if checksum comparison between source and destination fails (XRootD destination only) \n");
  exit(-1);
}

/**
 * Display the eoscp software information.
 * For now, only displays the EOS_CLIENT_VERSION and the EOS_CLIENT_RELEASE the same
 * way it is done by eos -v
 */
void displayInformation()
{
  std::stringstream infos;
  infos << "EOS " << VERSION << std::endl << std::endl;
  infos << "Developed by the CERN IT storage group" << std::endl;
  fprintf(stdout, "%s", infos.str().c_str());
  exit(0);
}

extern "C"
{
  //----------------------------------------------------------------------------
  // Function + macros to allow formatted print via cout,cerr
  //----------------------------------------------------------------------------

  void
  cout_print(const char* format, ...)
  {
    char cout_buff[4096];
    va_list args;
    va_start(args, format);
    vsprintf(cout_buff, format, args);
    va_end(args);
    std::cout << cout_buff;
  }

  void
  cerr_print(const char* format, ...)
  {
    char cerr_buff[4096];
    va_list args;
    va_start(args, format);
    vsprintf(cerr_buff, format, args);
    va_end(args);
    std::cerr << cerr_buff;
  }

#define COUT(s) do {                            \
    cout_print s;                               \
  } while (0)

#define CERR(s) do {                            \
    cerr_print s;                               \
  } while (0)

}

XferSummary createXferSummary(const VectLocationType& src,
                              const VectLocationType& dst, unsigned long long bytesread)
{
  XferSummary xferSummary;
  xferSummary.setJsonifier(std::make_shared<XferSummaryJson>());
  std::string src_clientinfo;
  std::string dst_clientinfo;

  if (src_lasturl.length()) {
    XrdCl::URL url(src_lasturl);
    XrdCl::URL::ParamsMap cgi = url.GetParams();
    std::string zclientinfo = cgi["eos.clientinfo"];
    eos::common::SymKey::ZDeBase64(zclientinfo, src_clientinfo);
    xferSummary.src_clientinfo = src_clientinfo;
  }

  if (dst_lasturl.length()) {
    XrdCl::URL url(dst_lasturl);
    XrdCl::URL::ParamsMap cgi = url.GetParams();
    std::string zclientinfo = cgi["eos.clientinfo"];
    eos::common::SymKey::ZDeBase64(zclientinfo, dst_clientinfo);
    xferSummary.dst_clientinfo = dst_clientinfo;
  }

  gettimeofday(&abs_stop_time, &tz);
  float abs_time = ((float)((abs_stop_time.tv_sec - abs_start_time.tv_sec) * 1000
                            +
                            (abs_stop_time.tv_usec - abs_start_time.tv_usec) / 1000));
  xferSummary.abs_time = abs_time;

  for (unsigned int i = 0; i < src.size(); i++) {
    xferSummary.sources.push_back("");
    auto& srcStr = xferSummary.sources.back();
    srcStr += src[i].first.c_str();
    srcStr += src[i].second.c_str();
    size_t pos = srcStr.rfind('?');

    if (pos != std::string::npos) {
      srcStr.erase(pos);
    }

    if (srcStr.find("//replicate:") != std::string::npos) {
      // disable client redirection eoscp
      XrdCl::DefaultEnv::GetEnv()->PutInt("RedirectLimit", 1);
    }
  }

  for (unsigned int i = 0; i < dst.size(); i++) {
    xferSummary.destinations.push_back("");
    auto& dstStr = xferSummary.destinations.back();
    dstStr += dst[i].first.c_str();
    dstStr += dst[i].second.c_str();
    size_t pos = dstStr.rfind('?');

    if (pos != std::string::npos) {
      dstStr.erase(pos);
    }

    if (dstStr.find("//replicate:") != std::string::npos) {
      // disable client redirection eoscp
      XrdCl::DefaultEnv::GetEnv()->PutInt("RedirectLimit", 1);
    }
  }

  time_t rawtime;
  struct tm* timeinfo;
  time(&rawtime);
  timeinfo = localtime(&rawtime);
  XrdOucString astime = asctime(timeinfo);
  astime.erase(astime.length() - 1);
  xferSummary.rawtime = rawtime;
  xferSummary.astime = astime.c_str();
  xferSummary.xrdsecprotocol = getenv("XrdSecPROTOCOL") ?
                               std::optional<std::string>(getenv("XrdSecPROTOCOL")) : std::nullopt;
  xferSummary.krb5ccname = getenv("KRB5CCNAME") ? std::optional<std::string>
                           (getenv("KRB5CCNAME")) : std::nullopt;
  xferSummary.x509userproxy = getenv("X509_USER_PROXY") ?
                              std::optional<std::string>(getenv("X509_USER_PROXY")) : std::nullopt;
  xferSummary.bytescopied = bytesread;
  xferSummary.totalbytescopied = bytesread * ndst;
  xferSummary.realtime = xferSummary.abs_time / 1000.0;
  xferSummary.copyrate = xferSummary.abs_time > 0 ? xferSummary.bytescopied /
                         xferSummary.abs_time / 1000.0 : 0;
  xferSummary.ingress_microseconds = ingress_microseconds;
  xferSummary.egress_microseconds = egress_microseconds;
  xferSummary.ingress_rate = xferSummary.ingress_microseconds  ? bytesread /
                             xferSummary.ingress_microseconds : 0;
  xferSummary.egress_rate = xferSummary. egress_microseconds  ? bytesread /
                            xferSummary.egress_microseconds : 0;
  xferSummary.bandwidth = bandwidth;

  if (computeXS) {
    xferSummary.checksum_type = xsString;
    xferSummary.checksum_value = xsValue;
  }

  xferSummary.write_start = startwritebyte;
  xferSummary.write_stop = stopwritebyte;
  xferSummary.read_start = startbyte;
  xferSummary.read_stop = stopbyte;
  xferSummary.ndst = ndst;
  return xferSummary;
}

//------------------------------------------------------------------------------
// Printing summary header
//------------------------------------------------------------------------------
void
print_summary_header(const XferSummary& xferSummary)
{
  if (!monitoring) {
    COUT(("[eoscp] #################################################################\n"));
    COUT(("[eoscp] # Date                     : ( %lu ) %s\n",
          (unsigned long) xferSummary.rawtime, xferSummary.astime.c_str()));
    COUT(("[eoscp] # auth forced=%s krb5=%s gsi=%s\n",
          xferSummary.xrdsecprotocol ? xferSummary.xrdsecprotocol->c_str() : "<none>",
          xferSummary.krb5ccname ? xferSummary.krb5ccname->c_str() : "<none>",
          xferSummary.x509userproxy ? xferSummary.x509userproxy->c_str() : "<none>"));

    for (unsigned int i = 0; i < xferSummary.sources.size(); i++) {
      COUT(("[eoscp] # Source Name [%02d]         : %s\n", i,
            xferSummary.sources[i].c_str()));
    }

    for (unsigned int i = 0; i < xferSummary.destinations.size(); i++) {
      COUT(("[eoscp] # Destination Name [%02d]    : %s\n", i,
            xferSummary.destinations[i].c_str()));
    }
  } else {
    COUT(("unixtime=%lu date=\"%s\" auth=\"%s\" ",
          (unsigned long) xferSummary.rawtime,
          xferSummary.astime.c_str(),
          xferSummary.xrdsecprotocol ? xferSummary.xrdsecprotocol->c_str() : "(null)"));

    for (unsigned int i = 0; i < xferSummary.sources.size(); i++) {
      COUT(("src_%d=%s ", i, xferSummary.sources[i].c_str()));
    }

    for (unsigned int i = 0; i < xferSummary.destinations.size(); i++) {
      COUT(("dst_%d=%s ", i, xferSummary.destinations[i].c_str()));
    }
  }
}


//------------------------------------------------------------------------------
// Print summary
//------------------------------------------------------------------------------

void
print_summary(const XferSummary& xferSummary)
{
  print_summary_header(xferSummary);

  if (!monitoring) {
    // This is a quick-and-dirty trick to keep the ':' after the checksum type label aligned with the rest
    // of the output (part 1)
    std::string key = "[eoscp] # Data Copied [bytes]      ";
    const size_t keyLen = key.size();
    key += ": %lld\n";
    COUT((key.c_str(), xferSummary.totalbytescopied));

    if (xferSummary.ndst > 1) {
      COUT(("[eoscp] # Tot. Data Copied [bytes] : %lld\n",
            xferSummary.totalbytescopied));
    }

    COUT(("[eoscp] # Realtime [s]             : %.03f\n", xferSummary.realtime));

    if (xferSummary.abs_time > 0) {
      COUT(("[eoscp] # Eff.Copy. Rate[MB/s]     : %.02f\n",
            xferSummary.copyrate));
    }

    if (xferSummary.ingress_microseconds) {
      COUT(("[eoscp] # INGRESS [MB/s]           : %.02f\n",
            xferSummary.ingress_rate));
    }

    if (xferSummary.egress_microseconds) {
      COUT(("[eoscp] # EGRESS [MB/s]            : %.02f\n",
            xferSummary.egress_rate));
    }

    if (xferSummary.bandwidth) {
      COUT(("[eoscp] # Bandwidth[MB/s]          : %d\n",
            (int) xferSummary.bandwidth));
    }

    if (xferSummary.checksum_type) {
      // This is a quick-and-dirty trick to keep the ':' after the checksum type label aligned with the rest
      // of the output (part 2)
      std::string cksumTypeTitle = "[eoscp] # Checksum Type " +
                                   *xferSummary.checksum_type;
      size_t paddingSize = int(keyLen - cksumTypeTitle.length()) > 0 ? keyLen -
                           cksumTypeTitle.length() : 0;

      if (paddingSize) {
        cksumTypeTitle += std::string(paddingSize, ' ');
      }

      COUT(((cksumTypeTitle + std::string(": ")).c_str()));
      COUT(("%s", xferSummary.checksum_value->c_str()));
      COUT(("\n"));
    }

    COUT(("[eoscp] # Write Start Position     : %lld\n", xferSummary.write_start));
    COUT(("[eoscp] # Write Stop  Position     : %lld\n", xferSummary.write_stop));

    if (xferSummary.read_start >= 0) {
      COUT(("[eoscp] # Read  Start Position     : %lld\n", xferSummary.read_start));
      COUT(("[eoscp] # Read  Stop  Position     : %lld\n", xferSummary.read_stop));
    }

    if (!xferSummary.src_clientinfo.empty()) {
      COUT(("[eoscp] # INGRESS Server Info      : %s\n",
            xferSummary.src_clientinfo.c_str()));
    }

    if (!xferSummary.dst_clientinfo.empty()) {
      COUT(("[eoscp] # EGRESS  Server info      : %s\n",
            xferSummary.dst_clientinfo.c_str()));
    }
  } else {
    COUT(("bytes_copied=%lld ", xferSummary.bytescopied));

    if (ndst > 1) {
      COUT(("totalbytes_copied=%lld ", xferSummary.totalbytescopied));
    }

    COUT(("realtime=%.02f ", xferSummary.abs_time / 1000.0));

    if (xferSummary.abs_time > 0) {
      COUT(("copy_rate=%f ", xferSummary.bytescopied / xferSummary.abs_time /
            1000.0));
    }

    if (xferSummary.ingress_microseconds) {
      COUT(("ingress_rate=%f ",
            xferSummary.ingress_rate));
    }

    if (xferSummary.egress_microseconds) {
      COUT(("egress_rate=%f ",
            xferSummary.egress_rate));
    }

    if (xferSummary.bandwidth) {
      COUT(("bandwidth=%d ", (int) xferSummary.bandwidth));
    }

    if (xferSummary.checksum_type) {
      COUT(("checksum_type=%s ", xferSummary.checksum_type->c_str()));
      COUT(("checksum=%s ", xferSummary.checksum_value->c_str()));
    }

    COUT(("write_start=%lld ", xferSummary.write_start));
    COUT(("write_stop=%lld ", xferSummary.write_stop));

    if (xferSummary.read_start >= 0) {
      COUT(("read_start=%lld ", xferSummary.read_start));
      COUT(("read_stop=%lld ", xferSummary.read_stop));
    }
  }
}

void print_json_summary(const XferSummary& xferSummary)
{
  std::stringstream ss;
  xferSummary.jsonify(ss);
  COUT((ss.str().c_str()));
}

//------------------------------------------------------------------------------
// Printing progress bar
//------------------------------------------------------------------------------

void
print_progbar(unsigned long long bytesread, unsigned long long size)
{
  if (!size) {
    bytesread = size = 1; // fake 100% in that case
  }

  CERR(("[eoscp] %-24s Total %.02f MB\t|", cpname.c_str(),
        (float) size / 1024 / 1024));

  for (int l = 0; l < 20; l++) {
    if (l < ((int)(20.0 * bytesread / size))) {
      CERR(("="));
    }

    if (l == ((int)(20.0 * bytesread / size))) {
      CERR((">"));
    }

    if (l > ((int)(20.0 * bytesread / size))) {
      CERR(("."));
    }
  }

  float abs_time = ((float)((abs_stop_time.tv_sec - abs_start_time.tv_sec) * 1000
                            +
                            (abs_stop_time.tv_usec - abs_start_time.tv_usec) / 1000));
  CERR(("| %.02f %% [%.01f MB/s]\r", 100.0 * bytesread / size,
        bytesread / abs_time / 1000.0));
}


//------------------------------------------------------------------------------
// Write progress
//------------------------------------------------------------------------------

void
write_progress(unsigned long long bytesread, unsigned long long size)
{
  static double lastprogress = 0;
  double progress = 100 * bytesread / (double)(size ? size : 1);

  if (progress > 100) {
    progress = 100;
  }

  if ((fabs(progress - lastprogress) <= 1.0) && (progress != 100.)) {
    // skip this update
    return;
  }

  std::string pf = progressFile;
  pf += ".tmp";
  FILE* fd = fopen(pf.c_str(), "w+");

  if (fd) {
    fprintf(fd, "%.02f %llu %llu\n", progress, bytesread, size);
    fclose(fd);

    if (rename(pf.c_str(), progressFile.c_str())) {
      fprintf(stderr, "error: renaming of progress file failed (%s=>%s)\n",
              pf.c_str(), progressFile.c_str());
    }
  }
}

struct CompareCksumResult {
  bool cksumMismatch = true;
  uint32_t xrdErrno = 0;
  std::string errMsg = "";
};

CompareCksumResult compareChecksum(XrdCl::FileSystem& fs,
                                   const std::string& destFilePath, std::string srcCksumType,
                                   const std::string& srcCksumValue)
{
  CompareCksumResult result;
  // Get the checksum of the file that got uploaded to the destination
  XrdCl::Buffer* responseRaw = 0;

  if (srcCksumType == "adler") {
    // xrootd adler32 checksum is called "adler32"
    srcCksumType = "adler32";
  }

  XrdCl::Buffer arg;
  arg.FromString(destFilePath + "?cks.type=" + srcCksumType);
  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::Checksum, arg,
                                        responseRaw);
  std::unique_ptr<XrdCl::Buffer> response(responseRaw);

  if (status.IsOK()) {
    // we got the checksum of the destination file
    // compare the checksums between source and destination
    std::string queryCksumResp = response->GetBuffer();
    auto splittedResp = eos::common::StringSplit(queryCksumResp, " ");

    if (splittedResp.size() == 2) {
      // The checksum response we received has a proper format
      const std::string destCksumType(splittedResp[0]);
      const std::string destCksumValue(splittedResp[1]);

      if (destCksumType == srcCksumType) {
        // Same checksum type between the source and the destination
        if (destCksumValue == srcCksumValue) {
          // Checksum match!
          result.cksumMismatch = false;
        } else {
          // Checksum mismatch
          result.xrdErrno = EIO;
          result.errMsg = "error: checksum mismatch between source (" + srcCksumValue +
                          ") and destination (" + destCksumValue + ")";
        }
      } else {
        // Different checksum type between source and destination
        result.errMsg =
          "error while extracting destination checksum: received a different checksum type from the destination ("
          + destCksumType + ") compared to the one computed on the source (" +
          srcCksumType + ")";
        result.xrdErrno = EINVAL;
      }
    } else {
      // Wrong response format received
      result.errMsg =
        "error while extracting the destination checksum: expected 'destCksumType destCksumValue', received:"
        + queryCksumResp;
      result.xrdErrno = EINVAL;
    }
  } else {
    // Problem while querying the destination checksum
    result.errMsg = "error while getting the destination checksum: " +
                    status.ToStr();
    result.xrdErrno = status.errNo;
  }

  return result;
}


//------------------------------------------------------------------------------
// Abort handler
//------------------------------------------------------------------------------

void
abort_handler(int)
{
  //  print_summary_header(src_location, dst_location);
  fprintf(stdout, "error: [eoscp] has been aborted\n");
  exit(EINTR);
}

//------------------------------------------------------------------------------
// Alarm handler
//------------------------------------------------------------------------------

void
alarm_handler(int)
{
  //  print_summary_header(src_location, dst_location);
  fprintf(stdout, "error: [eoscp] has timedout after %d seconds\n", gtimeout);
  exit(ETIMEDOUT);
}


//------------------------------------------------------------------------------
// Main function
//------------------------------------------------------------------------------

int
main(int argc, char* argv[])
{
  int c;
  mode_t dest_mode[MAXSRCDST];
  int set_mode = 0;
  extern char* optarg;
  extern int optind;
  // Ugly temporary hack for stopping the XRootD PostMaster environment no matter what happens (https://its.cern.ch/jira/projects/EOS/issues/EOS-6087)
  auto stopPostMaster = [&](void*) {
    XrdCl::DefaultEnv::GetPostMaster()->Stop();
  };
  std::unique_ptr<void, decltype(stopPostMaster)> stopPostMasterDeleter((void *)1, stopPostMaster);
  XrdCl::DefaultEnv::GetEnv()->PutInt("MetalinkProcessing", 0);
  XrdCl::DefaultEnv::GetEnv()->PutInt("ParallelEvtLoop",
                                      8);  // needed for high performance on 100GE
  // Define long options using struct option
  struct option long_options[] = {
    {"version", no_argument, nullptr, 'I'},
    {nullptr, 0, nullptr, 0} // Required end marker
  };

  while ((c = getopt_long(argc, argv,
                          "CEnshxdvlipfcje:P:X:b:m:u:g:t:S:D:5aA:r:N:L:RT:O:V0q:", long_options,
                          nullptr)) != -1) {
    switch (c) {
    case 'v':
      verbose = 1;
      break;

    case 'V':
      monitoring = 1;
      break;

    case 'j':
      jsonoutput = 1;
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

    case 'A':
      appendmode = 1;
      startwritebyte = strtoull(optarg, 0, 10);
      break;

    case 'c':
      doStoreRecovery = true;
      offsetXrd = -1;
      break;

    case 'f':
      break;

    case 'x':
      nooverwrite = true;
      break;

    case 'e':
      replicationType = optarg;

      if ((replicationType != "raiddp") && (replicationType != "reeds")) {
        fprintf(stderr, "error: no such RAID layout\n");
        exit(-1);
      }

      isRaidTransfer = true;
      break;

    case 'X': {
      xsString = optarg;

      if (find(xsTypeSet.begin(), xsTypeSet.end(), xsString) == xsTypeSet.end()) {
        fprintf(stderr, "error: no such checksum type: %s\n", optarg);
        exit(-1);
      }

      int layout = 0;
      unsigned long layoutId = 0;

      if (xsString == "adler") {
        layoutId = LayoutId::GetId(layout, LayoutId::kAdler);
      } else if (xsString == "crc32") {
        layoutId = LayoutId::GetId(layout, LayoutId::kCRC32);
      } else if (xsString == "md5") {
        layoutId = LayoutId::GetId(layout, LayoutId::kMD5);
      } else if (xsString == "sha1") {
        layoutId = LayoutId::GetId(layout, LayoutId::kSHA1);
      } else if (xsString == "crc32c") {
        layoutId = LayoutId::GetId(layout, LayoutId::kCRC32C);
      }

      xsObj = eos::fst::ChecksumPlugins::GetChecksumObject(layoutId);

      if (xsObj) {
        xsObj->Reset();
        computeXS = true;
      }

      break;
    }

    case 'P':
      nparitystripes = atoi(optarg);

      if (nparitystripes < 2) {
        fprintf(stderr, "error: number of parity stripes >= 2\n");
        exit(-1);
      }

      break;

    case '0':
      nopio = true;
      break;

    case 'O':
      progressFile = optarg;
      break;

    case 'u':
      euid = atoi(optarg);
      char tuid[128];
      sprintf(tuid, "%d", euid);

      if (strcmp(tuid, optarg)) {
        // this is not a number, try to map it with getpwnam
        struct passwd* pwinfo = getpwnam(optarg);

        if (pwinfo) {
          euid = pwinfo->pw_uid;

          if (debug) {
            fprintf(stdout, "[eoscp]: mapping user  %s=>UID:%d\n", optarg, euid);
          }
        } else {
          fprintf(stderr, "error: cannot map user %s to any unix id!\n", optarg);
          exit(-ENOENT);
        }
      }

      break;

    case 'g':
      egid = atoi(optarg);
      char tgid[128];
      sprintf(tgid, "%d", egid);

      if (strcmp(tgid, optarg)) {
        // this is not a number, try to map it with getgrnam
        struct group* grinfo = getgrnam(optarg);

        if (grinfo) {
          egid = grinfo->gr_gid;

          if (debug) {
            fprintf(stdout, "[eoscp]: mapping group %s=>GID:%d\n", optarg, egid);
          }
        } else {
          fprintf(stderr, "error: cannot map group %s to any unix id!\n", optarg);
          exit(-ENOENT);
        }
      }

      break;

    case 't':
      bandwidth = atoi(optarg);

      if ((bandwidth < 1) || (bandwidth > 2000)) {
        fprintf(stderr, "error: bandwidth can only be 1 <= bandwidth <= 2000 Mb/s\n");
        exit(-1);
      }

      break;

    case 'q':
      gtimeout = atoi(optarg);
      break;

    case 'S':
      nsrc = atoi(optarg);

      if ((nsrc < 1) || (nsrc > MAXSRCDST)) {
        fprintf(stderr, "error: # of sources must be 1 <= # <= %d\n", MAXSRCDST);
        exit(-1);
      }

      break;

    case 'D':
      ndst = atoi(optarg);

      if ((ndst < 1) || (ndst > MAXSRCDST)) {
        fprintf(stderr, "error: # of sources must be 1 <= # <= %d\n", MAXSRCDST);
        exit(-1);
      }

      break;

    case 'N':
      cpname = optarg;
      break;

    case 'b':
      buffersize = atoi(optarg);

      if ((buffersize < 4096) || (buffersize > 100 * 1024 * 1024)) {
        fprintf(stderr, "error: buffer size can only 4k <= size <= 100 M\n");
        exit(-1);
      }

      break;

    case 'T':
      targetsize = strtoull(optarg, 0, 10);
      break;

    case 'm':
      for (int i = 0; i < MAXSRCDST; i++) {
        dest_mode[i] = strtol(optarg, 0, 8);
      }

      set_mode = 1;
      break;

    case 'r':
      char* colon;
      colon = strchr(optarg, ':');

      if (colon == NULL) {
        fprintf(stderr, "error: range has to be given in the format "
                "<startbyte>:<stopbyte> e.g. 0:100000\n");
        exit(-1);
      }

      *colon = 0;
      startbyte = strtoll(optarg, 0, 0);
      stopbyte = strtoll(colon + 1, 0, 0);

      if (debug) {
        fprintf(stdout, "[eoscp]: reading range start=%lld stop=%lld\n", startbyte,
                stopbyte);
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

    case 'C':
      cksumcomparison = 1;
      break;

    case 'E':
      cksummismatchdelete = 1;
      break;

    case 'I':
      displayInformation();
      break;

    case 'h':
    default:
      usage();
      ;
    }
  }

  if (jsonoutput) {
    summary = 1;
    monitoring = 0;
    debug = 0;
    verbose = 0;
    progbar = 0;
  }

  if (debug) {
    eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
    g_logging.SetLogPriority(LOG_DEBUG);
  }

  if (optind - 1 + nsrc + ndst >= argc) {
    usage();
  }

  if (gtimeout) {
    signal(SIGALRM, alarm_handler);
    alarm(gtimeout);
  }

  //............................................................................
  // Allocate the buffer used for copy
  //............................................................................
  buffer = new char[2 * buffersize];

  if ((!buffer)) {
    fprintf(stderr, "error: cannot allocate buffer of size %u\n", 2 * buffersize);
    exit(-ENOMEM);
  }

  if (debug) {
    fprintf(stderr, "[eoscp]: allocate copy buffer with %u bytes\n",
            2 * buffersize);
  }

  //.............................................................................
  // Get the address and the file path from the input
  //.............................................................................
  std::string location;
  std::string address;
  std::string file_path;

  for (int i = 0; i < nsrc; i++) {
    location = argv[optind + i];
    size_t pos = location.find("://");
    pos = location.find("//", pos + 3);

    if (pos == std::string::npos) {
      address = "";
      file_path = location;
    } else {
      address = std::string(location, 0, pos + 1);
      file_path = std::string(location, pos + 1);
    }

    src_location.push_back(std::make_pair(address, file_path));

    if (verbose || debug) {
      if (i == 0) {
        fprintf(stdout, "[eoscp] ");
      }

      fprintf(stdout, "src<%d>=%s ", i, location.c_str());
    }
  }

  for (int i = 0; i < ndst; i++) {
    location = argv[optind + nsrc + i];
    size_t pos = location.find("://");
    pos = location.find("//", pos + 3);

    if (pos == std::string::npos) {
      address = "";
      file_path = location;
    } else {
      address = std::string(location, 0, pos + 1);
      file_path = std::string(location, pos + 1);
    }

    dst_location.push_back(std::make_pair(address, file_path));

    if (verbose || debug) {
      fprintf(stdout, "dst<%d>=%s ", i, location.c_str());
    }
  }

  if (verbose || debug) {
    fprintf(stdout, "\n");
  }

  if (cksumcomparison || cksummismatchdelete) {
    if (src_location.size() != 1 && dst_location.size() != 1) {
      fprintf(stderr,
              "error: only one source and one destination can be provided if the destination checksum check option is enabled (-C or -E)\n");
      exit(-EINVAL);
    }

    if (cksummismatchdelete && !cksumcomparison) {
      fprintf(stderr,
              "error: source and destination checksum comparison (-C) not enabled, automatic deletion option (-E) cannot be enabled\n");
      exit(-EINVAL);
    }
  }

  //.............................................................................
  // Get the type of access we will be doing
  //.............................................................................
  if (isRaidTransfer) {
    if (!nparitystripes) {
      fprintf(stderr, "error: number of parity stripes undefined\n");
      exit(-EINVAL);
    }

    if (nsrc > ndst) {
      isSrcRaid = true;
    } else {
      isSrcRaid = false;
    }
  }

  int stat_failed = 0;
  struct stat st[MAXSRCDST];

  //.............................................................................
  // Get sources access type
  //.............................................................................
  for (int i = 0; i < nsrc; i++) {
    if (src_location[i].first.find("root://") != std::string::npos) {
      if (isRaidTransfer && isSrcRaid) {
        src_type.push_back(RAID_ACCESS);
      } else {
        // If we don't need to recover the source and we were not told explicitly
        // that this is a RAIN transfer
        if (!isRaidTransfer && !doStoreRecovery) {
          //.......................................................................
          // Test if we can do parallel IO access
          //.......................................................................
          bool doPIO = false;
          XrdCl::Buffer arg;
          XrdCl::Buffer* response = 0;
          XrdCl::XRootDStatus status;
          file_path = src_location[i].first + src_location[i].second;

          if (file_path.find("//eos/") != std::string::npos) {
            // for any other URL it does not make sense to do the PIO access
            if (!nopio) {
              doPIO = true;
            }
          }

          size_t spos = file_path.rfind("//");
          std::string address = file_path.substr(0, spos + 1);
          XrdCl::URL url(address);

          if (!url.IsValid()) {
            fprintf(stderr, "URL is invalid: %s", address.c_str());
            exit(-1);
          }

          XrdCl::FileSystem fs(url);

          if (spos != std::string::npos) {
            file_path.erase(0, spos + 1);
          }

          std::string request = file_path;

          if ((file_path.find("?") == std::string::npos)) {
            request += "?mgm.pcmd=open";
          } else {
            request += "&mgm.pcmd=open";
          }

          arg.FromString(request);
          st[0].st_size = 0;
          st[0].st_mode = 0;

          if (doPIO) {
            status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg, response);
          }

          if (doPIO && status.IsOK()) {
            if (!getenv("EOS_FST_XRDIO_READAHEAD")) {
              setenv("EOS_FST_XRDIO_READAHEAD", "1", 1);
            }

            if (!getenv("EOS_FST_XRDIO_BLOCK_SIZE")) {
              setenv("EOS_FST_XRDIO_BLOCK_SIZE", "4194304 ", 1);
            }

            XrdCl::StatInfo* statresponse = 0;
            status = fs.Stat(file_path.c_str(), statresponse);

            if (status.IsOK()) {
              st[0].st_size = statresponse->GetSize();
              st[0].st_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

              if (statresponse->TestFlags(XrdCl::StatInfo::IsWritable)) {
                st[0].st_mode |= S_IWGRP;
              }
            }

            delete statresponse;

            //..................................................................
            // Parse output
            //..................................................................
            if (verbose || debug) {
              fprintf(stderr, "[eoscp] having PIO_ACCESS for source location=%i size=%llu \n",
                      i, (unsigned long long) st[0].st_size);
            }

            XrdOucString tag;
            XrdOucString stripe_path;
            std::string origResponse(response->GetBuffer(), response->GetSize());
            XrdOucString stringOpaque = origResponse.c_str();

            while (stringOpaque.replace("?", "&")) {
            }

            while (stringOpaque.replace("&&", "&")) {
            }

            XrdOucEnv* openOpaque = new XrdOucEnv(stringOpaque.c_str());
            char* opaque_info = (char*) strstr(origResponse.c_str(), "&mgm.logid");

            if (opaque_info == nullptr) {
              fprintf(stderr, "error: failed to parse opaque information from "
                      "PIO request.\n");
              exit(-EINVAL);
            }

            opaqueInfo = opaque_info;

            //..................................................................
            // Now that parallel IO is possible, we add the new stripes to the
            // src_location vector, we update the number of source files and then
            // we can use the RAID-like access mode where the stripe files are
            // given as input to the command line
            //...................................................................
            if (opaque_info) {
              LayoutId::layoutid_t layout = openOpaque->GetInt("mgm.lid");
              std::string orig_file = file_path;

              if (eos::common::LayoutId::GetLayoutType(layout) ==
                  eos::common::LayoutId::kRaidDP) {
                nsrc = eos::common::LayoutId::GetStripeNumber(layout) + 1;
                nparitystripes = 2;
                isRaidTransfer = true;
                isSrcRaid = true;
                src_location.clear();
                replicationType = "raiddp";
              } else if (eos::common::LayoutId::IsRain(layout)) {
                nsrc = eos::common::LayoutId::GetStripeNumber(layout) + 1;
                nparitystripes = eos::common::LayoutId::GetRedundancyStripeNumber(layout);
                isRaidTransfer = true;
                isSrcRaid = true;
                src_location.clear();
                replicationType = "reeds";
              } else {
                nsrc = 1;
                src_type.push_back(XRD_ACCESS);
                replicationType = "replica";
              }

              if (replicationType != "replica") {
                int qpos = orig_file.rfind("?");

                if (qpos != STR_NPOS) {
                  opaqueInfo += "&";
                  opaqueInfo += orig_file.substr(qpos + 1);
                  file_path.erase(qpos);
                }

                for (int j = 0; j < nsrc; j++) {
                  tag = "pio.";
                  tag += j;
                  stripe_path = "root://";
                  stripe_path += openOpaque->Get(tag.c_str());
                  stripe_path += "/";
                  stripe_path += orig_file.c_str();
                  int pos = stripe_path.rfind("//");

                  if (pos == STR_NPOS) {
                    address = "";
                    file_path = stripe_path.c_str();
                  } else {
                    address = std::string(stripe_path.c_str(), 0, pos + 1);
                    file_path = std::string(stripe_path.c_str(), pos + 1,
                                            stripe_path.length() - pos - 1);
                  }

                  // remove the ?xyz from the individual source URL
                  int qpos = file_path.rfind("?");

                  if (qpos != STR_NPOS) {
                    file_path.erase(qpos);
                  }

                  src_location.push_back(std::make_pair(address, file_path));
                  src_type.push_back(RAID_ACCESS);

                  if (verbose || debug) {
                    fprintf(stdout, "[eoscp] src<%d>=%s [%s]\n", j,
                            src_location.back().second.c_str(), src_location.back().first.c_str());
                  }
                }
              } else {
                //.....................................................................
                // The file is not suitable for PIO access, do normal XRD access
                //.....................................................................
                src_type.push_back(XRD_ACCESS);

                if (verbose || debug) {
                  fprintf(stdout, "[eoscp] doing standard access...\n");
                }
              }
            } else {
              fprintf(stderr,
                      "Error while parsing the opaque information from PIO request.\n");
              exit(-1);
            }

            delete openOpaque;
            delete response;
            break;
          } else {
            //.....................................................................
            // The file is not suitable for PIO access, do normal XRD access
            //.....................................................................
            src_type.push_back(XRD_ACCESS);
          }

          delete response;
        } else {
          //.....................................................................
          // Recovering a file in place or forcing recovery can not be done in
          // PIO mode, do normal XRD access (RAIN in gateway mode)
          //.....................................................................
          src_type.push_back(XRD_ACCESS);
        }
      }
    } else if (src_location[i].second == "-") {
      src_type.push_back(CONSOLE_ACCESS);

      if (i > 0) {
        fprintf(stderr, "error: you cannot read with several sources from stdin\n");
        exit(-EPERM);
      }
    } else if (src_location[i].first.find(":/") != std::string::npos) {
      src_type.push_back(RIO_ACCESS);
    } else {
      src_type.push_back(LOCAL_ACCESS);
    }
  }

  //............................................................................
  // Get destinations access type
  //............................................................................
  for (int i = 0; i < ndst; i++) {
    if (dst_location[i].first.find("root://") != std::string::npos) {
      if (isRaidTransfer && !isSrcRaid) {
        dst_type.push_back(RAID_ACCESS);
      } else {
        //.......................................................................
        // Here we rely on the fact that all destinations must be of the same type
        //.......................................................................
        dst_type.push_back(XRD_ACCESS);
      }
    } else if (dst_location[i].second == "-") {
      dst_type.push_back(CONSOLE_ACCESS);
    } else if (dst_location[i].first.find(":/") != std::string::npos) {
      dst_type.push_back(RIO_ACCESS);
    } else {
      dst_type.push_back(LOCAL_ACCESS);
    }

    //..........................................................................
    // Print the types of protocols involved
    //..........................................................................
    if (verbose || debug) {
      fprintf(stdout, "[eoscp]: copy protocol ");

      for (int j = 0; j < nsrc; j++) {
        fprintf(stdout, "%s:", protocols[src_type[j]]);
      }

      fprintf(stdout, "=>");

      for (int j = 0; j < ndst; j++) {
        fprintf(stdout, "%s:", protocols[dst_type[j]]);
      }

      fprintf(stdout, "\n");
    }
  }

  if (cksumcomparison) {
    size_t dst_type_sz = dst_type.size();

    if (dst_type_sz > 1) {
      fprintf(stderr,
              "error: too many destination provided. Checksum comparison between source and destination cannot be enabled.\n");
      exit(-EINVAL);
    }

    if (dst_type_sz == 1 && dst_type[0] != XRD_ACCESS) {
      fprintf(stderr,
              "error: source and checksum comparison (-C) only allowed for destination using root protocol.\n");
      exit(-EINVAL);
    }
  }

  if (verbose || debug) {
    fprintf(stderr, "\n");
  }

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

  //............................................................................
  // Start the performance measurement
  //............................................................................
  gettimeofday(&abs_start_time, &tz);
  bool got_rain_flags = false;
  int raid_url_failed_count = 0;

  if (!replicamode) {
    for (int i = 0; i < nsrc; i++) {
      // stat the source
      switch (src_type[i]) {
      case LOCAL_ACCESS: {
        if (debug) {
          fprintf(stdout, "[eoscp]: doing POSIX stat on %s\n",
                  src_location[i].second.c_str());
        }

        stat_failed = lstat(src_location[i].second.c_str(), &st[i]);
      }
      break;

      // TODO: Improve stat call for RAID_ACCESS
      //         - should stat the FST file physical path
      //         - stat_failed should affect even RAID access
      //       Possible merge with XRD_ACCESS stat call
      case RAID_ACCESS:
        if (!got_rain_flags) {
          XrdCl::URL url(src_location[i].first);

          if (!url.IsValid()) {
            fprintf(stderr, "warn: the url address is not valid url=%s\n",
                    src_location[i].first.c_str());
            raid_url_failed_count++;
            continue;
          }

          XrdCl::FileSystem fs(url);
          XrdCl::StatInfo* response = 0;
          status = fs.Stat(src_location[i].second, response);

          if (!status.IsOK()) {
            stat_failed = 1;
          } else {
            stat_failed = 0;
            st[i].st_size = response->GetSize();
            st[i].st_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

            if (response->TestFlags(XrdCl::StatInfo::IsWritable)) {
              st[i].st_mode |= S_IWGRP;
            }

            got_rain_flags = true;
          }

          if (got_rain_flags) {
            for (int j = 0; j < nsrc; j++) {
              if (j != i) {
                st[j].st_size = st[i].st_size;
                st[j].st_mode = st[i].st_mode;
              }
            }
          }
        }

        break;

      case XRD_ACCESS: {
        if (debug) {
          fprintf(stdout, "[eoscp]: doing XROOT/RAIDIO stat on %s\n",
                  src_location[i].second.c_str());
        }

        XrdCl::URL url(src_location[i].first);

        if (!url.IsValid()) {
          fprintf(stderr, "error: the url address is not valid url=%s\n",
                  src_location[i].first.c_str());
          exit(-EPERM);
        }

        XrdCl::FileSystem fs(url);
        XrdCl::StatInfo* response = 0;
        status = fs.Stat(src_location[i].second, response);

        if (!status.IsOK()) {
          stat_failed = 1;
        } else {
          stat_failed = 0;
          st[i].st_size = response->GetSize();
          st[i].st_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

          if (response->TestFlags(XrdCl::StatInfo::IsWritable)) {
            st[i].st_mode |= S_IWGRP;
          }
        }

        delete response;
      }
      break;

      case CONSOLE_ACCESS:
        stat_failed = 0;
        break;

      case RIO_ACCESS:
        stat_failed = 0;
        break;
      }

      if (!isRaidTransfer && stat_failed) {
        fprintf(stderr, "error: cannot stat source %s[%s]\n",
                src_location[i].first.c_str(), src_location[i].second.c_str());
        exit(-ENOENT);
      }
    }
  }

  //............................................................................
  // Start consistency check
  //............................................................................
  if ((isRaidTransfer) && (raid_url_failed_count > nparitystripes)) {
    fprintf(stderr,
            "error: not enough replicas for XROOT(RAIDIO) read");
    exit(-EINVAL);
  }

  if ((!isRaidTransfer) && (nsrc > 1)) {
    for (int i = 1; i < nsrc; i++) {
      if (st[0].st_size != st[i].st_size) {
        fprintf(stderr, "error: source files differ in size !\n");
        exit(-EINVAL);
      }
    }
  }

  //............................................................................
  // Check if this is a range link
  //............................................................................
  if (!replicamode) {
    for (int i = 0; i < nsrc; i++) {
      if (S_ISLNK(st[i].st_mode)) {
        int readlink_size = 0;
        char* readlinkbuff = (char*) malloc(4096);

        if (!readlinkbuff) {
          fprintf(stderr, "error: cannot allocate link buffer\n");
          exit(-ENOMEM);
        }

        readlinkbuff[0] = 0;

        switch (src_type[i]) {
        case LOCAL_ACCESS:
          if (debug) {
            fprintf(stdout, "[eoscp]: doing POSIX readlink on %s\n",
                    src_location[i].second.c_str());
          }

          readlink_size = readlink(src_location[i].second.c_str(), readlinkbuff, 4096);
          break;

        case RAID_ACCESS:
        case XRD_ACCESS:
        case RIO_ACCESS:
          readlink_size = 1;
          break;

        case CONSOLE_ACCESS:
          readlink_size = 0;
          break;
        }

        if (readlink_size < 0) {
          fprintf(stderr, "error: cannot read the link of %s\n",
                  src_location[i].second.c_str());
          exit(-errno);
        }

        char* space = strchr(readlinkbuff, ' ');

        if (space) {
          *space = 0;
          char* colon = strchr(space + 1, ':');

          if (colon) {
            *colon = 0;
            // yep, this is a range link
            startbyte = strtoll(space + 1, 0, 0);
            stopbyte = strtoll(colon + 1, 0, 0);
            src_location[i] = std::make_pair("", readlinkbuff);

            if (debug) {
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
  if (!transparentstaging) {
    for (int i = 0; i < nsrc; i++) {
      switch (src_type[i]) {
      case LOCAL_ACCESS:
        if (debug) {
          fprintf(stdout,
                  "[eoscp]: POSIX is transparent for staging - nothing to check\n");
        }

        break;

      case RAID_ACCESS:
        if (debug) {
          fprintf(stdout,
                  "[eoscp]: XROOT(RAIDIO) is transparent for staging - nothing to check\n");
        }

        break;

      case XRD_ACCESS:
        if (debug) {
          fprintf(stdout,
                  "[eoscp]: XROOT is transparent for staging - nothing to check\n");
        }

        break;

      case RIO_ACCESS:
        if (debug) {
          fprintf(stdout, "[eoscp]: RIO is transparent for staging - nothing to check\n");
        }

        break;

      case CONSOLE_ACCESS:
        if (debug) {
          fprintf(stdout,
                  "[eoscp]: STDIN is transparent for staging - nothing to check\n");
        }

        break;
      }
    }
  }

  //............................................................................
  // For the '-p' flag we create the needed destination directory tree
  //............................................................................
  struct stat dstst[MAXSRCDST];

  if ((!replicamode) && createdir) {
    mode_t mode = S_IRWXU | S_IRGRP | S_IROTH | S_IXGRP | S_IXOTH;

    //..........................................................................
    // Loop over the destination paths
    //..........................................................................
    for (int i = 0; i < ndst; i++) {
      int pos = 0;
      int mkdir_failed = 0;
      int chown_failed = 0;
      XrdOucString file_path = dst_location[i].second.c_str();
      XrdOucString opaque = dst_location[i].second.c_str();
      int npos;

      if ((npos = opaque.find("?")) != STR_NPOS) {
        opaque.erase(0, npos);
      } else {
        opaque = "";
      }

      while ((pos = file_path.find("/", pos + 1)) != STR_NPOS) {
        XrdOucString subpath = file_path;
        subpath.erase(pos);

        switch (dst_type[i]) {
        case LOCAL_ACCESS: {
          if (debug) {
            fprintf(stdout, "[eoscp]: doing POSIX stat on %s\n", subpath.c_str());
          }

          stat_failed = stat(const_cast<char*>(subpath.c_str()), &dstst[i]);

          if (stat_failed) {
            if (debug) {
              fprintf(stdout, "[eoscp]: doing POSIX mkdir on %s\n", subpath.c_str());
            }

            mkdir_failed = mkdir(const_cast<char*>(subpath.c_str()), mode);

            //..................................................................
            // The root user can also set the user/group as in the target location
            //..................................................................
            if (getuid() == 0) {
              if (!subpath.beginswith("/dev/")) {
                chown_failed = chown(const_cast<char*>(subpath.c_str()), st[0].st_uid,
                                     st[0].st_gid);
              }
            }
          }
        }
        break;

        case RAID_ACCESS:
        case XRD_ACCESS: {
          if (debug) {
            fprintf(stdout, "[eoscp]: doing XROOT(RAIDIO) stat on %s\n", subpath.c_str());
          }

          subpath += opaque.c_str();
          XrdCl::URL url(dst_location[i].first.c_str());
          XrdCl::FileSystem fs(url);
          XrdCl::StatInfo* response = 0;
          status = fs.Stat(subpath.c_str(), response);

          if (!status.IsOK()) {
            if (debug) {
              fprintf(stdout, "[eoscp]: doing XROOT mkdir on %s\n", subpath.c_str());
            }

            status = fs.MkDir(subpath.c_str(), XrdCl::MkDirFlags::None,
                              (XrdCl::Access::Mode)mode);

            if (!status.IsOK()) {
              mkdir_failed = 1;
            }
          }

          delete response;
          // Chown not supported by the standard xroot
        }
        break;

        case RIO_ACCESS:
          break;

        case CONSOLE_ACCESS:
          break;
        }

        if (mkdir_failed) {
          std::string errmsg = (status.IsOK()) ?
                               ("cannot create destination sub-directory " + subpath).c_str()
                               : status.GetErrorMessage();
          fprintf(stderr, "error: %s\n", errmsg.c_str());
          exit(-EPERM);
        }

        if (chown_failed) {
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
  for (int i = 0; i < nsrc; i++) {
    switch (src_type[i]) {
    case LOCAL_ACCESS: {
      if (debug) {
        fprintf(stdout, "[eoscp]: doing POSIX open to read  %s\n",
                src_location[i].second.c_str());
      }

      src_handler.push_back(std::make_pair(open(src_location[i].second.c_str(),
                                           O_RDONLY),
                                           static_cast<XrdCl::File*>(NULL)));
    }
    break;

    case RAID_ACCESS: {
      if (isSrcRaid) {
        int flags;
        mode_t mode_sfs = 0;
        std::vector<std::string> vectUrl;

        if (doStoreRecovery) {
          flags = SFS_O_RDWR;
        } else {
          flags = SFS_O_RDONLY;
        }

        for (int j = 0; j < nsrc; j++) {
          location = src_location[j].first + src_location[j].second;
          vectUrl.push_back(location);
        }

        LayoutId::layoutid_t layout = 0;

        if (replicationType == "raiddp") {
          layout = LayoutId::GetId(LayoutId::kRaidDP,
                                   1, nsrc,
                                   LayoutId::BlockSizeEnum(stripeWidth),
                                   LayoutId::OssXsBlockSize,
                                   0, nparitystripes);
          redundancyObj = new eos::fst::RaidDpLayout(NULL, layout, NULL, NULL,
              location.c_str(),
              0, doStoreRecovery);
        } else if (replicationType == "reeds") {
          layout = LayoutId::GetId(LayoutId::GetReedSLayoutByParity(nparitystripes),
                                   1, nsrc,
                                   LayoutId::BlockSizeEnum(stripeWidth),
                                   LayoutId::OssXsBlockSize,
                                   0, nparitystripes);
          redundancyObj = new eos::fst::ReedSLayout(NULL, layout, NULL, NULL,
              location.c_str(),
              0, doStoreRecovery);
        }

        if (debug) {
          fprintf(stdout, "[eoscp]: doing XROOT(RAID-PIO) open with flags: %x\n", flags);
        }

        if (redundancyObj->OpenPio(vectUrl, flags, mode_sfs, opaqueInfo.c_str())) {
          fprintf(stderr, "error: can not open RAID object for read/write\n");
          exit(-EIO);
        }
      }
    }
    break;

    case XRD_ACCESS: {
      if (debug) {
        fprintf(stdout, "[eoscp]: doing XROOT open to read  %s\n",
                src_location[i].second.c_str());
      }

      location = src_location[i].first + src_location[i].second;
      XrdCl::OpenFlags::Flags xrdcl_flags = XrdCl::OpenFlags::Read;
      XrdCl::Access::Mode xrdcl_mode = XrdCl::Access::UR |  XrdCl::Access::UW |
                                       XrdCl::Access::GR | XrdCl::Access::OR;

      if (doStoreRecovery) {
        xrdcl_flags = XrdCl::OpenFlags::Update;

        if ((location.find("?") == std::string::npos)) {
          location += "?eos.rain.store=1";
        } else {
          location += "&eos.rain.store=1";
        }
      }

      if (getenv("EOS_FUSE_SECRET")) {
        if ((location.find("?") == std::string::npos)) {
          location += "?eos.key=";
        } else {
          location += "&eos.key=";
        }

        location += getenv("EOS_FUSE_SECRET");
      }

      XrdCl::File* file = new XrdCl::File();
      status = file->Open(location, xrdcl_flags, xrdcl_mode);

      if (!status.IsOK()) {
        std::string errmsg;
        errmsg = status.GetErrorMessage();
        fprintf(stderr, "error: %s\n", status.ToStr().c_str());
        exit(-status.errNo ? -status.errNo : -EIO);
      } else {
        file->GetProperty("LastURL", src_lasturl);
      }

      src_handler.push_back(std::make_pair(0, (void*)file));
    }
    break;

    case RIO_ACCESS: {
      if (debug) {
        fprintf(stdout, "[eoscp]: doing RIO open to read  %s\n",
                src_location[i].second.c_str());
      }

      location = src_location[i].first + src_location[i].second;

      if (location.substr(0, 3) == "xrd") {
        location.replace(0, 3, "root");
      }

      eos::fst::FileIo* file = eos::fst::FileIoPluginHelper::GetIoObject(
                                 location.c_str());

      if (!file) {
        fprintf(stderr, "error: failed to get IO object for %s\n", location.c_str());
        exit(-1);
      }

      retc = file->fileOpen(0);

      if (retc) {
        eos::common::error_retc_map(file->GetLastErrNo());
        fprintf(stderr, "error: source file open failed - errno=%d : %s [%s]\n", errno,
                strerror(errno),
                file->GetLastErrMsg().c_str());
        exit(-errno);
      } else {
        src_lasturl = file->GetLastUrl();
      }

      src_handler.push_back(std::make_pair(0, (void*)file));
    }
    break;

    case CONSOLE_ACCESS:
      src_handler.push_back(std::make_pair(fileno(stdin),
                                           static_cast<XrdCl::File*>(NULL)));
      break;
    }

    if ((!isRaidTransfer) &&
        (src_handler[i].first < 0) &&
        (src_handler[i].second == NULL)) {
      std::string errmsg;
      errmsg = status.GetErrorMessage();
      fprintf(stderr, "error: %s\n", status.ToStr().c_str());
      exit(-status.errNo ? -status.errNo : -EIO);
    }

    if (isRaidTransfer && isSrcRaid) {
      break;
    }
  }

  //............................................................................
  // Seek the required start position
  //............................................................................
  if (startbyte > 0) {
    for (int i = 0; i < nsrc; i++) {
      if (debug) {
        fprintf(stdout, "[eoscp]: seeking in %s to position %lld\n",
                src_location[i].second.c_str(), startbyte);
      }

      switch (src_type[i]) {
      case LOCAL_ACCESS: {
        startbyte = lseek(src_handler[i].first, startbyte, SEEK_SET);
        offsetXS = startbyte;
      }
      break;

      case RAID_ACCESS: {
        offsetXrd = startbyte;
        offsetXS = startbyte;
      }
      break;

      case XRD_ACCESS: {
        //TODO::
        //startbyte = XrdPosixXrootd::Lseek( srcfd[i], startbyte, SEEK_SET );
        offsetXS = startbyte;
      }
      break;

      case RIO_ACCESS:
        offsetXrd = startbyte;
        offsetXS = startbyte;
        break;

      case CONSOLE_ACCESS:
        break;
      }

      if (startbyte < 0) {
        fprintf(stderr, "error: cannot seek start position of file %s %d\n",
                src_location[i].second.c_str(), errno);
        exit(-EIO);
      }
    }
  }

  //............................................................................
  // Open destination files
  //............................................................................
  for (int i = 0; i < ndst; i++) {
    retc = 0;

    switch (dst_type[i]) {
    case LOCAL_ACCESS: {
      if (debug) {
        fprintf(stdout, "[eoscp]: doing POSIX open to write  %s\n",
                dst_location[i].second.c_str());
      }

      if (nooverwrite) {
        struct stat buf;

        if (!stat(dst_location[i].second.c_str(), &buf)) {
          fprintf(stderr, "error: target file exists already!\n");
          exit(-EEXIST);
        }
      }

      if (appendmode) {
        dst_handler.push_back(std::make_pair(open(dst_location[i].second.c_str(),
                                             O_WRONLY | O_CREAT, st[i].st_mode),
                                             static_cast<eos::fst::XrdIo*>(NULL)));
      } else {
        dst_handler.push_back(std::make_pair(open(dst_location[i].second.c_str(),
                                             O_WRONLY | O_TRUNC | O_CREAT, st[i].st_mode),
                                             static_cast<eos::fst::XrdIo*>(NULL)));
      }
    }
    break;

    case RAID_ACCESS: {
      if (!isSrcRaid) {
        int flags;
        std::vector<std::string> vectUrl;
        flags = SFS_O_CREAT | SFS_O_WRONLY;

        for (int j = 0; j < ndst; j++) {
          location = dst_location[j].first + dst_location[j].second;
          vectUrl.push_back(location);
        }

        LayoutId::layoutid_t layout = 0;

        if (replicationType == "raiddp") {
          layout = LayoutId::GetId(LayoutId::kRaidDP,
                                   1, ndst,
                                   LayoutId::BlockSizeEnum(stripeWidth),
                                   LayoutId::OssXsBlockSize,
                                   0, nparitystripes);
          redundancyObj = new eos::fst::RaidDpLayout(NULL, layout, NULL, NULL,
              location.c_str(),
              0, doStoreRecovery, isStreamFile);
        } else if (replicationType == "reeds") {
          layout = LayoutId::GetId(LayoutId::GetReedSLayoutByParity(nparitystripes),
                                   1, ndst,
                                   LayoutId::BlockSizeEnum(stripeWidth),
                                   LayoutId::OssXsBlockSize,
                                   0, nparitystripes);
          redundancyObj = new eos::fst::ReedSLayout(NULL, layout, NULL, NULL,
              location.c_str(),
              0, doStoreRecovery, isStreamFile);
        }

        if (debug) {
          fprintf(stdout, "[eoscp]: doing XROOT(RAIDIO-PIO) open with flags: %x\n",
                  flags);
        }

        if (redundancyObj && redundancyObj->OpenPio(vectUrl, flags)) {
          fprintf(stderr, "error: can not open RAID object for write\n");
          exit(-EIO);
        }
      }
    }
    break;

    case XRD_ACCESS: {
      if (debug) {
        fprintf(stdout, "[eoscp]: doing XROOT open to write  %s\n",
                dst_location[i].second.c_str());
      }

      location = dst_location[i].first + dst_location[i].second;

      if (getenv("EOS_FUSE_SECRET")) {
        if ((location.find("?") == std::string::npos)) {
          location += "?eos.key=";
        } else {
          location += "&eos.key=";
        }

        location += getenv("EOS_FUSE_SECRET");
      }

      eos::fst::XrdIo* file = new eos::fst::XrdIo(location.c_str());

      if (appendmode || nooverwrite) {
        XrdCl::URL url(dst_location[i].first);

        if (!url.IsValid()) {
          fprintf(stderr, "error: the destination url address is not valid url=%s\n",
                  dst_location[i].first.c_str());
          exit(-EPERM);
        }

        XrdCl::FileSystem fs(url);
        XrdCl::StatInfo* response = 0;
        status = fs.Stat(dst_location[i].second, response);

        if (status.IsOK()) {
          if (nooverwrite) {
            fprintf(stderr, "error: target file exists already!\n");
            exit(-EEXIST);
          }

          retc = file->fileOpen(SFS_O_RDWR, st[i].st_mode, "");
        } else {
          retc = file->fileOpen(SFS_O_CREAT | SFS_O_RDWR,
                                S_IRUSR | S_IWUSR | S_IRGRP, "");
        }

        if (!startwritebyte && response) {
          startwritebyte = response->GetSize();
        }

        delete response;
      } else {
        retc = file->fileOpen(SFS_O_CREAT | SFS_O_RDWR,
                              S_IRUSR | S_IWUSR | S_IRGRP, "");
      }

      if (retc) {
        eos::common::error_retc_map(file->GetLastErrNo());
        fprintf(stderr, "error: target file open failed - errno=%d : %s [%s]\n",
                errno, strerror(errno),
                file->GetLastErrMsg().c_str());
        exit(-errno);
      } else {
        dst_lasturl = file->GetLastUrl();
      }

      dst_handler.push_back(std::make_pair(0, file));
    }
    break;

    case RIO_ACCESS: {
      if (debug) {
        fprintf(stdout, "[eoscp]: doing open to write  %s\n",
                dst_location[i].second.c_str());
      }

      location = dst_location[i].first + dst_location[i].second;

      if (location.substr(0, 3) == "xrd") {
        location.replace(0, 3, "root");
      }

      eos::fst::FileIo* file = eos::fst::FileIoPluginHelper::GetIoObject(
                                 location.c_str());
      location = src_location[i].first + src_location[i].second;

      if (!file->fileExists()) {
        if (nooverwrite) {
          fprintf(stderr, " error; target file exists already!\n");
          exit(-EEXIST);
        }

        retc = file->fileOpen(SFS_O_RDWR, st[i].st_mode, "");
      } else {
        retc = file->fileOpen(SFS_O_CREAT | SFS_O_RDWR,
                              st[i].st_mode, "");
      }

      if (retc) {
        eos::common::error_retc_map(file->GetLastErrNo());
        fprintf(stderr, "error: target file open failed - errno=%d : %s\n", errno,
                strerror(errno));
        exit(-errno);
      } else {
        dst_lasturl = file->GetLastUrl();
      }

      dst_handler.push_back(std::make_pair(0, file));
    }
    break;

    case CONSOLE_ACCESS:
      dst_handler.push_back(std::make_pair(fileno(stdout),
                                           static_cast<eos::fst::XrdIo*>(NULL)));
      break;
    }

    if ((!isRaidTransfer) &&
        (dst_handler[i].first <= 0) &&
        (dst_handler[i].second == NULL)) {
      std::string errmsg;
      errmsg = status.GetErrorMessage();

      if (status.errNo) {
        fprintf(stderr, "error: errc=%d msg=\"%s\"\n", status.errNo, errmsg.c_str());
      } else {
        fprintf(stderr, "error: errc=%d msg=\"%s\"\n", errno ? errno : EINVAL,
                strerror(errno ? errno : EINVAL));
      }

      exit(-status.errNo ? -status.errNo : -1);
    }

    if (isRaidTransfer && !isSrcRaid) {
      break;
    }
  }

  //............................................................................
  // In case the file exists, seek the end and print the offset
  //............................................................................
  if (appendmode) {
    for (int i = 0; i < ndst; i++) {
      switch (dst_type[i]) {
      case LOCAL_ACCESS:
        startwritebyte = lseek(dst_handler[i].first, 0, SEEK_END);
        break;

      case RAID_ACCESS:
        // Not supported
        break;

      case XRD_ACCESS:
        break;

      case RIO_ACCESS:
        break;

      case CONSOLE_ACCESS:
        // Not supported
        break;
      }

      if (startwritebyte < 0) {
        fprintf(stderr, "error: cannot seek from end to beginning of file %s\n",
                dst_location[i].second.c_str());
        exit(-EIO);
      }
    }
  }

  //............................................................................
  // Set the source mode or a specified one for the destination
  //............................................................................
  for (int i = 0; i < ndst; i++) {
    int chmod_failed = 0;
    int chown_failed = 0;

    switch (dst_type[i]) {
    case LOCAL_ACCESS: {
      if (!set_mode) {
        //........................................................................
        // If not specified on the command line, take the source mode
        //........................................................................
        if (S_ISREG(dstst[i].st_mode)) {
          // only for files !
          dest_mode[i] = st[0].st_mode;
        }
      }

      if ((S_ISREG(dstst[i].st_mode) &&
           (dst_location[i].second.substr(0, 5) != "/dev/"))) {
        chmod_failed = chmod(dst_location[i].second.c_str(), dest_mode[i]);

        if (getuid() == 0) {
          chown_failed = chown(dst_location[i].second.c_str(), st[0].st_uid,
                               st[0].st_gid);
        }
      }
    }
    break;

    case RAID_ACCESS:
    case XRD_ACCESS:
    case RIO_ACCESS:
    case CONSOLE_ACCESS:
      //........................................................................
      // Not supported, no such functionality in the standard xroot or console
      //........................................................................
      break;
    }

    if (chmod_failed) {
      fprintf(stderr, "error: cannot set permissions to %d for file %s\n",
              dest_mode[i], dst_location[i].second.c_str());
      exit(-EPERM);
    }

    if (chown_failed) {
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

  while (1) {
    if (progressFile.length()) {
      write_progress(totalbytes, st[0].st_size);
    }

    if (progbar) {
      gettimeofday(&abs_stop_time, &tz);

      for (int i = 0; i < nsrc; i++) {
        if ((src_type[i] == XRD_ACCESS) && (targetsize)) {
          st[i].st_size = targetsize;
        }
      }

      print_progbar(totalbytes, st[0].st_size);
    }

    if (bandwidth) {
      gettimeofday(&abs_stop_time, &tz);
      float abs_time = static_cast<float>((abs_stop_time.tv_sec -
                                           abs_start_time.tv_sec) * 1000 +
                                          (abs_stop_time.tv_usec - abs_start_time.tv_usec) / 1000);
      //........................................................................
      // Regulate the io - sleep as desired
      //........................................................................
      float exp_time = totalbytes / bandwidth / 1000.0;

      if (abs_time < exp_time) {
        usleep((int)(1000 * (exp_time - abs_time)));
      }
    }

    //..........................................................................
    // For ranges we have to adjust the last buffersize
    //..........................................................................
    if ((stopbyte >= 0) &&
        (((stopbyte - startbyte) - totalbytes) < buffersize)) {
      buffersize = (stopbyte - startbyte) - totalbytes;
    }

    int nread = -1;
    auto mReadStart = std::chrono::steady_clock::now();

    switch (src_type[0]) {
    case LOCAL_ACCESS:
    case CONSOLE_ACCESS:
      nread = read(src_handler[0].first,
                   static_cast<void*>(ptr_buffer),
                   buffersize);
      break;

    case RAID_ACCESS: {
      nread = redundancyObj->Read(offsetXrd, ptr_buffer, buffersize);
      offsetXrd += nread;
    }
    break;

    case XRD_ACCESS: {
      eos::common::Timing::GetTimeSpec(start);
      uint32_t xnread = 0;
      status = static_cast<XrdCl::File*>(src_handler[0].second)->Read(offsetXrd,
               buffersize, ptr_buffer, xnread);
      nread = xnread;

      if (!status.IsOK()) {
        fprintf(stderr, "Error while doing reading. \n");
        exit(-1);
      }

      eos::common::Timing::GetTimeSpec(end);
      wait_time = static_cast<double>((end.tv_sec * 1000 + end.tv_nsec / 1000000) -
                                      (start.tv_sec * 1000 + start.tv_nsec / 1000000));
      read_wait += wait_time;
      offsetXrd += nread;

      if (debug) {
        fprintf(stderr, "[eoscp] read=%d\n", nread);
      }
    }
    break;

    case RIO_ACCESS: {
      eos::common::Timing::GetTimeSpec(start);
      int64_t nread64;
      nread64 = static_cast<eos::fst::FileIo*>(src_handler[0].second)->fileRead(
                  offsetXrd, ptr_buffer, buffersize);

      if (nread64 < 0) {
        nread = -1;
      } else {
        nread = (int) nread64;
      }

      eos::common::Timing::GetTimeSpec(end);
      wait_time = static_cast<double>((end.tv_sec * 1000 + end.tv_nsec / 1000000) -
                                      (start.tv_sec * 1000 + start.tv_nsec / 1000000));
      read_wait += wait_time;
      offsetXrd += nread;

      if (debug) {
        fprintf(stderr, "[eoscp] read=%d\n", nread);
      }
    }
    break;
    }

    auto mReadStop = std::chrono::steady_clock::now();
    ingress_microseconds += std::chrono::duration_cast<std::chrono::microseconds>
                            (mReadStop - mReadStart).count();

    if (nread < 0) {
      fprintf(stderr, "error: read failed on file %s - destination file "
              "is incomplete!\n", src_location[0].second.c_str());
      exit(-EIO);
    }

    if (nread == 0) {
      // end of file
      break;
    }

    if (computeXS && xsObj) {
      xsObj->Add(static_cast<const char*>(ptr_buffer), nread, offsetXS);
      offsetXS += nread;
    }

    auto mWriteStart = std::chrono::steady_clock::now();
    int64_t nwrite = 0;

    for (int i = 0; i < ndst; i++) {
      switch (dst_type[i]) {
      case LOCAL_ACCESS:
      case CONSOLE_ACCESS:
        nwrite = write(dst_handler[i].first, ptr_buffer, nread);
        break;

      case RAID_ACCESS: {
        if (i == 0) {
          nwrite = redundancyObj->Write(stopwritebyte, ptr_buffer, nread);
          i = ndst;
        }
      }
      break;

      case XRD_ACCESS: {
        // Do writes in async mode
        eos::common::Timing::GetTimeSpec(start);
        nwrite = static_cast<eos::fst::FileIo*>(dst_handler[i].second)->fileWriteAsync(
                   stopwritebyte, ptr_buffer, nread);
        eos::common::Timing::GetTimeSpec(end);
        wait_time = static_cast<double>((end.tv_sec * 1000 + end.tv_nsec / 1000000) -
                                        (start.tv_sec * 1000 + start.tv_nsec / 1000000));
        write_wait += wait_time;

        if (debug) {
          fprintf(stderr, "[eoscp] write=%li\n", nwrite);
        }
      }
      break;

      case RIO_ACCESS: {
        eos::common::Timing::GetTimeSpec(start);
        int64_t nwrite64;
        nwrite64 = static_cast<eos::fst::FileIo*>(dst_handler[i].second)->fileWrite(
                     stopwritebyte, ptr_buffer, nread);

        if (nwrite64 < 0) {
          nwrite = -1;
        } else {
          nwrite = (int) nwrite64;
        }

        eos::common::Timing::GetTimeSpec(end);
        wait_time = static_cast<double>((end.tv_sec * 1000 + end.tv_nsec / 1000000) -
                                        (start.tv_sec * 1000 + start.tv_nsec / 1000000));
        write_wait += wait_time;

        if (debug) {
          fprintf(stderr, "[eoscp] write=%li\n", nwrite);
        }
      }
      break;
      }

      if (nwrite != nread) {
        fprintf(stderr, "error: write failed on destination file %s - "
                "wrote %lld/%lld bytes - destination file is incomplete!\n",
                dst_location[i].second.c_str(), (long long) nwrite, (long long) nread);
        exit(-EIO);
      }
    }

    auto mWriteStop = std::chrono::steady_clock::now();
    egress_microseconds += std::chrono::duration_cast<std::chrono::microseconds>
                           (mWriteStop - mWriteStart).count();
    totalbytes += nwrite;
    stopwritebyte += nwrite;
  } // end while(1)

  // Wait for all async write requests before moving on
  eos::common::Timing::GetTimeSpec(start);
  eos::fst::AsyncMetaHandler* ptr_handler = 0;
  bool write_error = false;

  for (int i = 0; i < ndst; i++) {
    if (dst_type[i] == XRD_ACCESS) {
      if (dst_handler[i].second) {
        ptr_handler = static_cast<eos::fst::AsyncMetaHandler*>(
                        static_cast<eos::fst::FileIo*>(dst_handler[i].second)->fileGetAsyncHandler());

        if (ptr_handler) {
          uint16_t error_type = ptr_handler->WaitOK();

          if (error_type != XrdCl::errNone) {
            fprintf(stderr, "Error while doing the async writing.\n");
            write_error = true;
          }
        }
      }
    }
  }

  eos::common::Timing::GetTimeSpec(end);
  wait_time = static_cast<double>((end.tv_sec * 1000 + end.tv_nsec / 1000000) -
                                  (start.tv_sec * 1000 + start.tv_nsec / 1000000));
  write_wait += wait_time;

  if (computeXS && xsObj) {
    xsObj->Finalize();
    xsValue = xsObj->GetHexChecksum();
  }

  if (progbar) {
    gettimeofday(&abs_stop_time, &tz);

    for (int i = 0; i < nsrc; i++) {
      if (src_type[i] == XRD_ACCESS) {
        st[i].st_size = totalbytes;
      }
    }

    print_progbar(totalbytes, st[0].st_size);
    std::cout << std::endl;
  }

  auto xferSummary = createXferSummary(src_location, dst_location, totalbytes);

  if (jsonoutput) {
    print_json_summary(xferSummary);
  } else {
    if (summary) {
      print_summary(xferSummary);
    }
  }

  //............................................................................
  // Close all files
  //............................................................................
  for (int i = 0; i < nsrc; i++) {
    switch (src_type[i]) {
    case LOCAL_ACCESS:
      close(src_handler[i].first);
      break;

    case RAID_ACCESS:
      if (i == 0) {
        redundancyObj->Close();
        i = nsrc;
        delete redundancyObj;
      }

      break;

    case XRD_ACCESS:
      status = static_cast<XrdCl::File*>(src_handler[i].second)->Close();

      if (!status.IsOK()) {
        fprintf(stderr,
                "error: close failed on source - file modified during replication\n");
        exit(-EIO);
      }

      delete static_cast<XrdCl::File*>(src_handler[i].second);
      break;

    case RIO_ACCESS:
      retc = static_cast<eos::fst::FileIo*>(src_handler[i].second)->fileClose();

      if (retc) {
        fprintf(stderr,
                "error: close failed on source - file modified during replication\n");
        exit(-EIO);
      }

      delete static_cast<eos::fst::FileIo*>(src_handler[i].second);
      break;

    case CONSOLE_ACCESS:
      break;
    }
  }

  for (int i = 0; i < ndst; i++) {
    switch (dst_type[i]) {
    case LOCAL_ACCESS:
      close(dst_handler[i].first);
      break;

    case RAID_ACCESS:
      if (i == 0) {
        errno = 0;
        redundancyObj->Close();

        if (errno) {
          fprintf(stderr, "error: %s\n", redundancyObj->GetLastErrMsg().c_str());
        }

        i = ndst;
        delete redundancyObj;
      }

      break;

    case XRD_ACCESS:
      retc = static_cast<eos::fst::FileIo*>(dst_handler[i].second)->fileClose();

      if (retc) {
        fprintf(stderr, "error: %s\n", status.ToStr().c_str());
        exit(-EIO);
      }

      delete static_cast<eos::fst::FileIo*>(dst_handler[i].second);
      break;

    case RIO_ACCESS:
      retc = static_cast<eos::fst::FileIo*>(dst_handler[i].second)->fileClose();

      if (retc) {
        fprintf(stderr, "error: close failed on target\n");
        exit(-EIO);
      }

      delete static_cast<eos::fst::FileIo*>(dst_handler[i].second);
      break;

    case CONSOLE_ACCESS:
      //........................................................................
      // Nothing to do
      //........................................................................
      break;
    }
  }

  if (dosymlink) {
    int symlink_failed = 0;
    char rangedestname[4096];

    if (appendmode) {
      sprintf(rangedestname, "%s %llu:%llu",
              dst_location[0].second.c_str(),
              static_cast<unsigned long long>(startwritebyte),
              static_cast<unsigned long long>(stopwritebyte));
    } else {
      sprintf(rangedestname, "%s", dst_location[0].second.c_str());
    }

    if (debug) {
      fprintf(stdout, "[eoscp]: creating symlink %s->%s\n", symlinkname,
              rangedestname);
    }

    switch (dst_type[0]) {
    case LOCAL_ACCESS: {
      unlink(symlinkname);
      symlink_failed = symlink(rangedestname, symlinkname);
    }
    break;

    case RAID_ACCESS:
    case XRD_ACCESS:
    case RIO_ACCESS:
    case CONSOLE_ACCESS:
      //........................................................................
      // Noting to do, xrootd has no symlink support in posix
      //........................................................................
      break;
    }

    if (symlink_failed) {
      fprintf(stderr, "error: cannot creat symlink from %s -> %s\n",
              symlinkname, rangedestname);
      exit(-ESPIPE);
    }
  }

  if (debug) {
    fprintf(stderr, "[eoscp] # Total read wait time     : %f ms  \n",
            read_wait);
    fprintf(stderr, "[eoscp] # Total write wait time    : %f ms \n",
            write_wait);
  }

  if (cksumcomparison) {
    // The client asked for some checksum comparison between the source and the destination
    std::string destServer = dst_location[0].first;
    std::string destFilePath = dst_location[0].second;
    XrdCl::URL url(destServer);
    // No need to check the URL consistency as the transfer already happened
    XrdCl::FileSystem fs(url);
    CompareCksumResult res = compareChecksum(fs, destFilePath, xsString, xsValue);

    if (res.cksumMismatch) {
      // Checksum mismatch, print related error
      fprintf(stderr, "%s\n", res.errMsg.c_str());

      if (cksummismatchdelete) {
        // The user wants to delete the file if the checksum mismatch between source and destination
        fprintf(stderr, "Deleting the file from the destination %s%s\n",
                destServer.c_str(), destFilePath.c_str());
        status = fs.Rm(destFilePath);

        if (!status.IsOK()) {
          fprintf(stderr,
                  "error while trying to delete the file from the destination (%s): %s\n",
                  destFilePath.c_str(), status.ToStr().c_str());
          exit(-status.errNo ? -status.errNo : -1);
        }
      }

      // Just return the error code set during the checksum checking
      exit(-res.xrdErrno ? -res.xrdErrno : -1);
    }
  }

  // Free memory
  delete[] buffer;

  if (write_error) {
    return -EIO;
  }

  return 0;
}
