#include "fst/filemd/FmdMgm.hh"
#include "fst/Config.hh"
#include "common/ShellCmd.hh"
#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include "common/LayoutId.hh"
#include "proto/FileMd.pb.h"
#include "proto/ConsoleRequest.pb.h"
#include "namespace/interface/IFileMD.hh"
#include <XrdCl/XrdClFileSystem.hh>

namespace
{

//------------------------------------------------------------------------------
// Parses a comma-separated string of location IDs and adds them to a FileMdProto object
//------------------------------------------------------------------------------
void ParseLocations(std::string locations, eos::ns::FileMdProto& fmd)
{
  std::vector<std::string> location_vector;
  eos::common::StringConversion::Tokenize(locations, location_vector, ",");

  for (const auto& elem : location_vector) {
    if (elem.empty()) {
      continue;
    }

    if (elem[0] == '!') {
      fmd.add_unlink_locations(std::stoull(elem.c_str() + 1));
    } else {
      fmd.add_locations(std::stoull(elem.c_str()));
    }
  }
}

//------------------------------------------------------------------------------
// Converts a hexadecimal string into its raw binary string representation
//------------------------------------------------------------------------------
std::string ParseChecksum(const std::string& hexStr)
{
  size_t size;
  auto xs = eos::common::StringConversion::Hex2BinDataChar(hexStr, size);

  if (xs) {
    return std::string(xs.release(), size);
  }

  return std::string();
}

//------------------------------------------------------------------------------
// Constructs the binary representation of a timestamp
//------------------------------------------------------------------------------
std::string ParseFileMDTime(XrdOucEnv& env, const char* key, const char* key_ns)
{
  char buff[sizeof(eos::IFileMD::ctime_t)];
  eos::IFileMD::ctime_t time;
  time.tv_sec = std::stoul(env.Get(key));
  time.tv_nsec = std::stoul(env.Get(key_ns));
  (void) memcpy(buff, &time, sizeof(time));
  return std::string(buff);
}
}

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Exclude unlinked locations from the given string representation
//------------------------------------------------------------------------------
std::string
FmdMgmHandler::ExcludeUnlinkedLoc(const std::string& slocations)
{
  std::ostringstream oss;
  std::vector<std::string> location_vector;
  eos::common::StringConversion::Tokenize(slocations, location_vector, ",");

  for (const auto& elem : location_vector) {
    if (!elem.empty() && elem[0] != '!') {
      oss << elem << ",";
    }
  }

  return oss.str();
}


//------------------------------------------------------------------------------
// Convert an MGM env representation to an Fmd struct
//------------------------------------------------------------------------------
bool
FmdMgmHandler::EnvMgmToFmd(XrdOucEnv& env, eos::ns::FileMdProto& fmd)
{
  // &name=random&id=705&ctime=1738852302&ctime_ns=871468404&mtime=1738852312&
  //mtime_ns=619640000&size=2117825536&cid=90&uid=0&gid=0&lid=543425858&flags=416&link=&location=9,2,1,6,7,3,
  //&checksum=0e27ecd900000000000000000000000000000000000000000000000000000000&container=/eos/dev/rain/
  // Check that all tags are present - except name which could be empty
  if (!env.Get("id") ||
      !env.Get("cid") ||
      !env.Get("ctime") ||
      !env.Get("ctime_ns") ||
      !env.Get("mtime") ||
      !env.Get("mtime_ns") ||
      !env.Get("size") ||
      !env.Get("checksum") ||
      !env.Get("lid") ||
      !env.Get("uid") ||
      !env.Get("gid") ||
      !env.Get("location")) {
    return false;
  }

  try {
    if (env.Get("name")) {
      fmd.set_name(env.Get("name"));
    }

    if (env.Get("link")) {
      fmd.set_link_name(env.Get("link"));
    }

    fmd.set_id(std::stoull(env.Get("id")));
    fmd.set_cont_id(std::stoull(env.Get("cid")));
    fmd.set_uid(std::stoull(env.Get("uid")));
    fmd.set_gid(std::stoull(env.Get("gid")));
    fmd.set_size(std::stoull(env.Get("size")));
    fmd.set_layout_id(std::stoul(env.Get("lid")));
    fmd.set_ctime(ParseFileMDTime(env, "ctime", "ctime_ns"));
    fmd.set_mtime(ParseFileMDTime(env, "mtime", "mtime_ns"));
    std::string xsVal = env.Get("checksum");

    if (xsVal != "none") {
      fmd.set_checksum(ParseChecksum(xsVal));
    } else {
      fmd.set_checksum("");
    }

    ParseLocations(env.Get("location"), fmd);
  } catch (...) {
    // not valid
    return false;
  }

  return true;
}

//----------------------------------------------------------------------------
// Convert namespace file proto object to an Fmd struct
//----------------------------------------------------------------------------
bool
FmdMgmHandler::NsFileProtoToFmd(eos::ns::FileMdProto&& filemd,
                                eos::common::FmdHelper& fmd)
{
  fmd.mProtoFmd.set_fid(filemd.id());
  fmd.mProtoFmd.set_cid(filemd.cont_id());
  eos::IFileMD::ctime_t ctime;
  (void) memcpy(&ctime, filemd.ctime().data(), sizeof(ctime));
  eos::IFileMD::ctime_t mtime;
  (void) memcpy(&mtime, filemd.mtime().data(), sizeof(mtime));
  fmd.mProtoFmd.set_ctime(ctime.tv_sec);
  fmd.mProtoFmd.set_ctime_ns(ctime.tv_nsec);
  fmd.mProtoFmd.set_mtime(mtime.tv_sec);
  fmd.mProtoFmd.set_mtime_ns(mtime.tv_nsec);
  fmd.mProtoFmd.set_mgmsize(filemd.size());
  fmd.mProtoFmd.set_lid(filemd.layout_id());
  fmd.mProtoFmd.set_uid(filemd.uid());
  fmd.mProtoFmd.set_gid(filemd.gid());
  std::string str_xs;
  uint8_t size = filemd.checksum().size();

  for (uint8_t i = 0; i < size; i++) {
    char hx[3];
    hx[0] = 0;
    snprintf(static_cast<char*>(hx), sizeof(hx), "%02x",
             *(unsigned char*)(filemd.checksum().data() + i));
    str_xs += static_cast<char*>(hx);
  }

  size_t cslen = eos::common::LayoutId::GetChecksumLen(filemd.layout_id()) * 2;
  // Truncate the checksum to the right string length
  str_xs.erase(std::min(str_xs.length(), cslen));
  fmd.mProtoFmd.set_mgmchecksum(str_xs);
  std::string slocations;

  for (const auto& loc : filemd.locations()) {
    slocations += std::to_string(loc);
    slocations += ",";
  }

  if (!slocations.empty()) {
    slocations.pop_back();
  }

  fmd.mProtoFmd.set_locations(slocations);
  return true;
}

//------------------------------------------------------------------------------
// Return Fmd from MGM doing getfmd command
//------------------------------------------------------------------------------
int
FmdMgmHandler::GetMgmFmd(const std::string& manager,
                         eos::common::FileId::fileid_t fid,
                         eos::ns::FileMdProto& fmd)
{
  if (!fid) {
    return EINVAL;
  }

  int rc = 0;
  std::string mgr;
  XrdCl::Buffer arg;
  std::string query = SSTR("/?mgm.pcmd=getfmd&mgm.getfmd.fid=" << fid).c_str();
  std::unique_ptr<XrdCl::Buffer> response;
  XrdCl::Buffer* resp_raw = nullptr;
  XrdCl::XRootDStatus status;

  do {
    mgr = manager;

    if (mgr.empty()) {
      mgr = gConfig.GetManager();

      if (mgr.empty()) {
        eos_static_err("msg=\"no manager info available\"");
        return EINVAL;
      }
    }

    std::string address = SSTR("root://" << mgr <<
                               "//dummy?xrd.wantprot=sss").c_str();
    XrdCl::URL url(address.c_str());

    if (!url.IsValid()) {
      eos_static_err("msg=\"invalid URL=%s\"", address.c_str());
      return EINVAL;
    }

    std::unique_ptr<XrdCl::FileSystem> fs {new XrdCl::FileSystem(url)};

    if (!fs) {
      eos_static_err("%s", "msg=\"failed to allocate FS object\"");
      return EINVAL;
    }

    arg.FromString(query.c_str());
    uint16_t timeout = 10;
    status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg, resp_raw, timeout);
    response.reset(resp_raw);
    resp_raw = nullptr;

    if (status.IsOK()) {
      rc = 0;
      eos_static_debug("msg=\"got metadata from mgm\" manager=%s fxid=%08llx",
                       mgr.c_str(), fid);
    } else {
      eos_static_err("msg=\"query error\" fxid=%08llx status=%d code=%d", fid,
                     status.status, status.code);

      if ((status.code >= 100) &&
          (status.code <= 300)) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        eos_static_info("msg=\"retry query\" fxid=%08llx query=\"%s\"", fid,
                        query.c_str());
      } else {
        eos_static_err("msg=\"failed to retrieve metadata from mgm\" manager=%s "
                       "fxid=%08llx", mgr.c_str(), fid);
        rc = ECOMM;
      }
    }
  } while ((status.code >= 100) && (status.code <= 300));

  if (rc) {
    return EIO;
  }

  // Check if response contains any data
  if (!response->GetBuffer()) {
    eos_static_err("msg=\"empty response buffer\" manager=%s fxid=%08llx",
                   mgr.c_str(), fid);
    return ENODATA;
  }

  std::string sresult = response->GetBuffer();
  std::string search_tag = "getfmd: retc=0 ";

  if ((sresult.find(search_tag)) == std::string::npos) {
    eos_static_info("msg=\"no metadata info at the mgm\" manager=%s fxid=%08llx "
                    " resp_buff=\"%s\"", mgr.c_str(), fid, response->GetBuffer());
    return ENODATA;
  } else {
    sresult.erase(0, search_tag.length());
  }

  // Get the remote file meta data into an env hash
  XrdOucEnv fmd_env(sresult.c_str());

  if (!EnvMgmToFmd(fmd_env, fmd)) {
    int envlen;
    eos_static_err("msg=\"failed to parse metadata info\" data=\"%s\" fxid=%08llx",
                   fmd_env.Env(envlen), fid);
    return EIO;
  }

  if (fmd.id() != fid) {
    eos_static_err("msg=\"received wrong meta data from mgm\" fxid=%08llx "
                   "recv_fxid=%08llx", fmd.id(), fid);
    return EIO;
  }

  return 0;
}

EOSFSTNAMESPACE_END
