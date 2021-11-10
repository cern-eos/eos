#include "fst/filemd/FmdMgm.hh"
#include "fst/Config.hh"
#include "common/ShellCmd.hh"
#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include "common/LayoutId.hh"
#include "proto/FileMd.pb.h"
#include "proto/ConsoleRequest.pb.h"
#include "namespace/interface/IFileMD.hh"
#include "XrdCl/XrdClFileSystem.hh"

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
FmdMgmHandler::EnvMgmToFmd(XrdOucEnv& env, eos::common::FmdHelper& fmd)
{
  // Check that all tags are present
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
      !env.Get("gid")) {
    return false;
  }

  fmd.mProtoFmd.set_fid(strtoull(env.Get("id"), 0, 10));
  fmd.mProtoFmd.set_cid(strtoull(env.Get("cid"), 0, 10));
  fmd.mProtoFmd.set_ctime(strtoul(env.Get("ctime"), 0, 10));
  fmd.mProtoFmd.set_ctime_ns(strtoul(env.Get("ctime_ns"), 0, 10));
  fmd.mProtoFmd.set_mtime(strtoul(env.Get("mtime"), 0, 10));
  fmd.mProtoFmd.set_mtime_ns(strtoul(env.Get("mtime_ns"), 0, 10));
  fmd.mProtoFmd.set_mgmsize(strtoull(env.Get("size"), 0, 10));
  fmd.mProtoFmd.set_lid(strtoul(env.Get("lid"), 0, 10));
  fmd.mProtoFmd.set_uid((uid_t) strtoul(env.Get("uid"), 0, 10));
  fmd.mProtoFmd.set_gid((gid_t) strtoul(env.Get("gid"), 0, 10));
  fmd.mProtoFmd.set_mgmchecksum(env.Get("checksum"));
  // Store only the valid locations, exclude the unlinked ones
  std::string locations = ExcludeUnlinkedLoc(env.Get("location") ?
                          env.Get("location") : "");
  fmd.mProtoFmd.set_locations(locations);
  size_t cslen = eos::common::LayoutId::GetChecksumLen(fmd.mProtoFmd.lid()) * 2;
  fmd.mProtoFmd.set_mgmchecksum(std::string(fmd.mProtoFmd.mgmchecksum()).erase
                                (std::min(fmd.mProtoFmd.mgmchecksum().length(), cslen)));
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
                           eos::common::FmdHelper& fmd)
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
      eos_static_debug("msg=\"got metadata from mgm\" manager=%s fid=%08llx",
                       mgr.c_str(), fid);
    } else {
      eos_static_err("msg=\"query error\" fid=%08llx status=%d code=%d", fid,
                     status.status, status.code);

      if ((status.code >= 100) &&
          (status.code <= 300)) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        eos_static_info("msg=\"retry query\" fid=%08llx query=\"%s\"", fid,
                        query.c_str());
      } else {
        eos_static_err("msg=\"failed to retrieve metadata from mgm\" manager=%s "
                       "fid=%08llx", mgr.c_str(), fid);
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

  if (fmd.mProtoFmd.fid() != fid) {
    eos_static_err("msg=\"received wrong meta data from mgm\" fid=%08llx "
                   "recv_fid=%08llx", fmd.mProtoFmd.fid(), fid);
    return EIO;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Execute "fs dumpmd" on the MGM node
//------------------------------------------------------------------------------
bool
FmdMgmHandler::ExecuteDumpmd(const std::string& mgm_host,
                               eos::common::FileSystem::fsid_t fsid,
                               std::string& fn_output)
{
  // Create temporary file used as output for the command
  char tmpfile[] = "/tmp/efstd.XXXXXX";
  int tmp_fd = mkstemp(tmpfile);

  if (tmp_fd == -1) {
    eos_static_err("failed to create a temporary file");
    return false;
  }

  (void) close(tmp_fd);
  fn_output = tmpfile;
  std::ostringstream cmd;
  // First try to do the dumpmd using protobuf requests
  using eos::console::FsProto_DumpMdProto;
  eos::console::RequestProto request;
  eos::console::FsProto* fs = request.mutable_fs();
  FsProto_DumpMdProto* dumpmd = fs->mutable_dumpmd();
  dumpmd->set_fsid(fsid);
  dumpmd->set_display(eos::console::FsProto::DumpMdProto::MONITOR);
  request.set_format(eos::console::RequestProto::FUSE);
  std::string b64buff;

  if (eos::common::SymKey::ProtobufBase64Encode(&request, b64buff)) {
    // Increase the request timeout to 4 hours
    cmd << "env XrdSecPROTOCOL=sss XRD_REQUESTTIMEOUT=14400 "
        << "xrdcp -f -s \"root://" << mgm_host.c_str() << "/"
        << "/proc/admin/?mgm.cmd.proto=" << b64buff << "\" "
        << tmpfile;
    eos::common::ShellCmd bootcmd(cmd.str().c_str());
    eos::common::cmd_status rc = bootcmd.wait(1800);

    if (rc.exit_code) {
      eos_static_err("%s returned %d", cmd.str().c_str(), rc.exit_code);
    } else {
      eos_static_debug("%s executed successfully", cmd.str().c_str());
      return true;
    }
  } else {
    eos_static_err("msg=\"failed to serialize protobuf request for dumpmd\"");
  }

  eos_static_info("msg=\"falling back to classic dumpmd command\"");
  cmd.str("");
  cmd.clear();
  cmd << "env XrdSecPROTOCOL=sss XRD_STREAMTIMEOUT=600 xrdcp -f -s \""
      << "root://" << mgm_host.c_str() << "/"
      << "/proc/admin/?&mgm.format=fuse&mgm.cmd=fs&mgm.subcmd=dumpmd&"
      << "mgm.dumpmd.option=m&mgm.fsid=" << fsid << "\" "
      << tmpfile;
  eos::common::ShellCmd bootcmd(cmd.str().c_str());
  eos::common::cmd_status rc = bootcmd.wait(1800);

  if (rc.exit_code) {
    eos_static_err("%s returned %d", cmd.str().c_str(), rc.exit_code);
    return false;
  }
  eos_static_debug("%s executed successfully", cmd.str().c_str());
  return true;
}

EOSFSTNAMESPACE_END
