//-----------------------------------------------------------------------------
// File: GrpcWncInterface.cc
// Author: Branko Blagojevic <branko.blagojevic@comtrade.com>
// Author: Ivan Arizanovic <ivan.arizanovic@comtrade.com>
//-----------------------------------------------------------------------------

#ifdef EOS_GRPC

//-----------------------------------------------------------------------------
#include "GrpcWncInterface.hh"
//-----------------------------------------------------------------------------
#include "common/Fmd.hh"
#include "common/Utils.hh"
#include "common/StringTokenizer.hh"
#include "console/commands/HealthCommand.hh"
#include "console/ConsoleMain.hh"
#include "mgm/Acl.hh"
#include "mgm/Egroup.hh"
#include "mgm/GeoTreeEngine.hh"
#include "mgm/proc/admin/AccessCmd.hh"
#include "mgm/proc/admin/ConfigCmd.hh"
#include "mgm/proc/admin/ConvertCmd.hh"
#include "mgm/proc/admin/DebugCmd.hh"
#include "mgm/proc/admin/FsCmd.hh"
#include "mgm/proc/admin/FsckCmd.hh"
#include "mgm/proc/admin/GroupCmd.hh"
#include "mgm/proc/admin/IoCmd.hh"
#include "mgm/proc/admin/NodeCmd.hh"
#include "mgm/proc/admin/NsCmd.hh"
#include "mgm/proc/admin/QuotaCmd.hh"
#include "mgm/proc/admin/SpaceCmd.hh"
#include "mgm/proc/admin/EvictCmd.hh"
#include "mgm/proc/user/AclCmd.hh"
#include "mgm/proc/user/NewfindCmd.hh"
#include "mgm/proc/user/QoSCmd.hh"
#include "mgm/proc/user/RecycleCmd.hh"
#include "mgm/proc/user/RmCmd.hh"
#include "mgm/proc/user/RouteCmd.hh"
#include "mgm/proc/user/TokenCmd.hh"
#include "mgm/XrdMgmOfs.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IView.hh"
//-----------------------------------------------------------------------------
#include <XrdPosix/XrdPosixXrootd.hh>
//-----------------------------------------------------------------------------

EOSMGMNAMESPACE_BEGIN

grpc::Status
GrpcWncInterface::ExecCmd(eos::common::VirtualIdentity& vid,
                          const eos::console::RequestProto* request,
                          eos::console::ReplyProto* reply)
{
  mVid = &vid;
  mRequest = request;
  mReply = reply;
  mJsonFormat = (mRequest->format() ==
                 eos::console::RequestProto_FormatType_JSON);
  RoleChanger();

  switch (mRequest->command_case()) {
  case eos::console::RequestProto::kAccess:
    return Access();
    break;

  case eos::console::RequestProto::kAcl:
    return Acl();
    break;

  case eos::console::RequestProto::kArchive:
    return Archive();
    break;

  case eos::console::RequestProto::kAttr:
    return Attr();
    break;

  case eos::console::RequestProto::kBackup:
    return Backup();
    break;

  case eos::console::RequestProto::kChmod:
    return Chmod();
    break;

  case eos::console::RequestProto::kChown:
    return Chown();
    break;

  case eos::console::RequestProto::kConfig:
    return Config();
    break;

  case eos::console::RequestProto::kConvert:
    return Convert();
    break;

  case eos::console::RequestProto::kCp:
    return Cp();
    break;

  case eos::console::RequestProto::kDebug:
    return Debug();
    break;

  case eos::console::RequestProto::kEvict:
    return Evict();
    break;

  case eos::console::RequestProto::kFile:
    return File();
    break;

  case eos::console::RequestProto::kFileinfo:
    return Fileinfo();
    break;

  case eos::console::RequestProto::kFs:
    return Fs();
    break;

  case eos::console::RequestProto::kFsck:
    return Fsck();
    break;

  case eos::console::RequestProto::kGeosched:
    return Geosched();
    break;

  case eos::console::RequestProto::kGroup:
    return Group();
    break;

  case eos::console::RequestProto::kHealth: {
    for (auto it : mVid->allowed_uids)
      if ((it == 0 && mVid->uid == 0) || it == 2 || it == 3) {
        return Health();
      }

    mReply->set_std_err("Error: Permission denied");
    mReply->set_retc(EACCES);
    break;
  }

  case eos::console::RequestProto::kIo:
    return Io();
    break;

  case eos::console::RequestProto::kMap:
    return Map();
    break;

  case eos::console::RequestProto::kMember:
    return Member();
    break;

  case eos::console::RequestProto::kMkdir:
    return Mkdir();
    break;

  case eos::console::RequestProto::kMv:
    return Mv();
    break;

  case eos::console::RequestProto::kNode:
    return Node();
    break;

  case eos::console::RequestProto::kNs:
    return Ns();
    break;

  case eos::console::RequestProto::kQos:
    return Qos();
    break;

  case eos::console::RequestProto::kQuota:
    return Quota();
    break;

  case eos::console::RequestProto::kRecycle:
    return Recycle();
    break;

  case eos::console::RequestProto::kRm:
    return Rm();
    break;

  case eos::console::RequestProto::kRmdir:
    return Rmdir();
    break;

  case eos::console::RequestProto::kRoute:
    return Route();
    break;

  case eos::console::RequestProto::kSpace:
    return Space();
    break;

  case eos::console::RequestProto::kStat:
    return Stat();
    break;

  case eos::console::RequestProto::kStatus:
    return Status();
    break;

  case eos::console::RequestProto::kToken:
    return Token();
    break;

  case eos::console::RequestProto::kTouch:
    return Touch();
    break;

  case eos::console::RequestProto::kVersion:
    return Version();
    break;

  case eos::console::RequestProto::kVid: {
    for (auto it : mVid->allowed_uids)
      if ((it == 0 && mVid->uid == 0) || it == 2 || it == 3) {
        return Vid();
      }

    mReply->set_std_err("Error: Permission denied");
    mReply->set_retc(EACCES);
    break;
  }

  case eos::console::RequestProto::kWho:
    return Who();
    break;

  case eos::console::RequestProto::kWhoami:
    return Whoami();
    break;

  default:
    mReply->set_std_err("error: command not supported");
    mReply->set_retc(EINVAL);
    break;
  }

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::ExecStreamCmd(eos::common::VirtualIdentity& vid,
                                const eos::console::RequestProto* request,
                                ServerWriter<eos::console::ReplyProto>* writer)
{
  mVid = &vid;
  mRequest = request;
  mWriter = writer;
  mJsonFormat = (mRequest->format() ==
                 eos::console::RequestProto_FormatType_JSON);
  grpc::Status retc;
  RoleChanger();

  switch (mRequest->command_case()) {
  case eos::console::RequestProto::kFind:
    retc = Find();
    break;

  case eos::console::RequestProto::kLs:
    retc = Ls();
    break;

  default:
    retc = grpc::Status::OK;
    break;
  }

  return retc;
}

void GrpcWncInterface::RoleChanger()
{
  int errc = 0;
  uid_t uid;
  gid_t gid;

  // Change the user role ID
  if (!mRequest->auth().role().username().empty()) {
    uid = eos::common::Mapping::UserNameToUid(mRequest->auth().role().username(),
          errc);
  } else if (mRequest->auth().role().uid() != 0) {
    uid = mRequest->auth().role().uid();
  } else {
    uid = mVid->uid;
  }

  if (mVid->uid != uid) {
    bool is_member = false;

    for (auto it : mVid->allowed_uids)
      if (it == uid) {
        mVid->uid = uid;
        is_member = true;
        break;
      }

    if (!is_member) {
      if (mVid->sudoer) {
        mVid->uid = uid;
        mVid->allowed_uids.insert(uid);
      } else {
        mVid->uid = 99;
      }
    }
  }

  // Change the group role ID
  if (!mRequest->auth().role().groupname().empty()) {
    gid = eos::common::Mapping::GroupNameToGid(mRequest->auth().role().groupname(),
          errc);
  } else if (mRequest->auth().role().gid() != 0) {
    gid = mRequest->auth().role().gid();
  } else {
    gid = mVid->gid;
  }

  if (mVid->gid != gid) {
    bool is_member = false;

    for (auto it : mVid->allowed_gids)
      if (it == gid) {
        mVid->gid = gid;
        is_member = true;
        break;
      }

    if (!is_member) {
      if (mVid->sudoer) {
        mVid->gid = gid;
        mVid->allowed_gids.insert(gid);
      } else {
        mVid->gid = 99;
      }
    }
  }
}


void GrpcWncInterface::ExecProcCmd(std::string input, bool admin)
{
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string std_out, std_err;

  if (mJsonFormat) {
    input += "&mgm.format=json";
  }

  if (admin) {
    cmd.open("/proc/admin", input.c_str(), *mVid, &error);
  } else {
    cmd.open("/proc/user", input.c_str(), *mVid, &error);
  }

  cmd.close();
  cmd.AddOutput(std_out, std_err);

  if (mJsonFormat) {
    std_out = cmd.GetStdJson();
  }

  mReply->set_std_out(std_out);
  mReply->set_std_err(std_err);
  mReply->set_retc(cmd.GetRetc());
}

grpc::Status GrpcWncInterface::Access()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::AccessCmd accesscmd(std::move(req), *mVid);
  *mReply = accesscmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Acl()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::AclCmd aclcmd(std::move(req), *mVid);
  *mReply = aclcmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Archive()
{
  std::string subcmd = mRequest->archive().command();
  std::string cmd_in = "mgm.cmd=archive&mgm.subcmd=" + subcmd;

  if (subcmd == "kill") {
    cmd_in += "&mgm.archive.option=" + mRequest->archive().job_uuid();
  } else if (subcmd == "transfers") {
    cmd_in += "&mgm.archive.option=" + mRequest->archive().selection();
  } else {
    if (mRequest->archive().retry()) {
      cmd_in += "&mgm.archive.option=r";
    }

    cmd_in += "&mgm.archive.path=" + mRequest->archive().path();
  }

  ExecProcCmd(cmd_in, false);
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Attr()
{
  std::string cmd_in;
  std::string path = mRequest->attr().md().path();
  eos::console::AttrCmd subcmd = mRequest->attr().cmd();
  std::string key = mRequest->attr().key();
  errno = 0;

  if (path.empty()) {
    if (mRequest->attr().md().type() == eos::console::FILE) {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(
                 gOFS->eosFileService->getFileMD(mRequest->attr().md().id()).get());
      } catch (eos::MDException& e) {
        path = "";
        errno = e.getErrno();
      }
    } else {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(
                 gOFS->eosDirectoryService->getContainerMD(mRequest->attr().md().id()).get());
      } catch (eos::MDException& e) {
        path = "";
        errno = e.getErrno();
      }
    }

    if (path.empty()) {
      mReply->set_std_err("error:path is empty");
      mReply->set_retc(EINVAL);
      return grpc::Status::OK;
    }
  }

  cmd_in = "mgm.cmd=attr&mgm.path=" + path;

  if (subcmd == eos::console::AttrCmd::ATTR_LS) {
    cmd_in += "&mgm.subcmd=ls";
  } else if (subcmd == eos::console::AttrCmd::ATTR_SET) {
    cmd_in += "&mgm.subcmd=set";
    std::string value = mRequest->attr().value();

    // Set default values based on layout
    if (key == "default") {
      std::vector<std::string> val;

      if (value == "replica")
        val = {"4k", "adler", "replica", "2", "default"};
      else if (value == "raiddp")
        val = {"1M", "adler", "raiddp", "6", "default", "crc32c"};
      else if (value == "raid5")
        val = {"1M", "adler", "raid5", "5", "default", "crc32c"};
      else if (value == "raid6")
        val = {"1M", "adler", "raid6", "6", "default", "crc32c"};
      else if (value == "archive")
        val = {"1M", "adler", "archive", "8", "default", "crc32c"};
      else if (value == "qrain")
        val = {"1M", "adler", "qrain", "12", "default", "crc32c"};
      else {
        mReply->set_std_err("Error: Value are not allowed");
        mReply->set_retc(EINVAL);
        return grpc::Status::OK;
      }

      ProcCommand cmd;
      XrdOucErrInfo error;
      std::string set_def;
      set_def = cmd_in + "&mgm.attr.key=sys.forced.blocksize&mgm.attr.value=" +
                val[0];
      cmd.open("/proc/user", set_def.c_str(), *mVid, &error);
      set_def = cmd_in + "&mgm.attr.key=sys.forced.checksum&mgm.attr.value=" + val[1];
      cmd.open("/proc/user", set_def.c_str(), *mVid, &error);
      set_def = cmd_in + "&mgm.attr.key=sys.forced.layout&mgm.attr.value=" + val[2];
      cmd.open("/proc/user", set_def.c_str(), *mVid, &error);
      set_def = cmd_in + "&mgm.attr.key=sys.forced.nstripes&mgm.attr.value=" + val[3];
      cmd.open("/proc/user", set_def.c_str(), *mVid, &error);
      set_def = cmd_in + "&mgm.attr.key=sys.forced.space&mgm.attr.value=" + val[4];
      cmd.open("/proc/user", set_def.c_str(), *mVid, &error);

      if (value != "replica") {
        set_def = cmd_in + "&mgm.attr.key=sys.forced.blockchecksum&mgm.attr.value=" +
                  val[5];
        cmd.open("/proc/user", set_def.c_str(), *mVid, &error);
      }
    }

    if (key == "sys.forced.placementpolicy" ||
        key == "user.forced.placementpolicy") {
      std::string policy;
      eos::common::SymKey::DeBase64(value, policy);

      // Check placement policy
      if (policy != "scattered" &&
          policy.rfind("hybrid:", 0) != 0 &&
          policy.rfind("gathered:", 0) != 0) {
        mReply->set_std_err("Error: placement policy '" + policy + "' is invalid\n");
        mReply->set_retc(EINVAL);
        return grpc::Status::OK;
      }

      // Check geotag in case of hybrid or gathered policy
      if (policy != "scattered") {
        std::string targetgeotag = policy.substr(policy.find(':') + 1);
        std::string tmp_geotag = eos::common::SanitizeGeoTag(targetgeotag);

        if (tmp_geotag != targetgeotag) {
          mReply->set_std_err(tmp_geotag);
          mReply->set_retc(EINVAL);
          return grpc::Status::OK;
        }
      }
    }

    cmd_in += "&mgm.attr.key=" + key;
    cmd_in += "&mgm.attr.value=" + value;
  } else if (subcmd == eos::console::AttrCmd::ATTR_GET) {
    cmd_in += "&mgm.subcmd=get";
    cmd_in += "&mgm.attr.key=" + key;
  } else if (subcmd == eos::console::AttrCmd::ATTR_RM) {
    cmd_in += "&mgm.subcmd=rm";
    cmd_in += "&mgm.attr.key=" + key;
  } else if (subcmd == eos::console::AttrCmd::ATTR_LINK) {
    cmd_in += "&mgm.subcmd=set";
    cmd_in += "&mgm.attr.key=sys.attr.link";
    cmd_in += "&mgm.attr.value=" + mRequest->attr().link();
  } else if (subcmd == eos::console::AttrCmd::ATTR_UNLINK) {
    cmd_in += "&mgm.subcmd=rm";
    cmd_in += "&mgm.attr.key=sys.attr.link";
  } else if (subcmd == eos::console::AttrCmd::ATTR_FOLD) {
    cmd_in += "&mgm.subcmd=fold";
  }

  if (mRequest->attr().recursive()) {
    cmd_in += "&mgm.option=r";
  }

  ExecProcCmd(cmd_in, false);
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Backup()
{
  std::string src = mRequest->backup().src_url();
  std::string dst = mRequest->backup().dst_url();
  XrdCl::URL src_url(src.c_str()), dst_url(dst.c_str());

  // Check that source is valid XRootD URL
  if (!src_url.IsValid()) {
    mReply->set_std_err("Error: Source is not valid XRootD URL: " + src);
    mReply->set_retc(EINVAL);
    return grpc::Status::OK;
  }

  // Check that destination is valid XRootD URL
  if (!dst_url.IsValid()) {
    mReply->set_std_err("Error: Destination is not valid XRootD URL: " + dst);
    mReply->set_retc(EINVAL);
    return grpc::Status::OK;
  }

  std::string cmd_in = "mgm.cmd=backup&mgm.backup.src=" + src + "&mgm.backup.dst="
                       + dst;

  if (mRequest->backup().ctime()) {
    struct timeval tv;

    if (gettimeofday(&tv, NULL)) {
      mReply->set_std_err("Error: Failed getting current timestamp");
      mReply->set_retc(EINVAL);
      return grpc::Status::OK;
    }

    cmd_in += "&mgm.backup.ttime=ctime&mgm.backup.vtime=" + std::to_string(
                tv.tv_sec - mRequest->backup().ctime());
  }

  if (mRequest->backup().mtime()) {
    struct timeval tv;

    if (gettimeofday(&tv, NULL)) {
      mReply->set_std_err("Error: Failed getting current timestamp");
      mReply->set_retc(errno);
      return grpc::Status::OK;
    }

    cmd_in += "&mgm.backup.ttime=mtime&mgm.backup.vtime=" + std::to_string(
                tv.tv_sec - mRequest->backup().mtime());
  }

  if (!mRequest->backup().xattr().empty()) {
    cmd_in += "&mgm.backup.excl_xattr=" + mRequest->backup().xattr();
  }

  ExecProcCmd(cmd_in);
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Chmod()
{
  std::string cmd_in;
  std::string path = mRequest->chmod().md().path();
  errno = 0;

  if (path.empty()) {
    if (mRequest->chmod().md().type() == eos::console::FILE) {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(gOFS->eosFileService->getFileMD(
                                       mRequest->chmod().md().id()
                                     ).get());
      } catch (eos::MDException& e) {
        path = "";
        errno = e.getErrno();
      }
    } else {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(gOFS->eosDirectoryService->getContainerMD(
                                       mRequest->chmod().md().id()
                                     ).get());
      } catch (eos::MDException& e) {
        path = "";
        errno = e.getErrno();
      }
    }

    if (path.empty()) {
      mReply->set_std_err("error:path is empty");
      mReply->set_retc(EINVAL);
      return grpc::Status::OK;
    }
  }

  cmd_in = "mgm.cmd=chmod";
  cmd_in += "&mgm.path=" + path;
  cmd_in += "&mgm.chmod.mode=" + std::to_string(mRequest->chmod().mode());

  if (mRequest->chmod().recursive()) {
    cmd_in += "&mgm.option=r";
  }

  ExecProcCmd(cmd_in, false);
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Chown()
{
  std::string path = mRequest->chown().md().path();
  uid_t uid = mRequest->chown().owner().uid();
  gid_t gid = mRequest->chown().owner().gid();
  std::string username = mRequest->chown().owner().username();
  std::string groupname = mRequest->chown().owner().groupname();
  errno = 0;
  std::string cmd_in = "mgm.cmd=chown";

  if (path.empty()) {
    if (mRequest->chown().md().type() == eos::console::FILE) {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(gOFS->eosFileService->getFileMD(
                                       mRequest->chown().md().id()
                                     ).get());
      } catch (eos::MDException& e) {
        path = "";
        errno = e.getErrno();
      }
    } else {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(gOFS->eosDirectoryService->getContainerMD(
                                       mRequest->chown().md().id()
                                     ).get());
      } catch (eos::MDException& e) {
        path = "";
        errno = e.getErrno();
      }
    }

    if (path.empty()) {
      mReply->set_std_err("error:path is empty");
      mReply->set_retc(EINVAL);
      return grpc::Status::OK;
    }
  }

  cmd_in += "&mgm.path=" + path;

  if (mRequest->chown().user_only() ||
      mRequest->chown().user_only() == mRequest->chown().group_only()) {
    if (!username.empty()) {
      cmd_in += "&mgm.chown.owner=" + username;
    } else {
      cmd_in += "&mgm.chown.owner=" + std::to_string(uid);
    }
  }

  if (mRequest->chown().group_only() ||
      mRequest->chown().user_only() == mRequest->chown().group_only()) {
    if (!groupname.empty()) {
      cmd_in += ":" + groupname;
    } else {
      cmd_in += ":" + std::to_string(gid);
    }
  }

  if (mRequest->chown().recursive() || mRequest->chown().nodereference()) {
    cmd_in += "&mgm.chown.option=";

    if (mRequest->chown().recursive()) {
      cmd_in += "r";
    }

    if (mRequest->chown().nodereference()) {
      cmd_in += "h";
    }
  }

  ExecProcCmd(cmd_in, false);
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Config()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::ConfigCmd configcmd(std::move(req), *mVid);
  *mReply = configcmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Convert()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::ConvertCmd convertcmd(std::move(req), *mVid);
  *mReply = convertcmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Cp()
{
  switch (mRequest->cp().subcmd_case()) {
  case eos::console::CpProto::kCksum: {
    XrdCl::URL url("root://localhost//dummy");
    auto* fs = new XrdCl::FileSystem(url);

    if (!fs) {
      mReply->set_std_err("Warning: failed to get new FS object [attempting checksum]\n");
      return grpc::Status::OK;
    }

    std::string path = mRequest->cp().cksum().path();
    size_t pos = path.rfind("//");

    if (pos != std::string::npos) {
      path.erase(0, pos + 1);
    }

    XrdCl::Buffer arg;
    XrdCl::XRootDStatus status;
    XrdCl::Buffer* response = nullptr;
    arg.FromString(path);
    status = fs->Query(XrdCl::QueryCode::Checksum, arg, response);

    if (status.IsOK()) {
      XrdOucString xsum = response->GetBuffer();
      xsum.replace("eos ", "");
      std::string msg = "checksum=";
      msg += xsum.c_str();
      mReply->set_std_out(msg);
    } else {
      std::string msg = "Warning: failed getting checksum for ";
      msg += path;
      mReply->set_std_err(msg);
    }

    delete response;
    delete fs;
    break;
  }

  case eos::console::CpProto::kKeeptime: {
    if (mRequest->cp().keeptime().set()) {
      // Set atime and mtime
      std::string path = mRequest->cp().keeptime().path();
      char update[1024];
      sprintf(update,
              "?eos.app=eoscp&mgm.pcmd=utimes&tv1_sec=%llu&tv1_nsec=%llu&tv2_sec=%llu&tv2_nsec=%llu",
              (unsigned long long) mRequest->cp().keeptime().atime().seconds(),
              (unsigned long long) mRequest->cp().keeptime().atime().nanos(),
              (unsigned long long) mRequest->cp().keeptime().mtime().seconds(),
              (unsigned long long) mRequest->cp().keeptime().mtime().nanos()
             );
      XrdOucString query = "root://localhost/";
      query += path.c_str();
      query += update;
      char value[4096];
      value[0] = 0;
      long long update_rc = XrdPosixXrootd::QueryOpaque(query.c_str(),
                            value, 4096);
      bool updateok = (update_rc >= 0);

      if (updateok) {
        // Parse the stat output
        char tag[1024];
        int tmp_retc;
        int items = sscanf(value, "%1023s retc=%d", tag, &tmp_retc);
        updateok = ((items == 2) && (strcmp(tag, "utimes:") == 0));
      }

      if (!updateok) {
        std::string msg;
        msg += "Warning: access and modification time could not be preserved for ";
        msg += path;
        msg += "\nQuery: ";
        msg += query.c_str();
        mReply->set_std_err(msg);
      }
    } else {
      // Get atime and mtime
      std::string path = mRequest->cp().keeptime().path();
      XrdOucString url = "root://localhost/";
      url += path.c_str();
      struct stat buf;

      if (XrdPosixXrootd::Stat(url.c_str(), &buf) == 0) {
        std::string msg;
        msg += "atime:";
        msg += std::to_string(buf.st_atime);
        msg += "mtime:";
        msg += std::to_string(buf.st_mtime);
        mReply->set_std_out(msg);
      } else {
        std::string msg = "Warning: failed getting stat information for ";
        msg += path;
        mReply->set_std_err(msg);
      }
    }

    break;
  }

  default: {
    mReply->set_std_err("Error: subcommand is not supported");
    mReply->set_retc(EINVAL);
  }
  }

  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Debug()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::DebugCmd debugcmd(std::move(req), *mVid);
  *mReply = debugcmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Evict()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::EvictCmd evictcmd(std::move(req), *mVid);
  *mReply = evictcmd.ProcessRequest();
  return grpc::Status::OK;
}

bool
File_EnvFstToFmd(XrdOucEnv& env, eos::common::FmdHelper& fmd);

int
File_GetRemoteAttribute(const char* manager, const char* key,
                        const char* path, XrdOucString& attribute);

int
File_GetRemoteFmdFromLocalDb(const char* manager, const char* shexfid,
                             const char* sfsid, eos::common::FmdHelper& fmd);

grpc::Status GrpcWncInterface::File()
{
  std::string path = mRequest->file().md().path();
  uint64_t fid = 0;

  if (path.empty() &&
      mRequest->file().FileCommand_case() != eos::console::FileProto::kSymlink) {
    // get by inode
    if (mRequest->file().md().ino()) {
      fid = eos::common::FileId::InodeToFid(mRequest->file().md().ino());
    }
    // get by fileid
    else if (mRequest->file().md().id()) {
      fid = mRequest->file().md().id();
    }

    try {
      eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
      path = gOFS->eosView->getUri(gOFS->eosFileService->getFileMD(fid).get());
    } catch (eos::MDException& e) {
      path = "";
      errno = e.getErrno();
    }
  }

  if (path.empty()) {
    mReply->set_std_err("error: path is empty");
    mReply->set_retc(EINVAL);
    return grpc::Status::OK;
  }

  std::string std_out, std_err;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string cmd_in = "mgm.cmd=file";

  switch (mRequest->file().FileCommand_case()) {
  case eos::console::FileProto::kAdjustreplica: {
    cmd_in += "&mgm.subcmd=adjustreplica";

    if (fid) {
      cmd_in += "&mgm.file.id=" + std::to_string(fid);
    } else {
      // this has a problem with '&' encoding, prefer to use create by fid ..
      cmd_in += "&mgm.path=" + path;
    }

    if (!mRequest->file().adjustreplica().space().empty()) {
      cmd_in += "&mgm.file.desiredspace=" + mRequest->file().adjustreplica().space();

      if (!mRequest->file().adjustreplica().subgroup().empty()) {
        cmd_in += "&mgm.file.desiredsubgroup=" +
                  mRequest->file().adjustreplica().subgroup();
      }
    }

    if (mRequest->file().adjustreplica().nodrop()) {
      cmd_in += "&mgm.file.option=--nodrop";
    }

    break;
  }

  case eos::console::FileProto::kCheck: {
    cmd_in += "&mgm.subcmd=getmdlocation";
    cmd_in += "&mgm.format=fuse";
    cmd_in += "&mgm.path=";
    cmd_in += path;
    XrdOucString option = mRequest->file().check().options().c_str();
    cmd.open("/proc/user", cmd_in.c_str(), *mVid, &error);
    cmd.AddOutput(std_out, std_err);
    cmd.close();
    XrdOucEnv* result = new XrdOucEnv(std_out.c_str());
    std_out = "";
    bool silent = false;

    if (!result) {
      mReply->set_std_err("error: getmdlocation query failed\n");
      mReply->set_retc(EINVAL);
      return grpc::Status::OK;
    }

    int envlen = 0;
    XrdOucEnv* newresult = new XrdOucEnv(result->Env(envlen));
    delete result;
    XrdOucString checksumattribute = "NOTREQUIRED";
    bool consistencyerror = false;

    if (envlen) {
      XrdOucString ns_path = newresult->Get("mgm.nspath");
      XrdOucString checksumtype = newresult->Get("mgm.checksumtype");
      XrdOucString checksum = newresult->Get("mgm.checksum");
      XrdOucString size = newresult->Get("mgm.size");

      if ((option.find("%silent") == STR_NPOS) && (!silent)) {
        std_out += "path=\"";
        std_out += ns_path.c_str();
        std_out += "\" fxid=\"";
        std_out += newresult->Get("mgm.fid0");
        std_out += "\" size=\"";
        std_out += size.c_str();
        std_out += "\" nrep=\"";
        std_out += newresult->Get("mgm.nrep");
        std_out += "\" checksumtype=\"";
        std_out += checksumtype.c_str();
        std_out += "\" checksum=\"";
        std_out += newresult->Get("mgm.checksum");
        std_out += "\"\n";
      }

      int i = 0;
      XrdOucString inconsistencylable = "";
      int nreplicaonline = 0;

      for (i = 0; i < 255; i++) {
        XrdOucString repurl = "mgm.replica.url";
        repurl += i;
        XrdOucString repfid = "mgm.fid";
        repfid += i;
        XrdOucString repfsid = "mgm.fsid";
        repfsid += i;
        XrdOucString repbootstat = "mgm.fsbootstat";
        repbootstat += i;
        XrdOucString repfstpath = "mgm.fstpath";
        repfstpath += i;

        if (newresult->Get(repurl.c_str())) {
          // Query
          XrdCl::StatInfo* stat_info = 0;
          XrdCl::XRootDStatus status;
          std::string address = "root://";
          address += newresult->Get(repurl.c_str());
          address += "//dummy";
          XrdCl::URL url(address.c_str());

          if (!url.IsValid()) {
            mReply->set_std_err("error=URL is not valid: " + address);
            mReply->set_retc(EINVAL);
            return grpc::Status::OK;
          }

          // Get XrdCl::FileSystem object
          std::unique_ptr<XrdCl::FileSystem> fs {new XrdCl::FileSystem(url)};

          if (!fs) {
            mReply->set_std_err("error=failed to get new FS object");
            mReply->set_retc(ECOMM);
            return grpc::Status::OK;
          }

          XrdOucString bs = newresult->Get(repbootstat.c_str());
          bool down = (bs != "booted");
          int retc = 0;
          int oldsilent = silent;
          eos::common::FmdHelper fmd;

          if ((option.find("%silent")) != STR_NPOS) {
            silent = true;
          }

          if (down && ((option.find("%force")) == STR_NPOS)) {
            consistencyerror = true;
            inconsistencylable = "DOWN";

            if (!silent) {
              std_err += "error: unable to retrieve file meta data from ";
              std_err += newresult->Get(repurl.c_str());
              std_err += " [ status=";
              std_err += bs.c_str();
              std_err += " ]\n";
            }
          } else {
            if ((option.find("%checksumattr") != STR_NPOS)) {
              checksumattribute = "";

              if ((retc = File_GetRemoteAttribute(newresult->Get(repurl.c_str()),
                                                  "user.eos.checksum",
                                                  newresult->Get(repfstpath.c_str()),
                                                  checksumattribute))) {
                if (!silent) {
                  std_err += "error: unable to retrieve extended attribute from ";
                  std_err += newresult->Get(repurl.c_str());
                  std_err += " [";
                  std_err += std::to_string(retc);
                  std_err += "]\n";
                }
              }
            }

            //..................................................................
            // Do a remote stat using XrdCl::FileSystem
            //..................................................................
            uint64_t rsize;
            XrdOucString statpath = newresult->Get(repfstpath.c_str());

            if (!statpath.beginswith("/")) {
              // base 64 encode this path
              XrdOucString statpath64 = "";
              eos::common::SymKey::Base64(statpath, statpath64);
              statpath = "/#/";
              statpath += statpath64;
            }

            status = fs->Stat(statpath.c_str(), stat_info);

            if (!status.IsOK()) {
              consistencyerror = true;
              inconsistencylable = "STATFAILED";
              rsize = -1;
            } else {
              rsize = stat_info->GetSize();
            }

            // Free memory
            delete stat_info;

            if ((retc = File_GetRemoteFmdFromLocalDb(newresult->Get(repurl.c_str()),
                        newresult->Get(repfid.c_str()),
                        newresult->Get(repfsid.c_str()), fmd))) {
              if (!silent) {
                std_err += "error: unable to retrieve file meta data from ";
                std_err += newresult->Get(repurl.c_str());
                std_err += " [";
                std_err += std::to_string(retc);
                std_err += "]\n";
              }

              consistencyerror = true;
              inconsistencylable = "NOFMD";
            } else {
              XrdOucString cx = fmd.mProtoFmd.checksum().c_str();

              for (unsigned int k = (cx.length() / 2); k < SHA_DIGEST_LENGTH; k++) {
                cx += "00";
              }

              XrdOucString disk_cx = fmd.mProtoFmd.diskchecksum().c_str();

              for (unsigned int k = (disk_cx.length() / 2); k < SHA_DIGEST_LENGTH; k++) {
                disk_cx += "00";
              }

              if ((option.find("%size")) != STR_NPOS) {
                char ss[1024];
                sprintf(ss, "%" PRIu64, fmd.mProtoFmd.size());
                XrdOucString sss = ss;

                if (sss != size) {
                  consistencyerror = true;
                  inconsistencylable = "SIZE";
                } else {
                  if (fmd.mProtoFmd.size() != (unsigned long long) rsize) {
                    if (!consistencyerror) {
                      consistencyerror = true;
                      inconsistencylable = "FSTSIZE";
                    }
                  }
                }
              }

              if ((option.find("%checksum")) != STR_NPOS) {
                if (cx != checksum) {
                  consistencyerror = true;
                  inconsistencylable = "CHECKSUM";
                }
              }

              if ((option.find("%checksumattr") != STR_NPOS)) {
                if ((checksumattribute.length() < 8) || (!cx.beginswith(checksumattribute))) {
                  consistencyerror = true;
                  inconsistencylable = "CHECKSUMATTR";
                }
              }

              nreplicaonline++;

              if (!silent) {
                std_out += "nrep=\"";
                std_out += std::to_string(i);
                std_out += "\" fsid=\"";
                std_out += newresult->Get(repfsid.c_str());
                std_out += "\" host=\"";
                std_out += newresult->Get(repurl.c_str());
                std_out += "\" fstpath=\"";
                std_out += newresult->Get(repfstpath.c_str());
                std_out += "\" size=\"";
                std_out += std::to_string(fmd.mProtoFmd.size());
                std_out += "\" statsize=\"";
                std_out += std::to_string(static_cast<long long>(rsize));
                std_out += "\" checksum=\"";
                std_out += cx.c_str();
                std_out += "\" diskchecksum=\"";
                std_out += disk_cx.c_str();
                std_out += "\"";

                if ((option.find("%checksumattr") != STR_NPOS)) {
                  std_out += " checksumattr=\"";
                  std_out += checksumattribute.c_str();
                  std_out += "\"";
                }

                std_out += "\n";
              }
            }
          }

          if ((option.find("%silent")) != STR_NPOS) {
            silent = oldsilent;
          }
        } else {
          break;
        }
      }

      if ((option.find("%nrep")) != STR_NPOS) {
        int nrep = 0;
        int stripes = 0;

        if (newresult->Get("mgm.stripes")) {
          stripes = atoi(newresult->Get("mgm.stripes"));
        }

        if (newresult->Get("mgm.nrep")) {
          nrep = atoi(newresult->Get("mgm.nrep"));
        }

        if (nrep != stripes) {
          consistencyerror = true;

          if (inconsistencylable != "NOFMD") {
            inconsistencylable = "REPLICA";
          }
        }
      }

      if ((option.find("%output")) != STR_NPOS) {
        if (consistencyerror) {
          std_out += "INCONSISTENCY ";
          std_out += inconsistencylable.c_str();
          std_out += " path=";
          std_out += path.c_str();
          std_out += " fxid=";
          std_out += newresult->Get("mgm.fid0");
          std_out += " size=";
          std_out += size.c_str();
          std_out += " stripes=";
          std_out += newresult->Get("mgm.stripes");
          std_out += " nrep=";
          std_out += newresult->Get("mgm.nrep");
          std_out += " nrepstored=";
          std_out += std::to_string(i);
          std_out += " nreponline=";
          std_out += std::to_string(nreplicaonline);
          std_out += " checksumtype=";
          std_out += checksumtype.c_str();
          std_out += " checksum=";
          std_out += newresult->Get("mgm.checksum");
          std_out += "\n";
        }
      }

      mReply->set_std_out(std_out);
      mReply->set_std_err(std_err);

      if (consistencyerror) {
        mReply->set_retc(EFAULT);
      } else {
        mReply->set_retc(0);
      }
    } else {
      mReply->set_std_err("error: couldn't get meta data information\n");
      mReply->set_retc(EIO);
    }

    delete newresult;
    return grpc::Status::OK;
  }

  case eos::console::FileProto::kConvert: {
    cmd_in += "&mgm.subcmd=convert";

    if (fid) {
      cmd_in += "&mgm.file.id=" + std::to_string(fid);
    } else {
      // this has a problem with '&' encoding, prefer to use create by fid ..
      cmd_in += "&mgm.path=" + path;
    }

    if (!mRequest->file().convert().layout().empty()) {
      cmd_in += "&mgm.convert.layout=" + mRequest->file().convert().layout();
    }

    if (!mRequest->file().convert().target_space().empty()) {
      cmd_in += "&mgm.convert.space=" + mRequest->file().convert().target_space();
    }

    if (!mRequest->file().convert().placement_policy().empty()) {
      cmd_in += "&mgm.convert.placementpolicy=" +
                mRequest->file().convert().placement_policy();
    }

    if (mRequest->file().convert().sync()) {
      mReply->set_std_err("error: --sync is currently not supported");
      mReply->set_retc(EINVAL);
      return grpc::Status::OK;
    }

    if (mRequest->file().convert().rewrite()) {
      cmd_in += "&mgm.option=rewrite";
    }

    break;
  }

  case eos::console::FileProto::kCopy: {
    cmd_in += "&mgm.subcmd=copy";

    if (fid) {
      cmd_in += "&mgm.file.id=" + std::to_string(fid);
    } else {
      // this has a problem with '&' encoding, prefer to use create by fid ..
      cmd_in += "&mgm.path=" + path;
    }

    cmd_in += "&mgm.file.target=" + mRequest->file().copy().dst();

    if (mRequest->file().copy().force() || mRequest->file().copy().clone() ||
        mRequest->file().copy().silent()) {
      cmd_in += "&mgm.file.option=";

      if (mRequest->file().copy().force()) {
        cmd_in += "-f";
      }

      if (mRequest->file().copy().clone()) {
        cmd_in += "-c";
      }

      if (mRequest->file().copy().silent()) {
        cmd_in += "-s";
      }
    }

    break;
  }

  case eos::console::FileProto::kDrop: {
    cmd_in += "&mgm.subcmd=drop";

    if (fid) {
      cmd_in += "&mgm.file.id=" + std::to_string(fid);
    } else {
      // this has a problem with '&' encoding, prefer to use create by fid ..
      cmd_in += "&mgm.path=" + path;
    }

    cmd_in += "&mgm.file.fsid=" + std::to_string(mRequest->file().drop().fsid());

    if (mRequest->file().drop().force()) {
      cmd_in += "&mgm.file.force=1";
    }

    break;
  }

  case eos::console::FileProto::kLayout: {
    cmd_in += "&mgm.subcmd=layout";

    if (fid) {
      cmd_in += "&mgm.file.id=" + std::to_string(fid);
    } else {
      // this has a problem with '&' encoding, prefer to use create by fid ..
      cmd_in += "&mgm.path=" + path;
    }

    if (mRequest->file().layout().stripes()) {
      cmd_in += "&mgm.file.layout.stripes=" + std::to_string(
                  mRequest->file().layout().stripes());
    }

    if (!mRequest->file().layout().checksum().empty()) {
      cmd_in += "&mgm.file.layout.checksum=" + mRequest->file().layout().checksum();
    }

    break;
  }

  case eos::console::FileProto::kMove: {
    cmd_in += "&mgm.subcmd=move";

    if (fid) {
      cmd_in += "&mgm.file.id=" + std::to_string(fid);
    } else {
      // this has a problem with '&' encoding, prefer to use create by fid ..
      cmd_in += "&mgm.path=" + path;
    }

    cmd_in += "&mgm.file.sourcefsid=" + std::to_string(
                mRequest->file().move().fsid1());
    cmd_in += "&mgm.file.targetfsid=" + std::to_string(
                mRequest->file().move().fsid2());
    break;
  }

  case eos::console::FileProto::kPurge: {
    cmd_in += "&mgm.subcmd=purge";

    if (fid) {
      cmd_in += "&mgm.file.id=" + std::to_string(fid);
    } else {
      // this has a problem with '&' encoding, prefer to use create by fid ..
      cmd_in += "&mgm.path=" + path;
    }

    cmd_in += "&mgm.purge.version=" + std::to_string(
                mRequest->file().purge().purge_version());
    break;
  }

  case eos::console::FileProto::kReplicate: {
    cmd_in += "&mgm.subcmd=replicate";

    if (fid) {
      cmd_in += "&mgm.file.id=" + std::to_string(fid);
    } else {
      // this has a problem with '&' encoding, prefer to use create by fid ..
      cmd_in += "&mgm.path=" + path;
    }

    cmd_in += "&mgm.file.sourcefsid=" + std::to_string(
                mRequest->file().replicate().fsid1());
    cmd_in += "&mgm.file.targetfsid=" + std::to_string(
                mRequest->file().replicate().fsid2());
    break;
  }

  case eos::console::FileProto::kResync: {
    auto fsid = mRequest->file().resync().fsid();
    
    if (gOFS->QueryResync(fid, fsid)) {
      std_out = "info: resynced fid=" + std::to_string(fid);
      std_out += " on fs=" + std::to_string(fsid);
      mReply->set_std_out(std_out);
      mReply->set_retc(0);
    } else {
      std_err = "error: failed to resync";
      mReply->set_std_err(std_err);
      mReply->set_retc(-1);
    }

    return grpc::Status::OK;
  }

  case eos::console::FileProto::kSymlink: {
    std::string target = mRequest->file().symlink().target_path();

    if (target.empty()) {
      mReply->set_std_err("error:target is empty");
      mReply->set_retc(EINVAL);
      return grpc::Status::OK;
    }

    XrdOucErrInfo error;
    errno = 0;

    if (gOFS->_symlink(path.c_str(), target.c_str(), error, *mVid)) {
      mReply->set_std_err(error.getErrText());
      mReply->set_retc(errno);
      return grpc::Status::OK;
    }

    std::string msg = "info: symlinked '";
    msg += path.c_str();
    msg += "' to '";
    msg += target.c_str();
    msg += "'";
    mReply->set_std_out(msg);
    mReply->set_retc(0);
    return grpc::Status::OK;
  }

  case eos::console::FileProto::kTag: {
    cmd_in += "&mgm.subcmd=tag";
    cmd_in += "&mgm.path=" + path;
    cmd_in += "&mgm.file.tag.fsid=";

    if (mRequest->file().tag().add()) {
      cmd_in += "+";
    }

    if (mRequest->file().tag().remove()) {
      cmd_in += "-";
    }

    if (mRequest->file().tag().unlink()) {
      cmd_in += "~";
    }

    cmd_in += std::to_string(mRequest->file().tag().fsid());
    break;
  }

  case eos::console::FileProto::kVerify: {
    cmd_in += "&mgm.subcmd=verify";
    cmd_in += "&mgm.path=" + path;
    cmd_in += "&mgm.file.verify.filterid=" + std::to_string(
                mRequest->file().verify().fsid());

    if (mRequest->file().verify().checksum()) {
      cmd_in += "&mgm.file.compute.checksum=1";
    }

    if (mRequest->file().verify().commitchecksum()) {
      cmd_in += "&mgm.file.commit.checksum=1";
    }

    if (mRequest->file().verify().commitsize()) {
      cmd_in += "&mgm.file.commit.size=1";
    }

    if (mRequest->file().verify().commitfmd()) {
      cmd_in += "&mgm.file.commit.fmd=1";
    }

    if (mRequest->file().verify().rate()) {
      cmd_in += "&mgm.file.verify.rate=" + std::to_string(
                  mRequest->file().verify().rate());
    }

    if (mRequest->file().verify().resync()) {
      cmd_in += "&mgm.file.resync=1";
    }

    break;
  }

  case eos::console::FileProto::kVersion: {
    cmd_in += "&mgm.subcmd=version";

    if (fid) {
      cmd_in += "&mgm.file.id=" + std::to_string(fid);
    } else {
      // this has a problem with '&' encoding, prefer to use create by fid ..
      cmd_in += "&mgm.path=" + path;
    }

    cmd_in += "&mgm.purge.version=" + std::to_string(
                mRequest->file().version().purge_version());
    break;
  }

  case eos::console::FileProto::kVersions: {
    cmd_in += "&mgm.subcmd=versions";

    if (fid) {
      cmd_in += "&mgm.file.id=" + std::to_string(fid);
    } else {
      // this has a problem with '&' encoding, prefer to use create by fid ..
      cmd_in += "&mgm.path=" + path;
    }

    if (!mRequest->file().versions().grab_version().empty()) {
      cmd_in += "&mgm.grab.version=" + mRequest->file().versions().grab_version();
    } else {
      cmd_in += "&mgm.grab.version=-1";
    }

    break;
  }

  case eos::console::FileProto::kShare: {
    cmd_in += "&mgm.subcmd=share";
    cmd_in += "&mgm.path=" + path;
    cmd_in += "&mgm.file.expires=" + std::to_string(
                mRequest->file().share().expires());
    break;
  }

  case eos::console::FileProto::kWorkflow: {
    cmd_in += "&mgm.subcmd=workflow";
    cmd_in += "&mgm.path=" + path;
    cmd_in += "&mgm.workflow=" + mRequest->file().workflow().workflow();
    cmd_in += "&mgm.event=" + mRequest->file().workflow().event();
    break;
  }

  default: {
    mReply->set_std_err("error: subcommand is not supported");
    mReply->set_retc(EINVAL);
    return grpc::Status::OK;
  }
  }

  ExecProcCmd(cmd_in, false);
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Fileinfo()
{
  std::string path = mRequest->fileinfo().md().path();

  if (path.empty()) {
    // get by inode
    if (mRequest->fileinfo().md().ino()) {
      path = "inode:" + std::to_string(mRequest->fileinfo().md().ino());
    }
    // get by fileid
    else if (mRequest->fileinfo().md().id()) {
      path = "fid:" + std::to_string(mRequest->fileinfo().md().id());
    }

    if (path.empty()) {
      mReply->set_std_err("error: path is empty");
      mReply->set_retc(EINVAL);
      return grpc::Status::OK;
    }
  }

  std::string std_out, std_err;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string cmd_in = "mgm.cmd=fileinfo";
  cmd_in += "&mgm.path=" + path;

  if (mRequest->fileinfo().path() || mRequest->fileinfo().fid() ||
      mRequest->fileinfo().fxid() || mRequest->fileinfo().size() ||
      mRequest->fileinfo().checksum() || mRequest->fileinfo().fullpath() ||
      mRequest->fileinfo().proxy() || mRequest->fileinfo().monitoring() ||
      mRequest->fileinfo().wnc() || mRequest->fileinfo().env()) {
    cmd_in += "&mgm.file.info.option=";
  }

  if (mRequest->fileinfo().path()) {
    cmd_in += "--path";
  }

  if (mRequest->fileinfo().fid()) {
    cmd_in += "--fid";
  }

  if (mRequest->fileinfo().fxid()) {
    cmd_in += "--fxid";
  }

  if (mRequest->fileinfo().size()) {
    cmd_in += "--size";
  }

  if (mRequest->fileinfo().checksum()) {
    cmd_in += "--checksum";
  }

  if (mRequest->fileinfo().fullpath()) {
    cmd_in += "--fullpath";
  }

  if (mRequest->fileinfo().proxy()) {
    cmd_in += "--proxy";
  }

  if (mRequest->fileinfo().monitoring() || mRequest->fileinfo().wnc()) {
    cmd_in += "-m";
  }

  if (mRequest->fileinfo().env()) {
    cmd_in += "--env";
  }

  if (mJsonFormat) {
    cmd_in += "&mgm.format=json";
  }

  cmd.open("/proc/user", cmd_in.c_str(), *mVid, &error);
  cmd.AddOutput(std_out, std_err);

  if (mJsonFormat) {
    std_out = cmd.GetStdJson();
  }

  cmd.close();

  // Complement EOS-Drive output with usernames and groupnames
  if (!std_out.empty() && mRequest->fileinfo().wnc()) {
    size_t pos;
    int errc = 0;

    // Add owner's username
    if ((pos = std_out.find("uid=")) != std::string::npos) {
      size_t pos1 = pos + 4;
      size_t pos2 = std_out.find(' ', pos1);

      if (pos1 < pos2) {
        uid_t id = std::stoull(std_out.substr(pos1, pos2 - pos1));
        std::string name = eos::common::Mapping::UidToUserName(id, errc);
        std_out += "wnc_username=" + name + " ";
      }
    }

    // Add owner's groupname
    if ((pos = std_out.find("gid=")) != std::string::npos) {
      size_t pos1 = pos + 4;
      size_t pos2 = std_out.find(' ', pos1);

      if (pos1 < pos2) {
        uid_t id = std::stoull(std_out.substr(pos1, pos2 - pos1));
        std::string name = eos::common::Mapping::GidToGroupName(id, errc);
        std_out += "wnc_groupname=" + name + " ";
      }
    }

    // Get user ACL with usernames/groupnames/egroupnames
    eos::console::RequestProto acl_request;
    eos::console::ReplyProto acl_reply;
    acl_request.mutable_acl()->set_op(eos::console::AclProto_OpType_LIST);
    acl_request.mutable_acl()->set_path(mRequest->fileinfo().md().path());
    GrpcWncInterface exec_acl;
    exec_acl.ExecCmd(*mVid, &acl_request, &acl_reply);

    if (!acl_reply.std_out().empty()) {
      std_out += "wnc_acl_user=" + acl_reply.std_out() + " ";
    }

    // Get sys ACL with usernames/groupnames/egroupnames
    acl_request.mutable_acl()->set_sys_acl(true);
    exec_acl.ExecCmd(*mVid, &acl_request, &acl_reply);

    if (!acl_reply.std_out().empty()) {
      std_out += "wnc_acl_sys=" + acl_reply.std_out() + " ";
    }
  }

  mReply->set_std_out(std_out);
  mReply->set_std_err(std_err);
  mReply->set_retc(cmd.GetRetc());
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Find()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::NewfindCmd findcmd(std::move(req), *mVid);
  findcmd.ProcessRequest(mWriter);
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Fs()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::FsCmd fscmd(std::move(req), *mVid);
  *mReply = fscmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Fsck()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::FsckCmd fsckcmd(std::move(req), *mVid);
  *mReply = fsckcmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Geosched()
{
  if (mVid->uid == 0) {
    std::string subcmd;
    mReply->set_retc(SFS_ERROR);

    if (mRequest->geosched().subcmd_case() ==
        eos::console::GeoschedProto::kAccess) {
      subcmd = mRequest->geosched().access().subcmd();
      // XrdOucString has to be manually initialized to avoid strange behaviour
      // of some GeoTreeEngine functions
      XrdOucString output = "";
      std::string geotag = mRequest->geosched().access().geotag();
      std::string geotag_list = mRequest->geosched().access().geotag_list();
      std::string proxy_group = mRequest->geosched().access().proxy_group();
      bool monitoring = mRequest->geosched().access().monitoring();

      if (!geotag.empty()) {
        std::string tmp_geotag = eos::common::SanitizeGeoTag(geotag);

        if (tmp_geotag != geotag) {
          mReply->set_std_err(tmp_geotag);
          mReply->set_retc(EINVAL);
          return grpc::Status::OK;
        }
      }

      if (subcmd == "cleardirect") {
        if (gOFS->mGeoTreeEngine->clearAccessGeotagMapping(&output,
            geotag == "all" ? "" : geotag)) {
          mReply->set_retc(SFS_OK);
        }
      }

      if (subcmd == "clearproxygroup") {
        if (gOFS->mGeoTreeEngine->clearAccessProxygroup(&output,
            geotag == "all" ? "" : geotag)) {
          mReply->set_retc(SFS_OK);
        }
      }

      if (subcmd == "setdirect") {
        auto geotags = eos::common::StringTokenizer::split<std::vector
                       <std::string>>(geotag_list, ',');

        for (const auto& tag : geotags) {
          std::string tmp_tag = eos::common::SanitizeGeoTag(tag);

          if (tmp_tag != tag) {
            mReply->set_std_err(tmp_tag);
            mReply->set_retc(EINVAL);
            return grpc::Status::OK;
          }
        }

        if (gOFS->mGeoTreeEngine->setAccessGeotagMapping(&output, geotag,
            geotag_list)) {
          mReply->set_retc(SFS_OK);
        }
      }

      if (subcmd == "setproxygroup") {
        if (gOFS->mGeoTreeEngine->setAccessProxygroup(&output, geotag, proxy_group)) {
          mReply->set_retc(SFS_OK);
        }
      }

      if (subcmd == "showdirect") {
        if (gOFS->mGeoTreeEngine->showAccessGeotagMapping(&output, monitoring)) {
          mReply->set_retc(SFS_OK);
        }
      }

      if (subcmd == "showproxygroup") {
        if (gOFS->mGeoTreeEngine->showAccessProxygroup(&output, monitoring)) {
          mReply->set_retc(SFS_OK);
        }
      }

      mReply->set_std_out(output.c_str());
    }

    if (mRequest->geosched().subcmd_case() ==
        eos::console::GeoschedProto::kDisabled) {
      subcmd = mRequest->geosched().disabled().subcmd();
      std::string sched_group = mRequest->geosched().disabled().group();
      std::string op_type = mRequest->geosched().disabled().op_type();
      std::string geotag = mRequest->geosched().disabled().geotag();
      XrdOucString output = "";
      bool save_config = true; // save it to the config

      if (!(geotag == "*" && subcmd != "add")) {
        std::string tmp_geotag = eos::common::SanitizeGeoTag(geotag);

        if (tmp_geotag != geotag) {
          mReply->set_std_err(tmp_geotag);
          mReply->set_retc(EINVAL);
          return grpc::Status::OK;
        }
      }

      if (subcmd == "add") {
        if (gOFS->mGeoTreeEngine->addDisabledBranch(sched_group, op_type, geotag,
            &output, save_config)) {
          mReply->set_retc(SFS_OK);
        }
      }

      if (subcmd == "rm") {
        if (gOFS->mGeoTreeEngine->rmDisabledBranch(sched_group, op_type, geotag,
            &output, save_config)) {
          mReply->set_retc(SFS_OK);
        }
      }

      if (subcmd == "show") {
        if (gOFS->mGeoTreeEngine->showDisabledBranches(sched_group, op_type, geotag,
            &output)) {
          mReply->set_retc(SFS_OK);
        }
      }

      mReply->set_std_out(output.c_str());
    }

    if (mRequest->geosched().subcmd_case() == eos::console::GeoschedProto::kRef) {
      if (gOFS->mGeoTreeEngine->forceRefresh()) {
        mReply->set_std_out("GeoTreeEngine has been refreshed.");
        mReply->set_retc(SFS_OK);
      } else {
        mReply->set_std_out("GeoTreeEngine could not be refreshed at the moment.");
      }
    }

    if (mRequest->geosched().subcmd_case() == eos::console::GeoschedProto::kSet) {
      std::string param_name  = mRequest->geosched().set().param_name();
      std::string param_index = mRequest->geosched().set().param_index();
      std::string param_value = mRequest->geosched().set().param_value();
      int index = -1;

      if (!param_index.empty()) {
        index = std::stoi(param_index);
      }

      bool save_config = true;

      if (gOFS->mGeoTreeEngine->setParameter(param_name, param_value, index,
                                             save_config)) {
        mReply->set_std_out("GeoTreeEngine parameter has been set.");
        mReply->set_retc(SFS_OK);
      } else {
        mReply->set_std_out("GeoTreeEngine parameter could not be set.");
      }
    }

    if (mRequest->geosched().subcmd_case() == eos::console::GeoschedProto::kShow) {
      subcmd = mRequest->geosched().show().subcmd();
      bool print_tree = (subcmd == "tree");
      bool print_snaps = (subcmd == "snapshot");
      bool print_param = (subcmd == "param");
      bool print_state = (subcmd == "state");
      std::string sched_group = mRequest->geosched().show().group();
      std::string op_type = mRequest->geosched().show().op_type();
      bool use_colors = mRequest->geosched().show().color();
      bool monitoring = mRequest->geosched().show().monitoring();
      std::string output;
      gOFS->mGeoTreeEngine->printInfo(output, print_tree, print_snaps, print_param,
                                      print_state,
                                      sched_group, op_type, use_colors, monitoring);
      mReply->set_std_out(output.c_str());
      mReply->set_retc(SFS_OK);
    }

    if (mRequest->geosched().subcmd_case() ==
        eos::console::GeoschedProto::kUpdater) {
      subcmd = mRequest->geosched().updater().subcmd();

      if (subcmd == "pause") {
        if (gOFS->mGeoTreeEngine->PauseUpdater()) {
          mReply->set_std_out("GeoTreeEngine has been paused.");
          mReply->set_retc(SFS_OK);
        } else {
          mReply->set_std_out("GeoTreeEngine could not be paused at the moment.");
        }
      }

      if (subcmd == "resume") {
        gOFS->mGeoTreeEngine->ResumeUpdater();
        mReply->set_std_out("GeoTreeEngine has been resumed.");
        mReply->set_retc(SFS_OK);
      }
    }
  } else {
    mReply->set_std_err("error: you have to take role 'root' to execute this command");
    mReply->set_retc(EPERM);
  }

  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Group()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::GroupCmd groupcmd(std::move(req), *mVid);
  *mReply = groupcmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Health()
{
  std::string output;
  std::string args = mRequest->health().section();

  if (mRequest->health().all_info()) {
    args += " -a";
  }

  if (mRequest->health().monitoring()) {
    args += " -m";
  }

  HealthCommand health(args.c_str());

  try {
    health.Execute(output);
    mReply->set_std_out(output.c_str());
    mReply->set_retc(0);
  } catch (std::string& err) {
    output = "Error: ";
    output += err;
    mReply->set_std_err(output.c_str());
    mReply->set_retc(errno);
  }

  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Io()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::IoCmd iocmd(std::move(req), *mVid);
  *mReply = iocmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Ls()
{
  std::string path = mRequest->ls().md().path();
  eos::console::ReplyProto StreamReply;
  errno = 0;

  if (path.empty()) {
    if (mRequest->ls().md().type() == eos::console::FILE) {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(gOFS->eosFileService->getFileMD(
                                       mRequest->ls().md().id()).get());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
      }
    } else {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(gOFS->eosDirectoryService->getContainerMD(
                                       mRequest->ls().md().id()).get());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
      }
    }

    if (errno) {
      StreamReply.set_std_out("");
      StreamReply.set_std_err("Error: Path is empty");
      StreamReply.set_retc(EINVAL);
      mWriter->Write(StreamReply);
      return grpc::Status::OK;
    }
  }

  std::string std_out, std_err;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string cmd_in = "mgm.cmd=ls&mgm.path=" + path;

  if (mRequest->ls().long_list() || mRequest->ls().tape() ||
      mRequest->ls().readable_sizes() || mRequest->ls().show_hidden() ||
      mRequest->ls().inode_info() || mRequest->ls().num_ids() ||
      mRequest->ls().append_dir_ind() || mRequest->ls().silent() ||
      mRequest->ls().wnc() || mRequest->ls().noglobbing()) {
    cmd_in += "&mgm.option=";

    if (mRequest->ls().long_list()) {
      cmd_in += "l";
    }

    if (mRequest->ls().tape()) {
      cmd_in += "y";
    }

    if (mRequest->ls().readable_sizes()) {
      cmd_in += "h";
    }

    if (mRequest->ls().show_hidden() || mRequest->ls().wnc()) {
      cmd_in += "a";
    }

    if (mRequest->ls().inode_info()) {
      cmd_in += "i";
    }

    if (mRequest->ls().num_ids()) {
      cmd_in += "n";
    }

    if (mRequest->ls().append_dir_ind() || mRequest->ls().wnc()) {
      cmd_in += "F";
    }

    if (mRequest->ls().silent()) {
      cmd_in += "s";
    }

    if (mRequest->ls().noglobbing()) {
      cmd_in += "N";
    }
  }

  cmd.open("/proc/user", cmd_in.c_str(), *mVid, &error);
  cmd.AddOutput(std_out, std_err);
  cmd.close();

  if (cmd.GetRetc() == 0) {
    std::stringstream list(std_out);
    std::string entry(""), out("");
    int counter = 0;

    while (std::getline(list, entry)) {
      if (mRequest->ls().wnc()) {
        uint64_t size = 0;
        eos::IFileMD::ctime_t mtime;
        eos::IFileMD::XAttrMap xattrs;
        // Get full path
        std::string full_path;

        if (entry == "../") {
          continue;
        } else if (entry == "./") {
          full_path = path;
        } else {
          full_path = path + entry;
        }

        // Get the parameters if entry is a file
        if (entry[entry.size() - 1] != '/') {
          std::shared_ptr<eos::IFileMD> fmd;

          try {
            fmd = gOFS->eosView->getFile(full_path.c_str());
          } catch (eos::MDException& e) {
            // Maybe this is a symlink pointing outside the EOS namespace
            try {
              fmd = gOFS->eosView->getFile(full_path.c_str(), false);
            } catch (eos::MDException& e) {
              out += entry + "\t\t\n";
              continue;
            }
          }

          if (fmd) {
            fmd->getMTime(mtime);
            xattrs = fmd->getAttributes();
            size = fmd->getSize();
          }
        }
        // Get the parameters if entry is a directory
        else {
          std::shared_ptr<eos::IContainerMD> cmd;

          try {
            cmd = gOFS->eosView->getContainer(full_path.c_str());
          } catch (eos::MDException& e) {
            out += entry + "\t\t\n";
            continue;
          }

          if (cmd) {
            cmd->getMTime(mtime);
            xattrs = cmd->getAttributes();
          }
        }

        // Print the parameters
        out += entry;
        out += "\t\tsize=" + std::to_string(size);
        out += " mtime=" + std::to_string(mtime.tv_sec);
        out += "." + std::to_string(mtime.tv_nsec);

        if (xattrs.count("sys.eos.btime")) {
          out += " btime=" + xattrs["sys.eos.btime"];
        }

        out += "\n";
      } else {
        out += entry + "\n";
      }

      // Write every 100 lines separately to gRPC
      counter++;

      if (counter >= 100) {
        StreamReply.set_std_out(out);
        StreamReply.set_retc(0);
        mWriter->Write(StreamReply);
        counter = 0;
        out.clear();
      }
    }

    // Write last part to gRPC, if exists
    if (!out.empty()) {
      StreamReply.set_std_out(out);
      StreamReply.set_retc(0);
      mWriter->Write(StreamReply);
    }
  } else {
    StreamReply.set_std_out(std_out);
    StreamReply.set_std_err(std_err);
    StreamReply.set_retc(cmd.GetRetc());
    mWriter->Write(StreamReply);
  }

  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Map()
{
  std::string subcmd = mRequest->map().command();
  std::string cmd_in = "mgm.cmd=map&mgm.subcmd=" + subcmd;

  if (subcmd == "link") {
    cmd_in += "&mgm.map.src=" + mRequest->map().src_path();
    cmd_in += "&mgm.map.dest=" + mRequest->map().dst_path();
  } else if (subcmd == "unlink") {
    cmd_in += "&mgm.map.src=" + mRequest->map().src_path();
  }

  ExecProcCmd(cmd_in, false);
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Member()
{
  std::string egroup = mRequest->member().egroup();
  int errc = 0;
  std::string uid_string = eos::common::Mapping::UidToUserName(mVid->uid, errc);
  std::string rs;

  if (!egroup.empty()) {
    if (mRequest->member().update()) {
      gOFS->EgroupRefresh->refresh(uid_string, egroup);
    }

    rs = gOFS->EgroupRefresh->DumpMember(uid_string, egroup);
  } else if (mVid->uid != 0) {
    mReply->set_std_err("error: you have to take role 'root' to execute this command");
    mReply->set_retc(EPERM);
    return grpc::Status::OK;
  } else {
    rs = gOFS->EgroupRefresh->DumpMembers();
  }

  if (mJsonFormat) {
    Json::Value json;

    try {
      json["errormsg"] = "";
      json["member"] = ProcCommand::CallJsonFormatter((const std::string)rs);
      json["retc"] = std::to_string(SFS_OK);
      mReply->set_std_out(SSTR(json).c_str());
    } catch (Json::Exception& e) {
      json["errormsg"] = "illegal string in json conversion";
      json["retc"] = std::to_string(EFAULT);
      mReply->set_std_err(SSTR(json).c_str());
      mReply->set_retc(EFAULT);
      return grpc::Status::OK;
    }
  } else {
    mReply->set_std_out(rs);
  }

  mReply->set_retc(SFS_OK);
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Mkdir()
{
  std::string cmd_in = "mgm.cmd=mkdir";
  std::string path = mRequest->mkdir().md().path();
  cmd_in += "&mgm.path=" + path;

  if (mRequest->mkdir().parents()) {
    cmd_in += "&mgm.option=p";
  }

  ExecProcCmd(cmd_in, false);

  if (mRequest->mkdir().mode() != 0 && mReply->retc() == 0) {
    eos::console::RequestProto chmod_request;
    eos::console::ReplyProto chmod_reply;
    chmod_request.mutable_chmod()->mutable_md()->set_path(path);
    chmod_request.mutable_chmod()->set_mode(mRequest->mkdir().mode());
    GrpcWncInterface exec_chmod;
    exec_chmod.ExecCmd(*mVid, &chmod_request, &chmod_reply);

    if (chmod_reply.retc() != 0) {
      mReply->set_std_err(chmod_reply.std_err());
      mReply->set_retc(chmod_reply.retc());
    }
  }

  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Mv()
{
  std::string path = mRequest->mv().md().path();
  std::string target = mRequest->mv().target();
  errno = 0;
  std::string cmd_in = "mgm.cmd=file";

  if (path.empty()) {
    if (mRequest->mv().md().type() == eos::console::FILE) {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(gOFS->eosFileService->getFileMD(
                                       mRequest->mv().md().id()).get());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
      }
    } else {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(gOFS->eosDirectoryService->getContainerMD(
                                       mRequest->mv().md().id()).get());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
      }
    }

    if (errno) {
      mReply->set_std_err("Error: Path is empty");
      mReply->set_retc(EINVAL);
      return grpc::Status::OK;
    }
  }

  cmd_in += "&mgm.subcmd=rename&mgm.path=" + path + "&mgm.file.target=" + target;
  ExecProcCmd(cmd_in, false);
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Node()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::NodeCmd nodecmd(std::move(req), *mVid);
  *mReply = nodecmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Ns()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::NsCmd nscmd(std::move(req), *mVid);
  *mReply = nscmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Qos()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::QoSCmd qoscmd(std::move(req), *mVid);
  *mReply = qoscmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Quota()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::QuotaCmd quotacmd(std::move(req), *mVid);
  *mReply = quotacmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Recycle()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::RecycleCmd recyclecmd(std::move(req), *mVid);
  *mReply = recyclecmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Rm()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::RmCmd rmcmd(std::move(req), *mVid);
  *mReply = rmcmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Rmdir()
{
  std::string path = mRequest->rmdir().md().path();
  errno = 0;

  if (path.empty()) {
    try {
      eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
      path = gOFS->eosView->getUri(gOFS->eosDirectoryService->getContainerMD(
                                     mRequest->rmdir().md().id()).get());
    } catch (eos::MDException& e) {
      errno = e.getErrno();
    }

    if (errno) {
      mReply->set_std_err("Error: Path is empty");
      mReply->set_retc(EINVAL);
      return grpc::Status::OK;
    }
  }

  std::string cmd_in = "mgm.cmd=rmdir&mgm.path=" + path;
  ExecProcCmd(cmd_in, false);
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Route()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::RouteCmd routecmd(std::move(req), *mVid);
  *mReply = routecmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Space()
{
  eos::console::RequestProto req = *mRequest;

  if (mRequest->space().subcmd_case() == eos::console::SpaceProto::kNodeSet) {
    // encoding the value to Base64
    std::string val = mRequest->space().nodeset().nodeset_value();

    if (val.substr(0, 5) != "file:") {
      XrdOucString val64 = "";
      eos::common::SymKey::Base64Encode((char*) val.c_str(), val.length(), val64);

      while (val64.replace("=", ":")) {}

      std::string nodeset = "base64:";
      nodeset += val64.c_str();
      req.mutable_space()->mutable_nodeset()->set_nodeset_value(nodeset);
    }
  }

  eos::mgm::SpaceCmd spacecmd(std::move(req), *mVid);
  *mReply = spacecmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Stat()
{
  struct stat buf;
  std::string path = mRequest->stat().path();
  std::string url = "root://localhost/" + path;

  if (!XrdPosixXrootd::Stat(url.c_str(), &buf)) {
    if (mRequest->stat().file()) {
      if (S_ISREG(buf.st_mode)) {
        mReply->set_retc(0);
      } else {
        mReply->set_retc(1);
      }
    } else if (mRequest->stat().directory()) {
      if (S_ISDIR(buf.st_mode)) {
        mReply->set_retc(0);
      } else {
        mReply->set_retc(1);
      }
    } else {
      std::string output = "Path: " + path + "\n";

      if (S_ISREG(buf.st_mode)) {
        XrdOucString sizestring = "";
        output += "Size: " + std::to_string(buf.st_size) + " (";
        output += eos::common::StringConversion::GetReadableSizeString(
                    sizestring, (unsigned long long)buf.st_size, "B");
        output += ")\n";
        output += "Type: regular file\n";
      } else if (S_ISDIR(buf.st_mode)) {
        output += "Type: directory\n";
      } else {
        output += "Type: symbolic link\n";
      }

      mReply->set_std_out(output);
      mReply->set_retc(0);
    }
  } else {
    mReply->set_std_err("error: failed to stat " + path);
    mReply->set_retc(EFAULT);
  }

  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Status()
{
  FILE* pipe = popen("eos-status", "r");
  char line[4096];
  std::string output = "";
  int rc = 0;

  if (!pipe) {
    mReply->set_std_err("Error: Failed to create pipe for eos-status execution");
    mReply->set_retc(errno);
    return grpc::Status::OK;
  }

  while (fgets(line, sizeof(line), pipe)) {
    output += line;
  }

  if ((rc = pclose(pipe)) == -1) {
    mReply->set_std_err("Error: Failed to close pipe for eos-status execution");
    mReply->set_retc(errno);
    return grpc::Status::OK;
  }

  mReply->set_std_out(output);
  mReply->set_retc(rc);
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Token()
{
  eos::console::RequestProto req = *mRequest;
  eos::mgm::TokenCmd tokencmd(std::move(req), *mVid);
  *mReply = tokencmd.ProcessRequest();
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Touch()
{
  std::string path = mRequest->touch().md().path();
  std::string cmd_in = "mgm.cmd=file&mgm.subcmd=touch&mgm.path=" + path;

  if (mRequest->touch().nolayout()) {
    cmd_in += "&mgm.file.touch.nolayout=true";
  }

  if (mRequest->touch().truncate()) {
    cmd_in += "&mgm.file.touch.truncate=true";
  }

  ExecProcCmd(cmd_in, false);

  // Create parent directories
  if (mRequest->touch().parents() && mReply->retc() == 2) {
    size_t pos = 0;

    if (!path.empty() && path[path.size() - 1] != '/' &&
        (pos = path.rfind('/')) != std::string::npos) {
      std::string parent_path = path.substr(0, pos);
      eos::console::RequestProto mkdir_request;
      eos::console::ReplyProto mkdir_reply;
      mkdir_request.mutable_mkdir()->mutable_md()->set_path(parent_path);
      mkdir_request.mutable_mkdir()->set_parents(true);
      GrpcWncInterface exec_mkdir;
      exec_mkdir.ExecCmd(*mVid, &mkdir_request, &mkdir_reply);

      // Run touch command again
      if (mkdir_reply.retc() == 0) {
        ExecProcCmd(cmd_in, false);
      }
    }
  }

  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Version()
{
  std::string cmd_in = "mgm.cmd=version";

  if (mRequest->version().monitoring() || mRequest->version().features()) {
    cmd_in += "&mgm.option=";
  }

  if (mRequest->version().features()) {
    cmd_in += "f";
  }

  if (mRequest->version().monitoring()) {
    cmd_in += "m";
  }

  ExecProcCmd(cmd_in, false);
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Vid()
{
  std::string std_out1, std_out2, std_err1, std_err2;
  ProcCommand cmd1, cmd2;
  XrdOucErrInfo error1, error2;
  std::string cmd_in1, cmd_in2;
  cmd_in1 = cmd_in2 = "mgm.cmd=vid";
  bool has_cmd2 = false;

  switch (mRequest->vid().subcmd_case()) {
  case eos::console::VidProto::kGateway: {
    eos::console::VidProto_GatewayProto_Protocol prot =
      mRequest->vid().gateway().protocol();
    std::string protocol;
    eos::console::VidProto_GatewayProto_Option option =
      mRequest->vid().gateway().option();
    std::string host = mRequest->vid().gateway().hostname();

    if (prot == eos::console::VidProto_GatewayProto_Protocol_ALL) {
      protocol = "*";
    } else if (prot == eos::console::VidProto_GatewayProto_Protocol_KRB5) {
      protocol = "krb5";
    } else if (prot == eos::console::VidProto_GatewayProto_Protocol_GSI) {
      protocol = "gsi";
    } else if (prot == eos::console::VidProto_GatewayProto_Protocol_SSS) {
      protocol = "sss";
    } else if (prot == eos::console::VidProto_GatewayProto_Protocol_UNIX) {
      protocol = "unix";
    } else if (prot == eos::console::VidProto_GatewayProto_Protocol_HTTPS) {
      protocol = "https";
    } else if (prot == eos::console::VidProto_GatewayProto_Protocol_GRPC) {
      protocol = "grpc";
    }

    if (option == eos::console::VidProto_GatewayProto_Option_ADD) {
      cmd_in1 += "&mgm.subcmd=set";
      cmd_in1 += "&mgm.vid.auth=tident";
      cmd_in1 += "&mgm.vid.cmd=map";
      cmd_in1 += "&mgm.vid.gid=0";
      cmd_in1 += "&mgm.vid.key=<key>";
      cmd_in1 += "&mgm.vid.pattern=\"" + protocol + "@" + host + "\"";
      cmd_in1 += "&mgm.vid.uid=0";
    } else if (option == eos::console::VidProto_GatewayProto_Option_REMOVE) {
      has_cmd2 = true;
      cmd_in1 += "&mgm.subcmd=rm";
      cmd_in1 += "&mgm.vid.cmd=unmap";
      cmd_in1 += "&mgm.vid.key=tident:\"" + protocol + "@" + host + "\":uid";
      cmd_in2 += "&mgm.subcmd=rm";
      cmd_in2 += "&mgm.vid.cmd=unmap";
      cmd_in2 += "&mgm.vid.key=tident:\"" + protocol + "@" + host + "\":gid";
    }

    break;
  }

  case eos::console::VidProto::kDefaultmapping: {
    eos::console::VidProto_DefaultMappingProto_Option opt =
      mRequest->vid().defaultmapping().option();
    eos::console::VidProto_DefaultMappingProto_Type type =
      mRequest->vid().defaultmapping().type();

    if (opt == eos::console::VidProto_DefaultMappingProto_Option_ENABLE) {
      cmd_in1 += "&mgm.subcmd=set";
      cmd_in1 += "&mgm.vid.cmd=map";
      cmd_in1 += "&mgm.vid.pattern=<pwd>";
      cmd_in1 += "&mgm.vid.key=<key>";

      if (type == eos::console::VidProto_DefaultMappingProto_Type_KRB5) {
        cmd_in1 += "&mgm.vid.auth=krb5";
        cmd_in1 += "&mgm.vid.uid=0";
        cmd_in1 += "&mgm.vid.gid=0";
      } else if (type == eos::console::VidProto_DefaultMappingProto_Type_GSI) {
        cmd_in1 += "&mgm.vid.auth=gsi";
        cmd_in1 += "&mgm.vid.uid=0";
        cmd_in1 += "&mgm.vid.gid=0";
      } else if (type == eos::console::VidProto_DefaultMappingProto_Type_SSS) {
        cmd_in1 += "&mgm.vid.auth=sss";
        cmd_in1 += "&mgm.vid.uid=0";
        cmd_in1 += "&mgm.vid.gid=0";
      } else if (type == eos::console::VidProto_DefaultMappingProto_Type_UNIX) {
        cmd_in1 += "&mgm.vid.auth=unix";
        cmd_in1 += "&mgm.vid.uid=99";
        cmd_in1 += "&mgm.vid.gid=99";
      } else if (type == eos::console::VidProto_DefaultMappingProto_Type_HTTPS) {
        cmd_in1 += "&mgm.vid.auth=https";
        cmd_in1 += "&mgm.vid.uid=0";
        cmd_in1 += "&mgm.vid.gid=0";
      } else if (type == eos::console::VidProto_DefaultMappingProto_Type_TIDENT) {
        cmd_in1 += "&mgm.vid.auth=tident";
        cmd_in1 += "&mgm.vid.uid=0";
        cmd_in1 += "&mgm.vid.gid=0";
      }
    } else if (opt == eos::console::VidProto_DefaultMappingProto_Option_DISABLE) {
      has_cmd2 = true;
      cmd_in1 += "&mgm.subcmd=rm";
      cmd_in1 += "&mgm.vid.cmd=unmap";
      cmd_in2 += "&mgm.subcmd=rm";
      cmd_in2 += "&mgm.vid.cmd=unmap";

      if (type == eos::console::VidProto_DefaultMappingProto_Type_KRB5) {
        cmd_in1 += "&mgm.vid.key=krb5:\"<pwd>\":uid";
        cmd_in2 += "&mgm.vid.key=krb5:\"<pwd>\":gid";
      } else if (type == eos::console::VidProto_DefaultMappingProto_Type_GSI) {
        cmd_in1 += "&mgm.vid.key=gsi:\"<pwd>\":uid";
        cmd_in2 += "&mgm.vid.key=gsi:\"<pwd>\":gid";
      } else if (type == eos::console::VidProto_DefaultMappingProto_Type_SSS) {
        cmd_in1 += "&mgm.vid.key=sss:\"<pwd>\":uid";
        cmd_in2 += "&mgm.vid.key=sss:\"<pwd>\":gid";
      } else if (type == eos::console::VidProto_DefaultMappingProto_Type_UNIX) {
        cmd_in1 += "&mgm.vid.key=unix:\"<pwd>\":uid";
        cmd_in2 += "&mgm.vid.key=unix:\"<pwd>\":gid";
      } else if (type == eos::console::VidProto_DefaultMappingProto_Type_HTTPS) {
        cmd_in1 += "&mgm.vid.key=https:\"<pwd>\":uid";
        cmd_in2 += "&mgm.vid.key=https:\"<pwd>\":gid";
      } else if (type == eos::console::VidProto_DefaultMappingProto_Type_TIDENT) {
        cmd_in1 += "&mgm.vid.key=tident:\"<pwd>\":uid";
        cmd_in2 += "&mgm.vid.key=tident:\"<pwd>\":gid";
      }
    }

    break;
  }

  case eos::console::VidProto::kLs: {
    cmd_in1 += "&mgm.subcmd=ls";

    if (mRequest->vid().ls().user_role() || mRequest->vid().ls().group_role() ||
        mRequest->vid().ls().sudoers() || mRequest->vid().ls().user_alias() ||
        mRequest->vid().ls().group_alias() || mRequest->vid().ls().gateway() ||
        mRequest->vid().ls().auth() || mRequest->vid().ls().deepness() ||
        mRequest->vid().ls().geo_location() || mRequest->vid().ls().num_ids()) {
      cmd_in1 += "&mgm.vid.option=";
    }

    if (mRequest->vid().ls().user_role()) {
      cmd_in1 += "u";
    }

    if (mRequest->vid().ls().group_role()) {
      cmd_in1 += "g";
    }

    if (mRequest->vid().ls().sudoers()) {
      cmd_in1 += "s";
    }

    if (mRequest->vid().ls().user_alias()) {
      cmd_in1 += "U";
    }

    if (mRequest->vid().ls().group_alias()) {
      cmd_in1 += "G";
    }

    if (mRequest->vid().ls().gateway()) {
      cmd_in1 += "y";
    }

    if (mRequest->vid().ls().auth()) {
      cmd_in1 += "a";
    }

    if (mRequest->vid().ls().deepness()) {
      cmd_in1 += "N";
    }

    if (mRequest->vid().ls().geo_location()) {
      cmd_in1 += "l";
    }

    if (mRequest->vid().ls().num_ids()) {
      cmd_in1 += "n";
    }

    break;
  }

  case eos::console::VidProto::kPublicaccesslevel: {
    cmd_in1 += "&mgm.subcmd=set";
    cmd_in1 += "&mgm.vid.cmd=publicaccesslevel";
    cmd_in1 += "&mgm.vid.key=publicaccesslevel";
    cmd_in1 += "&mgm.vid.level=";
    cmd_in1 += std::to_string(mRequest->vid().publicaccesslevel().level());
    break;
  }

  case eos::console::VidProto::kRm: {
    if (mRequest->vid().rm().membership()) {
      has_cmd2 = true;
      cmd_in1 += "&mgm.subcmd=rm";
      cmd_in1 += "&mgm.vid.key=vid:" + mRequest->vid().rm().key() + ":uids";
      cmd_in2 += "&mgm.subcmd=rm";
      cmd_in2 += "&mgm.vid.key=vid:" + mRequest->vid().rm().key() + ":gids";
    } else {
      cmd_in1 += "&mgm.subcmd=rm";
      cmd_in1 += "&mgm.vid.key=" + mRequest->vid().rm().key();
    }

    break;
  }

  case eos::console::VidProto::kSetgeotag: {
    // Check if geotag is valid
    std::string targetgeotag = mRequest->vid().setgeotag().geotag();
    std::string geotag = eos::common::SanitizeGeoTag(targetgeotag);

    if (geotag != targetgeotag) {
      mReply->set_std_err(geotag);
      mReply->set_retc(EINVAL);
      return grpc::Status::OK;
    }

    cmd_in1 += "&mgm.subcmd=set";
    cmd_in1 += "&mgm.vid.cmd=geotag";
    cmd_in1 += "&mgm.vid.key=geotag:" + mRequest->vid().setgeotag().prefix();
    cmd_in1 += "&mgm.vid.geotag=" + targetgeotag;
    break;
  }

  case eos::console::VidProto::kSetmembership: {
    eos::console::VidProto_SetMembershipProto_Option opt =
      mRequest->vid().setmembership().option();
    std::string user = mRequest->vid().setmembership().user();
    std::string members = mRequest->vid().setmembership().members();
    cmd_in1 += "&mgm.subcmd=set";
    cmd_in1 += "&mgm.vid.cmd=membership";
    cmd_in1 += "&mgm.vid.source.uid=" + mRequest->vid().setmembership().user();

    if (opt == eos::console::VidProto_SetMembershipProto_Option_USER) {
      cmd_in1 += "&mgm.vid.key=" + user + ":uids";
      cmd_in1 += "&mgm.vid.target.uid=" + members;
    } else if (opt == eos::console::VidProto_SetMembershipProto_Option_GROUP) {
      cmd_in1 += "&mgm.vid.key=" + user + ":gids";
      cmd_in1 += "&mgm.vid.target.gid=" + members;
    } else if (opt == eos::console::VidProto_SetMembershipProto_Option_ADD_SUDO) {
      cmd_in1 += "&mgm.vid.key=" + user + ":root";
      cmd_in1 += "&mgm.vid.target.sudo=true";
    } else if (opt ==
               eos::console::VidProto_SetMembershipProto_Option_REMOVE_SUDO) {
      cmd_in1 += "&mgm.vid.key=" + user + ":root";
      cmd_in1 += "&mgm.vid.target.sudo=false";
    }

    break;
  }

  case eos::console::VidProto::kSetmap: {
    eos::console::VidProto_SetMapProto_Type type = mRequest->vid().setmap().type();
    cmd_in1 += "&mgm.subcmd=set";
    cmd_in1 += "&mgm.vid.cmd=map";

    if (type == eos::console::VidProto_SetMapProto_Type_KRB5) {
      cmd_in1 += "&mgm.vid.auth=krb5";
    } else if (type == eos::console::VidProto_SetMapProto_Type_GSI) {
      cmd_in1 += "&mgm.vid.auth=gsi";
    } else if (type == eos::console::VidProto_SetMapProto_Type_HTTPS) {
      cmd_in1 += "&mgm.vid.auth=https";
    } else if (type == eos::console::VidProto_SetMapProto_Type_SSS) {
      cmd_in1 += "&mgm.vid.auth=sss";
    } else if (type == eos::console::VidProto_SetMapProto_Type_UNIX) {
      cmd_in1 += "&mgm.vid.auth=unix";
    } else if (type == eos::console::VidProto_SetMapProto_Type_TIDENT) {
      cmd_in1 += "&mgm.vid.auth=tident";
    } else if (type == eos::console::VidProto_SetMapProto_Type_VOMS) {
      cmd_in1 += "&mgm.vid.auth=voms";
    } else if (type == eos::console::VidProto_SetMapProto_Type_GRPC) {
      cmd_in1 += "&mgm.vid.auth=grpc";
    }

    cmd_in1 += "&mgm.vid.key=<key>";
    cmd_in1 += "&mgm.vid.pattern=" + mRequest->vid().setmap().pattern();

    if (!mRequest->vid().setmap().vgid_only()) {
      cmd_in1 += "&mgm.vid.uid=" + std::to_string(mRequest->vid().setmap().vuid());
    }

    if (!mRequest->vid().setmap().vuid_only()) {
      cmd_in1 += "&mgm.vid.gid=" + std::to_string(mRequest->vid().setmap().vgid());
    }

    break;
  }

  default:
    mReply->set_std_err("error: subcommand is not supported");
    mReply->set_retc(EINVAL);
    return grpc::Status::OK;
  }

  cmd1.open("/proc/admin", cmd_in1.c_str(), *mVid, &error1);
  cmd1.AddOutput(std_out1, std_err1);
  cmd1.close();

  if (has_cmd2) {
    cmd2.open("/proc/admin", cmd_in2.c_str(), *mVid, &error2);
    cmd2.AddOutput(std_out2, std_err2);
    cmd2.close();

    if (!std_out1.empty()) {
      std_out1.insert(0, "UID: ");
    }

    if (!std_err1.empty()) {
      std_err1.insert(0, "UID: ");
      std_err1 += "\n";
    }

    if (!std_out2.empty()) {
      std_out2.insert(0, "GID: ");
    }

    if (!std_err2.empty()) {
      std_err2.insert(0, "GID: ");
      std_err2 += "\n";
    }
  }

  mReply->set_std_out(std_out1 + std_out2);
  mReply->set_std_err(std_err1 + std_err2);
  mReply->set_retc((cmd1.GetRetc() > cmd2.GetRetc()) ? cmd1.GetRetc() :
                   cmd2.GetRetc());
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Who()
{
  std::string cmd_in = "mgm.cmd=who";

  if (mRequest->who().showclients() || mRequest->who().showauth() ||
      mRequest->who().showall() || mRequest->who().showsummary() ||
      mRequest->who().monitoring()) {
    cmd_in += "&mgm.option=";
  }

  if (mRequest->who().showclients()) {
    cmd_in += "c";
  }

  if (mRequest->who().showauth()) {
    cmd_in += "z";
  }

  if (mRequest->who().showall()) {
    cmd_in += "a";
  }

  if (mRequest->who().showsummary()) {
    cmd_in += "s";
  }

  if (mRequest->who().monitoring()) {
    cmd_in += "m";
  }

  ExecProcCmd(cmd_in, false);
  return grpc::Status::OK;
}

grpc::Status GrpcWncInterface::Whoami()
{
  std::string cmd_in = "mgm.cmd=whoami";
  ExecProcCmd(cmd_in, false);
  return grpc::Status::OK;
}

//-----------------------------------------------------------------------------
//-----------------------------------------------------------------------------
//  Additional functions needed by GrpcWncInterface::File function
//-----------------------------------------------------------------------------
//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
//! Convert an FST env representation to an Fmd struct
//! (Specific for 'eos file check' command)
//!
//! @param env env representation
//! @param fmd reference to Fmd struct
//!
//! @return true if successful otherwise false
//-----------------------------------------------------------------------------
bool
File_EnvFstToFmd(XrdOucEnv& env, eos::common::FmdHelper& fmd)
{
  // Check that all tags are present
  if (!env.Get("id") ||
      !env.Get("cid") ||
      !env.Get("ctime") ||
      !env.Get("ctime_ns") ||
      !env.Get("mtime") ||
      !env.Get("mtime_ns") ||
      !env.Get("size") ||
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
  fmd.mProtoFmd.set_size(strtoull(env.Get("size"), 0, 10));
  fmd.mProtoFmd.set_lid(strtoul(env.Get("lid"), 0, 10));
  fmd.mProtoFmd.set_uid((uid_t) strtoul(env.Get("uid"), 0, 10));
  fmd.mProtoFmd.set_gid((gid_t) strtoul(env.Get("gid"), 0, 10));

  if (env.Get("checksum")) {
    fmd.mProtoFmd.set_checksum(env.Get("checksum"));

    if (fmd.mProtoFmd.checksum() == "none") {
      fmd.mProtoFmd.set_checksum("");
    }
  } else {
    fmd.mProtoFmd.set_checksum("");
  }

  if (env.Get("diskchecksum")) {
    fmd.mProtoFmd.set_diskchecksum(env.Get("diskchecksum"));

    if (fmd.mProtoFmd.diskchecksum() == "none") {
      fmd.mProtoFmd.set_diskchecksum("");
    }
  } else {
    fmd.mProtoFmd.set_diskchecksum("");
  }

  return true;
}

//-----------------------------------------------------------------------------
//! Return a remote file attribute
//! (Specific for 'eos file check' command)
//!
//! @param manager host:port of the server to contact
//! @param key extended attribute key to get
//! @param path file path to read attributes from
//! @param attribute reference where to store the attribute value
//-----------------------------------------------------------------------------
int
File_GetRemoteAttribute(const char* manager, const char* key,
                        const char* path, XrdOucString& attribute)
{
  if ((!key) || (!path)) {
    return EINVAL;
  }

  int rc = 0;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status;
  XrdOucString fmdquery = "/?fst.pcmd=getxattr&fst.getxattr.key=";
  fmdquery += key;
  fmdquery += "&fst.getxattr.path=";
  fmdquery += path;
  XrdOucString address = "root://";
  address += manager;
  address += "//dummy";
  XrdCl::URL url(address.c_str());

  if (!url.IsValid()) {
    eos_static_err("error=URL is not valid: %s", address.c_str());
    return EINVAL;
  }

  std::unique_ptr<XrdCl::FileSystem> fs(new XrdCl::FileSystem(url));

  if (!fs) {
    eos_static_err("error=failed to get new FS object");
    return EINVAL;
  }

  arg.FromString(fmdquery.c_str());
  status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg, response);

  if (status.IsOK()) {
    rc = 0;
    eos_static_debug("got attribute meta data from server %s for key=%s path=%s"
                     " attribute=%s", manager, key, path, response->GetBuffer());
  } else {
    rc = ECOMM;
    eos_static_err("Unable to retrieve meta data from server %s for key=%s path=%s",
                   manager, key, path);
  }

  if (rc) {
    delete response;
    return EIO;
  }

  if (!strncmp(response->GetBuffer(), "ERROR", 5)) {
    // remote side couldn't get the record
    eos_static_info("Unable to retrieve meta data on remote server %s for key=%s "
                    "path=%s", manager, key, path);
    delete response;
    return ENODATA;
  }

  attribute = response->GetBuffer();
  delete response;
  return 0;
}

//-----------------------------------------------------------------------------
//! Return Fmd from a remote filesystem
//! (Specific for 'eos file check' command)
//!
//! @param manager host:port of the server to contact
//! @param shexfid hex string of the file id
//! @param sfsid string of filesystem id
//! @param fmd reference to the Fmd struct to store Fmd
//-----------------------------------------------------------------------------
int
File_GetRemoteFmdFromLocalDb(const char* manager, const char* shexfid,
                             const char* sfsid, eos::common::FmdHelper& fmd)
{
  if ((!manager) || (!shexfid) || (!sfsid)) {
    return EINVAL;
  }

  int rc = 0;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status;
  XrdOucString fmdquery = "/?fst.pcmd=getfmd&fst.getfmd.fid=";
  fmdquery += shexfid;
  fmdquery += "&fst.getfmd.fsid=";
  fmdquery += sfsid;
  XrdOucString address = "root://";
  address += manager;
  address += "//dummy";
  XrdCl::URL url(address.c_str());

  if (!url.IsValid()) {
    eos_static_err("error=URL is not valid: %s", address.c_str());
    return EINVAL;
  }

  std::unique_ptr<XrdCl::FileSystem> fs(new XrdCl::FileSystem(url));

  if (!fs) {
    eos_static_err("error=failed to get new FS object");
    return EINVAL;
  }

  arg.FromString(fmdquery.c_str());
  status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg, response);

  if (status.IsOK()) {
    rc = 0;
    eos_static_debug("got replica file meta data from server %s for fxid=%s fsid=%s",
                     manager, shexfid, sfsid);
  } else {
    rc = ECOMM;
    eos_static_err("Unable to retrieve meta data from server %s for fxid=%s fsid=%s",
                   manager, shexfid, sfsid);
  }

  if (rc) {
    delete response;
    return EIO;
  }

  if (!strncmp(response->GetBuffer(), "ERROR", 5)) {
    // remote side couldn't get the record
    eos_static_info("Unable to retrieve meta data on remote server %s for fxid=%s fsid=%s",
                    manager, shexfid, sfsid);
    delete response;
    return ENODATA;
  }

  // get the remote file meta data into an env hash
  XrdOucEnv fmdenv(response->GetBuffer());

  if (!File_EnvFstToFmd(fmdenv, fmd)) {
    int envlen;
    eos_static_err("Failed to unparse file meta data %s", fmdenv.Env(envlen));
    delete response;
    return EIO;
  }

  // very simple check
  if (fmd.mProtoFmd.fid() != eos::common::FileId::Hex2Fid(shexfid)) {
    eos_static_err("Uups! Received wrong meta data from remote server - fid "
                   "is %lu instead of %lu !", fmd.mProtoFmd.fid(),
                   eos::common::FileId::Hex2Fid(shexfid));
    delete response;
    return EIO;
  }

  delete response;
  return 0;
}

EOSMGMNAMESPACE_END

#endif // EOS_GRPC
