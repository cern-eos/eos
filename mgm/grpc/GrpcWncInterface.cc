//-----------------------------------------------------------------------------
// File: GrpcWncInterface.cc
// Author: Branko Blagojevic <branko.blagojevic@comtrade.com>
// Author: Ivan Arizanovic <ivan.arizanovic@comtrade.com>
//-----------------------------------------------------------------------------

#ifdef EOS_GRPC

//-----------------------------------------------------------------------------
#include "GrpcWncInterface.hh"
//-----------------------------------------------------------------------------
#include "mgm/proc/admin/AccessCmd.hh"
#include "mgm/proc/user/AclCmd.hh"
#include "mgm/proc/admin/ConfigCmd.hh"
#include "mgm/proc/admin/DebugCmd.hh"
#include "mgm/proc/admin/FsCmd.hh"
#include "mgm/proc/admin/GroupCmd.hh"
#include "mgm/proc/admin/IoCmd.hh"
#include "mgm/proc/admin/NodeCmd.hh"
#include "mgm/proc/admin/NsCmd.hh"
#include "mgm/proc/admin/QuotaCmd.hh"
#include "mgm/proc/user/RecycleCmd.hh"
#include "mgm/proc/user/RmCmd.hh"
#include "mgm/proc/user/RouteCmd.hh"
#include "mgm/proc/admin/SpaceCmd.hh"
#include "mgm/proc/admin/StagerRmCmd.hh"
//-----------------------------------------------------------------------------
#include "common/Fmd.hh"
#include "mgm/Acl.hh"
#include "mgm/FsView.hh"
#include "mgm/grpc/GrpcNsInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "namespace/interface/ContainerIterators.hh"
#include "namespace/Prefetcher.hh"
//-----------------------------------------------------------------------------
#include "XrdPosix/XrdPosixXrootd.hh"
//-----------------------------------------------------------------------------

EOSMGMNAMESPACE_BEGIN

grpc::Status
GrpcWncInterface::ExecCmd(eos::common::VirtualIdentity& vid,
                          const eos::console::RequestProto* request,
                          eos::console::ReplyProto* reply)
{
  RoleChanger(vid, request);

  switch(request->command_case()) {
    case eos::console::RequestProto::kAccess:
      return Access(vid, request, reply);
      break;
    case eos::console::RequestProto::kAcl:
      return Acl(vid, request, reply);
      break;
    case eos::console::RequestProto::kAttr:
      return Attr(vid, request, reply);
      break;
    case eos::console::RequestProto::kChmod:
      return Chmod(vid, request, reply);
      break;
    case eos::console::RequestProto::kChown:
      return Chown(vid, request, reply);
      break;
    case eos::console::RequestProto::kConfig:
      return Config(vid, request, reply);
      break;
    case eos::console::RequestProto::kCp:
      return Cp(vid, request, reply);
      break;
    case eos::console::RequestProto::kDebug:
      return Debug(vid, request, reply);
      break;
    case eos::console::RequestProto::kFile:
      return File(vid, request, reply);
      break;
    case eos::console::RequestProto::kFileinfo:
      return Fileinfo(vid, request, reply);
      break;
    case eos::console::RequestProto::kFs:
      return Fs(vid, request, reply);
      break;
    case eos::console::RequestProto::kGroup:
      return Group(vid, request, reply);
      break;
    case eos::console::RequestProto::kIo:
      return Io(vid, request, reply);
      break;
    case eos::console::RequestProto::kLs:
      return Ls(vid, request, reply);
      break;
    case eos::console::RequestProto::kMkdir:
      return Mkdir(vid, request, reply);
      break;
    case eos::console::RequestProto::kMv:
      return Mv(vid, request, reply);
      break;
    case eos::console::RequestProto::kNode:
      return Node(vid, request, reply);
      break;
    case eos::console::RequestProto::kNs:
      return Ns(vid, request, reply);
      break;
    case eos::console::RequestProto::kQuota:
      return Quota(vid, request, reply);
      break;
    case eos::console::RequestProto::kRecycle:
      return Recycle(vid, request, reply);
      break;
    case eos::console::RequestProto::kRm:
      return Rm(vid, request, reply);
      break;
    case eos::console::RequestProto::kRmdir:
      return Rmdir(vid, request, reply);
      break;
    case eos::console::RequestProto::kRoute:
      return Route(vid, request, reply);
      break;
    case eos::console::RequestProto::kSpace:
      return Space(vid, request, reply);
      break;
    case eos::console::RequestProto::kStagerRm:
      return StagerRm(vid, request, reply);
      break;
    case eos::console::RequestProto::kStat:
      return Stat(vid, request, reply);
      break;
    case eos::console::RequestProto::kTouch:
      return Touch(vid, request, reply);
      break;
    case eos::console::RequestProto::kTransfer: {
      ServerWriter<eos::console::StreamReplyProto>* writer = nullptr;
      return Transfer(vid, request, reply, writer);
      break;
    }
    case eos::console::RequestProto::kVersion:
      return Version(vid, request, reply);
      break;
    case eos::console::RequestProto::kVid: {
      for (auto it : vid.allowed_uids)
        if ((it == 0 && vid.uid == 0) || it == 2 || it == 3)
          return Vid(vid, request, reply);

      reply->set_retc(EACCES);
      reply->set_std_err("Error: Permission denied");
      break;
    }
    case eos::console::RequestProto::kWho:
      return Who(vid, request, reply);
      break;
    case eos::console::RequestProto::kWhoami:
      return Whoami(vid, request, reply);
      break;
    default:
      reply->set_retc(EINVAL);
      reply->set_std_err("error: command not supported");
      break;
  }

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::ExecStreamCmd(eos::common::VirtualIdentity& vid,
                                const eos::console::RequestProto* request,
                                ServerWriter<eos::console::StreamReplyProto>* writer)
{
  grpc::Status retc;

  RoleChanger(vid, request);

  switch(request->command_case()) {
    case eos::console::RequestProto::kList: {

      if (request->list().cmd() == eos::console::LIST_CMD::LS)
        retc = List(vid, request, writer);
      else if (request->list().cmd() == eos::console::LIST_CMD::FIND)
        retc = Find(vid, request, writer);

      break;
    }
    case eos::console::RequestProto::kTransfer: {
      eos::console::ReplyProto* reply = nullptr;
      retc = Transfer(vid, request, reply, writer);
      break;
    }
    default:
      retc = grpc::Status::OK;
      break;
  }

  return retc;
}

void
GrpcWncInterface::RoleChanger(eos::common::VirtualIdentity& vid,
                              const eos::console::RequestProto* request)
{
  int errc = 0;
  uid_t uid;
  gid_t gid;

  // Change the user role ID
  if (!request->auth().role().username().empty())
    uid = eos::common::Mapping::UserNameToUid(request->auth().role().username(), errc);
  else if (request->auth().role().uid() != 0)
    uid = request->auth().role().uid();
  else
    uid = vid.uid;

  if (vid.uid != uid) {
    bool is_member = false;

    for (auto it : vid.allowed_uids)
      if (it == uid) {
        vid.uid = uid;
        is_member = true;
        break;
      }

    if (!is_member) {
      if (vid.sudoer) {
        vid.uid = uid;
        vid.allowed_uids.insert(uid);
      }
      else
        vid.uid = 99;
    }
  }

  // Change the group role ID
  if (!request->auth().role().groupname().empty())
    gid = eos::common::Mapping::GroupNameToGid(request->auth().role().groupname(), errc);
  else if (request->auth().role().gid() != 0)
    gid = request->auth().role().gid();
  else
    gid = vid.gid;

  if (vid.gid != gid) {
    bool is_member = false;
    for (auto it : vid.allowed_gids)
      if (it == gid) {
        vid.gid = gid;
        is_member = true;
        break;
      }

    if (!is_member) {
      if (vid.sudoer) {
        vid.gid = gid;
        vid.allowed_gids.insert(gid);
      }
      else
        vid.gid = 99;
    }
  }
}

grpc::Status
GrpcWncInterface::Access(eos::common::VirtualIdentity& vid,
                         const eos::console::RequestProto* request,
                         eos::console::ReplyProto* reply)
{
  eos::console::RequestProto req = *request;
  eos::mgm::AccessCmd accesscmd(std::move(req), vid);

  *reply = accesscmd.ProcessRequest();

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Acl(eos::common::VirtualIdentity& vid,
                      const eos::console::RequestProto* request,
                      eos::console::ReplyProto* reply)
{
  eos::console::RequestProto req = *request;
  eos::mgm::AclCmd aclcmd(std::move(req), vid);

  *reply = aclcmd.ProcessRequest();

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Attr(eos::common::VirtualIdentity& vid,
                       const eos::console::RequestProto* request,
                       eos::console::ReplyProto* reply)
{
  errno = 0;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string stdOut, stdErr, in;
  std::string path = request->attr().md().path();
  std::string key = request->attr().key();
  std::string value = request->attr().value();

  if (path.empty()) {
    if (request->attr().md().type() == eos::console::FILE) {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(
          gOFS->eosFileService->getFileMD(request->attr().md().id()).get());
      } catch (eos::MDException& e) {
        path = "";
        errno = e.getErrno();
      }
    } else {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(
          gOFS->eosDirectoryService->getContainerMD(request->attr().md().id()).get());
      } catch (eos::MDException& e) {
        path = "";
        errno = e.getErrno();
      }
    }

    if (path.empty()) {
      reply->set_retc(EINVAL);
      reply->set_std_err("error:path is empty");
      return grpc::Status::OK;
    }
  }

  in = "mgm.cmd=attr";
  in += "&mgm.path=" + path;
  if (request->attr().cmd() == eos::console::AttrCmd::ATTR_LS) {
    in += "&mgm.subcmd=ls";
  }
  else if (request->attr().cmd() == eos::console::AttrCmd::ATTR_SET) {
    in += "&mgm.subcmd=set";
    // Set additionaly for key "default"
    if (key == "default") {
      std::vector<std::string> val;
      if (value == "replica")
        val = {"4k","adler","replica","2","default"};
      else if (value == "raiddp")
        val = {"1M","adler","raiddp","6","default","crc32c"};
      else if (value == "raid5")
        val = {"1M","adler","raid5","5","default","crc32c"};
      else if (value == "raid6")
        val = {"1M","adler","raid6","6","default","crc32c"};
      else if (value == "archive")
        val = {"1M","adler","archive","8","default","crc32c"};
      else if (value == "qrain")
        val = {"1M","adler","qrain","12","default","crc32c"};
      else {
        reply->set_retc(EINVAL);
        reply->set_std_err("Error: Value are not allowed");
        return grpc::Status::OK;
      }

      if (!val.empty()) {
        std::string default_in = in + "&mgm.attr.key=sys.forced.blocksize&mgm.attr.value=" + val[0];
        cmd.open("/proc/user", default_in.c_str(), vid, &error);
        default_in = in + "&mgm.attr.key=sys.forced.checksum&mgm.attr.value=" + val[1];
        cmd.open("/proc/user", default_in.c_str(), vid, &error);
        default_in = in + "&mgm.attr.key=sys.forced.layout&mgm.attr.value=" + val[2];
        cmd.open("/proc/user", default_in.c_str(), vid, &error);
        default_in = in + "&mgm.attr.key=sys.forced.nstripes&mgm.attr.value=" + val[3];
        cmd.open("/proc/user", default_in.c_str(), vid, &error);
        default_in = in + "&mgm.attr.key=sys.forced.space&mgm.attr.value=" + val[4];
        cmd.open("/proc/user", default_in.c_str(), vid, &error);
        if (value != "replica") {
          default_in = in + "&mgm.attr.key=sys.forced.blockchecksum&mgm.attr.value=" + val[5];
          cmd.open("/proc/user", default_in.c_str(), vid, &error);
        }
      }
    }
    in += "&mgm.attr.key=" + key;
    in += "&mgm.attr.value=" + value;
  }
  else if (request->attr().cmd() == eos::console::AttrCmd::ATTR_GET) {
    in += "&mgm.subcmd=get";
    in += "&mgm.attr.key=" + key;
  }
  else if (request->attr().cmd() == eos::console::AttrCmd::ATTR_RM) {
    in += "&mgm.subcmd=rm";
    in += "&mgm.attr.key=" + key;
  }
  else if (request->attr().cmd() == eos::console::AttrCmd::ATTR_LINK) {
    in += "&mgm.subcmd=set";
    in += "&mgm.attr.key=sys.attr.link";
    in += "&mgm.attr.value=" + request->attr().link();
  }
  else if (request->attr().cmd() == eos::console::AttrCmd::ATTR_UNLINK) {
    in += "&mgm.subcmd=rm";
    in += "&mgm.attr.key=sys.attr.link";
  }
  else if (request->attr().cmd() == eos::console::AttrCmd::ATTR_FOLD) {
    in += "&mgm.subcmd=fold";
  }
  // Recursively
  if (request->attr().recursive())
    in += "&mgm.option=r";

  cmd.open("/proc/user", in.c_str(), vid, &error);
  cmd.AddOutput(stdOut, stdErr);
  cmd.close();

  reply->set_retc(cmd.GetRetc());
  if (cmd.GetRetc())
    reply->set_std_err(stdOut + stdErr);
  else
    reply->set_std_out(stdOut + stdErr);

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Chmod(eos::common::VirtualIdentity& vid,
                        const eos::console::RequestProto* request,
                        eos::console::ReplyProto* reply)
{
  std::string path = request->chmod().md().path();
  errno = 0;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string stdOut, stdErr, in;

  if (path.empty()) {
    if (request->chmod().md().type() == eos::console::FILE) {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(gOFS->eosFileService->getFileMD(
          request->chmod().md().id()
        ).get());
      } catch (eos::MDException& e) {
        path = "";
        errno = e.getErrno();
      }
    } else {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(gOFS->eosDirectoryService->getContainerMD(
          request->chmod().md().id()
        ).get());
      } catch (eos::MDException& e) {
        path = "";
        errno = e.getErrno();
      }
    }

    if (path.empty()) {
      reply->set_retc(EINVAL);
      reply->set_std_err("error:path is empty");
      return grpc::Status::OK;
    }
  }

  in = "mgm.cmd=chmod";
  in += "&mgm.path=" + path;
  in += "&mgm.chmod.mode=" + std::to_string(request->chmod().mode());

  // Recursively
  if (request->chmod().recursive())
    in += "&mgm.option=r";

  cmd.open("/proc/user", in.c_str(), vid, &error);
  cmd.AddOutput(stdOut, stdErr);
  cmd.close();

  reply->set_retc(cmd.GetRetc());
  if (cmd.GetRetc())
    reply->set_std_err(stdOut + stdErr);
  else
    reply->set_std_out(stdOut + stdErr);

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Chown(eos::common::VirtualIdentity& vid,
                        const eos::console::RequestProto* request,
                        eos::console::ReplyProto* reply)
{
  std::string path = request->chown().md().path();
  uid_t uid = request->chown().owner().uid();
  gid_t gid = request->chown().owner().gid();
  std::string username = request->chown().owner().username();
  std::string groupname = request->chown().owner().groupname();
  errno = 0;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string stdOut, stdErr, in;

  if (path.empty()) {
    if (request->chown().md().type() == eos::console::FILE) {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(gOFS->eosFileService->getFileMD(
          request->chown().md().id()
        ).get());
      } catch (eos::MDException& e) {
        path = "";
        errno = e.getErrno();
      }
    } else {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(gOFS->eosDirectoryService->getContainerMD(
          request->chown().md().id()
        ).get());
      } catch (eos::MDException& e) {
        path = "";
        errno = e.getErrno();
      }
    }

    if (path.empty()) {
      reply->set_retc(EINVAL);
      reply->set_std_err("error:path is empty");
      return grpc::Status::OK;
    }
  }

  in = "mgm.cmd=chown";
  in += "&mgm.path=" + path;
  in += "&mgm.chown.owner=";

  if (request->chown().user_only() ||
      request->chown().user_only() == request->chown().group_only()) {
    if (!username.empty())
      in += username;
    else
      in += std::to_string(uid);
  }
  if (request->chown().group_only() ||
      request->chown().user_only() == request->chown().group_only()) {
    if (!groupname.empty())
      in += ":" + groupname;
    else
      in += ":" + std::to_string(gid);
  }

  // Options
  if (request->chown().recursive() || request->chown().nodereference()) {
    in += "&mgm.chown.option=";
    if (request->chown().recursive())
      in += "r";
    if (request->chown().nodereference())
      in += "h";
  }

  // Command execution
  cmd.open("/proc/user", in.c_str(), vid, &error);
  cmd.AddOutput(stdOut, stdErr);
  cmd.close();

  reply->set_retc(cmd.GetRetc());
  if (cmd.GetRetc())
    reply->set_std_err(stdOut + stdErr);
  else
    reply->set_std_out(stdOut + stdErr);

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Config(eos::common::VirtualIdentity& vid,
                         const eos::console::RequestProto* request,
                         eos::console::ReplyProto* reply)
{
  eos::console::RequestProto req = *request;
  eos::mgm::ConfigCmd configcmd(std::move(req), vid);

  *reply = configcmd.ProcessRequest();

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Cp(eos::common::VirtualIdentity& vid,
                     const eos::console::RequestProto* request,
                     eos::console::ReplyProto* reply)
{
  switch (request->cp().subcmd_case()) {
    case eos::console::CpProto::kCksum: {
      XrdCl::URL url("root://localhost//dummy");

      auto* fs = new XrdCl::FileSystem(url);

      if (!fs) {
        reply->set_std_err("Warning: failed to get new FS object [attempting checksum]\n");
        return grpc::Status::OK;
      }

      std::string path = request->cp().cksum().path();
      size_t pos = path.rfind("//");

      if (pos != std::string::npos)
        path.erase(0, pos + 1);

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
        reply->set_std_out(msg);
      }
      else {
        std::string msg = "Warning: failed getting checksum for ";
        msg += path;
        reply->set_std_err(msg);
      }

      delete response;
      delete fs;

      break;
    }
    case eos::console::CpProto::kKeeptime: {
      if (request->cp().keeptime().set()) {
        // Set atime and mtime
        std::string path = request->cp().keeptime().path();

        char update[1024];
        sprintf(update,
                "?eos.app=eoscp&mgm.pcmd=utimes&tv1_sec=%llu&tv1_nsec=%llu&tv2_sec=%llu&tv2_nsec=%llu",
                (unsigned long long) request->cp().keeptime().atime().seconds(),
                (unsigned long long) request->cp().keeptime().atime().nanos(),
                (unsigned long long) request->cp().keeptime().mtime().seconds(),
                (unsigned long long) request->cp().keeptime().mtime().nanos()
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
          reply->set_std_err(msg);
        }
      }
      else {
        // Get atime and mtime
        std::string path = request->cp().keeptime().path();

        XrdOucString url = "root://localhost/";
        url += path.c_str();

        // Stat EOS file
        struct stat buf;
        if (XrdPosixXrootd::Stat(url.c_str(), &buf) == 0) {
          std::string msg;
          msg += "atime:";
          msg += std::to_string(buf.st_atime);
          msg += "mtime:";
          msg += std::to_string(buf.st_mtime);
          reply->set_std_out(msg);
        }
        else {
          std::string msg = "Warning: failed getting stat information for ";
          msg += path;
          reply->set_std_err(msg);
        }
      }

      break;
    }
    default: {
      reply->set_retc(EINVAL);
      reply->set_std_err("Error: subcommand is not supported");
    }
  }

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Debug(eos::common::VirtualIdentity& vid,
                        const eos::console::RequestProto* request,
                        eos::console::ReplyProto* reply)
{
  eos::console::RequestProto req = *request;
  eos::mgm::DebugCmd debugcmd(std::move(req), vid);

  *reply = debugcmd.ProcessRequest();

  return grpc::Status::OK;
}

//------------------------------------------------------------------------------
//! Convert an FST env representation to an Fmd struct
//! (Specific for 'eos file check' command)
//!
//! @param env env representation
//! @param fmd reference to Fmd struct
//!
//! @return true if successful otherwise false
//------------------------------------------------------------------------------
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

//------------------------------------------------------------------------------
//! Return a remote file attribute
//! (Specific for 'eos file check' command)
//!
//! @param manager host:port of the server to contact
//! @param key extended attribute key to get
//! @param path file path to read attributes from
//! @param attribute reference where to store the attribute value
//------------------------------------------------------------------------------
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

//------------------------------------------------------------------------------
//! Return Fmd from a remote filesystem
//! (Specific for 'eos file check' command)
//!
//! @param manager host:port of the server to contact
//! @param shexfid hex string of the file id
//! @param sfsid string of filesystem id
//! @param fmd reference to the Fmd struct to store Fmd
//------------------------------------------------------------------------------
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

grpc::Status
GrpcWncInterface::File(eos::common::VirtualIdentity& vid,
                       const eos::console::RequestProto* request,
                       eos::console::ReplyProto* reply)
{
  std::string path = request->file().md().path();
  uint64_t fid = 0;

  if (path.empty() && request->file().FileCommand_case() != eos::console::FileProto::kSymlink) {
    // get by inode
    if (request->file().md().ino())
      fid = eos::common::FileId::InodeToFid(request->file().md().ino());
    // get by fileid
    else if (request->file().md().id())
      fid = request->file().md().id();

    try {
      eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
      path = gOFS->eosView->getUri(gOFS->eosFileService->getFileMD(fid).get());
    } catch (eos::MDException& e) {
      path = "";
      errno = e.getErrno();
    }
  }

  if (path.empty()) {
    reply->set_retc(EINVAL);
    reply->set_std_err("error: path is empty");
    return grpc::Status::OK;
  }

  std::string stdOut, stdErr;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string in = "mgm.cmd=file";

  switch (request->file().FileCommand_case()) {
    case eos::console::FileProto::kAdjustreplica: {
      in += "&mgm.subcmd=adjustreplica";
      if (fid)
        in += "&mgm.file.id=" + std::to_string(fid);
      else
        in += "&mgm.path=" + path; // this has a problem with '&' encoding, prefer to use create by fid ..

      if (!request->file().adjustreplica().space().empty()) {
        in += "&mgm.file.desiredspace=" + request->file().adjustreplica().space();
        if (!request->file().adjustreplica().subgroup().empty())
          in += "&mgm.file.desiredsubgroup=" + request->file().adjustreplica().subgroup();
      }
      if (request->file().adjustreplica().nodrop())
        in += "&mgm.file.option=--nodrop";

      break;
    }
    case eos::console::FileProto::kCheck: {
      in += "&mgm.subcmd=getmdlocation";
      in += "&mgm.format=fuse";
      in += "&mgm.path=";
      in += path;
      XrdOucString option = request->file().check().options().c_str();
      cmd.open("/proc/user", in.c_str(), vid, &error);
      cmd.AddOutput(stdOut, stdErr);
      cmd.close();

      XrdOucEnv* result = new XrdOucEnv(stdOut.c_str());
      stdOut = "";
      bool silent = false;

      if (!result) {
        reply->set_retc(EINVAL);
        reply->set_std_err("error: getmdlocation query failed\n");
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
          stdOut += "path=\"";
          stdOut += ns_path.c_str();
          stdOut += "\" fxid=\"";
          stdOut += newresult->Get("mgm.fid0");
          stdOut += "\" size=\"";
          stdOut += size.c_str();
          stdOut += "\" nrep=\"";
          stdOut += newresult->Get("mgm.nrep");
          stdOut += "\" checksumtype=\"";
          stdOut += checksumtype.c_str();
          stdOut += "\" checksum=\"";
          stdOut += newresult->Get("mgm.checksum");
          stdOut += "\"\n";
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
              reply->set_retc(EINVAL);
              reply->set_std_err("error=URL is not valid: " + address);
              return grpc::Status::OK;
            }

            // Get XrdCl::FileSystem object
            std::unique_ptr<XrdCl::FileSystem> fs {new XrdCl::FileSystem(url)};

            if (!fs) {
              reply->set_retc(ECOMM);
              reply->set_std_err("error=failed to get new FS object");
              return grpc::Status::OK;
            }

            XrdOucString bs = newresult->Get(repbootstat.c_str());
            bool down = (bs != "booted");
            int retc = 0;
            int oldsilent = silent;
            eos::common::FmdHelper fmd;

            if ((option.find("%silent")) != STR_NPOS)
              silent = true;

            if (down && ((option.find("%force")) == STR_NPOS)) {
              consistencyerror = true;
              inconsistencylable = "DOWN";

              if (!silent) {
                stdErr += "error: unable to retrieve file meta data from ";
                stdErr += newresult->Get(repurl.c_str());
                stdErr += " [ status=";
                stdErr += bs.c_str();
                stdErr += " ]\n";
              }
            } else {
              if ((option.find("%checksumattr") != STR_NPOS)) {
                checksumattribute = "";

                if ((retc = File_GetRemoteAttribute(newresult->Get(repurl.c_str()),
                                              "user.eos.checksum",
                                              newresult->Get(repfstpath.c_str()),
                                              checksumattribute))) {
                  if (!silent) {
                    stdErr += "error: unable to retrieve extended attribute from ";
                    stdErr += newresult->Get(repurl.c_str());
                    stdErr += " [";
                    stdErr += std::to_string(retc);
                    stdErr += "]\n";
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
                XrdOucString statpath64;
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
                  stdErr += "error: unable to retrieve file meta data from ";
                  stdErr += newresult->Get(repurl.c_str());
                  stdErr += " [";
                  stdErr += std::to_string(retc);
                  stdErr += "]\n";
                }

                consistencyerror = true;
                inconsistencylable = "NOFMD";
              }
              else {
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
                  stdOut += "nrep=\"";
                  stdOut += std::to_string(i);
                  stdOut += "\" fsid=\"";
                  stdOut += newresult->Get(repfsid.c_str());
                  stdOut += "\" host=\"";
                  stdOut += newresult->Get(repurl.c_str());
                  stdOut += "\" fstpath=\"";
                  stdOut += newresult->Get(repfstpath.c_str());
                  stdOut += "\" size=\"";
                  stdOut += std::to_string(fmd.mProtoFmd.size());
                  stdOut += "\" statsize=\"";
                  stdOut += std::to_string(static_cast<long long>(rsize));
                  stdOut += "\" checksum=\"";
                  stdOut += cx.c_str();
                  stdOut += "\" diskchecksum=\"";
                  stdOut += disk_cx.c_str();
                  stdOut += "\"";
                  if ((option.find("%checksumattr") != STR_NPOS)) {
                    stdOut += " checksumattr=\"";
                    stdOut += checksumattribute.c_str();
                    stdOut += "\"";
                  }
                  stdOut += "\n";
                }
              }
            }

            if ((option.find("%silent")) != STR_NPOS)
              silent = oldsilent;
          } else {
            break;
          }
        }

        if ((option.find("%nrep")) != STR_NPOS) {
          int nrep = 0;
          int stripes = 0;

          if (newresult->Get("mgm.stripes"))
            stripes = atoi(newresult->Get("mgm.stripes"));

          if (newresult->Get("mgm.nrep"))
            nrep = atoi(newresult->Get("mgm.nrep"));

          if (nrep != stripes) {
            consistencyerror = true;

            if (inconsistencylable != "NOFMD")
              inconsistencylable = "REPLICA";
          }
        }

        if ((option.find("%output")) != STR_NPOS) {
          if (consistencyerror) {
            stdOut += "INCONSISTENCY ";
            stdOut += inconsistencylable.c_str();
            stdOut += " path=";
            stdOut += path.c_str();
            stdOut += " fxid=";
            stdOut += newresult->Get("mgm.fid0");
            stdOut += " size=";
            stdOut += size.c_str();
            stdOut += " stripes=";
            stdOut += newresult->Get("mgm.stripes");
            stdOut += " nrep=";
            stdOut += newresult->Get("mgm.nrep");
            stdOut += " nrepstored=";
            stdOut += std::to_string(i);
            stdOut += " nreponline=";
            stdOut += std::to_string(nreplicaonline);
            stdOut += " checksumtype=";
            stdOut += checksumtype.c_str();
            stdOut += " checksum=";
            stdOut += newresult->Get("mgm.checksum");
            stdOut += "\n";
          }
        }

        if (consistencyerror)
          reply->set_retc(EFAULT);

      reply->set_retc(0);
      reply->set_std_err(stdErr);
      reply->set_std_out(stdOut);
      }
      else {
        reply->set_retc(EIO);
        reply->set_std_err("error: couldn't get meta data information\n");
      }

      delete newresult;
      return grpc::Status::OK;
    }
    case eos::console::FileProto::kConvert: {
      in += "&mgm.subcmd=convert";
      if (fid)
        in += "&mgm.file.id=" + std::to_string(fid);
      else
        in += "&mgm.path=" + path; // this has a problem with '&' encoding, prefer to use create by fid ..

      if (!request->file().convert().layout().empty())
        in += "&mgm.convert.layout=" + request->file().convert().layout();
      if (!request->file().convert().target_space().empty())
        in += "&mgm.convert.space=" + request->file().convert().target_space();
      if (!request->file().convert().placement_policy().empty())
        in += "&mgm.convert.placementpolicy=" + request->file().convert().placement_policy();
      if (request->file().convert().sync()) {
        reply->set_retc(EINVAL);
        reply->set_std_err("error: --sync is currently not supported");
        return grpc::Status::OK;
      }
      if (request->file().convert().rewrite())
        in += "&mgm.option=rewrite";
      break;
    }
    case eos::console::FileProto::kCopy: {
      in += "&mgm.subcmd=copy";
      if (fid)
        in += "&mgm.file.id=" + std::to_string(fid);
      else
        in += "&mgm.path=" + path; // this has a problem with '&' encoding, prefer to use create by fid ..

      in += "&mgm.file.target=" + request->file().copy().dst();

      if (request->file().copy().force() || request->file().copy().clone() ||
          request->file().copy().silent()) {
        in += "&mgm.file.option=";
        if (request->file().copy().force())
          in += "-f";
        if (request->file().copy().clone())
          in += "-c";
        if (request->file().copy().silent())
          in += "-s";
      }
      break;
    }
    case eos::console::FileProto::kDrop: {
      in += "&mgm.subcmd=drop";
      if (fid)
        in += "&mgm.file.id=" + std::to_string(fid);
      else
        in += "&mgm.path=" + path; // this has a problem with '&' encoding, prefer to use create by fid ..

      in += "&mgm.file.fsid=" + std::to_string(request->file().drop().fsid());

      if (request->file().drop().force())
        in += "&mgm.file.force=1";

      break;
    }
    case eos::console::FileProto::kLayout: {
      in += "&mgm.subcmd=layout";
      if (fid)
        in += "&mgm.file.id=" + std::to_string(fid);
      else
        in += "&mgm.path=" + path; // this has a problem with '&' encoding, prefer to use create by fid ..

      if (request->file().layout().stripes())
        in += "&mgm.file.layout.stripes=" + std::to_string(request->file().layout().stripes());
      if (!request->file().layout().checksum().empty())
        in += "&mgm.file.layout.checksum=" + request->file().layout().checksum();

      break;
    }
    case eos::console::FileProto::kMove: {
      in += "&mgm.subcmd=move";
      if (fid)
        in += "&mgm.file.id=" + std::to_string(fid);
      else
        in += "&mgm.path=" + path; // this has a problem with '&' encoding, prefer to use create by fid ..

      in += "&mgm.file.sourcefsid=" + std::to_string(request->file().move().fsid1());
      in += "&mgm.file.targetfsid=" + std::to_string(request->file().move().fsid2());

      break;
    }
    case eos::console::FileProto::kPurge: {
      in += "&mgm.subcmd=purge";
      if (fid)
        in += "&mgm.file.id=" + std::to_string(fid);
      else
        in += "&mgm.path=" + path; // this has a problem with '&' encoding, prefer to use create by fid ..

      in += "&mgm.purge.version=" + std::to_string(request->file().purge().purge_version());
      break;
    }
    case eos::console::FileProto::kReplicate: {
      in += "&mgm.subcmd=replicate";
      if (fid)
        in += "&mgm.file.id=" + std::to_string(fid);
      else
        in += "&mgm.path=" + path; // this has a problem with '&' encoding, prefer to use create by fid ..

      in += "&mgm.file.sourcefsid=" + std::to_string(request->file().replicate().fsid1());
      in += "&mgm.file.targetfsid=" + std::to_string(request->file().replicate().fsid2());
      break;
    }
    case eos::console::FileProto::kResync: {
      XrdMqMessage message("resync");

      std::string msgbody = "mgm.cmd=resync";
      unsigned long fsid = request->file().resync().fsid();
      msgbody = "&mgm.fsid=" + std::to_string(fsid);
      msgbody = "&mgm.fid=" + std::to_string(fid);
      message.SetBody(msgbody.c_str());

      // figure out the receiver
      std::string receiver = "/eos/*/fst";
      XrdMqClient mqc;
      if (!mqc.IsInitOK()) {
        reply->set_retc(-1);
        reply->set_std_err("error: failed to initialize MQ Client\n");
        return grpc::Status::OK;
      }

      XrdOucString broker = "root://localhost";
      if (getenv("EOS_MGM_URL"))
        broker = getenv("EOS_MGM_URL");

      if (!broker.endswith("//")) {
        if (!broker.endswith("/")) {
          broker += ":1097//";
        } else {
          broker.erase(broker.length() - 2);
          broker += ":1097//";
        }
      } else {
        broker.erase(broker.length() - 3);
        broker += ":1097//";
      }

      broker += "eos/";
      broker += getenv("HOSTNAME");
      broker += ":";
      broker += (int) getpid();
      broker += ":";
      broker += (int) getppid();
      broker += "/cli";

      if (!mqc.AddBroker(broker.c_str())) {
        reply->set_retc(-1);
        stdErr = "error: failed to add broker";
        stdErr += broker.c_str();
        stdErr += "\n";
        reply->set_std_err(stdErr);
      } else {
        if (!mqc.SendMessage(message, receiver.c_str())) {
          reply->set_retc(-1);
          stdErr = "unable to send resync message to " + receiver;
          reply->set_std_err(stdErr);
        } else {
          reply->set_retc(0);
          stdOut = "info: resynced fid=" + std::to_string(fid);
          stdOut += " on fs=" + std::to_string(fsid) + "\n";
          reply->set_std_out(stdOut);
        }
      }
      return grpc::Status::OK;
    }
    case eos::console::FileProto::kSymlink: {
      std::string target = request->file().symlink().target_path();
      if (target.empty()) {
        reply->set_retc(EINVAL);
        reply->set_std_err("error:target is empty");
        return grpc::Status::OK;
      }

      XrdOucErrInfo error;
      errno = 0;
      if (gOFS->_symlink(path.c_str(), target.c_str(), error, vid)) {
        reply->set_retc(errno);
        reply->set_std_err(error.getErrText());
        return grpc::Status::OK;
      }

      reply->set_retc(0);
      std::string msg = "info: symlinked '";
      msg += path.c_str();
      msg += "' to '";
      msg += target.c_str();
      msg += "'";
      reply->set_std_out(msg);
      return grpc::Status::OK;
    }
    case eos::console::FileProto::kTag: {
      in += "&mgm.subcmd=tag";
      in += "&mgm.path=" + path;

      in += "&mgm.file.tag.fsid=";
      if (request->file().tag().add())
        in += "+";
      if (request->file().tag().remove())
        in += "-";
      if (request->file().tag().unlink())
        in += "~";
      in += std::to_string(request->file().tag().fsid());

      break;
    }
    case eos::console::FileProto::kVerify: {
      in += "&mgm.subcmd=verify";
      in += "&mgm.path=" + path;

      in += "&mgm.file.verify.filterid=" + std::to_string(request->file().verify().fsid());

      if (request->file().verify().checksum())
        in += "&mgm.file.compute.checksum=1";
      if (request->file().verify().commitchecksum())
        in += "&mgm.file.commit.checksum=1";
      if (request->file().verify().commitsize())
        in += "&mgm.file.commit.size=1";
      if (request->file().verify().commitfmd())
        in += "&mgm.file.commit.fmd=1";
      if (request->file().verify().rate())
        in += "&mgm.file.verify.rate=" + std::to_string(request->file().verify().rate());
      if (request->file().verify().resync())
        in += "&mgm.file.resync=1";
      break;
    }
    case eos::console::FileProto::kVersion: {
      in += "&mgm.subcmd=version";
      if (fid)
        in += "&mgm.file.id=" + std::to_string(fid);
      else
        in += "&mgm.path=" + path; // this has a problem with '&' encoding, prefer to use create by fid ..

      in += "&mgm.purge.version=" + std::to_string(request->file().version().purge_version());
      break;
    }
    case eos::console::FileProto::kVersions: {
      in += "&mgm.subcmd=versions";
      if (fid)
        in += "&mgm.file.id=" + std::to_string(fid);
      else
        in += "&mgm.path=" + path; // this has a problem with '&' encoding, prefer to use create by fid ..

      if (!request->file().versions().grab_version().empty())
        in += "&mgm.grab.version=" + request->file().versions().grab_version();
      else
        in += "&mgm.grab.version=-1";

      break;
    }
    case eos::console::FileProto::kShare: {
      in += "&mgm.subcmd=share";
      in += "&mgm.path=" + path;

      in += "&mgm.file.expires=" + std::to_string(request->file().share().expires());

      break;
    }
    case eos::console::FileProto::kWorkflow: {
      in += "&mgm.subcmd=workflow";
      in += "&mgm.path=" + path;

      in += "&mgm.workflow=" + request->file().workflow().workflow();
      in += "&mgm.event=" + request->file().workflow().event();

      break;
    }
    default: {
      reply->set_retc(EINVAL);
      reply->set_std_err("error: subcommand is not supported");
      return grpc::Status::OK;
    }
  }

  cmd.open("/proc/user", in.c_str(), vid, &error);
  cmd.AddOutput(stdOut, stdErr);
  cmd.close();

  reply->set_retc(cmd.GetRetc());
  reply->set_std_err(stdErr);
  reply->set_std_out(stdOut);

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Fileinfo(eos::common::VirtualIdentity& vid,
                           const eos::console::RequestProto* request,
                           eos::console::ReplyProto* reply)
{
  std::string path = request->fileinfo().md().path();

  if (path.empty()) {
    // get by inode
    if (request->fileinfo().md().ino())
      path = "inode:" + std::to_string(request->fileinfo().md().ino());
    // get by fileid
    else if (request->fileinfo().md().id())
      path = "fid:" + std::to_string(request->fileinfo().md().id());

    if (path.empty()) {
      reply->set_retc(EINVAL);
      reply->set_std_err("error: path is empty");
      return grpc::Status::OK;
    }
  }

  std::string stdOut, stdErr;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string in = "mgm.cmd=fileinfo";
    in += "&mgm.path=" + path;
  if (request->fileinfo().path() || request->fileinfo().fid() ||
      request->fileinfo().fxid() || request->fileinfo().size() ||
      request->fileinfo().checksum() || request->fileinfo().fullpath() ||
      request->fileinfo().proxy() || request->fileinfo().monitoring() ||
      request->fileinfo().env())
    in += "&mgm.file.info.option=";
  if (request->fileinfo().path())
    in += "--path";
  if (request->fileinfo().fid())
    in += "--fid";
  if (request->fileinfo().fxid())
    in += "--fxid";
  if (request->fileinfo().size())
    in += "--size";
  if (request->fileinfo().checksum())
    in += "--checksum";
  if (request->fileinfo().fullpath())
    in += "--fullpath";
  if (request->fileinfo().proxy())
    in += "--proxy";
  if (request->fileinfo().monitoring())
    in += "-m";
  if (request->fileinfo().env())
    in += "--env";

  cmd.open("/proc/user", in.c_str(), vid, &error);
  cmd.AddOutput(stdOut, stdErr);
  cmd.close();

  reply->set_retc(cmd.GetRetc());
  reply->set_std_err(stdErr);
  reply->set_std_out(stdOut);

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Find(eos::common::VirtualIdentity& vid,
                       const eos::console::RequestProto* request,
                       ServerWriter<eos::console::StreamReplyProto>* reply)
{
  uint64_t deepness = 0;
  std::vector< std::vector<uint64_t> > found_dirs;
  found_dirs.resize(1);
  found_dirs[0].resize(1);
  found_dirs[0][0] = 0;

  // find a single file, if path is for file
  eos::console::RequestProto request_stat;
  eos::console::ReplyProto reply_stat;
  request_stat.mutable_stat()->set_path(request->list().md().path());
  request_stat.mutable_stat()->set_file(true);
  Stat(vid, &request_stat, &reply_stat);
  if (reply_stat.retc() == 0) {
    eos::console::RequestProto single_file;
    single_file.mutable_list()->CopyFrom(request->list());
    single_file.mutable_list()->set_type(eos::console::FILE);
    return MdGet(vid, &single_file, reply);
  }

  // find for a single directory
  if (request->list().maxdepth() == 0) {
    grpc::Status status = grpc::Status::OK;
    eos::console::RequestProto c_dir;
    *( c_dir.mutable_list()->mutable_md() ) = request->list().md();
    if (request->list().type() != eos::console::FILE) {
      c_dir.mutable_list()->mutable_selection()->CopyFrom(request->list().selection());
      c_dir.mutable_list()->set_type(eos::console::CONTAINER);
      status = MdGet(vid, &c_dir, reply, true, false);
    }
    return status;
  }

  // find for multiple directories/files
  do {
    bool streamparent = false;
    found_dirs.resize(deepness + 2);

    // loop over all directories in that deepness
    for (unsigned int i = 0; i < found_dirs[deepness].size(); i++) {
      uint64_t id = found_dirs[deepness][i];
      eos::console::RequestProto lrequest;
      if ( (deepness == 0 ) && (id == 0 ) ) {
        // that is the root of a find
        *(lrequest.mutable_list()->mutable_md()) = request->list().md();
        eos_static_warning("%s %llu %llu", lrequest.mutable_list()->md().path().c_str(),
                           lrequest.mutable_list()->md().id(),
                           lrequest.mutable_list()->md().ino());
        streamparent = true;
      } else {
        lrequest.mutable_list()->mutable_md()->set_id(id);
        streamparent = false;
      }

      lrequest.mutable_list()->set_type(request->list().type());
      lrequest.mutable_list()->mutable_selection()->CopyFrom(request->list().selection());
      *(lrequest.mutable_auth()->mutable_role()) = request->auth().role();
      std::vector<uint64_t> children;
      grpc::Status status = MdStream(vid, &lrequest, reply, streamparent, &children);
      if (!status.ok()) {
        return status;
      }
      for (auto const& value: children) {
        // stream dirs under path into cpath
        found_dirs[deepness + 1].push_back(value);
      }
    }

    deepness++;
    if (deepness >= request->list().maxdepth()) {
      break;
    }
  } while (found_dirs[deepness].size());

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Fs(eos::common::VirtualIdentity& vid,
                     const eos::console::RequestProto* request,
                     eos::console::ReplyProto* reply)
{
  eos::console::RequestProto req = *request;
  eos::mgm::FsCmd fscmd(std::move(req), vid);

  *reply = fscmd.ProcessRequest();

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Group(eos::common::VirtualIdentity& vid,
                        const eos::console::RequestProto* request,
                        eos::console::ReplyProto* reply)
{
  eos::console::RequestProto req = *request;
  eos::mgm::GroupCmd groupcmd(std::move(req), vid);

  *reply = groupcmd.ProcessRequest();

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Io(eos::common::VirtualIdentity& vid,
                     const eos::console::RequestProto* request,
                     eos::console::ReplyProto* reply)
{
  eos::console::RequestProto req = *request;
  eos::mgm::IoCmd iocmd(std::move(req), vid);

  *reply = iocmd.ProcessRequest();

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::List(eos::common::VirtualIdentity& vid,
                       const eos::console::RequestProto* request,
                       ServerWriter<eos::console::StreamReplyProto>* reply)
{
  switch (request->list().type()) {
    case eos::console::FILE:
    case eos::console::CONTAINER:
      return MdGet(vid, request, reply);
    case eos::console::LISTING: {
      grpc::Status status = MdStream(vid, request, reply);
      if (!status.ok()) {
        eos::console::RequestProto request_tmp;
        request_tmp.mutable_list()->mutable_md()->CopyFrom(request->list().md());
        request_tmp.mutable_list()->set_type(eos::console::FILE);
        return MdGet(vid, &request_tmp, reply);
      }
      else
        return grpc::Status::OK;
    }
    default:
      return grpc::Status::OK;
  }
}

grpc::Status
GrpcWncInterface::Ls(eos::common::VirtualIdentity& vid,
                     const eos::console::RequestProto* request,
                     eos::console::ReplyProto* reply)
{
  std::string path = request->ls().md().path();
  errno = 0;
  if (path.empty()) {
    if (request->ls().md().type() == eos::console::FILE) {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(gOFS->eosFileService->getFileMD(request->ls().md().id()).get());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
      }
    } else {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(gOFS->eosDirectoryService->getContainerMD(request->ls().md().id()).get());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
      }
    }

    if (errno) {
      reply->set_retc(EINVAL);
      reply->set_std_err("Error: Path is empty");
      return grpc::Status::OK;
    }
  }

  // initialization
  std::string stdOut, stdErr;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string in = "mgm.cmd=ls";

  // set the path
  in += "&mgm.path=" + path;

  // set the options
  if (request->ls().long_list() || request->ls().tape() ||
      request->ls().readable_sizes() || request->ls().show_hidden() ||
      request->ls().inode_info() || request->ls().num_ids() ||
      request->ls().append_dir_ind() || request->ls().silent()) {
    in += "&mgm.option=";
    if (request->ls().long_list())
      in += "l";
    if (request->ls().tape())
      in += "y";
    if (request->ls().readable_sizes())
      in += "h";
    if (request->ls().show_hidden())
      in += "a";
    if (request->ls().inode_info())
      in += "i";
    if (request->ls().num_ids())
      in += "n";
    if (request->ls().append_dir_ind())
      in += "F";
    if (request->ls().silent())
      in += "s";
  }

  // running the command
  cmd.open("/proc/user", in.c_str(), vid, &error);
  cmd.AddOutput(stdOut, stdErr);
  cmd.close();

  reply->set_retc(cmd.GetRetc());
  reply->set_std_err(stdErr);
  reply->set_std_out(stdOut);

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Mkdir(eos::common::VirtualIdentity& vid,
                        const eos::console::RequestProto* request,
                        eos::console::ReplyProto* reply)
{
  // initialization
  std::string stdOut, stdErr;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string in = "mgm.cmd=mkdir";

  // set the path
  std::string path = request->mkdir().md().path();
  in += "&mgm.path=" + path;

  // set the options
  if (request->mkdir().parents())
    in += "&mgm.option=p";

  // running the command
  cmd.open("/proc/user", in.c_str(), vid, &error);
  cmd.AddOutput(stdOut, stdErr);
  cmd.close();

  reply->set_retc(cmd.GetRetc());
  reply->set_std_err(stdErr);
  reply->set_std_out(stdOut);

  // Set the change mode
  if (request->mkdir().mode() != 0 && cmd.GetRetc() == 0) {
    eos::console::RequestProto chmod_request;
    eos::console::ReplyProto chmod_reply;
    chmod_request.mutable_chmod()->mutable_md()->set_path(path);
    chmod_request.mutable_chmod()->set_mode(request->mkdir().mode());
    Chmod(vid, &chmod_request, &chmod_reply);
    if (chmod_reply.retc() != 0) {
      reply->set_retc(chmod_reply.retc());
      reply->set_std_err(chmod_reply.std_err());
    }
  }

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Mv(eos::common::VirtualIdentity& vid,
                     const eos::console::RequestProto* request,
                     eos::console::ReplyProto* reply)
{
  std::string path = request->mv().md().path();
  std::string target = request->mv().target();
  errno = 0;
  if (path.empty()) {
    if (request->mv().md().type() == eos::console::FILE) {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(gOFS->eosFileService->getFileMD(request->mv().md().id()).get());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
      }
    } else {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(gOFS->eosDirectoryService->getContainerMD(request->mv().md().id()).get());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
      }
    }

    if (errno) {
      reply->set_retc(EINVAL);
      reply->set_std_err("Error: Path is empty");
      return grpc::Status::OK;
    }
  }

  // initialization
  std::string stdOut, stdErr;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string in = "mgm.cmd=file";
  in += "&mgm.subcmd=rename";

  // set source path
  in += "&mgm.path=" + path;

  // set target path
  in += "&mgm.file.target=" + target;

  // running the command
  cmd.open("/proc/user", in.c_str(), vid, &error);
  cmd.AddOutput(stdOut, stdErr);
  cmd.close();

  reply->set_retc(cmd.GetRetc());
  reply->set_std_err(stdErr);
  reply->set_std_out(stdOut);

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Node(eos::common::VirtualIdentity& vid,
                       const eos::console::RequestProto* request,
                       eos::console::ReplyProto* reply)
{
  eos::console::RequestProto req = *request;
  eos::mgm::NodeCmd nodecmd(std::move(req), vid);

  *reply = nodecmd.ProcessRequest();

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Ns(eos::common::VirtualIdentity& vid,
                     const eos::console::RequestProto* request,
                     eos::console::ReplyProto* reply)
{
  eos::console::RequestProto req = *request;
  eos::mgm::NsCmd nscmd(std::move(req), vid);

  *reply = nscmd.ProcessRequest();

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Quota(eos::common::VirtualIdentity& vid,
                        const eos::console::RequestProto* request,
                        eos::console::ReplyProto* reply)
{
  eos::console::RequestProto req = *request;
  eos::mgm::QuotaCmd quotacmd(std::move(req), vid);

  *reply = quotacmd.ProcessRequest();

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Recycle(eos::common::VirtualIdentity& vid,
                          const eos::console::RequestProto* request,
                          eos::console::ReplyProto* reply)
{
  eos::console::RequestProto req = *request;
  eos::mgm::RecycleCmd recyclecmd(std::move(req), vid);

  *reply = recyclecmd.ProcessRequest();

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Rm(eos::common::VirtualIdentity& vid,
                     const eos::console::RequestProto* request,
                     eos::console::ReplyProto* reply)
{
  eos::console::RequestProto req = *request;
  eos::mgm::RmCmd rmcmd(std::move(req), vid);

  *reply = rmcmd.ProcessRequest();

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Rmdir(eos::common::VirtualIdentity& vid,
                        const eos::console::RequestProto* request,
                        eos::console::ReplyProto* reply)
{
  std::string path = request->rmdir().md().path();
  errno = 0;
  if (path.empty()) {
    try {
      eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
      path = gOFS->eosView->getUri(gOFS->eosDirectoryService->getContainerMD(request->rmdir().md().id()).get());
    } catch (eos::MDException& e) {
      errno = e.getErrno();
    }

    if (errno) {
      reply->set_retc(EINVAL);
      reply->set_std_err("Error: Path is empty");
      return grpc::Status::OK;
    }
  }

  // initialization
  std::string stdOut, stdErr;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string in = "mgm.cmd=rmdir";

  // set the path
  in += "&mgm.path=" + path;

  // running the command
  cmd.open("/proc/user", in.c_str(), vid, &error);
  cmd.AddOutput(stdOut, stdErr);
  cmd.close();

  reply->set_retc(cmd.GetRetc());
  reply->set_std_err(stdErr);
  reply->set_std_out(stdOut);

  return grpc::Status::OK;

}

grpc::Status
GrpcWncInterface::Route(eos::common::VirtualIdentity& vid,
                        const eos::console::RequestProto* request,
                        eos::console::ReplyProto* reply)
{
  eos::console::RequestProto req = *request;
  eos::mgm::RouteCmd routecmd(std::move(req), vid);

  *reply = routecmd.ProcessRequest();

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Space(eos::common::VirtualIdentity& vid,
                        const eos::console::RequestProto* request,
                        eos::console::ReplyProto* reply)
{
  eos::console::RequestProto req = *request;

  if (request->space().subcmd_case() == eos::console::SpaceProto::kNodeSet) {
    // encoding the value to Base64
    std::string val = request->space().nodeset().nodeset_value();
    if (val.substr(0,5) != "file:") {
      XrdOucString val64 = "";
      eos::common::SymKey::Base64Encode((char*) val.c_str(), val.length(), val64);
      while (val64.replace("=", ":")) {}
      std::string nodeset = "base64:";
      nodeset += val64.c_str();
      req.mutable_space()->mutable_nodeset()->set_nodeset_value(nodeset);
    }
  }

  eos::mgm::SpaceCmd spacecmd(std::move(req), vid);

  *reply = spacecmd.ProcessRequest();

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::StagerRm(eos::common::VirtualIdentity& vid,
                           const eos::console::RequestProto* request,
                           eos::console::ReplyProto* reply)
{
  eos::console::RequestProto req = *request;
  eos::mgm::StagerRmCmd stagerrmcmd(std::move(req), vid);

  *reply = stagerrmcmd.ProcessRequest();

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Stat(eos::common::VirtualIdentity& vid,
                       const eos::console::RequestProto* request,
                       eos::console::ReplyProto* reply)
{
  struct stat buf;
  std::string path = request->stat().path();
  std::string url = "root://localhost/" + path;

  if (!XrdPosixXrootd::Stat(url.c_str(), &buf)) {
    if (request->stat().file()) {
      if (S_ISREG(buf.st_mode))
        reply->set_retc(0);
      else
        reply->set_retc(1);
    }
    else if (request->stat().directory()) {
      if (S_ISDIR(buf.st_mode))
        reply->set_retc(0);
      else
        reply->set_retc(1);
    }
    else {
      std::string output = "Path: " + path + "\n";

      if (S_ISREG(buf.st_mode)) {
        XrdOucString sizestring;
        output += "Size: " + std::to_string(buf.st_size) + " (";
        output += eos::common::StringConversion::GetReadableSizeString(
                    sizestring, (unsigned long long)buf.st_size, "B");
        output += ")\n";
        output += "Type: regular file\n";
      }
      else if (S_ISDIR(buf.st_mode))
        output += "Type: directory\n";
      else
        output += "Type: symbolic link\n";

      reply->set_retc(0);
      reply->set_std_out(output);
    }
  }
  else {
    reply->set_retc(EFAULT);
    reply->set_std_err("error: failed to stat " + path);
  }

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Touch(eos::common::VirtualIdentity& vid,
                        const eos::console::RequestProto* request,
                        eos::console::ReplyProto* reply)
{
  // initialization
  std::string stdOut, stdErr;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string in = "mgm.cmd=file";
  in += "&mgm.subcmd=touch";

  // set the path
  in += "&mgm.path=" + request->touch().md().path();

  // set the options
  if (request->touch().nolayout())
    in += "&mgm.file.touch.nolayout=true";

  if (request->touch().truncate())
    in += "&mgm.file.touch.truncate=true";

  // running the command
  cmd.open("/proc/user", in.c_str(), vid, &error);
  cmd.AddOutput(stdOut, stdErr);
  cmd.close();

  reply->set_retc(cmd.GetRetc());
  reply->set_std_err(stdErr);
  reply->set_std_out(stdOut);

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Transfer(eos::common::VirtualIdentity& vid,
                           const eos::console::RequestProto* request,
                           eos::console::ReplyProto* reply,
                           ServerWriter<eos::console::StreamReplyProto>* writer)
{
  std::string stdOut, stdErr;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string in = "mgm.cmd=transfer";

  switch (request->transfer().subcommand()) {
    case eos::console::TransferProto_SubCommand_CANCEL: {
      in += "&mgm.subcmd=cancel";
      break;
    }
    case eos::console::TransferProto_SubCommand_CLEAR: {
      in += "&mgm.subcmd=clear";
      break;
    }
    case eos::console::TransferProto_SubCommand_ENABLE: {
      in += "&mgm.subcmd=enable";
      break;
    }
    case eos::console::TransferProto_SubCommand_DISABLE: {
      in += "&mgm.subcmd=disable";
      break;
    }
    case eos::console::TransferProto_SubCommand_KILL: {
      in += "&mgm.subcmd=kill";
      break;
    }
    case eos::console::TransferProto_SubCommand_LOG: {
      in += "&mgm.subcmd=log";
      break;
    }
    case eos::console::TransferProto_SubCommand_LS: {
      in += "&mgm.subcmd=ls";
      if (!request->transfer().common().groupname().empty())
        in += "&mgm.txgroup=" + request->transfer().common().groupname();
      if (request->transfer().ls().is_id())
        in += "&mgm.txid=" + std::to_string(request->transfer().common().id());

      if (request->transfer().ls().all() ||
          request->transfer().ls().monitoring() ||
          request->transfer().ls().progress() ||
          request->transfer().ls().summary())
        in += "&mgm.txoption=";

      if (request->transfer().ls().all())
        in += "a";
      if (request->transfer().ls().monitoring())
        in += "m";
      if (request->transfer().ls().progress())
        in += "mp";
      if (request->transfer().ls().summary())
        in += "s";

      break;
    }
    case eos::console::TransferProto_SubCommand_PURGE: {
      in += "&mgm.subcmd=purge";
      break;
    }
    case eos::console::TransferProto_SubCommand_RESET: {
      in += "&mgm.subcmd=reset";
      break;
    }
    case eos::console::TransferProto_SubCommand_RESUBMIT: {
      in += "&mgm.subcmd=resubmit";
      break;
    }
    case eos::console::TransferProto_SubCommand_SUBMIT: {
      in += "&mgm.subcmd=submit";
      in += "&mgm.txsrc=" + request->transfer().submit().url1();
      in += "&mgm.txdst=" + request->transfer().submit().url2();
      in += "&mgm.txrate=" + std::to_string(request->transfer().submit().rate());
      in += "&mgm.txstreams=" + std::to_string(request->transfer().submit().streams());
      if (!request->transfer().submit().group().empty())
        in += "&mgm.txgroup=" + request->transfer().submit().group();
      if (request->transfer().submit().noauth())
        in += "&mgm.txnoauth=1";
      if (request->transfer().submit().silent())
        in += "&mgm.txoption=s";

      if (request->transfer().submit().sync()) {
        eos::console::StreamReplyProto StreamReply;
        time_t starttime = time(NULL);
        in += "&mgm.txoption=s";
        cmd.open("/proc/admin", in.c_str(), vid, &error);
        cmd.AddOutput(stdOut, stdErr);
        cmd.close();
        StreamReply.mutable_realtime_output()->set_std_out(stdOut + "\n");
        StreamReply.mutable_realtime_output()->set_retc(0);
        writer->Write(StreamReply);

        if (!cmd.GetRetc()) {
          std::string id;
          size_t pos = 0;
          if (!stdOut.empty() && (pos = stdOut.find(" id=")) != std::string::npos) {
            id = stdOut;
            id.erase(0, pos + 4);
          }

          // now poll the state
          errno = 0;
          long lid = strtol(id.c_str(), 0, 10);

          if (stdOut.empty() || errno || (lid == LONG_MIN) || (lid == LONG_MAX)) {
            StreamReply.mutable_realtime_output()->set_std_err("error: submission of transfer probably failed - check with 'transfer ls'\n");
            StreamReply.mutable_realtime_output()->set_retc(EFAULT);
            writer->Write(StreamReply);
            return grpc::Status::OK;
          }

          // prepare the get progress command
          in = "mgm.cmd=transfer";
          in += "&mgm.subcmd=ls";
          in += "&mgm.txoption=mp";
          in += "&mgm.txid=" + id;
          std::string incp = in;

          while (1) {
            stdOut = "";
            cmd.open("/proc/admin", in.c_str(), vid, &error);
            cmd.AddOutput(stdOut, stdErr);
            cmd.close();
            in = incp;

            if (stdOut.empty()) {
              StreamReply.mutable_realtime_output()->set_std_err("error: transfer has been canceled externnaly!\n");
              StreamReply.mutable_realtime_output()->set_retc(EFAULT);
              writer->Write(StreamReply);
              return grpc::Status::OK;
            }
            XrdOucString stdOut_xrd = stdOut.c_str();
            while (stdOut_xrd.replace(" ", "&")) {}

            XrdOucEnv txinfo(stdOut_xrd.c_str());
            XrdOucString status = txinfo.Get("tx.status");

            if (!request->transfer().submit().noprogress()) {
              std::stringstream output;
              output << "[eoscp TX] [ " << std::setw(10) << txinfo.Get("tx.status") << " ]\t|";

              int progress = atoi(txinfo.Get("tx.progress"));

              for (int l = 0; l < 20; l++) {
                if (l < ((int)(0.2 * progress)))
                  output << "=";
                else if (l == ((int)(0.2 * progress)))
                  output << ">";
                else if (l > ((int)(0.2 * progress)))
                  output << ".";
              }
              output << "| " << std::setw(5) << txinfo.Get("tx.progress");
              output << "% : " << std::to_string((time(NULL) - starttime));
              if ((status != "done") && (status != "failed"))
                output << "s\r";
              else
                output << "s\n";
              StreamReply.mutable_realtime_output()->set_std_out(output.str());
              StreamReply.mutable_realtime_output()->set_retc(0);
              writer->Write(StreamReply);
            }

            if ((status == "done") || (status == "failed")) {
              if (!request->transfer().submit().silent()) {
                // get the log
                in = "mgm.cmd=transfer&mgm.subcmd=log&mgm.txid=" + id;
                stdOut = "";
                cmd.open("/proc/admin", in.c_str(), vid, &error);
                cmd.AddOutput(stdOut, stdErr);
                cmd.close();
                StreamReply.mutable_realtime_output()->set_std_out(stdOut);
                StreamReply.mutable_realtime_output()->set_std_err(stdErr);
                if (status == "done")
                  StreamReply.mutable_realtime_output()->set_retc(0);
                else
                  StreamReply.mutable_realtime_output()->set_retc(EFAULT);
                writer->Write(StreamReply);
              }
              return grpc::Status::OK;
            }
            sleep(1);
          }
        }
        return grpc::Status::OK;
      }

      break;
    }
    default: {
      reply->set_retc(EINVAL);
      reply->set_std_err("error: subcommand is not supported");
      return grpc::Status::OK;
    }
  }

  if (request->transfer().subcommand() == eos::console::TransferProto_SubCommand_CANCEL ||
      request->transfer().subcommand() == eos::console::TransferProto_SubCommand_KILL ||
      request->transfer().subcommand() == eos::console::TransferProto_SubCommand_LOG ||
      (request->transfer().common().IsInitialized() && (
       request->transfer().subcommand() == eos::console::TransferProto_SubCommand_PURGE ||
       request->transfer().subcommand() == eos::console::TransferProto_SubCommand_RESET)) ||
      request->transfer().subcommand() == eos::console::TransferProto_SubCommand_RESUBMIT) {
    if (!request->transfer().common().groupname().empty())
      in += "&mgm.txgroup=" + request->transfer().common().groupname();
    else
      in += "&mgm.txid=" + std::to_string(request->transfer().common().id());
  }

  cmd.open("/proc/admin", in.c_str(), vid, &error);
  cmd.AddOutput(stdOut, stdErr);
  cmd.close();

  reply->set_retc(cmd.GetRetc());
  reply->set_std_err(stdErr);
  reply->set_std_out(stdOut);

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Version(eos::common::VirtualIdentity& vid,
                          const eos::console::RequestProto* request,
                          eos::console::ReplyProto* reply)
{
  std::string stdOut, stdErr;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string in = "mgm.cmd=version";

  if (request->version().monitoring() || request->version().features())
    in += "&mgm.option=";
  if (request->version().features())
    in += "f";
  if (request->version().monitoring())
    in += "m";

  cmd.open("/proc/user", in.c_str(), vid, &error);
  cmd.AddOutput(stdOut, stdErr);
  cmd.close();

  reply->set_retc(cmd.GetRetc());
  reply->set_std_err(stdErr);
  reply->set_std_out(stdOut);

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Vid(eos::common::VirtualIdentity& vid,
                      const eos::console::RequestProto* request,
                      eos::console::ReplyProto* reply)
{
  std::string stdOut1 = "", stdErr1 = "";
  ProcCommand cmd1;
  XrdOucErrInfo error1;
  std::string in1 = "mgm.cmd=vid";

  bool has_cmd2 = false;
  std::string stdOut2 = "", stdErr2 = "";
  ProcCommand cmd2;
  XrdOucErrInfo error2;
  std::string in2 = "mgm.cmd=vid";

  switch (request->vid().subcmd_case()) {
    case eos::console::VidProto::kGateway: {
      std::string host = request->vid().gateway().hostname();
      std::string protocol;
      if (request->vid().gateway().protocol() == eos::console::VidProto_GatewayProto_Protocol_ALL)
        protocol = "*";
      else if (request->vid().gateway().protocol() == eos::console::VidProto_GatewayProto_Protocol_KRB5)
        protocol = "krb5";
      else if (request->vid().gateway().protocol() == eos::console::VidProto_GatewayProto_Protocol_GSI)
        protocol = "gsi";
      else if (request->vid().gateway().protocol() == eos::console::VidProto_GatewayProto_Protocol_SSS)
        protocol = "sss";
      else if (request->vid().gateway().protocol() == eos::console::VidProto_GatewayProto_Protocol_UNIX)
        protocol = "unix";
      else if (request->vid().gateway().protocol() == eos::console::VidProto_GatewayProto_Protocol_HTTPS)
        protocol = "https";
      else if (request->vid().gateway().protocol() == eos::console::VidProto_GatewayProto_Protocol_GRPC)
        protocol = "grpc";

      if (request->vid().gateway().option() == eos::console::VidProto_GatewayProto_Option_ADD) {
        in1 += "&mgm.subcmd=set";
        in1 += "&mgm.vid.auth=tident";
        in1 += "&mgm.vid.cmd=map";
        in1 += "&mgm.vid.gid=0";
        in1 += "&mgm.vid.key=<key>";
        in1 += "&mgm.vid.pattern=\"" + protocol + "@" + host + "\"";
        in1 += "&mgm.vid.uid=0";
      }
      else if (request->vid().gateway().option() == eos::console::VidProto_GatewayProto_Option_REMOVE) {
        has_cmd2 = true;

        in1 += "&mgm.subcmd=rm";
        in1 += "&mgm.vid.cmd=unmap";
        in1 += "&mgm.vid.key=tident:\"" + protocol + "@" + host + "\":uid";

        in2 += "&mgm.subcmd=rm";
        in2 += "&mgm.vid.cmd=unmap";
        in2 += "&mgm.vid.key=tident:\"" + protocol + "@" + host + "\":gid";
      }
      break;
    }
    case eos::console::VidProto::kDefaultmapping: {
      if (request->vid().defaultmapping().option() == eos::console::VidProto_DefaultMappingProto_Option_ENABLE) {
        in1 += "&mgm.subcmd=set";
        in1 += "&mgm.vid.cmd=map";
        in1 += "&mgm.vid.pattern=<pwd>";
        in1 += "&mgm.vid.key=<key>";

        if (request->vid().defaultmapping().type() == eos::console::VidProto_DefaultMappingProto_Type_KRB5) {
          in1 += "&mgm.vid.auth=krb5";
          in1 += "&mgm.vid.uid=0";
          in1 += "&mgm.vid.gid=0";
        }
        else if (request->vid().defaultmapping().type() == eos::console::VidProto_DefaultMappingProto_Type_GSI) {
          in1 += "&mgm.vid.auth=gsi";
          in1 += "&mgm.vid.uid=0";
          in1 += "&mgm.vid.gid=0";
        }
        else if (request->vid().defaultmapping().type() == eos::console::VidProto_DefaultMappingProto_Type_SSS) {
          in1 += "&mgm.vid.auth=sss";
          in1 += "&mgm.vid.uid=0";
          in1 += "&mgm.vid.gid=0";
        }
        else if (request->vid().defaultmapping().type() == eos::console::VidProto_DefaultMappingProto_Type_UNIX) {
          in1 += "&mgm.vid.auth=unix";
          in1 += "&mgm.vid.uid=99";
          in1 += "&mgm.vid.gid=99";
        }
        else if (request->vid().defaultmapping().type() == eos::console::VidProto_DefaultMappingProto_Type_HTTPS) {
          in1 += "&mgm.vid.auth=https";
          in1 += "&mgm.vid.uid=0";
          in1 += "&mgm.vid.gid=0";
        }
        else if (request->vid().defaultmapping().type() == eos::console::VidProto_DefaultMappingProto_Type_TIDENT) {
          in1 += "&mgm.vid.auth=tident";
          in1 += "&mgm.vid.uid=0";
          in1 += "&mgm.vid.gid=0";
        }
      }
      else if (request->vid().defaultmapping().option() == eos::console::VidProto_DefaultMappingProto_Option_DISABLE) {
        has_cmd2 = true;
        in1 += "&mgm.subcmd=rm";
        in1 += "&mgm.vid.cmd=unmap";
        in2 += "&mgm.subcmd=rm";
        in2 += "&mgm.vid.cmd=unmap";

        if (request->vid().defaultmapping().type() == eos::console::VidProto_DefaultMappingProto_Type_KRB5) {
          in1 += "&mgm.vid.key=krb5:\"<pwd>\":uid";
          in2 += "&mgm.vid.key=krb5:\"<pwd>\":gid";
        }
        else if (request->vid().defaultmapping().type() == eos::console::VidProto_DefaultMappingProto_Type_GSI) {
          in1 += "&mgm.vid.key=gsi:\"<pwd>\":uid";
          in2 += "&mgm.vid.key=gsi:\"<pwd>\":gid";
        }
        else if (request->vid().defaultmapping().type() == eos::console::VidProto_DefaultMappingProto_Type_SSS) {
          in1 += "&mgm.vid.key=sss:\"<pwd>\":uid";
          in2 += "&mgm.vid.key=sss:\"<pwd>\":gid";
        }
        else if (request->vid().defaultmapping().type() == eos::console::VidProto_DefaultMappingProto_Type_UNIX) {
          in1 += "&mgm.vid.key=unix:\"<pwd>\":uid";
          in2 += "&mgm.vid.key=unix:\"<pwd>\":gid";
        }
        else if (request->vid().defaultmapping().type() == eos::console::VidProto_DefaultMappingProto_Type_HTTPS) {
          in1 += "&mgm.vid.key=https:\"<pwd>\":uid";
          in2 += "&mgm.vid.key=https:\"<pwd>\":gid";
        }
        else if (request->vid().defaultmapping().type() == eos::console::VidProto_DefaultMappingProto_Type_TIDENT) {
          in1 += "&mgm.vid.key=tident:\"<pwd>\":uid";
          in2 += "&mgm.vid.key=tident:\"<pwd>\":gid";
        }
      }
      break;
    }
    case eos::console::VidProto::kLs: {
      in1 += "&mgm.subcmd=ls";

      if (request->vid().ls().user_role() || request->vid().ls().group_role() ||
          request->vid().ls().sudoers() || request->vid().ls().user_alias() ||
          request->vid().ls().group_alias() || request->vid().ls().gateway() ||
          request->vid().ls().auth() || request->vid().ls().deepness() ||
          request->vid().ls().geo_location() || request->vid().ls().num_ids())
        in1 += "&mgm.vid.option=";

      if (request->vid().ls().user_role())
        in1 += "u";
      if (request->vid().ls().group_role())
        in1 += "g";
      if (request->vid().ls().sudoers())
        in1 += "s";
      if (request->vid().ls().user_alias())
        in1 += "U";
      if (request->vid().ls().group_alias())
        in1 += "G";
      if (request->vid().ls().gateway())
        in1 += "y";
      if (request->vid().ls().auth())
        in1 += "a";
      if (request->vid().ls().deepness())
        in1 += "N";
      if (request->vid().ls().geo_location())
        in1 += "l";
      if (request->vid().ls().num_ids())
        in1 += "n";

      break;
    }
    case eos::console::VidProto::kPublicaccesslevel: {
      in1 += "&mgm.subcmd=set";
      in1 += "&mgm.vid.cmd=publicaccesslevel";
      in1 += "&mgm.vid.key=publicaccesslevel";
      in1 += "&mgm.vid.level=";
      in1 += std::to_string(request->vid().publicaccesslevel().level());
      break;
    }
    case eos::console::VidProto::kRm: {
      if (request->vid().rm().membership()) {
        has_cmd2 = true;
        in1 += "&mgm.subcmd=rm";
        in1 += "&mgm.vid.key=vid:" + request->vid().rm().key() + ":uids";
        in2 += "&mgm.subcmd=rm";
        in2 += "&mgm.vid.key=vid:" + request->vid().rm().key() + ":gids";
      }
      else {
        in1 += "&mgm.subcmd=rm";
        in1 += "&mgm.vid.key=" + request->vid().rm().key();
      }
      break;
    }
    case eos::console::VidProto::kSetgeotag: {
      in1 += "&mgm.subcmd=set";
      in1 += "&mgm.vid.cmd=geotag";
      in1 += "&mgm.vid.key=geotag:" + request->vid().setgeotag().prefix();
      in1 += "&mgm.vid.geotag=" + request->vid().setgeotag().geotag();
      break;
    }
    case eos::console::VidProto::kSetmembership: {
      std::string user = request->vid().setmembership().user();
      std::string members = request->vid().setmembership().members();

      in1 += "&mgm.subcmd=set";
      in1 += "&mgm.vid.cmd=membership";
      in1 += "&mgm.vid.source.uid=" + request->vid().setmembership().user();

      if (request->vid().setmembership().option() == eos::console::VidProto_SetMembershipProto_Option_USER) {
        in1 += "&mgm.vid.key=" + user + ":uids";
        in1 += "&mgm.vid.target.uid=" + members;
      }
      else if (request->vid().setmembership().option() == eos::console::VidProto_SetMembershipProto_Option_GROUP) {
        in1 += "&mgm.vid.key=" + user + ":gids";
        in1 += "&mgm.vid.target.gid=" + members;
      }
      else if (request->vid().setmembership().option() == eos::console::VidProto_SetMembershipProto_Option_ADD_SUDO) {
        in1 += "&mgm.vid.key=" + user + ":root";
        in1 += "&mgm.vid.target.sudo=true";
      }
      else if (request->vid().setmembership().option() == eos::console::VidProto_SetMembershipProto_Option_REMOVE_SUDO) {
        in1 += "&mgm.vid.key=" + user + ":root";
        in1 += "&mgm.vid.target.sudo=false";
      }
      break;
    }
    case eos::console::VidProto::kSetmap: {
      in1 += "&mgm.subcmd=set";
      in1 += "&mgm.vid.cmd=map";
      if (request->vid().setmap().type() == eos::console::VidProto_SetMapProto_Type_KRB5)
        in1 += "&mgm.vid.auth=krb5";
      else if (request->vid().setmap().type() == eos::console::VidProto_SetMapProto_Type_GSI)
        in1 += "&mgm.vid.auth=gsi";
      else if (request->vid().setmap().type() == eos::console::VidProto_SetMapProto_Type_HTTPS)
        in1 += "&mgm.vid.auth=https";
      else if (request->vid().setmap().type() == eos::console::VidProto_SetMapProto_Type_SSS)
        in1 += "&mgm.vid.auth=sss";
      else if (request->vid().setmap().type() == eos::console::VidProto_SetMapProto_Type_UNIX)
        in1 += "&mgm.vid.auth=unix";
      else if (request->vid().setmap().type() == eos::console::VidProto_SetMapProto_Type_TIDENT)
        in1 += "&mgm.vid.auth=tident";
      else if (request->vid().setmap().type() == eos::console::VidProto_SetMapProto_Type_VOMS)
        in1 += "&mgm.vid.auth=voms";
      else if (request->vid().setmap().type() == eos::console::VidProto_SetMapProto_Type_GRPC)
        in1 += "&mgm.vid.auth=grpc";

      in1 += "&mgm.vid.key=<key>";
      in1 += "&mgm.vid.pattern=" + request->vid().setmap().pattern();
      if (!request->vid().setmap().vgid_only())
        in1 += "&mgm.vid.uid=" + std::to_string(request->vid().setmap().vuid());
      if (!request->vid().setmap().vuid_only())
        in1 += "&mgm.vid.gid=" + std::to_string(request->vid().setmap().vgid());
      break;
    }
    default: {
      reply->set_retc(EINVAL);
      reply->set_std_err("error: subcommand is not supported");

      return grpc::Status::OK;
    }
  }

  cmd1.open("/proc/admin", in1.c_str(), vid, &error1);
  cmd1.AddOutput(stdOut1, stdErr1);
  cmd1.close();

  if (has_cmd2) {
    cmd2.open("/proc/admin", in2.c_str(), vid, &error2);
    cmd2.AddOutput(stdOut2, stdErr2);
    cmd2.close();

    if (!stdOut1.empty())
      stdOut1.insert(0, "UID: ");
    if (!stdErr1.empty()) {
      stdErr1.insert(0, "UID: ");
      stdErr1 += "\n";
    }

    if (!stdOut2.empty())
      stdOut2.insert(0, "GID: ");
    if (!stdErr2.empty()) {
      stdErr2.insert(0, "GID: ");
      stdErr2 += "\n";
    }
  }

  reply->set_retc((cmd1.GetRetc() > cmd2.GetRetc()) ? cmd1.GetRetc() : cmd2.GetRetc());
  reply->set_std_err(stdErr1 + stdErr2);
  reply->set_std_out(stdOut1 + stdOut2);

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Who(eos::common::VirtualIdentity& vid,
                      const eos::console::RequestProto* request,
                      eos::console::ReplyProto* reply)
{
  std::string stdOut, stdErr;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string in = "mgm.cmd=who";

  if (request->who().showclients() || request->who().showauth() ||
      request->who().showall() || request->who().showsummary() ||
      request->who().monitoring())
    in += "&mgm.option=";
  if (request->who().showclients())
    in += "c";
  if (request->who().showauth())
    in += "z";
  if (request->who().showall())
    in += "a";
  if (request->who().showsummary())
    in += "s";
  if (request->who().monitoring())
    in += "m";

  cmd.open("/proc/user", in.c_str(), vid, &error);
  cmd.AddOutput(stdOut, stdErr);
  cmd.close();

  reply->set_retc(cmd.GetRetc());
  reply->set_std_err(stdErr);
  reply->set_std_out(stdOut);

  return grpc::Status::OK;
}

grpc::Status
GrpcWncInterface::Whoami(eos::common::VirtualIdentity& vid,
                         const eos::console::RequestProto* request,
                         eos::console::ReplyProto* reply)
{
  std::string stdOut, stdErr;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string in = "mgm.cmd=whoami";

  cmd.open("/proc/user", in.c_str(), vid, &error);
  cmd.AddOutput(stdOut, stdErr);
  cmd.close();

  reply->set_retc(cmd.GetRetc());
  reply->set_std_err(stdErr);
  reply->set_std_out(stdOut);

  return grpc::Status::OK;
}

bool
GrpcWncInterface::MdFilterContainer(std::shared_ptr<eos::IContainerMD> md,
                                    const eos::console::MDSelection& filter)
{
  errno = 0;
  // see if filtering is enabled
  if (!filter.select())
    return false;

  eos::IContainerMD::ctime_t ctime;
  eos::IContainerMD::ctime_t mtime;
  eos::IContainerMD::ctime_t stime;
  md->getCTime(ctime);
  md->getMTime(mtime);
  md->getTMTime(stime);

  size_t nchildren = md->getNumContainers() + md->getNumFiles();
  uint64_t treesize = md->getTreeSize();

  // find faultyacl
  if (filter.faultyacl()) {
    XrdOucErrInfo errInfo;
    std::string acl;
    if (gOFS->_attr_get(*md.get(), "sys.acl", acl)) {
      if (Acl::IsValid(acl.c_str(), errInfo))
        return true;
    } else
      return true;

    if (gOFS->_attr_get(*md.get(), "user.acl", acl)) {
      if (Acl::IsValid(acl.c_str(), errInfo))
        return true;
    } else
      return true;
  }

  // empty file
  if (filter.children().zero()) {
    if (!nchildren) {
      // accepted
    } else {
      return true;
    }
  } else {
    if ( (filter.children().min() <= nchildren) &&
         ((nchildren <= filter.children().max()) || (!filter.children().max()))
       ) {
      // accepted
    } else {
      return true;
    }
  }

  if (filter.treesize().zero()) {
    if (!treesize) {
      // accepted
    } else {
      return true;
    }
  } else {
    if ( (filter.treesize().min() <= treesize) &&
         ((treesize <= filter.treesize().max()) || (!filter.treesize().max()))
       ) {
      // accepted
    } else {
      return true;
    }
  }

  if (filter.ctime().zero()) {
    if (!ctime.tv_sec && !ctime.tv_nsec) {
      // accepted
    } else {
      return true;
    }
  } else {
    if ( (filter.ctime().min() <= (uint64_t)(ctime.tv_sec)) &&
         ( (filter.ctime().max() >= (uint64_t)(ctime.tv_sec)) ||
           (!filter.ctime().max()) ) ) {
      // accepted
    } else {
      return true;
    }
  }

  if (filter.mtime().zero()) {
    if (!mtime.tv_sec && !mtime.tv_nsec) {
      // accepted
    } else {
      return true;
    }
  } else {
    if ( (filter.mtime().min() <= (uint64_t)(mtime.tv_sec)) &&
         ( (filter.mtime().max() >= (uint64_t)(mtime.tv_sec)) ||
           (!filter.mtime().max()) ) ) {
      // accepted
    } else {
      return true;
    }
  }

  if (filter.stime().zero()) {
    if (!stime.tv_sec && !stime.tv_nsec) {
      // accepted
    } else {
      return true;
    }
  } else {
    if ( (filter.stime().min() <= (uint64_t)(stime.tv_sec)) &&
         ( (filter.stime().max() >= (uint64_t)(stime.tv_sec)) ||
           (!filter.stime().max()) ) ) {
      // accepted
    } else {
      return true;
    }
  }

  if (filter.owner_root()) {
    if (!md->getCUid()) {
      // accepted
    } else {
      return true;
    }
  } else {
    if (filter.owner()) {
      if (filter.owner() == md->getCUid()) {
        // accepted
      } else {
        return true;
      }
    }
  }

  if (filter.group_root()) {
    if (!md->getCGid()) {
      // accepted
    } else {
      return true;
    }
  } else {
    if (filter.group()) {
      if (filter.group() == md->getCGid()) {
        // accepted
      } else {
        return true;
      }
    }
  }

  if (filter.flags()) {
    if (md->getFlags() != filter.flags()) {
      return true;
    }
  }

  eos::IContainerMD::XAttrMap xattr = md->getAttributes();
  for (const auto& elem : filter.xattr()) {
    if (xattr.count(elem.first)) {
      if (elem.second.length()) {
        if (xattr[elem.first] == elem.second) {
          // accepted
        } else {
          return true;
        }
      } else {
        // accepted
      }
    } else {
      return true;
    }
  }

  if (filter.regexp_dirname().length()) {
    int regexErrorCode;
    int result;
    regex_t regex;
    std::string regexString = filter.regexp_dirname();

    regexErrorCode = regcomp(&regex, regexString.c_str(), REG_EXTENDED);

    if (regexErrorCode) {
      regfree(&regex);
      errno = EINVAL;
      return true;
    }

    result = regexec(&regex, md->getName().c_str(), 0, NULL, 0);
    regfree(&regex);
    if (result == 0) {
      // accepted
    } else if (result == REG_NOMATCH) {
      return true;
    } else {
      errno = ENOMEM;
      return true;
    }
  }

  return false;
}

bool
GrpcWncInterface::MdFilterFile(std::shared_ptr<eos::IFileMD> md,
                               const eos::console::MDSelection& filter)
{
  errno = 0;
  // see if filtering is enabled
  if (!filter.select())
    return false;

  eos::IFileMD::ctime_t ctime;
  eos::IFileMD::ctime_t mtime;
  md->getCTime(ctime);
  md->getMTime(mtime);

  // find files which have replicas on mixed scheduling groups
  if (filter.mixed_group()) {
    std::string sGroupRef = "";
    std::string sGroup = "";
    bool hasMixedGroups = false;
    for (const auto& loca : md->getLocations()) {
      // lookup filesystem
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      eos::common::FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(loca);

      if (filesystem != nullptr)
        sGroup = filesystem->GetString("schedgroup");
      else
        sGroup = "none";

      if (!sGroupRef.empty()) {
        if (sGroup != sGroupRef)
          hasMixedGroups = true;
      }
      else
        sGroupRef = sGroup;
    }
    if (!hasMixedGroups)
      return true;
  }

  // filter stripediff
  if (filter.stripediff() &&
      md->getNumLocation() == eos::common::LayoutId::GetStripeNumber(md->getLayoutId()) + 1)
    return true;

  // empty file
  if (filter.size().zero()) {
    if (!md->getSize()) {
      // accepted
    } else {
      return true;
    }
  } else {
    if ( (filter.size().min() <= md->getSize()) &&
         ( (md->getSize() <= filter.size().max()) || (!filter.size().max())) ) {
      // accepted
    } else {
      return true;
    }
  }

  if (filter.ctime().zero()) {
    if (!ctime.tv_sec && !ctime.tv_nsec) {
      // accepted
    } else {
      return true;
    }
  } else {
    if ( (filter.ctime().min() <= (uint64_t)(ctime.tv_sec)) &&
         ( (filter.ctime().max() >= (uint64_t)(ctime.tv_sec)) ||
           (!filter.ctime().max()) ) ) {
      // accepted
    } else {
      return true;
    }
  }

  if (filter.mtime().zero()) {
    if (!mtime.tv_sec && !mtime.tv_nsec) {
      // accepted
    } else {
      return true;
    }
  } else {
    if ( (filter.mtime().min() <= (uint64_t)(mtime.tv_sec)) &&
         ( (filter.mtime().max() >= (uint64_t)(mtime.tv_sec)) ||
           (!filter.mtime().max()) ) ) {
      // accepted
    } else {
      return true;
    }
  }

  if (filter.mtime().zero()) {
    if (!mtime.tv_sec && !mtime.tv_nsec) {
      // accepted
    } else {
      return true;
    }
  } else {
    if ( (filter.mtime().min() <= (uint64_t)(mtime.tv_sec)) &&
         ( (filter.mtime().max() >= (uint64_t)(mtime.tv_sec)) ||
           (!filter.mtime().max()) ) ) {
      // accepted
    } else {
      return true;
    }
  }

  if (filter.locations().zero()) {
    if (!mtime.tv_sec && !mtime.tv_nsec) {
      // accepted
    } else {
      return true;
    }
  } else {
    if ( (filter.locations().min() <= (uint64_t)(mtime.tv_sec)) &&
         ( (filter.locations().max() >= (uint64_t)(mtime.tv_sec)) ||
           (!filter.locations().max()) ) ) {
      // accepted
    } else {
      return true;
    }
  }

  if (filter.owner_root()) {
    if (!md->getCUid()) {
      // accepted
    } else {
      return true;
    }
  } else {
    if (filter.owner()) {
      if (filter.owner() == md->getCUid()) {
        // accepted
      } else {
        return true;
      }
    }
  }

  if (filter.group_root()) {
    if (!md->getCGid()) {
      // accepted
    } else {
      return true;
    }
  } else {
    if (filter.group()) {
      if (filter.group() == md->getCGid()) {
        // accepted
      } else {
        return true;
      }
    }
  }

  if (filter.layoutid()) {
    if (md->getLayoutId() != filter.layoutid()) {
      return true;
    }
  }

  if (filter.flags()) {
    if (md->getFlags() != filter.flags()) {
      return true;
    }
  }

  if (filter.symlink()) {
    if (!md->isLink()) {
      return true;
    }
  }

  if (filter.checksum().type().length()) {
    if (filter.checksum().type() !=
        eos::common::LayoutId::GetChecksumStringReal(md->getLayoutId())) {
      return true;
    }
  }

  if (filter.checksum().value().length()) {
    std::string cks(md->getChecksum().getDataPtr(), md->getChecksum().size());
    if (filter.checksum().value() != cks) {
      return true;
    }
  }

  eos::IFileMD::XAttrMap xattr = md->getAttributes();
  for (const auto& elem : filter.xattr()) {
    if (xattr.count(elem.first)) {
      if (elem.second.length()) {
        if (xattr[elem.first] == elem.second) {
          // accepted
        } else {
          return true;
        }
      } else {
        // accepted
      }
    } else {
      return true;
    }
  }

  if (filter.regexp_filename().length()) {
    int regexErrorCode;
    int result;
    regex_t regex;
    std::string regexString = filter.regexp_filename();

    regexErrorCode = regcomp(&regex, regexString.c_str(), REG_EXTENDED);

    if (regexErrorCode) {
      regfree(&regex);
      errno = EINVAL;
      return true;
    }

    result = regexec(&regex, md->getName().c_str(), 0, NULL, 0);
    regfree(&regex);
    if (result == 0) {
      // accepted
    } else if (result == REG_NOMATCH) {
      return true;
    } else {
      errno = ENOMEM;
      return true;
    }
  }

  return false;
}

grpc::Status
GrpcWncInterface::MdGet(eos::common::VirtualIdentity& vid,
                        const eos::console::RequestProto* request,
                        ServerWriter<eos::console::StreamReplyProto>* reply,
                        bool check_perms,
                        bool lock)
{
  if (request->list().type() == eos::console::FILE) {
    // stream file meta data
    eos::common::RWMutexReadLock viewReadLock;
    std::shared_ptr<eos::IFileMD> fmd;
    std::shared_ptr<eos::IContainerMD> pmd;
    unsigned long fid = 0;
    uint64_t clock = 0;
    std::string path;

    if (request->list().md().ino()) {
      // get by inode
      fid = eos::common::FileId::InodeToFid(request->list().md().ino());
    } else if (request->list().md().id()) {
      // get by fileid
      fid = request->list().md().id();
    }

    if (fid) {
      eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, fid);
    } else {
      eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView,
                                             request->list().md().path());
    }

    if (lock) {
    viewReadLock.Grab(gOFS->eosViewRWMutex);
    }

    if (fid) {
      try {
        fmd = gOFS->eosFileService->getFileMD(fid, &clock);
        path = gOFS->eosView->getUri(fmd.get());

        if (check_perms) {
          pmd = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
        }
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_static_debug("caught exception %d %s\n", e.getErrno(),
                         e.getMessage().str().c_str());
        return grpc::Status((grpc::StatusCode)(errno),
                            e.getMessage().str().c_str());
      }
    } else {
      try {
        fmd = gOFS->eosView->getFile(request->list().md().path());
        path = gOFS->eosView->getUri(fmd.get());

        if (check_perms) {
          pmd = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
        }
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_static_debug("caught exception %d %s\n", e.getErrno(),
                         e.getMessage().str().c_str());
        return grpc::Status((grpc::StatusCode)(errno),
                            e.getMessage().str().c_str());
      }
    }

    if (check_perms && !GrpcNsInterface::Access(vid, R_OK, pmd)) {
      return grpc::Status(grpc::StatusCode::PERMISSION_DENIED,
                          "access to parent container denied");
    }

    if (MdFilterFile(fmd, request->list().selection())) {
      // short-cut for filtered MD
      return grpc::Status::OK;
    }

    // create GRPC protobuf object
    eos::console::StreamReplyProto gRPCResponse;
    eos::console::FileMd gRPCFmd;

    struct stat buf;
    XrdOucErrInfo Error;
    gOFS->_stat(path.c_str(), &buf, Error, vid, (const char*) 0, 0, false);
    gRPCResponse.mutable_fmd()->set_backendstatus(buf.st_nlink);
    gRPCResponse.mutable_fmd()->set_backendmode((buf.st_mode & EOS_TAPE_MODE_T) ? 1 : 0);

    eos::console::RequestProto fileinfo_request;
    eos::console::ReplyProto fileinfo_reply;
    fileinfo_request.mutable_fileinfo()->mutable_md()->set_path(path);
    fileinfo_request.mutable_fileinfo()->set_monitoring(true);
    Fileinfo(vid, &fileinfo_request, &fileinfo_reply);
    gRPCResponse.mutable_fmd()->set_fileinfo(fileinfo_reply.std_out());

    gRPCResponse.mutable_fmd()->set_name(fmd->getName());
    gRPCResponse.mutable_fmd()->set_id(fmd->getId());
    gRPCResponse.mutable_fmd()->set_cont_id(fmd->getContainerId());
    gRPCResponse.mutable_fmd()->mutable_owner()->set_uid(fmd->getCUid());
    gRPCResponse.mutable_fmd()->mutable_owner()->set_username(
      eos::common::Mapping::UidToUserName(fmd->getCUid(), errno));
    gRPCResponse.mutable_fmd()->mutable_owner()->set_gid(fmd->getCGid());
    gRPCResponse.mutable_fmd()->mutable_owner()->set_groupname(
      eos::common::Mapping::GidToGroupName(fmd->getCGid(), errno));
    gRPCResponse.mutable_fmd()->set_size(fmd->getSize());
    gRPCResponse.mutable_fmd()->set_layout_id(fmd->getLayoutId());
    gRPCResponse.mutable_fmd()->set_flags(fmd->getFlags());
    gRPCResponse.mutable_fmd()->set_link_name(fmd->getLink());
    eos::IFileMD::ctime_t ctime;
    eos::IFileMD::ctime_t mtime;
    fmd->getCTime(ctime);
    fmd->getMTime(mtime);
    gRPCResponse.mutable_fmd()->mutable_ctime()->set_sec(ctime.tv_sec);
    gRPCResponse.mutable_fmd()->mutable_ctime()->set_n_sec(ctime.tv_nsec);
    gRPCResponse.mutable_fmd()->mutable_mtime()->set_sec(mtime.tv_sec);
    gRPCResponse.mutable_fmd()->mutable_mtime()->set_n_sec(mtime.tv_nsec);
    gRPCResponse.mutable_fmd()->mutable_checksum()->set_value(
      fmd->getChecksum().getDataPtr(), fmd->getChecksum().size());
    gRPCResponse.mutable_fmd()->mutable_checksum()->set_type(
      eos::common::LayoutId::GetChecksumStringReal(fmd->getLayoutId()));

    for (const auto& loca : fmd->getLocations()) {
      eos::console::FileMd_Location location;
      location.set_fsid(loca);

      // lookup filesystem
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      eos::common::FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(loca);

      if (!filesystem)
        continue;

      // get hosts and partitions
      eos::common::FileSystem::fs_snapshot_t fs;
      if (filesystem->SnapShotFileSystem(fs, true)) {
        if (filesystem->GetActiveStatus(true) == eos::common::ActiveStatus::kOnline)
          location.set_online(true);
        location.set_host(fs.mHost);
        location.set_partition(fs.mPath);
        location.set_space(fs.mSpace);
        location.set_group(fs.mGroup);
      }
      gRPCResponse.mutable_fmd()->add_locations()->MergeFrom(location);
    }

    for (const auto& loca : fmd->getUnlinkedLocations()) {
      gRPCResponse.mutable_fmd()->add_unlink_locations(loca);
    }

    for (const auto& elem : fmd->getAttributes()) {
      (*gRPCResponse.mutable_fmd()->mutable_xattrs())[elem.first] = elem.second;
    }

    gRPCResponse.mutable_fmd()->set_path(path);
    reply->Write(gRPCResponse);
    return grpc::Status::OK;
  }
  else if (request->list().type() == eos::console::CONTAINER) {
    // stream container meta data
    eos::common::RWMutexReadLock viewReadLock;
    std::shared_ptr<eos::IContainerMD> cmd;
    std::shared_ptr<eos::IContainerMD> pmd;
    unsigned long cid = 0;
    uint64_t clock = 0;
    std::string path;

    if (request->list().md().ino()) {
      // get by inode
      cid = request->list().md().ino();
    } else if (request->list().md().id()) {
      // get by containerid
      cid = request->list().md().id();
    }

    if (!cid) {
      eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView,
          request->list().md().path());
    } else {
      eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, cid);
    }

    if (lock) {
      viewReadLock.Grab(gOFS->eosViewRWMutex);
    }

    if (cid) {
      try {
        cmd = gOFS->eosDirectoryService->getContainerMD(cid, &clock);
        path = gOFS->eosView->getUri(cmd.get());
        pmd = gOFS->eosDirectoryService->getContainerMD(cmd->getParentId());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_static_debug("caught exception %d %s\n", e.getErrno(),
                         e.getMessage().str().c_str());
        return grpc::Status((grpc::StatusCode)(errno),
                            e.getMessage().str().c_str());
      }
    } else {
      try {
        cmd = gOFS->eosView->getContainer(request->list().md().path());
        path = gOFS->eosView->getUri(cmd.get());
        pmd = gOFS->eosDirectoryService->getContainerMD(cmd->getParentId());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_static_debug("caught exception %d %s\n", e.getErrno(),
                         e.getMessage().str().c_str());
        return grpc::Status((grpc::StatusCode)(errno),
                            e.getMessage().str().c_str());
      }
    }

    if (!GrpcNsInterface::Access(vid, R_OK, pmd)) {
      return grpc::Status(grpc::StatusCode::PERMISSION_DENIED,
                          "access to parent container denied");
    }

    if (MdFilterContainer(cmd, request->list().selection())) {
      // short-cut for filtered MD
      return grpc::Status::OK;
    }

    // create GRPC protobuf object
    eos::console::StreamReplyProto gRPCResponse;
    eos::console::ContainerMd gRPCFmd;

    struct stat buf;
    XrdOucErrInfo Error;
    gOFS->_stat(path.c_str(), &buf, Error, vid, (const char*) 0, 0, false);
    gRPCResponse.mutable_cmd()->set_backendstatus(buf.st_nlink);
    gRPCResponse.mutable_cmd()->set_backendmode((buf.st_mode & EOS_TAPE_MODE_T ? 1 : 0));

    eos::console::RequestProto fileinfo_request;
    eos::console::ReplyProto fileinfo_reply;
    fileinfo_request.mutable_fileinfo()->mutable_md()->set_path(path);
    fileinfo_request.mutable_fileinfo()->set_monitoring(true);
    Fileinfo(vid, &fileinfo_request, &fileinfo_reply);
    gRPCResponse.mutable_cmd()->set_fileinfo(fileinfo_reply.std_out());

    gRPCResponse.mutable_cmd()->set_name(cmd->getName());
    gRPCResponse.mutable_cmd()->set_id(cmd->getId());
    gRPCResponse.mutable_cmd()->set_parent_id(cmd->getParentId());
    gRPCResponse.mutable_cmd()->mutable_owner()->set_uid(cmd->getCUid());
    gRPCResponse.mutable_cmd()->mutable_owner()->set_username(
      eos::common::Mapping::UidToUserName(cmd->getCUid(), errno));
    gRPCResponse.mutable_cmd()->mutable_owner()->set_gid(cmd->getCGid());
    gRPCResponse.mutable_cmd()->mutable_owner()->set_groupname(
      eos::common::Mapping::GidToGroupName(cmd->getCGid(), errno));
    gRPCResponse.mutable_cmd()->set_tree_size(cmd->getTreeSize());
    gRPCResponse.mutable_cmd()->set_flags(cmd->getFlags());
    gRPCResponse.mutable_cmd()->set_mode(cmd->getMode());
    eos::IContainerMD::ctime_t ctime;
    eos::IContainerMD::ctime_t mtime;
    eos::IContainerMD::ctime_t stime;
    cmd->getCTime(ctime);
    cmd->getMTime(mtime);
    cmd->getTMTime(stime);
    gRPCResponse.mutable_cmd()->mutable_ctime()->set_sec(ctime.tv_sec);
    gRPCResponse.mutable_cmd()->mutable_ctime()->set_n_sec(ctime.tv_nsec);
    gRPCResponse.mutable_cmd()->mutable_mtime()->set_sec(mtime.tv_sec);
    gRPCResponse.mutable_cmd()->mutable_mtime()->set_n_sec(mtime.tv_nsec);
    gRPCResponse.mutable_cmd()->mutable_stime()->set_sec(stime.tv_sec);
    gRPCResponse.mutable_cmd()->mutable_stime()->set_n_sec(stime.tv_nsec);

    for (const auto& elem : cmd->getAttributes()) {
      (*gRPCResponse.mutable_cmd()->mutable_xattrs())[elem.first] = elem.second;
    }

    gRPCResponse.mutable_cmd()->set_path(path);

    reply->Write(gRPCResponse);
    return grpc::Status::OK;
  }

  return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid argument");
}

grpc::Status
GrpcWncInterface::MdStream(eos::common::VirtualIdentity& vid,
                           const eos::console::RequestProto* request,
                           ServerWriter<eos::console::StreamReplyProto>* reply,
                           bool streamparent,
                           std::vector<uint64_t>* childdirs)
{
  // stream container meta data
  eos::common::RWMutexReadLock viewReadLock;
  std::shared_ptr<eos::IContainerMD> cmd;
  unsigned long cid = 0;
  uint64_t clock = 0;
  std::string path;

  if (request->list().md().ino()) {
    // get by inode
    cid = request->list().md().ino();
  } else if (request->list().md().id()) {
    // get by containerid
    cid = request->list().md().id();
  }

  if (!cid) {
    eos::Prefetcher::prefetchContainerMDWithChildrenAndWait(gOFS->eosView,
        request->list().md().path());
  } else {
    eos::Prefetcher::prefetchContainerMDWithChildrenAndWait(gOFS->eosView,
        cid);
  }

  viewReadLock.Grab(gOFS->eosViewRWMutex);

  if (cid) {
    try {
      cmd = gOFS->eosDirectoryService->getContainerMD(cid, &clock);
      path = gOFS->eosView->getUri(cmd.get());
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_static_debug("caught exception %d %s\n", e.getErrno(),
                      e.getMessage().str().c_str());
      return grpc::Status((grpc::StatusCode)(errno),
                          e.getMessage().str().c_str());
    }
  } else {
    try {
      cmd = gOFS->eosView->getContainer(request->list().md().path());
      cid = cmd->getId();
      path = gOFS->eosView->getUri(cmd.get());
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_static_debug("caught exception %d %s\n", e.getErrno(),
                      e.getMessage().str().c_str());
      return grpc::Status((grpc::StatusCode)(errno),
                          e.getMessage().str().c_str());
    }
  }

  grpc::Status status;

  if (streamparent && (request->list().type() != eos::console::FILE)) {
    // stream the requested container
    eos::console::RequestProto c_dir;
    c_dir.mutable_list()->mutable_selection()->CopyFrom(
      request->list().selection());
    c_dir.mutable_list()->mutable_md()->set_id(cid);
    c_dir.mutable_list()->set_type(eos::console::CONTAINER);
    status = MdGet(vid, &c_dir, reply, true, false);
    if (!status.ok())
      return status;
    c_dir.mutable_list()->mutable_md()->set_id(cmd->getParentId());
    status = MdGet(vid, &c_dir, reply, true, false);
    if (!status.ok())
      return status;
  }

  bool first = true;

  // stream for listing and file type
  if (request->list().type() != eos::console::CONTAINER) {
    // stream all the children files
    for (auto it = eos::FileMapIterator(cmd); it.valid(); it.next()) {
      eos::console::RequestProto c_file;
      c_file.mutable_list()->mutable_selection()->CopyFrom(
        request->list().selection());
      c_file.mutable_list()->mutable_md()->set_id(it.value());
      c_file.mutable_list()->set_type(eos::console::FILE);
      status = MdGet(vid, &c_file, reply, first);

      if (!status.ok()) {
        return status;
      }

      first = false;
    }
  }

  // stream all the children container
  for (auto it = eos::ContainerMapIterator(cmd); it.valid(); it.next()) {
    if (request->list().type() != eos::console::FILE) {
      // stream for listing and container type
      eos::console::RequestProto c_dir;
      c_dir.mutable_list()->mutable_md()->set_id(it.value());
      c_dir.mutable_list()->mutable_selection()->CopyFrom(
        request->list().selection());
      c_dir.mutable_list()->set_type(eos::console::CONTAINER);
      status = MdGet(vid, &c_dir, reply, first);

      if (!status.ok()) {
        return status;
      }
    }

    if (childdirs) {
      childdirs->push_back(it.value());
    }
    first = false;
  }

  // finished streaming
  return grpc::Status::OK;
}

EOSMGMNAMESPACE_END

#endif // EOS_GRPC
