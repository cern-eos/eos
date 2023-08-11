#ifdef EOS_GRPC

//-----------------------------------------------------------------------------
#include "GrpcEchoInterface.hh"
//-----------------------------------------------------------------------------
#include "common/Fmd.hh"
#include "common/ParseUtils.hh"

#include "mgm/proc/admin/AccessCmd.hh"
#include "mgm/proc/user/AclCmd.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/GeoTreeEngine.hh"
//-----------------------------------------------------------------------------
#include "XrdPosix/XrdPosixXrootd.hh"
//-----------------------------------------------------------------------------

EOSMGMNAMESPACE_BEGIN

grpc::Status GrpcEchoInterface::AclCall(const AclProto* aclRequest, ReplyProto* reply)
{
  // wrap the AclProto object into a RequestProto object
  AclProto aclRequestCopy;
  aclRequestCopy.CopyFrom(*aclRequest);
  eos::console::RequestProto req;
  req.mutable_acl()->CopyFrom(aclRequestCopy);

  // initialise VirtualIdentity object
  auto rootvid = eos::common::VirtualIdentity::Root();

  eos::mgm::AclCmd aclcmd(std::move(req), rootvid);
  *reply = aclcmd.ProcessRequest();

  return grpc::Status::OK;
}

grpc::Status GrpcEchoInterface::AccessCall(const AccessProto* accessRequest, ReplyProto* reply)
{
  // wrap the AccessProto object into a RequestProto object
  AccessProto accessRequestCopy;
  accessRequestCopy.CopyFrom(*accessRequest);
  eos::console::RequestProto req;
  req.mutable_access()->CopyFrom(accessRequestCopy);

  // initialise VirtualIdentity object
  auto rootvid = eos::common::VirtualIdentity::Root();

  eos::mgm::AccessCmd accesscmd(std::move(req), rootvid);
  *reply = accesscmd.ProcessRequest();

  return grpc::Status::OK;
}

grpc::Status GrpcEchoInterface::AttrCall(const AttrProto* attrRequest, ReplyProto* reply)
{
  // wrap the AttrProto object into a RequestProto object
  AttrProto attrRequestCopy;
  attrRequestCopy.CopyFrom(*attrRequest);
  eos::console::RequestProto req;
  req.mutable_attr()->CopyFrom(attrRequestCopy);

  // initialise VirtualIdentity object
  auto rootvid = eos::common::VirtualIdentity::Root();

  std::string cmd_in;
  std::string path = req.attr().md().path();
  eos::console::AttrCmd subcmd = req.attr().cmd();
  std::string key = req.attr().key();
  errno = 0;

  if (path.empty()) {
    if (req.attr().md().type() == eos::console::FILE) {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(
                 gOFS->eosFileService->getFileMD(req.attr().md().id()).get());
      } catch (eos::MDException& e) {
        path = "";
        errno = e.getErrno();
      }
    }
    else {
      try {
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        path = gOFS->eosView->getUri(
                 gOFS->eosDirectoryService->getContainerMD(req.attr().md().id()).get());
      } catch (eos::MDException& e) {
        path = "";
        errno = e.getErrno();
      }
    }

    if (path.empty()) {
      reply->set_std_err("error:path is empty");
      reply->set_retc(EINVAL);
      return grpc::Status::OK;
    }
  }

  cmd_in = "mgm.cmd=attr&mgm.path=" + path;

  if (subcmd == eos::console::AttrCmd::ATTR_LS) {
    cmd_in += "&mgm.subcmd=ls";
  }
  else if (subcmd == eos::console::AttrCmd::ATTR_SET) {
    cmd_in += "&mgm.subcmd=set";
    std::string value = req.attr().value();

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
        reply->set_std_err("Error: Value are not allowed");
        reply->set_retc(EINVAL);
        return grpc::Status::OK;
      }

      ProcCommand cmd;
      XrdOucErrInfo error;
      std::string set_def;

      set_def = cmd_in + "&mgm.attr.key=sys.forced.blocksize&mgm.attr.value=" + val[0];
      cmd.open("/proc/user", set_def.c_str(), rootvid, &error);

      set_def = cmd_in + "&mgm.attr.key=sys.forced.checksum&mgm.attr.value=" + val[1];
      cmd.open("/proc/user", set_def.c_str(), rootvid, &error);

      set_def = cmd_in + "&mgm.attr.key=sys.forced.layout&mgm.attr.value=" + val[2];
      cmd.open("/proc/user", set_def.c_str(), rootvid, &error);

      set_def = cmd_in + "&mgm.attr.key=sys.forced.nstripes&mgm.attr.value=" + val[3];
      cmd.open("/proc/user", set_def.c_str(), rootvid, &error);

      set_def = cmd_in + "&mgm.attr.key=sys.forced.space&mgm.attr.value=" + val[4];
      cmd.open("/proc/user", set_def.c_str(), rootvid, &error);

      if (value != "replica") {
        set_def = cmd_in + "&mgm.attr.key=sys.forced.blockchecksum&mgm.attr.value=" + val[5];
        cmd.open("/proc/user", set_def.c_str(), rootvid, &error);
      }
    }

    if (key == "sys.forced.placementpolicy" ||
        key == "user.forced.placementpolicy")
    {
      std::string policy;
      eos::common::SymKey::DeBase64(value, policy);

      // Check placement policy
      if (policy != "scattered" &&
          policy.rfind("hybrid:", 0) != 0 &&
          policy.rfind("gathered:", 0) != 0)
      {
        reply->set_std_err("Error: placement policy '" + policy + "' is invalid\n");
        reply->set_retc(EINVAL);
        return grpc::Status::OK;
      }

      // Check geotag in case of hybrid or gathered policy
      if (policy != "scattered") {
        std::string targetgeotag = policy.substr(policy.find(':') + 1);
        std::string tmp_geotag = eos::common::SanitizeGeoTag(targetgeotag);
        if (tmp_geotag != targetgeotag) {
          reply->set_std_err(tmp_geotag);
          reply->set_retc(EINVAL);
          return grpc::Status::OK;
        }
      }
    }

    cmd_in += "&mgm.attr.key=" + key;
    cmd_in += "&mgm.attr.value=" + value;
  }
  else if (subcmd == eos::console::AttrCmd::ATTR_GET) {
    cmd_in += "&mgm.subcmd=get";
    cmd_in += "&mgm.attr.key=" + key;
  }
  else if (subcmd == eos::console::AttrCmd::ATTR_RM) {
    cmd_in += "&mgm.subcmd=rm";
    cmd_in += "&mgm.attr.key=" + key;
  }
  else if (subcmd == eos::console::AttrCmd::ATTR_LINK) {
    cmd_in += "&mgm.subcmd=set";
    cmd_in += "&mgm.attr.key=sys.attr.link";
    cmd_in += "&mgm.attr.value=" + req.attr().link();
  }
  else if (subcmd == eos::console::AttrCmd::ATTR_UNLINK) {
    cmd_in += "&mgm.subcmd=rm";
    cmd_in += "&mgm.attr.key=sys.attr.link";
  }
  else if (subcmd == eos::console::AttrCmd::ATTR_FOLD) {
    cmd_in += "&mgm.subcmd=fold";
  }

  if (req.attr().recursive()) {
    cmd_in += "&mgm.option=r";
  }

  ExecProcCmd(rootvid, reply, cmd_in, false);

  return grpc::Status::OK;
}

void GrpcEchoInterface::ExecProcCmd(eos::common::VirtualIdentity vid,
                              ReplyProto* reply, std::string input, bool admin)
{
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string std_out, std_err;

  if (admin) {
    cmd.open("/proc/admin", input.c_str(), vid, &error);
  }
  else {
    cmd.open("/proc/user", input.c_str(), vid, &error);
  }

  cmd.close();
  cmd.AddOutput(std_out, std_err);

  reply->set_std_out(std_out);
  reply->set_std_err(std_err);
  reply->set_retc(cmd.GetRetc());
}

EOSMGMNAMESPACE_END

#endif // EOS_GRPC
