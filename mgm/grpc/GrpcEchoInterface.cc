#ifdef EOS_GRPC

//-----------------------------------------------------------------------------
#include "GrpcEchoInterface.hh"
//-----------------------------------------------------------------------------
#include "mgm/proc/user/AclCmd.hh"
//-----------------------------------------------------------------------------
#include "XrdPosix/XrdPosixXrootd.hh"
//-----------------------------------------------------------------------------

EOSMGMNAMESPACE_BEGIN

Status GrpcEchoInterface::AclCall(const AclProto* aclRequest, ReplyProto* reply)
{
  // initialise RequestProto object
  // AclProto* aclRequestCopy = new AclProto();
  // aclRequestCopy->CopyFrom(*aclRequest);
  // eos::console::RequestProto* req = new eos::console::RequestProto();
  // req->set_allocated_acl(aclRequestCopy);

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

EOSMGMNAMESPACE_END

#endif // EOS_GRPC
