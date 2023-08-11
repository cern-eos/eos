#pragma once

#ifdef EOS_GRPC

//-----------------------------------------------------------------------------
#include "common/VirtualIdentity.hh"
#include "mgm/Namespace.hh"
#include "proto/go/echo_service.grpc.pb.h"
//-----------------------------------------------------------------------------
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::Status;
using eos::echo::service::EchoService;
using eos::echo::service::SimpleMessage;
using eos::console::AccessProto;
using eos::console::AclProto;
using eos::console::AttrProto;
using eos::console::ReplyProto;
//-----------------------------------------------------------------------------

EOSMGMNAMESPACE_BEGIN

/**
 * @file   GrpcEchoInterface.hh
 *
 * @brief  This class bridges Http client commands to gRPC requests
 *
 */
class GrpcEchoInterface
{
public:

//-----------------------------------------------------------------------------
//  Execute specific EOS command for the EOS gRPC request
//-----------------------------------------------------------------------------
  Status AclCall(const AclProto* aclRequest, ReplyProto* reply);
  Status AccessCall(const AccessProto* accessRequest, ReplyProto* reply);
  Status AttrCall(const AttrProto* attrRequest, ReplyProto* reply);
//-----------------------------------------------------------------------------

private:

  void ExecProcCmd(eos::common::VirtualIdentity vid, ReplyProto* reply,
                   std::string input, bool admin = true);
};

EOSMGMNAMESPACE_END

#endif // EOS_GRPC
