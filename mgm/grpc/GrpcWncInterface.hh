//-----------------------------------------------------------------------------
// File: GrpcWncInterface.hh
// Author: Branko Blagojevic <branko.blagojevic@comtrade.com>
// Author: Ivan Arizanovic <ivan.arizanovic@comtrade.com>
//-----------------------------------------------------------------------------

#pragma once

#ifdef EOS_GRPC

//-----------------------------------------------------------------------------
#include "common/VirtualIdentity.hh"
#include "mgm/Namespace.hh"
#include "proto/EosWnc.grpc.pb.h"
//-----------------------------------------------------------------------------
using grpc::ServerWriter;
//-----------------------------------------------------------------------------

EOSMGMNAMESPACE_BEGIN

/**
 * @file   GrpcWncInterface.hh
 *
 * @brief  This class bridges CLI of EOS Windows native client to gRPC requests
 *
 */
class GrpcWncInterface
{
public:

//-----------------------------------------------------------------------------
// Call appropriate function to execute command from the EOS-wnc gRPC request
//-----------------------------------------------------------------------------
  grpc::Status ExecCmd(eos::common::VirtualIdentity& vid,
                       const eos::console::RequestProto* request,
                       eos::console::ReplyProto* reply);

  grpc::Status ExecStreamCmd(eos::common::VirtualIdentity& vid,
                             const eos::console::RequestProto* request,
                             ServerWriter<eos::console::ReplyProto>* writer);
//-----------------------------------------------------------------------------

private:

//-----------------------------------------------------------------------------
// Class member variables
//-----------------------------------------------------------------------------
  bool mJsonFormat;
  eos::common::VirtualIdentity* mVid;
  const eos::console::RequestProto* mRequest;
  eos::console::ReplyProto* mReply;
  ServerWriter<eos::console::ReplyProto>* mWriter;
//-----------------------------------------------------------------------------

  void RoleChanger();

  void ExecProcCmd(std::string input, bool admin = true);

//-----------------------------------------------------------------------------
//  Execute specific EOS command from the EOS-wnc gRPC request
//-----------------------------------------------------------------------------
  grpc::Status Access();

  grpc::Status Acl();

  grpc::Status Archive();

  grpc::Status Attr();

  grpc::Status Backup();

  grpc::Status Chmod();

  grpc::Status Chown();

  grpc::Status Config();

  grpc::Status Convert();

  grpc::Status Cp();

  grpc::Status Debug();

  grpc::Status Evict();

  grpc::Status File();

  grpc::Status Fileinfo();

  grpc::Status Find();

  grpc::Status Fs();

  grpc::Status Fsck();

  grpc::Status Geosched();

  grpc::Status Group();

  grpc::Status Health();

  grpc::Status Io();

  grpc::Status Ls();

  grpc::Status Map();

  grpc::Status Member();

  grpc::Status Mkdir();

  grpc::Status Mv();

  grpc::Status Node();

  grpc::Status Ns();

  grpc::Status Quota();

  grpc::Status Recycle();

  grpc::Status Rm();

  grpc::Status Rmdir();

  grpc::Status Route();

  grpc::Status Space();

  grpc::Status Stat();

  grpc::Status Status();

  grpc::Status Token();

  grpc::Status Touch();

  grpc::Status Version();

  grpc::Status Vid();

  grpc::Status Who();

  grpc::Status Whoami();
//-----------------------------------------------------------------------------
};

EOSMGMNAMESPACE_END

#endif // EOS_GRPC
