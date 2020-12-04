//-----------------------------------------------------------------------------
// File: GrpcWncInterface.hh
// Author: Branko Blagojevic <branko.blagojevic@comtrade.com>
// Author: Ivan Arizanovic <ivan.arizanovic@comtrade.com>
//-----------------------------------------------------------------------------

#pragma once

#ifdef EOS_GRPC

//-----------------------------------------------------------------------------
#include "namespace/interface/IContainerMD.hh"
#include "GrpcWncServer.hh"
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
  static grpc::Status ExecCmd(eos::common::VirtualIdentity& vid,
                              const eos::console::RequestProto* request,
                              eos::console::ReplyProto* reply);

  static grpc::Status ExecStreamCmd(eos::common::VirtualIdentity& vid,
                                    const eos::console::RequestProto* request,
                                    ServerWriter<eos::console::StreamReplyProto>* writer);
//-----------------------------------------------------------------------------

private:

  static void RoleChanger(eos::common::VirtualIdentity& vid,
                          const eos::console::RequestProto* request);

//-----------------------------------------------------------------------------
//  Execute specific EOS command from the EOS-wnc gRPC request
//-----------------------------------------------------------------------------
  static grpc::Status Access(eos::common::VirtualIdentity& vid,
                             const eos::console::RequestProto* request,
                             eos::console::ReplyProto* reply);

  static grpc::Status Acl(eos::common::VirtualIdentity& vid,
                          const eos::console::RequestProto* request,
                          eos::console::ReplyProto* reply);

  static grpc::Status Attr(eos::common::VirtualIdentity& vid,
                           const eos::console::RequestProto* request,
                           eos::console::ReplyProto* reply);

  static grpc::Status Chmod(eos::common::VirtualIdentity& vid,
                            const eos::console::RequestProto* request,
                            eos::console::ReplyProto* reply);

  static grpc::Status Chown(eos::common::VirtualIdentity& vid,
                            const eos::console::RequestProto* request,
                            eos::console::ReplyProto* reply);

  static grpc::Status Config(eos::common::VirtualIdentity& vid,
                             const eos::console::RequestProto* request,
                             eos::console::ReplyProto* reply);

  static grpc::Status Debug(eos::common::VirtualIdentity& vid,
                            const eos::console::RequestProto* request,
                            eos::console::ReplyProto* reply);

  static grpc::Status File(eos::common::VirtualIdentity& vid,
                           const eos::console::RequestProto* request,
                           eos::console::ReplyProto* reply);

  static grpc::Status Fileinfo(eos::common::VirtualIdentity& vid,
                               const eos::console::RequestProto* request,
                               eos::console::ReplyProto* reply);

  static grpc::Status Find(eos::common::VirtualIdentity& vid,
                           const eos::console::RequestProto* request,
                           std::vector<eos::console::StreamReplyProto>& reply);

  static grpc::Status Fs(eos::common::VirtualIdentity& vid,
                         const eos::console::RequestProto* request,
                         eos::console::ReplyProto* reply);

  static grpc::Status Group(eos::common::VirtualIdentity& vid,
                            const eos::console::RequestProto* request,
                            eos::console::ReplyProto* reply);

  static grpc::Status Io(eos::common::VirtualIdentity& vid,
                         const eos::console::RequestProto* request,
                         eos::console::ReplyProto* reply);

  static grpc::Status List(eos::common::VirtualIdentity& vid,
                           const eos::console::RequestProto* request,
                           std::vector<eos::console::StreamReplyProto>& reply);

  static grpc::Status Mkdir(eos::common::VirtualIdentity& vid,
                            const eos::console::RequestProto* request,
                            eos::console::ReplyProto* reply);

  static grpc::Status Mv(eos::common::VirtualIdentity& vid,
                         const eos::console::RequestProto* request,
                         eos::console::ReplyProto* reply);

  static grpc::Status Node(eos::common::VirtualIdentity& vid,
                           const eos::console::RequestProto* request,
                           eos::console::ReplyProto* reply);

  static grpc::Status Ns(eos::common::VirtualIdentity& vid,
                         const eos::console::RequestProto* request,
                         eos::console::ReplyProto* reply);

  static grpc::Status Quota(eos::common::VirtualIdentity& vid,
                            const eos::console::RequestProto* request,
                            eos::console::ReplyProto* reply);

  static grpc::Status Recycle(eos::common::VirtualIdentity& vid,
                              const eos::console::RequestProto* request,
                              eos::console::ReplyProto* reply);

  static grpc::Status Rm(eos::common::VirtualIdentity& vid,
                         const eos::console::RequestProto* request,
                         eos::console::ReplyProto* reply);

  static grpc::Status Rmdir(eos::common::VirtualIdentity& vid,
                            const eos::console::RequestProto* request,
                            eos::console::ReplyProto* reply);

  static grpc::Status Route(eos::common::VirtualIdentity& vid,
                            const eos::console::RequestProto* request,
                            eos::console::ReplyProto* reply);

  static grpc::Status Space(eos::common::VirtualIdentity& vid,
                            const eos::console::RequestProto* request,
                            eos::console::ReplyProto* reply);

  static grpc::Status StagerRm(eos::common::VirtualIdentity& vid,
                               const eos::console::RequestProto* request,
                               eos::console::ReplyProto* reply);

  static grpc::Status Stat(eos::common::VirtualIdentity& vid,
                           const eos::console::RequestProto* request,
                           eos::console::ReplyProto* reply);

  static grpc::Status Touch(eos::common::VirtualIdentity& vid,
                            const eos::console::RequestProto* request,
                            eos::console::ReplyProto* reply);

  static grpc::Status Transfer(eos::common::VirtualIdentity& vid,
                               const eos::console::RequestProto* request,
                               eos::console::ReplyProto* reply,
                               ServerWriter<eos::console::StreamReplyProto>* writer);

  static grpc::Status Version(eos::common::VirtualIdentity& vid,
                              const eos::console::RequestProto* request,
                              eos::console::ReplyProto* reply);

  static grpc::Status Vid(eos::common::VirtualIdentity& vid,
                          const eos::console::RequestProto* request,
                          eos::console::ReplyProto* reply);

  static grpc::Status Who(eos::common::VirtualIdentity& vid,
                          const eos::console::RequestProto* request,
                          eos::console::ReplyProto* reply);

  static grpc::Status Whoami(eos::common::VirtualIdentity& vid,
                             const eos::console::RequestProto* request,
                             eos::console::ReplyProto* reply);
//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// Metadata common functions
//-----------------------------------------------------------------------------
  static bool MdFilterContainer(std::shared_ptr<eos::IContainerMD> md,
                                const eos::console::MDSelection& filter);

  static bool MdFilterFile(std::shared_ptr<eos::IFileMD> md,
                           const eos::console::MDSelection& filter);

  static grpc::Status MdGet(eos::common::VirtualIdentity& vid,
                            const eos::console::RequestProto* request,
                            std::vector<eos::console::StreamReplyProto>& reply,
                            bool check_perms = true,
                            bool lock = true);

  static grpc::Status MdStream(eos::common::VirtualIdentity& ivid,
                               const eos::console::RequestProto* request,
                               std::vector<eos::console::StreamReplyProto>& reply,
                               bool streamparent = true,
                               std::vector<uint64_t>* childdirs = 0);
//-----------------------------------------------------------------------------
};

EOSMGMNAMESPACE_END

#endif // EOS_GRPC