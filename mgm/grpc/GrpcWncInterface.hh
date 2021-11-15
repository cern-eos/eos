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

  //---------------------------------------------------------------------------
  //! Complement ACL with usernames and groupnames
  //! for fileinfo output for EOS-Drive
  //!
  //! @param input  ACL string
  //! @return       ACL string with added usernames and groupnames
  //---------------------------------------------------------------------------
  static std::string AddNamesToACL(std::string input);

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

  static grpc::Status Archive(eos::common::VirtualIdentity& vid,
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

  static grpc::Status Cp(eos::common::VirtualIdentity& vid,
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

  static grpc::Status Fs(eos::common::VirtualIdentity& vid,
                         const eos::console::RequestProto* request,
                         eos::console::ReplyProto* reply);

  static grpc::Status Geosched(eos::common::VirtualIdentity& vid,
                               const eos::console::RequestProto* request,
                               eos::console::ReplyProto* reply);

  static grpc::Status Group(eos::common::VirtualIdentity& vid,
                            const eos::console::RequestProto* request,
                            eos::console::ReplyProto* reply);

  static grpc::Status Health(eos::common::VirtualIdentity& vid,
                             const eos::console::RequestProto* request,
                             eos::console::ReplyProto* reply);

  static grpc::Status Io(eos::common::VirtualIdentity& vid,
                         const eos::console::RequestProto* request,
                         eos::console::ReplyProto* reply);

  static grpc::Status Ls(eos::common::VirtualIdentity& vid,
                         const eos::console::RequestProto* request,
                         eos::console::ReplyProto* reply);

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
};

EOSMGMNAMESPACE_END

#endif // EOS_GRPC