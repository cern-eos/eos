//-----------------------------------------------------------------------------
// File: GrpcWncServer.cc
// Author: Branko Blagojevic <branko.blagojevic@comtrade.com>
// Author: Ivan Arizanovic <ivan.arizanovic@comtrade.com>
//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
#include "GrpcWncServer.hh"
//-----------------------------------------------------------------------------
#include "GrpcServer.hh"
#include "GrpcWncInterface.hh"
#include "console/ConsoleMain.hh"
#include "mgm/Macros.hh"
//-----------------------------------------------------------------------------
#ifdef EOS_GRPC

#include "proto/EosWnc.grpc.pb.h"
using eos::console::EosWnc;
using grpc::ServerContext;

#endif // EOS_GRPC
//-----------------------------------------------------------------------------

EOSMGMNAMESPACE_BEGIN

#ifdef EOS_GRPC

class WncService final : public EosWnc::Service
{
  // Process gRPC request from the EOS Windows native client
  grpc::Status ProcessSingle(ServerContext* context,
                             const eos::console::RequestProto* request,
                             eos::console::ReplyProto* reply)
  {
    std::string command;

    switch (request->command_case()) {
    case eos::console::RequestProto::kAccess:
      command = "Access";
      break;

    case eos::console::RequestProto::kAcl:
      command = "Acl";
      break;

    case eos::console::RequestProto::kAttr:
      command = "Attr";
      break;

    case eos::console::RequestProto::kChmod:
      command = "Chmod";
      break;

    case eos::console::RequestProto::kChown:
      command = "Chown";
      break;

    case eos::console::RequestProto::kConfig:
      command = "Config";
      break;

    case eos::console::RequestProto::kDebug:
      command = "Debug";
      break;

    case eos::console::RequestProto::kFile:
      command = "File";
      break;

    case eos::console::RequestProto::kFileinfo:
      command = "Fileinfo";
      break;

    case eos::console::RequestProto::kFs:
      command = "Fs";
      break;

    case eos::console::RequestProto::kFsck:
      command = "Fsck";
      break;

    case eos::console::RequestProto::kGroup:
      command = "Group";
      break;

    case eos::console::RequestProto::kIo:
      command = "Io";
      break;

    case eos::console::RequestProto::kMkdir:
      command = "Mkdir";
      break;

    case eos::console::RequestProto::kMv:
      command = "Mv";
      break;

    case eos::console::RequestProto::kNode:
      command = "Node";
      break;

    case eos::console::RequestProto::kNs:
      command = "Ns";
      break;

    case eos::console::RequestProto::kQuota:
      command = "Quota";
      break;

    case eos::console::RequestProto::kRecycle:
      command = "Recycle";
      break;

    case eos::console::RequestProto::kRm:
      command = "Rm";
      break;

    case eos::console::RequestProto::kRmdir:
      command = "Rmdir";
      break;

    case eos::console::RequestProto::kRoute:
      command = "Route";
      break;

    case eos::console::RequestProto::kSpace:
      command = "Space";
      break;

    case eos::console::RequestProto::kStagerRm:
      command = "StagerRm";
      break;

    case eos::console::RequestProto::kStat:
      command = "Stat";
      break;

    case eos::console::RequestProto::kTouch:
      command = "Touch";
      break;

    case eos::console::RequestProto::kTransfer:
      command = "Transfer";
      break;

    case eos::console::RequestProto::kVersion:
      command = "Version";
      break;

    case eos::console::RequestProto::kVid:
      command = "Vid";
      break;

    case eos::console::RequestProto::kWho:
      command = "Who";
      break;

    case eos::console::RequestProto::kWhoami:
      command = "Whoami";
      break;

    default:
      command = "ping";
      break;
    }

    eos_static_debug("eos-wnc request from peer=%s IP=%s DN=%s token=%s command='%s'",
                     context->peer().c_str(),
                     GrpcServer::IP(context).c_str(),
                     GrpcServer::DN(context).c_str(),
                     request->auth().authkey().c_str(),
                     command.c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->auth().authkey());
    WAIT_BOOT;
    return GrpcWncInterface::ExecCmd(vid, request, reply);
  }

  // Process gRPC request from the EOS Windows native client for metadata or realtime reply
  grpc::Status ProcessStream(ServerContext* context,
                             const eos::console::RequestProto* request,
                             grpc::ServerWriter<eos::console::StreamReplyProto>* writer)
  {
    std::string command;

    switch (request->command_case()) {
    default:
      command = "unknown";
      break;
    }

    eos_static_debug("eos-wnc request from peer=%s IP=%s DN=%s token=%s command='%s'",
                     context->peer().c_str(),
                     GrpcServer::IP(context).c_str(),
                     GrpcServer::DN(context).c_str(),
                     request->auth().authkey().c_str(),
                     command.c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->auth().authkey());
    WAIT_BOOT;
    return GrpcWncInterface::ExecStreamCmd(vid, request, writer);
  }
};

#endif

//-----------------------------------------------------------------------------
// Run gRPC server for EOS Windows native client
//-----------------------------------------------------------------------------
void
GrpcWncServer::RunWnc(ThreadAssistant& assistant) noexcept
{
#ifdef EOS_GRPC

  if (getenv("EOS_MGM_WNC_SSL_CERT") &&
      getenv("EOS_MGM_WNC_SSL_KEY") &&
      getenv("EOS_MGM_WNC_SSL_CA")) {
    mSSL = true;
    mSSLCertFile = getenv("EOS_MGM_WNC_SSL_CERT");
    mSSLKeyFile = getenv("EOS_MGM_WNC_SSL_KEY");
    mSSLCaFile = getenv("EOS_MGM_WNC_SSL_CA");

    if (eos::common::StringConversion::LoadFileIntoString(mSSLCertFile.c_str(),
        mSSLCert) && !mSSLCert.length()) {
      eos_static_crit("Unable to load SSL certificate file '%s'",
                      mSSLCertFile.c_str());
      mSSL = false;
    }

    if (eos::common::StringConversion::LoadFileIntoString(mSSLKeyFile.c_str(),
        mSSLKey) && !mSSLKey.length()) {
      eos_static_crit("Unable to load SSL key file '%s'", mSSLKeyFile.c_str());
      mSSL = false;
    }

    if (eos::common::StringConversion::LoadFileIntoString(mSSLCaFile.c_str(),
        mSSLCa) && !mSSLCa.length()) {
      eos_static_crit("Unable to load SSL CA file '%s'", mSSLCaFile.c_str());
      mSSL = false;
    }
  }

  if (gGlobalOpts.mMgmUri.empty()) {
    if (getenv("EOS_MGM_URL"))
      gGlobalOpts.mMgmUri = getenv("EOS_MGM_URL");
    else
      gGlobalOpts.mMgmUri = "root://localhost";
  }

  eos_static_info("Creating gRPC server for EOS-wnc.");
  grpc::ServerBuilder wncBuilder;
  std::string bind_address_Wnc = "0.0.0.0:";
  bind_address_Wnc += std::to_string(mWncPort);

  if (mSSL) {
    grpc::SslServerCredentialsOptions::PemKeyCertPair keycert = {
      mSSLKey,
      mSSLCert
    };
    grpc::SslServerCredentialsOptions sslOps(
      GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY);
    sslOps.pem_root_certs = mSSLCa;
    sslOps.pem_key_cert_pairs.push_back(keycert);
    wncBuilder.AddListeningPort(bind_address_Wnc,
                                grpc::SslServerCredentials(sslOps));
    eos_static_info("SSL authentication is enabled on gRPC server for EOS-wnc.");
  } else {
    wncBuilder.AddListeningPort(bind_address_Wnc,
                                grpc::InsecureServerCredentials());
  }

  WncService wncService;
  wncBuilder.RegisterService(&wncService);
  mWncServer = wncBuilder.BuildAndStart();
  eos_static_info("gRPC server for EOS-wnc is running on port %i.", mWncPort);
  /*WARNING: The server must be either shutting down or
   *some other thread must call a Shutdown for Wait function to ever return.*/
  mWncServer->Wait();
#else
  (void) mWncPort;
  (void) mSSL;
#endif
}

EOSMGMNAMESPACE_END