#include "GrpcEchoServer.hh"
#include <google/protobuf/util/json_util.h>
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "mgm/Macros.hh"
#include "XrdSec/XrdSecEntity.hh"

#ifdef EOS_GRPC
#include "proto/go/echo_service.grpc.pb.h"
#include "libgateway.h"

#include <grpc++/security/credentials.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::Status;
using eos::echo::service::EchoService;
using eos::echo::service::SimpleMessage;
using eos::console::AclProto;
using eos::console::ReplyProto;

#endif

EOSMGMNAMESPACE_BEGIN

#ifdef EOS_GRPC

class EchoServiceImpl final : public EchoService::Service, public eos::common::LogId
{
  Status Echo(ServerContext* context, const SimpleMessage* request,
               SimpleMessage* reply) override
  {
    std::string json_out;
    (void) google::protobuf::util::MessageToJsonString(*request, &json_out);
    eos_static_info("msg=\"received echo request\" data=\"%s\"", json_out.c_str());
    reply->CopyFrom(*request);
    return Status::OK;
  }

  Status EchoBody(ServerContext* context, const SimpleMessage* request,
                  SimpleMessage* reply) override
  {
    reply->CopyFrom(*request);
    return Status::OK;
  }

  Status AclRequest(ServerContext* context, const AclProto* request,
                  ReplyProto* reply) override
  {
    std::string json_out;
    (void) google::protobuf::util::MessageToJsonString(*request, &json_out);
    eos_static_info("msg=\"received acl request\" data=\"%s\"", json_out.c_str());

    GrpcEchoInterface echoInterface;
    return echoInterface.AclCall(request, reply);
  }
};

/* return client DN*/
std::string
GrpcEchoServer::DN(grpc::ServerContext* context)
{
  /*
    The methods GetPeerIdentityPropertyName() and GetPeerIdentity() from grpc::ServerContext.auth_context
    will prioritize SAN fields (x509_subject_alternative_name) in favor of x509_common_name
  */
  std::string tag = "x509_common_name";
  auto resp = context->auth_context()->FindPropertyValues(tag);

  if (resp.empty()) {
    tag = "x509_subject_alternative_name";
    auto resp = context->auth_context()->FindPropertyValues(tag);

    if (resp.empty()) {
      return "";
    }
  }

  return resp[0].data();
}



/* return client IP */
std::string GrpcEchoServer::IP(grpc::ServerContext* context, std::string* id,
                           std::string* port)
{
  // format is ipv4:<ip>:<..> or ipv6:<ip>:<..> - we just return the IP address
  // butq net and id are populated as well with the prefix and suffix, respectively
  std::vector<std::string> tokens;
  eos::common::StringConversion::Tokenize(context->peer(),
                                          tokens,
                                          "[]");

  if (tokens.size() == 3) {
    if (id) {
      *id = tokens[0].substr(0, tokens[0].size() - 1);
    }

    if (port) {
      *port = tokens[2].substr(1, tokens[2].size() - 1);
    }

    return "[" + tokens[1] + "]";
  } else {
    tokens.clear();
    eos::common::StringConversion::Tokenize(context->peer(),
                                            tokens,
                                            ":");

    if (tokens.size() == 3) {
      if (id) {
        *id = tokens[0].substr(0, tokens[0].size());
      }

      if (port) {
        *port = tokens[2].substr(0, tokens[2].size());
      }

      return tokens[1];
    }

    return "";
  }
}

/* return VID for a given call */
void
GrpcEchoServer::Vid(grpc::ServerContext* context,
                eos::common::VirtualIdentity& vid,
                const std::string& authkey)
{
  XrdSecEntity client("grpc");
  std::string dn = DN(context);
  client.name = const_cast<char*>(dn.c_str());
  bool isEosToken = (authkey.substr(0, 8) == "zteos64:");
  std::string tident = dn.length() ? dn.c_str() : (isEosToken ? "eostoken" :
                       authkey.c_str());
  std::string id;
  std::string ip = GrpcEchoServer::IP(context, &id).c_str();
  tident += ".1:";
  tident += id;
  tident += "@";
  tident += ip;
  client.tident = tident.c_str();

  if (authkey.length()) {
    client.endorsements = const_cast<char*>(authkey.c_str());
  }

  eos::common::Mapping::IdMap(&client, "eos.app=grpc", client.tident, vid);
}

#endif

void
GrpcEchoServer::Run(ThreadAssistant& assistant) noexcept
{
#ifdef EOS_GRPC

//   if (getenv("EOS_MGM_GRPC_SSL_CERT") &&
//       getenv("EOS_MGM_GRPC_SSL_KEY") &&
//       getenv("EOS_MGM_GRPC_SSL_CA")) {
//     mSSL = true;
//     mSSLCertFile = getenv("EOS_MGM_GRPC_SSL_CERT");
//     mSSLKeyFile = getenv("EOS_MGM_GRPC_SSL_KEY");
//     mSSLCaFile = getenv("EOS_MGM_GRPC_SSL_CA");

//     if (eos::common::StringConversion::LoadFileIntoString(mSSLCertFile.c_str(),
//         mSSLCert) && !mSSLCert.length()) {
//       eos_static_crit("unable to load ssl certificate file '%s'",
//                       mSSLCertFile.c_str());
//       mSSL = false;
//     }

//     if (eos::common::StringConversion::LoadFileIntoString(mSSLKeyFile.c_str(),
//         mSSLKey) && !mSSLKey.length()) {
//       eos_static_crit("unable to load ssl key file '%s'", mSSLKeyFile.c_str());
//       mSSL = false;
//     }

//     if (eos::common::StringConversion::LoadFileIntoString(mSSLCaFile.c_str(),
//         mSSLCa) && !mSSLCa.length()) {
//       eos_static_crit("unable to load ssl ca file '%s'", mSSLCaFile.c_str());
//       mSSL = false;
//     }
//   }

  EchoServiceImpl service;
  grpc::ServerBuilder builder;

  // server bind address
  std::string bind_address = "0.0.0.0:";
  bind_address += std::to_string(mPort);
  // gateway bind address
  std::string gw_bind_address = "0.0.0.0:";
  gw_bind_address += "40054";

  // if (mSSL) {
  //   grpc::SslServerCredentialsOptions::PemKeyCertPair keycert = {
  //     mSSLKey,
  //     mSSLCert
  //   };
  //   grpc::SslServerCredentialsOptions sslOps(
  //     GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY);
  //   sslOps.pem_root_certs = mSSLCa;
  //   sslOps.pem_key_cert_pairs.push_back(keycert);
  //   builder.AddListeningPort(bind_address, grpc::SslServerCredentials(sslOps));
  // } else {
  //   builder.AddListeningPort(bind_address, grpc::InsecureServerCredentials());
  // }

  builder.AddListeningPort(bind_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  mEchoServer = builder.BuildAndStart();

  // spawn grpc gateway
  char* const addr = const_cast<char*>(bind_address.c_str());
  char* const gwaddr = const_cast<char*>(gw_bind_address.c_str());
  char* path = (char *)"../../../../protos/examplepb";
  const auto gatewayServer = SpawnGrpcGateway(gwaddr, "tcp", addr, path);
  std::cerr << "Done spawning GrpcGateway" << std::endl;

  if (mEchoServer) {
    mEchoServer->Wait();
  }

  WaitForGrpcGateway(gatewayServer);

#else
  // Make the compiler happy
  (void) mPort;
  (void) mSSL;
#endif
}

EOSMGMNAMESPACE_END
