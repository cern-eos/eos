#include "GrpcAndreeaServer.hh"
#include <google/protobuf/util/json_util.h>
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "mgm/Macros.hh"
#include "XrdSec/XrdSecEntity.hh"

#ifdef EOS_GRPC
#include "proto/SimpleService.grpc.pb.h"
#include <grpc++/security/credentials.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::Status;
using simple_service::SimpleService;
using simple_service::PingRequest;
using simple_service::PingReply;

#endif

EOSMGMNAMESPACE_BEGIN

#ifdef EOS_GRPC

class SimpleServiceImpl final : public simple_service::SimpleService::Service
{

  Status Ping(ServerContext* context, const simple_service::PingRequest* request,
              simple_service::PingReply* reply) override
  {
    eos_static_info("grpc::ping from client peer=%s len=%lu",
                    context->peer().c_str(),
                    request->message().length());
    reply->set_message(request->message());
    return Status::OK;
  }
};

/* return client DN*/
std::string
GrpcAndreeaServer::DN(grpc::ServerContext* context)
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
std::string GrpcAndreeaServer::IP(grpc::ServerContext* context, std::string* id,
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
GrpcAndreeaServer::Vid(grpc::ServerContext* context,
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
  std::string ip = GrpcAndreeaServer::IP(context, &id).c_str();
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
GrpcAndreeaServer::Run(ThreadAssistant& assistant) noexcept
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

  SimpleServiceImpl service;
  std::string bind_address = "0.0.0.0:";
  bind_address += std::to_string(mPort);
  grpc::ServerBuilder builder;

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
  mAndreeaServer = builder.BuildAndStart();

  if (mAndreeaServer) {
    mAndreeaServer->Wait();
  }

#else
  // Make the compiler happy
  (void) mPort;
  (void) mSSL;
#endif
}

EOSMGMNAMESPACE_END
