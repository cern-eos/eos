#include "GrpcRestGwServer.hh"
#include <google/protobuf/util/json_util.h>
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "mgm/Macros.hh"
#include "XrdSec/XrdSecEntity.hh"

#ifdef EOS_GRPC
#include "proto/eos_rest_gateway/eos_rest_gateway_service.grpc.pb.h"
#include "libgateway.h"

#include <grpc++/security/credentials.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::Status;
using eos::rest::gateway::service::EosRestGatewayService;
using eos::console::AccessProto;
using eos::console::AclProto;
using eos::console::ArchiveProto;
using eos::console::AttrProto;
using eos::console::BackupProto;
using eos::console::ChmodProto;
using eos::console::ChownProto;
using eos::console::ConfigProto;
using eos::console::ConfigProto;
using eos::console::ConvertProto;
using eos::console::CpProto;
using eos::console::DebugProto;
using eos::console::FileProto;
using eos::console::FileinfoProto;
using eos::console::FsProto;
using eos::console::FsckProto;
using eos::console::GeoschedProto;
using eos::console::GroupProto;
using eos::console::HealthProto;
using eos::console::MapProto;
using eos::console::MemberProto;
using eos::console::IoProto;
using eos::console::MkdirProto;
using eos::console::MoveProto;
using eos::console::NodeProto;
using eos::console::NsProto;
using eos::console::QoSProto;
using eos::console::QuotaProto;
using eos::console::RecycleProto;
using eos::console::ReplyProto;
using eos::console::RmProto;
using eos::console::RmdirProto;
using eos::console::RouteProto;
using eos::console::SpaceProto;
using eos::console::StatProto;
using eos::console::StatusProto;
using eos::console::TokenProto;
using eos::console::TouchProto;
using eos::console::VersionProto;
using eos::console::VidProto;
using eos::console::WhoProto;
using eos::console::WhoamiProto;

#endif

EOSMGMNAMESPACE_BEGIN

#ifdef EOS_GRPC

class EosRestGatewayServiceImpl final : public EosRestGatewayService::Service, public eos::common::LogId
{
  Status AclRequest(ServerContext* context, const AclProto* request,
                    ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.AclCall(request, reply);
  }

  Status AccessRequest(ServerContext* context, const AccessProto* request,
                        ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.AccessCall(request, reply);
  }

  Status ArchiveRequest(ServerContext* context, const ArchiveProto* request,
                        ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.ArchiveCall(request, reply);
  }

  Status AttrRequest(ServerContext* context, const AttrProto* request,
                  ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.AttrCall(request, reply);
  }

  Status BackupRequest(ServerContext* context, const BackupProto* request,
                        ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.BackupCall(request, reply);
  }

  Status ChmodRequest(ServerContext* context, const ChmodProto* request,
                  ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.ChmodCall(request, reply);
  }

  Status ChownRequest(ServerContext* context, const ChownProto* request,
                      ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.ChownCall(request, reply);
  }

  Status ConfigRequest(ServerContext* context, const ConfigProto* request,
                        ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.ConfigCall(request, reply);
  }

  Status ConvertRequest(ServerContext* context, const ConvertProto* request,
                        ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.ConvertCall(request, reply);
  }

  Status CpRequest(ServerContext* context, const CpProto* request,
                    ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.CpCall(request, reply);
  }

  Status DebugRequest(ServerContext* context, const DebugProto* request,
                      ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.DebugCall(request, reply);
  }

  Status FileRequest(ServerContext* context, const FileProto* request,
                      ReplyProto* reply)
  {
    std::string json_out;
    (void) google::protobuf::util::MessageToJsonString(*request, &json_out);
    eos_static_info("msg=\"received file request\" data=\"%s\"", json_out.c_str());

    GrpcRestGwInterface restGwInterface;
    return restGwInterface.FileCall(request, reply);
  }

  Status FileinfoRequest(ServerContext* context, const FileinfoProto* request,
                          ReplyProto* reply)
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.FileinfoCall(request, reply);
  }

  Status FsRequest(ServerContext* context, const FsProto* request,
                    ReplyProto* reply)
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.FsCall(request, reply);
  }

  Status FsckRequest(ServerContext* context, const FsckProto* request,
                      ReplyProto* reply)
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.FsckCall(request, reply);
  }

  Status GeoschedRequest(ServerContext* context, const GeoschedProto* request,
                          ReplyProto* reply)
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.GeoschedCall(request, reply);
  }

  Status GroupRequest(ServerContext* context, const GroupProto* request,
                      ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.GroupCall(request, reply);
  }

  Status HealthRequest(ServerContext* context, const HealthProto* request,
                    ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.HealthCall(request, reply);
  }

  Status IoRequest(ServerContext* context, const IoProto* request,
                    ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.IoCall(request, reply);
  }

  Status MapRequest(ServerContext* context, const MapProto* request,
                    ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.MapCall(request, reply);
  }

  Status MemberRequest(ServerContext* context, const MemberProto* request,
                        ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.MemberCall(request, reply);
  }

  Status MkdirRequest(ServerContext* context, const MkdirProto* request,
                      ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.MkdirCall(request, reply);
  }

  Status MvRequest(ServerContext* context, const MoveProto* request,
                    ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.MvCall(request, reply);
  }

  Status NodeRequest(ServerContext* context, const NodeProto* request,
                    ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.NodeCall(request, reply);
  }

  Status NsRequest(ServerContext* context, const NsProto* request,
                    ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.NsCall(request, reply);
  }

  Status QoSRequest(ServerContext* context, const QoSProto* request,
                    ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.QoSCall(request, reply);
  }

  Status QuotaRequest(ServerContext* context, const QuotaProto* request,
                      ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.QuotaCall(request, reply);
  }

  Status RecycleRequest(ServerContext* context, const RecycleProto* request,
                        ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.RecycleCall(request, reply);
  }
  
  Status RmRequest(ServerContext* context, const RmProto* request,
                    ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.RmCall(request, reply);
  }

  Status RmdirRequest(ServerContext* context, const RmdirProto* request,
                      ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.RmdirCall(request, reply);
  }

  Status RouteRequest(ServerContext* context, const RouteProto* request,
                      ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.RouteCall(request, reply);
  }

  Status SpaceRequest(ServerContext* context, const SpaceProto* request,
                      ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.SpaceCall(request, reply);
  }

  Status StatRequest(ServerContext* context, const StatProto* request,
                    ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.StatCall(request, reply);
  }

  Status StatusRequest(ServerContext* context, const StatusProto* request,
                        ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.StatusCall(request, reply);
  }

  Status TokenRequest(ServerContext* context, const TokenProto* request,
                      ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.TokenCall(request, reply);
  }

  Status TouchRequest(ServerContext* context, const TouchProto* request,
                      ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.TouchCall(request, reply);
  }

  Status VersionRequest(ServerContext* context, const VersionProto* request,
                        ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.VersionCall(request, reply);
  }

  Status VidRequest(ServerContext* context, const VidProto* request,
                    ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.VidCall(request, reply);
  }

  Status WhoRequest(ServerContext* context, const WhoProto* request,
                    ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.WhoCall(request, reply);
  }

  Status WhoamiRequest(ServerContext* context, const WhoamiProto* request,
                        ReplyProto* reply) override
  {
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.WhoamiCall(request, reply);
  }
};

/* return client DN*/
std::string
GrpcRestGwServer::DN(grpc::ServerContext* context)
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
std::string GrpcRestGwServer::IP(grpc::ServerContext* context, std::string* id,
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
GrpcRestGwServer::Vid(grpc::ServerContext* context,
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
  std::string ip = GrpcRestGwServer::IP(context, &id).c_str();
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
GrpcRestGwServer::Run(ThreadAssistant& assistant) noexcept
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

  EosRestGatewayServiceImpl service;
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
  mRestGwServer = builder.BuildAndStart();

  // spawn grpc gateway
  char* const addr = const_cast<char*>(bind_address.c_str());
  char* const gwaddr = const_cast<char*>(gw_bind_address.c_str());
  char* path = (char *)"../../../../protos/examplepb";
  const auto gatewayServer = SpawnGrpcGateway(gwaddr, "tcp", addr, path);
  std::cerr << "Done spawning GrpcGateway" << std::endl;

  if (mRestGwServer) {
    mRestGwServer->Wait();
  }

  WaitForGrpcGateway(gatewayServer);

#else
  // Make the compiler happy
  (void) mPort;
  (void) mSSL;
#endif
}

EOSMGMNAMESPACE_END
