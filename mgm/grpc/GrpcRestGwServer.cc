#include "GrpcRestGwServer.hh"
#include <google/protobuf/util/json_util.h>
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "mgm/Macros.hh"
#include <XrdSec/XrdSecEntity.hh>

#ifdef EOS_GRPC_GATEWAY
#include "proto/eos_rest_gateway/eos_rest_gateway_service.grpc.pb.h"
#include "EosGrpcGateway.h"

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
using eos::console::EvictProto;
using eos::console::FileProto;
using eos::console::FileinfoProto;
using eos::console::FindProto;
using eos::console::FsProto;
using eos::console::FsckProto;
using eos::console::GeoschedProto;
using eos::console::GroupProto;
using eos::console::HealthProto;
using eos::console::MapProto;
using eos::console::MemberProto;
using eos::console::IoProto;
using eos::console::LsProto;
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

#endif // EOS_GRPC_GATEWAY

EOSMGMNAMESPACE_BEGIN

#ifdef EOS_GRPC_GATEWAY

class EosRestGatewayServiceImpl final : public EosRestGatewayService::Service,
  public eos::common::LogId
{
  Status AclRequest(ServerContext* context, const AclProto* request,
                    ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.AclCall(vid, request, reply);
  }

  Status AccessRequest(ServerContext* context, const AccessProto* request,
                       ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.AccessCall(vid, request, reply);
  }

  Status ArchiveRequest(ServerContext* context, const ArchiveProto* request,
                        ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.ArchiveCall(vid, request, reply);
  }

  Status AttrRequest(ServerContext* context, const AttrProto* request,
                     ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.AttrCall(vid, request, reply);
  }

  Status BackupRequest(ServerContext* context, const BackupProto* request,
                       ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.BackupCall(vid, request, reply);
  }

  Status ChmodRequest(ServerContext* context, const ChmodProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.ChmodCall(vid, request, reply);
  }

  Status ChownRequest(ServerContext* context, const ChownProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.ChownCall(vid, request, reply);
  }

  Status ConfigRequest(ServerContext* context, const ConfigProto* request,
                       ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.ConfigCall(vid, request, reply);
  }

  Status ConvertRequest(ServerContext* context, const ConvertProto* request,
                        ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.ConvertCall(vid, request, reply);
  }

  Status CpRequest(ServerContext* context, const CpProto* request,
                   ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.CpCall(vid, request, reply);
  }

  Status DebugRequest(ServerContext* context, const DebugProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.DebugCall(vid, request, reply);
  }

  Status EvictRequest(ServerContext* context, const EvictProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.EvictCall(vid, request, reply);
  }

  Status FileRequest(ServerContext* context, const FileProto* request,
                     ReplyProto* reply)
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.FileCall(vid, request, reply);
  }

  Status FileinfoRequest(ServerContext* context, const FileinfoProto* request,
                         ReplyProto* reply)
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.FileinfoCall(vid, request, reply);
  }

  Status FindRequest(ServerContext* context, const FindProto* request,
                     ServerWriter<ReplyProto>* writer)
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.FindCall(vid, request, writer);
  }

  Status FsRequest(ServerContext* context, const FsProto* request,
                   ReplyProto* reply)
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.FsCall(vid, request, reply);
  }

  Status FsckRequest(ServerContext* context, const FsckProto* request,
                     ServerWriter<ReplyProto>* writer)
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.FsckCall(vid, request, writer);
  }

  Status GeoschedRequest(ServerContext* context, const GeoschedProto* request,
                         ReplyProto* reply)
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.GeoschedCall(vid, request, reply);
  }

  Status GroupRequest(ServerContext* context, const GroupProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.GroupCall(vid, request, reply);
  }

  Status HealthRequest(ServerContext* context, const HealthProto* request,
                       ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.HealthCall(vid, request, reply);
  }

  Status IoRequest(ServerContext* context, const IoProto* request,
                   ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.IoCall(vid, request, reply);
  }

  Status LsRequest(ServerContext* context, const LsProto* request,
                   ServerWriter<ReplyProto>* writer)
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.LsCall(vid, request, writer);
  }

  Status MapRequest(ServerContext* context, const MapProto* request,
                    ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.MapCall(vid, request, reply);
  }

  Status MemberRequest(ServerContext* context, const MemberProto* request,
                       ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.MemberCall(vid, request, reply);
  }

  Status MkdirRequest(ServerContext* context, const MkdirProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.MkdirCall(vid, request, reply);
  }

  Status MvRequest(ServerContext* context, const MoveProto* request,
                   ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.MvCall(vid, request, reply);
  }

  Status NodeRequest(ServerContext* context, const NodeProto* request,
                     ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.NodeCall(vid, request, reply);
  }

  Status NsRequest(ServerContext* context, const NsProto* request,
                   ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.NsCall(vid, request, reply);
  }

  Status QoSRequest(ServerContext* context, const QoSProto* request,
                    ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.QoSCall(vid, request, reply);
  }

  Status QuotaRequest(ServerContext* context, const QuotaProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.QuotaCall(vid, request, reply);
  }

  Status RecycleRequest(ServerContext* context, const RecycleProto* request,
                        ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.RecycleCall(vid, request, reply);
  }

  Status RmRequest(ServerContext* context, const RmProto* request,
                   ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.RmCall(vid, request, reply);
  }

  Status RmdirRequest(ServerContext* context, const RmdirProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.RmdirCall(vid, request, reply);
  }

  Status RouteRequest(ServerContext* context, const RouteProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.RouteCall(vid, request, reply);
  }

  Status SpaceRequest(ServerContext* context, const SpaceProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.SpaceCall(vid, request, reply);
  }

  Status StatRequest(ServerContext* context, const StatProto* request,
                     ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.StatCall(vid, request, reply);
  }

  Status StatusRequest(ServerContext* context, const StatusProto* request,
                       ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.StatusCall(vid, request, reply);
  }

  Status TokenRequest(ServerContext* context, const TokenProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.TokenCall(vid, request, reply);
  }

  Status TouchRequest(ServerContext* context, const TouchProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.TouchCall(vid, request, reply);
  }

  Status VersionRequest(ServerContext* context, const VersionProto* request,
                        ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.VersionCall(vid, request, reply);
  }

  Status VidRequest(ServerContext* context, const VidProto* request,
                    ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.VidCall(vid, request, reply);
  }

  Status WhoRequest(ServerContext* context, const WhoProto* request,
                    ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.WhoCall(vid, request, reply);
  }

  Status WhoamiRequest(ServerContext* context, const WhoamiProto* request,
                       ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    GrpcRestGwServer::Vid(context, vid);
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.WhoamiCall(vid, request, reply);
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
  // but net and id are populated as well with the prefix and suffix, respectively
  // The context peer information is curl encoded
  const std::string decoded_peer =
    eos::common::StringConversion::curl_default_unescaped(context->peer().c_str());
  std::vector<std::string> tokens;
  eos::common::StringConversion::Tokenize(decoded_peer, tokens, "[]");

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
    eos::common::StringConversion::Tokenize(decoded_peer, tokens, ":");

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
                      eos::common::VirtualIdentity& vid)
{
  static const std::string hdr_name = "client-name";
  static const std::string hdr_tident = "client-tident";
  static const std::string hdr_authz = "client-authorization";
  const auto& map_hdrs = context->client_metadata();
  std::string name_val, tident_val, authz_val;
  XrdSecEntity client("https");
  // Populate client name
  auto it = map_hdrs.find(hdr_name);

  if (it != map_hdrs.end()) {
    name_val = std::string(it->second.data(), it->second.length());
    client.name = const_cast<char*>(name_val.c_str());
  }

  // Populate client tident
  it = map_hdrs.find(hdr_tident);

  if (it != map_hdrs.end()) {
    tident_val = std::string(it->second.data(), it->second.length());
    client.tident = const_cast<char*>(tident_val.c_str());
  }

  // Populate client endorsemetns if authz info present
  it = map_hdrs.find(hdr_authz);

  if (it != map_hdrs.end()) {
    authz_val = std::string(it->second.data(), it->second.length());
    client.endorsements = const_cast<char*>(authz_val.c_str());
  }

  eos::common::Mapping::IdMap(&client, "eos.app=grpc", client.tident, vid);
}

#endif // EOS_GRPC_GATEWAY

void
GrpcRestGwServer::Run(ThreadAssistant& assistant) noexcept
{
#ifdef EOS_GRPC_GATEWAY
  EosRestGatewayServiceImpl service;
  grpc::ServerBuilder builder;
  // server bind address
  std::string bind_address = "0.0.0.0:";
  bind_address += std::to_string(mPort);
  // gateway bind address
  std::string gw_bind_address = "0.0.0.0:";
  gw_bind_address += "40054";
  builder.AddListeningPort(bind_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  mRestGwServer = builder.BuildAndStart();
  // spawn grpc gateway
  char* const addr = const_cast<char*>(bind_address.c_str());
  char* const gwaddr = const_cast<char*>(gw_bind_address.c_str());
  char* path = (char*)"../../../../protos/examplepb";
  char* network = (char*)"tcp";
  const auto gatewayServer = SpawnGrpcGateway(gwaddr, network, addr, path);
  eos_static_notice("%s", "msg=\"spawning GRPC GATEWAY, REST API available\"");

  if (mRestGwServer) {
    mRestGwServer->Wait();
  }

  WaitForGrpcGateway(gatewayServer);
#else
  eos_static_notice("%s", "msg=\"no GRPC GATEWAY support, "
                    "REST API unavailable\"");
  // Make the compiler happy
  (void) mPort;
  (void) mSSL;
#endif
}

EOSMGMNAMESPACE_END
