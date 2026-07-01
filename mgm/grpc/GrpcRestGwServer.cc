// ----------------------------------------------------------------------
// File: GrpcRestGwServer.cc
// Author: Elvin Sindrilaru - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "GrpcRestGwServer.hh"
#ifdef EOS_GRPC_GATEWAY
#include "EosGrpcGateway.h"
#include "GrpcAuth.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "mgm/macros/Macros.hh"
#include "proto/eos_rest_gateway/eos_rest_gateway_service.grpc.pb.h"
#include <XrdSec/XrdSecEntity.hh>
#include <cerrno>
#include <google/protobuf/util/json_util.h>
#include <grpc++/security/credentials.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>

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

namespace {

std::string
RestGatewayAuthKey(ServerContext* context)
{
  static const std::string hdr_authz = "client-authorization";
  const auto& map_hdrs = context->client_metadata();
  const auto it = map_hdrs.find(hdr_authz);

  if (it == map_hdrs.end()) {
    return "";
  }

  return std::string(it->second.data(), it->second.length());
}

bool
RestGatewayPeerIsLocal(ServerContext* context, std::string* peer_ip = nullptr)
{
  std::string ip = GrpcRestGwServer::IP(context);

  if (peer_ip) {
    *peer_ip = ip;
  }

  return (ip == "127.0.0.1") || (ip == "[::1]") || (ip == "[::ffff:127.0.0.1]");
}

void
SetRestScopeDenied(ReplyProto* reply, const GrpcAuthDecision& decision,
                   const eos::common::VirtualIdentity& vid)
{
  GrpcAuth::LogDenied(vid, decision, "eos.rest");
  reply->set_retc(EACCES);
  reply->set_std_err("error: grpc scope denied");
}

void
SetRestPeerDenied(ServerContext* context, ReplyProto* reply,
                  eos::common::VirtualIdentity& vid)
{
  std::string peer_ip;
  RestGatewayPeerIsLocal(context, &peer_ip);
  eos_static_warning("msg=\"REST gateway request denied from non-loopback peer\" "
                     "peer=\"%s\" ip=\"%s\"",
                     context->peer().c_str(), peer_ip.c_str());
  vid = eos::common::VirtualIdentity::Nobody();
  reply->set_retc(EACCES);
  reply->set_std_err("error: REST gateway gRPC endpoint is loopback-only");
}

bool
AuthorizeRestGateway(ServerContext* context, const std::string& action,
                     eos::common::VirtualIdentity& vid, ReplyProto* reply)
{
  if (!RestGatewayPeerIsLocal(context)) {
    SetRestPeerDenied(context, reply, vid);
    return false;
  }

  GrpcRestGwServer::Vid(context, vid);
  const std::string authkey = RestGatewayAuthKey(context);
  const auto decision = GrpcAuth::Authorize(vid, authkey, action);

  if (!decision.allowed) {
    SetRestScopeDenied(reply, decision, vid);
    return false;
  }

  return true;
}

bool
AuthorizeRestGateway(ServerContext* context, const std::string& action,
                     eos::common::VirtualIdentity& vid, ServerWriter<ReplyProto>* writer)
{
  ReplyProto reply;

  if (!AuthorizeRestGateway(context, action, vid, &reply)) {
    writer->Write(reply);
    return false;
  }

  return true;
}

} // namespace

//------------------------------------------------------------------------------
// Get client IP based on the context information
//------------------------------------------------------------------------------
std::string
GrpcRestGwServer::IP(grpc::ServerContext* context, std::string* id, std::string* port)
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

//------------------------------------------------------------------------------
// Populate virtual identity based on the context information
//------------------------------------------------------------------------------
void
GrpcRestGwServer::Vid(grpc::ServerContext* context, eos::common::VirtualIdentity& vid)
{
  // C-1: the REST gateway has no transport-level authentication and the
  // identity is taken from three client-supplied gRPC metadata headers.
  // This is only safe when the peer is co-located on the same host as the
  // MGM (the gateway is intended to run as a sidecar). For any non-loopback
  // peer we must refuse to honour the headers. Normal request handling denies
  // those peers before calling Vid(); the fallback here keeps direct future
  // callers from accidentally trusting caller-supplied identity metadata.
  std::string peer_ip;
  const bool peer_is_local = RestGatewayPeerIsLocal(context, &peer_ip);

  if (!peer_is_local) {
    eos_static_warning("msg=\"refusing REST gateway request from non-loopback "
                       "peer - falling back to nobody\" peer=\"%s\" ip=\"%s\"",
                       context->peer().c_str(), peer_ip.c_str());
    vid = eos::common::VirtualIdentity::Nobody();
    return;
  }

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

  // Populate client tident.
  //
  // SECURITY/STABILITY: XrdSecEntity declares tident as a const char* with no
  // in-class initialiser; the XRootD invariant is that the security plugin
  // sets it before the entity reaches consumer code. Because we stack-
  // construct the entity here and the value is supplied via an *optional*
  // gRPC metadata header, tident would otherwise stay nullptr whenever the
  // peer omits the header. Downstream code in eos::common::Mapping::IdMap
  // (see common/Mapping.cc) calls strlen(client->tident) unconditionally to
  // detect the XrdHttp "http" tident workaround and would segfault the
  // entire MGM process on a single anonymous gRPC request.
  //
  // We therefore always synthesise a non-null tident: the caller-supplied
  // header value if present, otherwise a sentinel derived from the peer
  // identity so the audit trail still reflects the originator. The backing
  // std::string lives in this stack frame, which outlives the IdMap call.
  it = map_hdrs.find(hdr_tident);

  if (it != map_hdrs.end()) {
    tident_val = std::string(it->second.data(), it->second.length());
  } else {
    tident_val = "grpc.";
    tident_val += (peer_ip.empty() ? std::string("local") : peer_ip);
  }

  client.tident = tident_val.c_str();

  // Populate client endorsemetns if authz info present
  it = map_hdrs.find(hdr_authz);

  if (it != map_hdrs.end()) {
    authz_val = std::string(it->second.data(), it->second.length());
    client.endorsements = const_cast<char*>(authz_val.c_str());
  }

  eos::common::Mapping::IdMap(&client, "eos.app=grpc", client.tident, vid);
}

//------------------------------------------------------------------------------
// Class EosRestGatewayServiceImpl
//------------------------------------------------------------------------------
class EosRestGatewayServiceImpl final : public EosRestGatewayService::Service,
  public eos::common::LogId
{
  Status AclRequest(ServerContext* context, const AclProto* request,
                    ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.AclCall(vid, request, reply);
  }

  Status AccessRequest(ServerContext* context, const AccessProto* request,
                       ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.AccessCall(vid, request, reply);
  }

  Status ArchiveRequest(ServerContext* context, const ArchiveProto* request,
                        ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.ArchiveCall(vid, request, reply);
  }

  Status AttrRequest(ServerContext* context, const AttrProto* request,
                     ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.AttrCall(vid, request, reply);
  }

  Status BackupRequest(ServerContext* context, const BackupProto* request,
                       ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.BackupCall(vid, request, reply);
  }

  Status ChmodRequest(ServerContext* context, const ChmodProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.ChmodCall(vid, request, reply);
  }

  Status ChownRequest(ServerContext* context, const ChownProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.ChownCall(vid, request, reply);
  }

  Status ConfigRequest(ServerContext* context, const ConfigProto* request,
                       ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.ConfigCall(vid, request, reply);
  }

  Status ConvertRequest(ServerContext* context, const ConvertProto* request,
                        ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.ConvertCall(vid, request, reply);
  }

  Status CpRequest(ServerContext* context, const CpProto* request,
                   ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.CpCall(vid, request, reply);
  }

  Status DebugRequest(ServerContext* context, const DebugProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.DebugCall(vid, request, reply);
  }

  Status EvictRequest(ServerContext* context, const EvictProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.EvictCall(vid, request, reply);
  }

  Status FileRequest(ServerContext* context, const FileProto* request,
                     ReplyProto* reply)
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.FileCall(vid, request, reply);
  }

  Status FileinfoRequest(ServerContext* context, const FileinfoProto* request,
                         ReplyProto* reply)
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.FileinfoCall(vid, request, reply);
  }

  Status FindRequest(ServerContext* context, const FindProto* request,
                     ServerWriter<ReplyProto>* writer)
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, writer)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.FindCall(vid, request, writer);
  }

  Status FsRequest(ServerContext* context, const FsProto* request,
                   ReplyProto* reply)
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.FsCall(vid, request, reply);
  }

  Status FsckRequest(ServerContext* context, const FsckProto* request,
                     ServerWriter<ReplyProto>* writer)
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, writer)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.FsckCall(vid, request, writer);
  }

  Status GeoschedRequest(ServerContext* context, const GeoschedProto* request,
                         ReplyProto* reply)
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.GeoschedCall(vid, request, reply);
  }

  Status GroupRequest(ServerContext* context, const GroupProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.GroupCall(vid, request, reply);
  }

  Status HealthRequest(ServerContext* context, const HealthProto* request,
                       ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.HealthCall(vid, request, reply);
  }

  Status IoRequest(ServerContext* context, const IoProto* request,
                   ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.IoCall(vid, request, reply);
  }

  Status LsRequest(ServerContext* context, const LsProto* request,
                   ServerWriter<ReplyProto>* writer)
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, writer)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.LsCall(vid, request, writer);
  }

  Status MapRequest(ServerContext* context, const MapProto* request,
                    ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.MapCall(vid, request, reply);
  }

  Status MemberRequest(ServerContext* context, const MemberProto* request,
                       ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.MemberCall(vid, request, reply);
  }

  Status MkdirRequest(ServerContext* context, const MkdirProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.MkdirCall(vid, request, reply);
  }

  Status MvRequest(ServerContext* context, const MoveProto* request,
                   ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.MvCall(vid, request, reply);
  }

  Status NodeRequest(ServerContext* context, const NodeProto* request,
                     ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.NodeCall(vid, request, reply);
  }

  Status NsRequest(ServerContext* context, const NsProto* request,
                   ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.NsCall(vid, request, reply);
  }

  Status QuotaRequest(ServerContext* context, const QuotaProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(*request), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.QuotaCall(vid, request, reply);
  }

  Status RecycleRequest(ServerContext* context, const RecycleProto* request,
                        ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.RecycleCall(vid, request, reply);
  }

  Status RmRequest(ServerContext* context, const RmProto* request,
                   ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.RmCall(vid, request, reply);
  }

  Status RmdirRequest(ServerContext* context, const RmdirProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.RmdirCall(vid, request, reply);
  }

  Status RouteRequest(ServerContext* context, const RouteProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.RouteCall(vid, request, reply);
  }

  Status SpaceRequest(ServerContext* context, const SpaceProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.SpaceCall(vid, request, reply);
  }

  Status StatRequest(ServerContext* context, const StatProto* request,
                     ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.StatCall(vid, request, reply);
  }

  Status StatusRequest(ServerContext* context, const StatusProto* request,
                       ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.StatusCall(vid, request, reply);
  }

  Status TokenRequest(ServerContext* context, const TokenProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.TokenCall(vid, request, reply);
  }

  Status TouchRequest(ServerContext* context, const TouchProto* request,
                      ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.TouchCall(vid, request, reply);
  }

  Status VersionRequest(ServerContext* context, const VersionProto* request,
                        ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.VersionCall(vid, request, reply);
  }

  Status VidRequest(ServerContext* context, const VidProto* request,
                    ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.VidCall(vid, request, reply);
  }

  Status WhoRequest(ServerContext* context, const WhoProto* request,
                    ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.WhoCall(vid, request, reply);
  }

  Status WhoamiRequest(ServerContext* context, const WhoamiProto* request,
                       ReplyProto* reply) override
  {
    eos::common::VirtualIdentity vid;
    if (!AuthorizeRestGateway(context, GrpcAuth::RestScope(__func__), vid, reply)) {
      return Status::OK;
    }
    GrpcRestGwInterface restGwInterface;
    return restGwInterface.WhoamiCall(vid, request, reply);
  }
};

#endif // EOS_GRPC_GATEWAY

void
GrpcRestGwServer::Run(ThreadAssistant& assistant) noexcept
{
#ifdef EOS_GRPC_GATEWAY
  eos_static_crit("msg=\"REST gateway gRPC server starting in INSECURE "
                  "mode - bound to loopback only\" http_port=%i grpc_port=%i",
                  mHttpGwPort, mGrpcGwPort);
  EosRestGatewayServiceImpl service;
  // This line is often optional if the plugin is linked correctly,
  // but calling it explicitly ensures the plugin is registered.
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  grpc::ServerBuilder builder;
  // GRPC server bind address - loopback only since transport is unauthenticated
  std::string ipv4_grpc_localhost = "127.0.0.1:" + std::to_string(mGrpcGwPort);
  builder.AddListeningPort(ipv4_grpc_localhost, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  mRestGwServer = builder.BuildAndStart();
  // Http gateway bind address - loopback only
  std::string ipv4_http_localhost = "127.0.0.1:" + std::to_string(mHttpGwPort);
  // Spawn HTTP-GRPC gateway
  char* const grpc_addr = const_cast<char*>(ipv4_grpc_localhost.c_str());
  char* const http_addr = const_cast<char*>(ipv4_http_localhost.c_str());
  char* path = (char*)"../../../../protos/examplepb";
  char* network = (char*)"tcp";
  const auto gatewayServer = SpawnGrpcGateway(http_addr, network, grpc_addr, path);
  eos_static_notice("%s", "msg=\"spawning gRPC REST GATEWAY\"");

  if (mRestGwServer) {
    eos_static_info("msg=\"gRPC REST server is running\" port=%i", mGrpcGwPort);
    mRestGwServer->Wait();
  }

  WaitForGrpcGateway(gatewayServer);
#else
  eos_static_notice("%s", "msg=\"no GRPC GATEWAY support, "
                    "REST API unavailable\"");
  // Make the compiler happy
  (void)mHttpGwPort;
  (void)mGrpcGwPort;
  (void) mSSL;
#endif
}

EOSMGMNAMESPACE_END
