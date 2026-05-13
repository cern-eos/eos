// ----------------------------------------------------------------------
// File: GrpcServer.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "GrpcServer.hh"
#include "GrpcNsInterface.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include "mgm/macros/Macros.hh"
#include "mgm/proc/admin/IoCmd.hh"
#include <XrdSec/XrdSecEntity.hh>
#include <chrono>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <thread>
#include <vector>

#ifdef EOS_GRPC
#include "proto/Rpc.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::Status;
using eos::rpc::Eos;
using eos::rpc::PingRequest;
using eos::rpc::PingReply;
using eos::rpc::FileInsertRequest;
using eos::rpc::ContainerInsertRequest;
using eos::rpc::InsertReply;

#endif

EOSMGMNAMESPACE_BEGIN

#ifdef EOS_GRPC

class RequestServiceImpl final : public Eos::Service {
  Status Ping(ServerContext* context, const eos::rpc::PingRequest* request,
              eos::rpc::PingReply* reply) override
  {
    eos_static_info("grpc::ping from client peer=%s ip=%s DN=%s len=%lu",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->message().length());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    reply->set_message(request->message());
    return Status::OK;
  }

  Status FileInsert(ServerContext* context,
                    const eos::rpc::FileInsertRequest* request,
                    eos::rpc::InsertReply* reply) override
  {
    eos_static_info("grpc::fileinsert from client peer=%s ip=%s DN=%s",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;
    return GrpcNsInterface::FileInsert(vid, reply, request);
  }

  Status ContainerInsert(ServerContext* context,
                         const eos::rpc::ContainerInsertRequest* request,
                         eos::rpc::InsertReply* reply) override
  {
    eos_static_info("grpc::containerinsert from client peer=%s ip=%s DN=%s",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;
    return GrpcNsInterface::ContainerInsert(vid, reply, request);
  }

  Status MD(ServerContext* context, const eos::rpc::MDRequest* request,
            ServerWriter<eos::rpc::MDResponse>* writer) override
  {
    eos_static_info("grpc::md from client peer=%s ip=%s DN=%s", context->peer().c_str(),
                    GrpcServer::IP(context).c_str(), GrpcServer::DN(context).c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;

    switch (request->type()) {
    case eos::rpc::FILE:
    case eos::rpc::CONTAINER:
    case eos::rpc::STAT:
      return GrpcNsInterface::Stat(vid, writer, request);
      break;

    case eos::rpc::LISTING:
      return GrpcNsInterface::StreamMD(vid, writer, request);
      break;

    default:
      ;
    }

    return Status(grpc::StatusCode::INVALID_ARGUMENT, "request is not supported");
  }

  Status Find(ServerContext* context, const eos::rpc::FindRequest* request,
              ServerWriter<eos::rpc::MDResponse>* writer) override
  {
    eos_static_info("grpc::find from client peer=%s ip=%s DN=%s", context->peer().c_str(),
                    GrpcServer::IP(context).c_str(), GrpcServer::DN(context).c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;
    return GrpcNsInterface::Find(vid, writer, request);
  }

  Status NsStat(ServerContext* context,
                const eos::rpc::NsStatRequest* request,
                eos::rpc::NsStatResponse* reply) override
  {
    eos_static_info("grpc::nsstat::request from client peer=%s ip=%s DN=%s",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;
    return GrpcNsInterface::NsStat(vid, reply, request);
  }

  Status Exec(ServerContext* context,
              const eos::rpc::NSRequest* request,
              eos::rpc::NSResponse* reply) override
  {
    eos_static_info("grpc::exec::request from client peer=%s ip=%s DN=%s "
                    "req_type=%lu",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->command_case());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;
    return GrpcNsInterface::Exec(vid, reply, request);
  }

  Status
  TrafficShapingRate(
      ServerContext* context,
      const eos::traffic_shaping::TrafficShapingRateRequest* request,
      ServerWriter<eos::traffic_shaping::TrafficShapingRateResponse>* writer) override
  {
    eos_static_info("msg=\"Monitoring Stream Start\" peer=%s ip=%s DN=%s",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str());
    // C-2: require an authenticated admin / sudoer identity. The stream
    // exposes per-uid / per-gid / per-app / per-fsid throughput which is
    // sensitive monitoring data and must not be readable by anonymous or
    // ordinary callers.
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, "");

    if ((vid.uid != 0) && !vid.sudoer) {
      eos_static_warning("msg=\"TrafficShapingRate denied (not admin)\" "
                         "peer=%s uid=%u gid=%u sudoer=%d DN=%s",
                         context->peer().c_str(), vid.uid, vid.gid,
                         (int) vid.sudoer, GrpcServer::DN(context).c_str());
      return Status(grpc::StatusCode::PERMISSION_DENIED,
                    "TrafficShapingRate requires an admin or sudoer identity");
    }

    while (!context->IsCancelled()) {
      auto start = std::chrono::steady_clock::now();

      eos::traffic_shaping::TrafficShapingRateResponse report;
      std::string error;
      if (!BuildTrafficShapingRateReport(*request, report, &error)) {
        return Status(grpc::StatusCode::UNAVAILABLE, error);
      }

      if (!writer->Write(report)) {
        break;
      }

      std::this_thread::sleep_until(start + std::chrono::milliseconds(100));
    }
    return grpc::Status::OK;
  }
};

/* return client DN*/
std::string
GrpcServer::DN(grpc::ServerContext* context)
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
std::string GrpcServer::IP(grpc::ServerContext* context, std::string* id,
                           std::string* port)
{
  // format is ipv4:<ip>:<..> or ipv6:<ip>:<..> - we just return the IP address
  // butq net and id are populated as well with the prefix and suffix, respectively
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
GrpcServer::Vid(grpc::ServerContext* context,
                eos::common::VirtualIdentity& vid,
                const std::string& authkey)
{
  XrdSecEntity client("grpc");
  std::string dn = DN(context);
  client.name = const_cast<char*>(dn.c_str());
  // ----------------------------------------------------------------------
  // SECURITY: build the tident prefix.
  //
  // Previously the code used the raw authkey string as the tident prefix
  // whenever no DN was present and the authkey was not a "zteos64:" EOS
  // token (i.e. SSS / shared-secret style credentials). The resulting
  // tident is stored on vid.tident by Mapping::IdMap and propagates into
  // many log lines that run at >= INFO severity, including the
  // unauthorized-access path in mgm/macros/Macros.hh which uses eos_err.
  // That turned every audit/error log into a verbatim credential dump.
  //
  // We now emit a stable, non-secret sentinel for the no-DN case and
  // attach a short HexSha256 fingerprint (16 hex chars = 64 bits) only
  // at DEBUG severity so operators can still correlate calls without the
  // credential bytes ever touching INFO+ logs.
  // ----------------------------------------------------------------------
  const bool isEosToken = (authkey.compare(0, 8, "zteos64:") == 0);
  std::string tident;

  if (dn.length()) {
    tident = dn;
  } else if (isEosToken) {
    tident = "eostoken";
  } else if (authkey.length()) {
    tident = "grpc-key";
  } else {
    tident = "grpc-anon";
  }

  if (EOS_LOGS_DEBUG && authkey.length() && !isEosToken && !dn.length()) {
    std::string fp = eos::common::SymKey::HexSha256(authkey);

    if (fp.length() > 16) {
      fp.resize(16);
    }

    eos_static_debug("msg=\"grpc authkey fingerprint\" fp=%s len=%zu",
                     fp.c_str(), authkey.length());
  }

  std::string id;
  std::string ip = GrpcServer::IP(context, &id).c_str();
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
GrpcServer::Run(ThreadAssistant& assistant) noexcept
{
#ifdef EOS_GRPC

  if (getenv("EOS_MGM_GRPC_SSL_CERT") &&
      getenv("EOS_MGM_GRPC_SSL_KEY") &&
      getenv("EOS_MGM_GRPC_SSL_CA")) {
    mSSL = true;
    mSSLCertFile = getenv("EOS_MGM_GRPC_SSL_CERT");
    mSSLKeyFile = getenv("EOS_MGM_GRPC_SSL_KEY");
    mSSLCaFile = getenv("EOS_MGM_GRPC_SSL_CA");

    if (eos::common::StringConversion::LoadFileIntoString(mSSLCertFile.c_str(),
        mSSLCert) && !mSSLCert.length()) {
      eos_static_crit("unable to load ssl certificate file '%s'",
                      mSSLCertFile.c_str());
      mSSL = false;
    }

    if (eos::common::StringConversion::LoadFileIntoString(mSSLKeyFile.c_str(),
        mSSLKey) && !mSSLKey.length()) {
      eos_static_crit("unable to load ssl key file '%s'", mSSLKeyFile.c_str());
      mSSL = false;
    }

    if (eos::common::StringConversion::LoadFileIntoString(mSSLCaFile.c_str(),
        mSSLCa) && !mSSLCa.length()) {
      eos_static_crit("unable to load ssl ca file '%s'", mSSLCaFile.c_str());
      mSSL = false;
    }
  }

  int selected_port = 0;
  RequestServiceImpl service;
  // C-3: refuse to expose the gRPC service on a public interface without
  // TLS. If TLS is not configured, the operator must explicitly set
  // EOS_MGM_GRPC_ALLOW_INSECURE=1 to acknowledge the risk; in that case we
  // bind to the loopback interface only.
  const bool allow_insecure = (getenv("EOS_MGM_GRPC_ALLOW_INSECURE") != nullptr);

  if (!mSSL && !allow_insecure) {
    eos_static_crit("msg=\"refusing to start gRPC server without TLS - "
                    "set EOS_MGM_GRPC_SSL_{CERT,KEY,CA} or "
                    "EOS_MGM_GRPC_ALLOW_INSECURE=1 (loopback only)\" "
                    "port=%i", mPort);
    return;
  }

  std::string bind_address = (mSSL ? "0.0.0.0:" : "127.0.0.1:");
  bind_address += std::to_string(mPort);
  grpc::ServerBuilder builder;

  if (mSSL) {
    grpc_ssl_client_certificate_request_type gsccrt =
      GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;

    // C-4: this env downgrades the auth surface to token-only. Make the
    // downgrade loud at startup so operators do not enable it accidentally.
    if (getenv("EOS_MGM_GRPC_DONT_REQUEST_CLIENT_CERTIFICATE")) {
      gsccrt = GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE;
      eos_static_crit("msg=\"client certificate verification DISABLED via "
                      "EOS_MGM_GRPC_DONT_REQUEST_CLIENT_CERTIFICATE - "
                      "authentication relies on EOS tokens only\" port=%i",
                      mPort);
    }

    grpc::SslServerCredentialsOptions::PemKeyCertPair keycert = {
      mSSLKey,
      mSSLCert
    };
    grpc::SslServerCredentialsOptions sslOps(gsccrt);
    sslOps.pem_root_certs = mSSLCa;
    sslOps.pem_key_cert_pairs.push_back(keycert);
    builder.AddListeningPort(bind_address, grpc::SslServerCredentials(sslOps),
                             &selected_port);
  } else {
    eos_static_crit("msg=\"gRPC server starting in INSECURE mode - "
                    "bound to loopback only\" bind=\"%s\"",
                    bind_address.c_str());
    builder.AddListeningPort(bind_address, grpc::InsecureServerCredentials());
  }

  builder.RegisterService(&service);
  mServer = builder.BuildAndStart();

  if (mSSL && (selected_port == 0)) {
    eos_static_err("msg=\"server failed to bind to port with SSL, "
                   "port %i is taken or certs not valid\"", mPort);
    return;
  }

  if (mServer) {
    eos_static_info("msg=\"gRPC server for EOS is running\" port=%i", mPort);
    mServer->Wait();
  }

#else
  // Make the compiler happy
  (void) mPort;
  (void) mSSL;
#endif
}

EOSMGMNAMESPACE_END
