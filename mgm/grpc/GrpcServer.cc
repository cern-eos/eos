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

/*----------------------------------------------------------------------------*/
#include "GrpcServer.hh"
#include "GrpcNsInterface.hh"
#include "proto/Rpc.grpc.pb.h"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "common/StringConversion.hh"
#include "XrdSec/XrdSecEntity.hh"
/*----------------------------------------------------------------------------*/


EOSMGMNAMESPACE_BEGIN

#ifdef EOS_GRPC

#include <grpc++/security/credentials.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::Status;

using eos::rpc::Eos;
using eos::rpc::PingRequest;
using eos::rpc::PingReply;

class RequestServiceImpl final : public Eos::Service
{

  Status Ping(ServerContext* context, const eos::rpc::PingRequest* request,
              eos::rpc::PingReply* reply) override
  {
    eos_static_info("grpc::ping from client peer=%s ip=%s DN=%s token=%s",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->authkey().c_str());
    eos::common::Mapping::VirtualIdentity_t vid;
    GrpcServer::Vid(context, &vid, request->authkey());
    reply->set_message(request->message());
    return Status::OK;
  }

  Status MD(ServerContext* context, const eos::rpc::MDRequest* request,
            ServerWriter<eos::rpc::MDResponse>* writer) override
  {
    switch (request->type()) {
    case eos::rpc::FILE:
    case eos::rpc::CONTAINER:
      return GrpcNsInterface::GetMD(writer, request);
      break;

    case eos::rpc::LISTING:
      return GrpcNsInterface::StreamMD(writer, request);
      break;

    default:
      ;
    }

    return Status(grpc::StatusCode::INVALID_ARGUMENT, "request is not supported");
  }
};

/* return client DN*/
std::string
GrpcServer::DN(grpc::ServerContext* context)
{
  std::string property =
    context->auth_context()->GetPeerIdentityPropertyName().c_str();

  if (property == "x509_subject_alternative_name") {
    std::vector<grpc::string_ref> identities =
      context->auth_context()->GetPeerIdentity();

    if (identities.size() == 1) {
      return identities[0].data();
    }
  }

  if (property == "x509_common_name") {
    std::vector<grpc::string_ref> identities =
      context->auth_context()->GetPeerIdentity();

    if (identities.size() == 1) {
      return identities[0].data();
    }
  }

  return "";
}

/* return client IP */
std::string GrpcServer::IP(grpc::ServerContext* context, std::string* id,
                           std::string* net)
{
  std::vector<std::string> tokens;
  eos::common::StringConversion::Tokenize(std::string(context->peer().c_str()),
                                          tokens,
                                          ":");

  if (tokens.size() == 3) {
    // format is ipv4:<ip>:<..> or ipv6:<ip>:<..> - we just return the IP address
    if (id) {
      *id = tokens[2];
    }

    if (net) {
      *net = tokens[0];
    }

    return tokens[1];
  }

  if ((tokens.size() > 3) && tokens[0] == "ipv6") {
    std::string ip;

    for (size_t it = 1; it < tokens.size() - 1; ++it) {
      ip += tokens[it];

      if (it != tokens.size() - 2) {
        ip += ":";
      }
    }

    if (id) {
      *id = tokens[tokens.size() - 1];
    }

    if (net) {
      *net = tokens[0];
    }

    return ip;
  }

  return "";
}

/* return VID for a given call */
void
GrpcServer::Vid(grpc::ServerContext* context,
                eos::common::Mapping::VirtualIdentity_t* vid,
                const std::string& authkey)
{
  XrdSecEntity client("grpc");
  std::string dn = DN(context);
  client.name = const_cast<char*>(dn.c_str());
  std::string tident = dn.length() ? dn.c_str() : authkey.c_str();
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

  vid = new eos::common::Mapping::VirtualIdentity();
  eos::common::Mapping::IdMap(&client, "eos.app=grpc", client.tident, *vid, true);
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

  RequestServiceImpl service;
  std::string bind_address = "0.0.0.0:";
  bind_address += std::to_string(mPort);
  grpc::ServerBuilder builder;

  if (mSSL) {
    grpc::SslServerCredentialsOptions::PemKeyCertPair keycert = {
      mSSLKey,
      mSSLCert
    };
    grpc::SslServerCredentialsOptions sslOps(
      GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY);
    sslOps.pem_root_certs = mSSLCa;
    sslOps.pem_key_cert_pairs.push_back(keycert);
    builder.AddListeningPort(bind_address, grpc::SslServerCredentials(sslOps));
  } else {
    builder.AddListeningPort(bind_address, grpc::InsecureServerCredentials());
  }

  builder.RegisterService(&service);
  mServer = builder.BuildAndStart();
  mServer->Wait();
#endif
}

EOSMGMNAMESPACE_END

