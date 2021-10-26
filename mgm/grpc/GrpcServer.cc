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
#include "GrpcManilaInterface.hh"
#include <google/protobuf/util/json_util.h>
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "mgm/Macros.hh"
#include "XrdSec/XrdSecEntity.hh"

#ifdef EOS_GRPC
#include "proto/Rpc.grpc.pb.h"
#include <grpc++/security/credentials.h>

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
using eos::rpc::ManilaRequest;
using eos::rpc::ManilaResponse;

#endif

EOSMGMNAMESPACE_BEGIN

#ifdef EOS_GRPC

class RequestServiceImpl final : public Eos::Service
{

  Status Ping(ServerContext* context, const eos::rpc::PingRequest* request,
              eos::rpc::PingReply* reply) override
  {
    eos_static_info("grpc::ping from client peer=%s ip=%s DN=%s token=%s len=%lu",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->authkey().c_str(),
                    request->message().length());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    reply->set_message(request->message());
    return Status::OK;
  }

  Status FileInsert(ServerContext* context,
                    const eos::rpc::FileInsertRequest* request,
                    eos::rpc::InsertReply* reply) override
  {
    eos_static_info("grpc::fileinsert from client peer=%s ip=%s DN=%s token=%s",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->authkey().c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;
    return GrpcNsInterface::FileInsert(vid, reply, request);
  }

  Status ContainerInsert(ServerContext* context,
                         const eos::rpc::ContainerInsertRequest* request,
                         eos::rpc::InsertReply* reply) override
  {
    eos_static_info("grpc::containerinsert from client peer=%s ip=%s DN=%s token=%s",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->authkey().c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;
    return GrpcNsInterface::ContainerInsert(vid, reply, request);
  }

  Status MD(ServerContext* context, const eos::rpc::MDRequest* request,
            ServerWriter<eos::rpc::MDResponse>* writer) override
  {
    eos_static_info("grpc::md from client peer=%s ip=%s DN=%s token=%s",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->authkey().c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;

    switch (request->type()) {
    case eos::rpc::FILE:
    case eos::rpc::CONTAINER:
    case eos::rpc::STAT:
      return GrpcNsInterface::GetMD(vid, writer, request);
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
    eos_static_info("grpc::find from client peer=%s ip=%s DN=%s token=%s",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->authkey().c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;
    return GrpcNsInterface::Find(vid, writer, request);
  }

  Status NsStat(ServerContext* context,
                const eos::rpc::NsStatRequest* request,
                eos::rpc::NsStatResponse* reply) override
  {
    eos_static_info("grpc::nsstat::request from client peer=%s ip=%s DN=%s token=%s",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->authkey().c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;
    return GrpcNsInterface::NsStat(vid, reply, request);
  }

  Status ManilaServerRequest(ServerContext* context,
                             const eos::rpc::ManilaRequest* request,
                             eos::rpc::ManilaResponse* reply) override
  {
    std::string jsonstring;
    google::protobuf::util::JsonPrintOptions options;
    options.add_whitespace = true;
    options.always_print_primitive_fields = true;
    google::protobuf::util::MessageToJsonString(*request,
        &jsonstring, options);
    eos_static_notice("grpc::manila::server::request from client peer=%s ip=%s DN=%s token=%s type=%d \nrequest:\n%s",
                      context->peer().c_str(), GrpcServer::IP(context).c_str(),
                      GrpcServer::DN(context).c_str(), request->auth_key().c_str(),
                      request->request_type(),
                      jsonstring.c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->auth_key());
    WAIT_BOOT;
    Status st = GrpcManilaInterface::Process(vid, reply, request);
    google::protobuf::util::MessageToJsonString(*reply,
        &jsonstring, options);
    eos_static_notice("\nreply:\n%s", jsonstring.c_str());
    return st;
  }

  Status Exec(ServerContext* context,
              const eos::rpc::NSRequest* request,
              eos::rpc::NSResponse* reply) override
  {
    eos_static_info("grpc::exec::request from client peer=%s ip=%s DN=%s token=%s",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->authkey().c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;
    return GrpcNsInterface::Exec(vid, reply, request);
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
  if(resp.empty()){
    tag = "x509_subject_alternative_name";
    auto resp = context->auth_context()->FindPropertyValues(tag);
    if (resp.empty()) { return "";}
  }
  return resp[0].data();
}



/* return client IP */
std::string GrpcServer::IP(grpc::ServerContext* context, std::string* id,
                           std::string* port)
{
  // format is ipv4:<ip>:<..> or ipv6:<ip>:<..> - we just return the IP address
  // butq net and id are populated as well with the prefix and suffix, respectively
  std::vector<std::string> tokens;
  eos::common::StringConversion::Tokenize(context->peer(),
                                          tokens,
                                          "[]");
  if (tokens.size() == 3){
    if (id) {
      *id = tokens[0].substr(0,tokens[0].size()-1);
    }
    if (port) {
      *port = tokens[2].substr(1,tokens[2].size()-1);
    }
    return "["+tokens[1]+"]";
  }else {
    tokens.clear();
    eos::common::StringConversion::Tokenize(context->peer(),
                                            tokens,
                                            ":");
    if (tokens.size() == 3){
      if (id) {
          *id = tokens[0].substr(0,tokens[0].size());
        }
        if (port) {
          *port = tokens[2].substr(0,tokens[2].size());
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

  eos::common::Mapping::IdMap(&client, "eos.app=grpc", client.tident, vid, true);
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
#else
  // Make the compiler happy
  (void) mPort;
  (void) mSSL;
#endif
}

EOSMGMNAMESPACE_END

