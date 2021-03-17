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
#include <google/protobuf/util/json_util.h>
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "common/StringConversion.hh"
#include "XrdSec/XrdSecEntity.hh"

#ifdef EOS_GRPC
#include "flatb/fst.grpc.fb.h"
#include <grpc++/security/credentials.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::Status;
using eos::PingRequest;
using eos::PingReply;

using eos::GetRequest;
using eos::GetReply;

#endif

EOSFSTNAMESPACE_BEGIN

#ifdef EOS_GRPC

class RequestServiceImpl final : public Eos::Service
{



  Status Ping(ServerContext* context,
	      const flatbuffers::grpc::Message<PingRequest>* request,
	      flatbuffers::grpc::Message<PingReply>* response) override
  {
    eos_static_info("grpc::ping from client peer=%s ip=%s DN=%s token=%s len=%lu",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->GetRoot()->authkey()->str().c_str(),
                    request->GetRoot()->message()->str().length());
    // this is really aweful to say the least ...
    flatbuffers::grpc::MessageBuilder mb;
    auto resp = CreatePingReplyDirect(mb, request->GetRoot()->message()->str().c_str());
    mb.Finish(resp);
    *response = mb.ReleaseMessage<PingReply>();
    return Status::OK;
  }

  Status Get(ServerContext* context,
	      const flatbuffers::grpc::Message<GetRequest>* request,
	      flatbuffers::grpc::Message<GetReply>* response) override
  {
    eos_static_info("grpc::ping from client peer=%s ip=%s DN=%s token=%s name=%s offset=%lu size=%lu",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->GetRoot()->authkey()->str().c_str(),
                    request->GetRoot()->name()->str().c_str(),
		    request->GetRoot()->offset(),
		    request->GetRoot()->len());
    // this is really aweful to say the least ...
    flatbuffers::grpc::MessageBuilder mb;
    std::vector<int8_t> buffer;
    // create requested buffer
    //    buffer.resize(request->GetRoot()->len(),0);
    buffer.resize(request->GetRoot()->len(),0);
    fprintf(stderr,"answering %lu\n", request->GetRoot()->len());
    auto resp = CreateGetReplyDirect(mb, &buffer);
    mb.Finish(resp);
    *response = mb.ReleaseMessage<GetReply>();
    return Status::OK;
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

#endif

void
GrpcServer::Run(ThreadAssistant& assistant) noexcept
{
#ifdef EOS_GRPC
  if (getenv("EOS_FST_GRPC_SSL_CERT") &&
      getenv("EOS_FST_GRPC_SSL_KEY") &&
      getenv("EOS_FST_GRPC_SSL_CA")) {
    mSSL = true;
    mSSLCertFile = getenv("EOS_FST_GRPC_SSL_CERT");
    mSSLKeyFile = getenv("EOS_FST_GRPC_SSL_KEY");
    mSSLCaFile = getenv("EOS_FST_GRPC_SSL_CA");

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

EOSFSTNAMESPACE_END
