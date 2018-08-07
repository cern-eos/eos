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

    if (!eos::common::StringConversion::LoadFileIntoString(mSSLCertFile.c_str(),
        mSSLCert)) {
      eos_static_crit("unsable to load ssl certificate file '%s'",
                      mSSLCertFile.c_str());
      mSSL = false;
    }

    if (!eos::common::StringConversion::LoadFileIntoString(mSSLKeyFile.c_str(),
        mSSLKey)) {
      eos_static_crit("unsable to load ssl key file '%s'", mSSLKeyFile.c_str());
      mSSL = false;
    }

    if (!eos::common::StringConversion::LoadFileIntoString(mSSLCaFile.c_str(),
        mSSLCa)) {
      eos_static_crit("unsable to load ssl ca file '%s'", mSSLCaFile.c_str());
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

