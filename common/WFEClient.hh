// ----------------------------------------------------------------------
// File: WFEClient.hh
// Author: Konstantina Skovola - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#include <grpc++/grpc++.h>
#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include "common/Logging.hh"

#include <grpcpp/security/credentials.h>

#include <fstream>
#include <iostream>
#include <sstream>

static std::string file2string(std::string filename){
  std::ifstream as_stream(filename);
  std::ostringstream as_string;
  as_string << as_stream.rdbuf();
  return as_string.str();
}

class WFEClient {
public:
  virtual cta::xrd::Response::ResponseType send(const cta::xrd::Request& request, cta::xrd::Response& response) = 0;
  virtual ~WFEClient() = default;
};

// need to configure call credentials here, add the token
class JWTAuthenticator : public grpc::MetadataCredentialsPlugin {
public:
    JWTAuthenticator(const grpc::string& grpcstrToken) : m_grpcstrToken(grpcstrToken) {}

    grpc::Status GetMetadata(
        grpc::string_ref serviceUrl, grpc::string_ref methodName,
        const grpc::AuthContext& channelAuthContext,
        std::multimap<grpc::string, grpc::string>* metadata) override {
        metadata->insert(std::make_pair("cta-grpc-jwt-auth-token", m_grpcstrToken));
        return grpc::Status::OK;
    }

private:
    grpc::string m_grpcstrToken;
};

class WFEGrpcClient : public WFEClient {
public:
  WFEGrpcClient(std::string endpoint_str, std::optional<std::string> root_certs) {
    endpoint = endpoint_str;
    constexpr char RootCertificate[] = "/etc/grid-security/certificates/ca.crt";
    grpc::SslCredentialsOptions ssl_options;
    if (root_certs.has_value())
      ssl_options.pem_root_certs = file2string(root_certs.value());
    else
      ssl_options.pem_root_certs = "";
    eos_static_info("loaded root certificate, it is %s", file2string(RootCertificate).c_str());
    eos_static_info("value used in pem_root_certs is %s", ssl_options.pem_root_certs.c_str()); // /tmp/mgm/.xrdtls/ca_file.pem
    // Create a channel with SSL credentials
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(endpoint_str, grpc::SslCredentials(ssl_options));
    client_stub = cta::xrd::CtaRpc::NewStub(channel);
    eos_static_info("successfully created the client stub in EOS");
  }

  // for gRPC the default is to retry a failed request (see GRPC_ARG_ENABLE_RETRIES)
  cta::xrd::Response::ResponseType send(const cta::xrd::Request& request, cta::xrd::Response& response) override {
    grpc::ClientContext context;
    grpc::Status status;

    std::string token_contents;
    // read the token from the expected place
    std::string token_path("/etc/grid-security/jwt-token-grpc"); // this path will be provided in the config eventually
    eos::common::StringConversion::LoadFileIntoString(token_path.c_str(), token_contents);

    std::shared_ptr<::grpc::CallCredentials> call_credentials =
          ::grpc::MetadataCredentialsFromPlugin(std::unique_ptr<::grpc::MetadataCredentialsPlugin>(
          new JWTAuthenticator(token_contents)));
    context.set_credentials(call_credentials);
    eos_static_info("successfully attached call credentials in the send method");

    switch (request.notification().wf().event()) {
      // this is prepare
      case cta::eos::Workflow::CREATE:
        status = client_stub->Create(&context, request, &response);
        break;
      case cta::eos::Workflow::CLOSEW:
        status = client_stub->Archive(&context, request, &response);
        break;
      case cta::eos::Workflow::PREPARE:
        status = client_stub->Retrieve(&context, request, &response);
        break;
      case cta::eos::Workflow::ABORT_PREPARE:
        status = client_stub->CancelRetrieve(&context, request, &response);
        break;
      case cta::eos::Workflow::DELETE:
        status = client_stub->Delete(&context, request, &response);
        break;
      case cta::eos::Workflow::OPENW:
        // this does nothing and we don't have a gRPC method for it
        /* fallthrough */
      case cta::eos::Workflow::UPDATE_FID:
        /* fallthrough */
      default:
        // should probably have an error here that we don't have a gRPC method for this
        status = grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "gRPC method not implemented for " + cta::eos::Workflow_EventType_Name(request.notification().wf().event()));
        break;
    }
    if (status.ok()){
      return cta::xrd::Response::RSP_SUCCESS;
    } else {
      switch (status.error_code()) {
        // user-code (CTA) generated errors,
        case grpc::StatusCode::INVALID_ARGUMENT:
          return cta::xrd::Response::RSP_ERR_PROTOBUF;
        case grpc::StatusCode::ABORTED:
          return cta::xrd::Response::RSP_ERR_USER;
        case grpc::StatusCode::FAILED_PRECONDITION:
          return cta::xrd::Response::RSP_ERR_CTA;
        // something went wrong in the gRPC code, throw an exception
        default:
          throw std::runtime_error("gRPC call failed internally. Error code: " + std::to_string(status.error_code()) + " Error message: " + status.error_message());
      }
    }
  }
private:
  std::string endpoint;
  std::unique_ptr<cta::xrd::CtaRpc::Stub> client_stub;
};

class WFEXrdClient : public WFEClient {
public:
  WFEXrdClient(std::string endpoint, std::string resource, XrdSsiPb::Config &config) : service(XrdSsiPbServiceType(endpoint, resource, config)) {}
  cta::xrd::Response::ResponseType send(const cta::xrd::Request& request, cta::xrd::Response& response) override {
    try {
      service.Send(request, response, false);
      return response.type();
    } catch (std::runtime_error& err) {
      eos_static_err("Could not send request to outside service. Retrying with DNS cache refresh.");
      service.Send(request, response, true);
      return response.type();
    }
  }
private:
  XrdSsiPbServiceType service;
};

std::unique_ptr<WFEClient>
CreateRequestSender(bool protowfusegrpc, std::string endpoint, std::string ssi_resource, std::optional<std::string> root_certs) {
  if (protowfusegrpc) {
    return std::make_unique<WFEGrpcClient>(endpoint, root_certs);
  } else {
    XrdSsiPb::Config config;

    if (getenv("XRDDEBUG")) {
      config.set("log", "all");
    } else {
      config.set("log", "info");
    }

    config.set("request_timeout", "120");
    return std::make_unique<WFEXrdClient>(endpoint, ssi_resource, config);
  }
}