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

#include "XrdSsiPbConfig.hpp"
#include "common/Logging.hh"
#include "cta_frontend.grpc.pb.h"
#include "cta_frontend.pb.h"
#include "xrootd-ssi-protobuf-interface/eos_cta/include/CtaFrontendApi.hpp"

#include <XrdSsiPbIStreamBuffer.hpp>
#include <grpc++/grpc++.h>
#include <grpcpp/security/credentials.h>

class WFEClient {
public:
  virtual cta::xrd::Response::ResponseType send(const cta::xrd::Request& request, cta::xrd::Response& response) = 0;
  virtual ~WFEClient() = default;
};

class WFEGrpcClient : public WFEClient {
public:
  WFEGrpcClient(const std::string& endpoint_str, std::optional<std::string> root_certs, const std::string& token_path_str, bool protowfusegrpctls) {
    endpoint = endpoint_str;
    token_path = token_path_str;

    std::shared_ptr<grpc::ChannelCredentials> credentials;
    grpc::SslCredentialsOptions ssl_options;

    if (protowfusegrpctls) {
      if (root_certs.has_value()) {
        std::string root_certs_contents;
        eos::common::StringConversion::LoadFileIntoString(root_certs.value().c_str(), root_certs_contents);
        ssl_options.pem_root_certs = root_certs_contents;
      } else {
        ssl_options.pem_root_certs = "";
      }
      eos_static_info("value used in pem_root_certs is %s", ssl_options.pem_root_certs.c_str());
      credentials = grpc::SslCredentials(ssl_options);
    } else {
      credentials = grpc::InsecureChannelCredentials();
    }
    // Create a channel with SSL credentials
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(endpoint_str, credentials);
    client_stub = cta::xrd::CtaRpc::NewStub(channel);
  }

  // for gRPC the default is to retry a failed request (see GRPC_ARG_ENABLE_RETRIES)
  cta::xrd::Response::ResponseType send(const cta::xrd::Request& request, cta::xrd::Response& response) override {
    grpc::ClientContext context;
    grpc::Status status;

    std::string token_contents;
    // read the token from the path
    eos_static_info("JWT token path is %s", token_path.c_str());
    eos::common::StringConversion::LoadFileIntoString(token_path.c_str(), token_contents);

    context.AddMetadata("authorization", "Bearer " + token_contents);
    eos_static_info("successfully attached call credentials in the send method, token contents are %s", token_contents.c_str());

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
        // we need to do response.set_message_txt here because apparently, gRPC does not
        // guarantee that the protobuf fields will be filled in, in case of error
        case grpc::StatusCode::INVALID_ARGUMENT:
          response.set_message_txt(status.error_message());
          return cta::xrd::Response::RSP_ERR_PROTOBUF;
        case grpc::StatusCode::ABORTED:
          response.set_message_txt(status.error_message());
          return cta::xrd::Response::RSP_ERR_USER;
        case grpc::StatusCode::FAILED_PRECONDITION:
          response.set_message_txt(status.error_message());
          return cta::xrd::Response::RSP_ERR_CTA;
        case grpc::StatusCode::UNAUTHENTICATED:
          response.set_message_txt(status.error_message());
          return cta::xrd::Response::RSP_ERR_USER;
        // something went wrong in the gRPC code, throw an exception
        default:
          throw std::runtime_error("gRPC call failed internally. Error code: " + std::to_string(status.error_code()) + " Error message: " + status.error_message());
      }
    }
  }
private:
  std::string endpoint;
  std::unique_ptr<cta::xrd::CtaRpc::Stub> client_stub;
  std::string token_path;
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
CreateRequestSender(bool protowfusegrpc, std::string endpoint, std::string ssi_resource, std::optional<std::string> root_certs, std::string token_path, bool protowfusegrpctls) {
  if (protowfusegrpc) {
    return std::make_unique<WFEGrpcClient>(endpoint, root_certs, token_path, protowfusegrpctls);
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
