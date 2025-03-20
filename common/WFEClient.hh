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

class WFEClient {
public:
  virtual cta::xrd::Response::ResponseType send(const cta::xrd::Request& request, cta::xrd::Response& response) = 0;
  virtual ~WFEClient() = default;
};

class WFEGrpcClient : public WFEClient {
public:
  WFEGrpcClient(std::string endpoint_str) : endpoint(endpoint_str), client_stub(cta::xrd::CtaRpc::NewStub(grpc::CreateChannel(endpoint_str, grpc::InsecureChannelCredentials()))) {}

  // for gRPC the default is to retry a failed request (see GRPC_ARG_ENABLE_RETRIES)
  cta::xrd::Response::ResponseType send(const cta::xrd::Request& request, cta::xrd::Response& response) override {
    grpc::ClientContext context;
    grpc::Status status;

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
        case grpc::StatusCode::UNKNOWN:
          throw std::runtime_error("UNKNOWN error from the gRPC framework");
        case grpc::StatusCode::DEADLINE_EXCEEDED:
          throw std::runtime_error("DEADLINE_EXCEEDED error from the gRPC framework");
        case grpc::StatusCode::PERMISSION_DENIED:
          throw std::runtime_error("PERMISSION_DENIED error from the gRPC framework");
        case grpc::StatusCode::UNIMPLEMENTED:
          throw std::runtime_error("UNIMPLEMENTED error from the gRPC framework");
        case grpc::StatusCode::INTERNAL:
          throw std::runtime_error("INTERNAL error from the gRPC framework");
        case grpc::StatusCode::UNAVAILABLE:
          throw std::runtime_error("UNAVAILABLE error from the gRPC framework");
        case grpc::StatusCode::UNAUTHENTICATED:
          throw std::runtime_error("UNAUTHENTICATED error from the gRPC framework");
        case grpc::StatusCode::CANCELLED:
          throw std::runtime_error("CANCELLED error from the gRPC framework");
        default:
          return cta::xrd::Response::RSP_INVALID;
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
CreateRequestSender(bool protowfusegrpc, std::string endpoint, std::string ssi_resource) {
  if (protowfusegrpc) {
    return std::make_unique<WFEGrpcClient>(endpoint);
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