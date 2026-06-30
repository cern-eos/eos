// ----------------------------------------------------------------------
// File: WFEClient.cc
// Author: Pedro Ferreira - CERN
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

#include "common/wfe/WFEClient.hh"
#include "jwt-cpp/jwt.h"

// GRPC_JWT: insecure channel with JWT token
WFEGrpcClient::WFEGrpcClient(const WFEndpoint endpoint, const std::string& token_path_str)
    : endpoint(endpoint)
    , token_path(token_path_str)
    , cert_path(std::nullopt)
    , key_path(std::nullopt)
{
  auto credentials = grpc::InsecureChannelCredentials();

  eos_static_info("Connecting to endpoint %s with scheme grpc",
                  endpoint.address().c_str());
  eos_static_info("Using JWT. Token path=\"%s\"", token_path_str.c_str());

  std::shared_ptr<grpc::Channel> channel =
      grpc::CreateChannel(endpoint.address(), credentials);
  client_stub = cta::xrd::CtaRpc::NewStub(channel);
}

// GRPCS_JWT: TLS with root certs and JWT token
WFEGrpcClient::WFEGrpcClient(const WFEndpoint endpoint,
                             const std::optional<std::string>& root_certs,
                             const std::string& token_path_str)
    : endpoint(endpoint)
    , token_path(token_path_str)
    , cert_path(std::nullopt)
    , key_path(std::nullopt)
{
  grpc::SslCredentialsOptions ssl_options;

  if (root_certs.has_value()) {
    std::string root_certs_contents;
    eos::common::StringConversion::LoadFileIntoString(root_certs.value().c_str(),
                                                      root_certs_contents);
    ssl_options.pem_root_certs = root_certs_contents;
  } else {
    ssl_options.pem_root_certs = ""; // grpc will use default root certs if left blank
  }
  eos_static_info("value used in pem_root_certs is %s",
                  ssl_options.pem_root_certs.c_str());

  auto credentials = grpc::SslCredentials(ssl_options);

  eos_static_info("Connecting to endpoint %s with scheme grpcs",
                  endpoint.address().c_str());
  eos_static_info("Using JWT. Token path=\"%s\"", token_path_str.c_str());

  std::shared_ptr<grpc::Channel> channel =
      grpc::CreateChannel(endpoint.address(), credentials);
  client_stub = cta::xrd::CtaRpc::NewStub(channel);
}

// GRPCS_MTLS: TLS with root certs and client certificates
WFEGrpcClient::WFEGrpcClient(const WFEndpoint endpoint,
                             const std::optional<std::string>& root_certs,
                             const std::string& cert_path_str,
                             const std::string& key_path_str)
    : endpoint(endpoint)
    , token_path(std::nullopt)
    , cert_path(cert_path_str)
    , key_path(key_path_str)
{
  grpc::SslCredentialsOptions ssl_options;

  if (root_certs.has_value()) {
    std::string root_certs_contents;
    eos::common::StringConversion::LoadFileIntoString(root_certs.value().c_str(),
                                                      root_certs_contents);
    ssl_options.pem_root_certs = root_certs_contents;
  } else {
    ssl_options.pem_root_certs = ""; // grpc will use default root certs if left blank
  }
  eos_static_info("value used in pem_root_certs is %s",
                  ssl_options.pem_root_certs.c_str());

  eos::common::StringConversion::LoadFileIntoString(cert_path_str.c_str(),
                                                    ssl_options.pem_cert_chain);
  eos::common::StringConversion::LoadFileIntoString(key_path_str.c_str(),
                                                    ssl_options.pem_private_key);
  eos_static_info("Using mTLS. Client cert path=\"%s\" key path=\"%s\"",
                  cert_path_str.c_str(), key_path_str.c_str());

  auto credentials = grpc::SslCredentials(ssl_options);

  eos_static_info("Connecting to endpoint %s with scheme grpcs",
                  endpoint.address().c_str());

  std::shared_ptr<grpc::Channel> channel =
      grpc::CreateChannel(endpoint.address(), credentials);
  client_stub = cta::xrd::CtaRpc::NewStub(channel);
}

// for gRPC the default is to retry a failed request (see GRPC_ARG_ENABLE_RETRIES)
cta::xrd::Response::ResponseType
WFEGrpcClient::send(const cta::xrd::Request& request, cta::xrd::Response& response)
{
  grpc::ClientContext context;
  grpc::Status status;

  std::string token_contents;

  if ((endpoint.type == WFEndpoint::ClientType::GRPC_JWT ||
       endpoint.type == WFEndpoint::ClientType::GRPCS_JWT) &&
      token_path.has_value()) {
    // read the token from the path
    eos::common::StringConversion::LoadFileIntoString(token_path.value().c_str(),
                                                      token_contents);

    // before adding the metadata, ensure that the token contents are not
    // malformed: if decoding works, it should be a valid JWT
    try {
      auto decoded = jwt::decode(token_contents);
    } catch (std::invalid_argument& ex) {
      throw std::runtime_error(std::string("Token is not in correct format:") +
                               ex.what());
    } catch (std::runtime_error& ex) {
      throw;
    }

    context.AddMetadata("authorization", "Bearer " + token_contents);
    eos_static_debug("msg=\"using JWT for authentication: successfully attached call "
                     "credentials\" token=\"%s\"",
                     token_contents.c_str());
  } else if (endpoint.type == WFEndpoint::ClientType::GRPCS_MTLS) {
    eos_static_debug("msg=\"using mTLS for authentication\"");
  }

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
    status = grpc::Status(
        grpc::StatusCode::UNIMPLEMENTED,
        "gRPC method not implemented for " +
            cta::eos::Workflow_EventType_Name(request.notification().wf().event()));
    break;
  }
  if (status.ok()) {
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
      throw std::runtime_error("gRPC call failed internally. Error code: " +
                               std::to_string(status.error_code()) +
                               " Error message: " + status.error_message());
    }
  }
}

cta::xrd::Response::ResponseType
WFEXrdClient::send(const cta::xrd::Request& request, cta::xrd::Response& response)
{
  try {
    service.Send(request, response, false);
    return response.type();
  } catch (std::runtime_error& err) {
    eos_static_err(
        "Could not send request to outside service. Retrying with DNS cache refresh.");
    service.Send(request, response, true);
    return response.type();
  }
}

std::unique_ptr<WFEClient>
CreateRequestSender(const RequestSenderConfig& cf)
{
  if (cf.endpoint.type == WFEndpoint::ClientType::GRPC_JWT) {
    return std::make_unique<WFEGrpcClient>(cf.endpoint, cf.token_path.value());
  } else if (cf.endpoint.type == WFEndpoint::ClientType::GRPCS_JWT) {
    return std::make_unique<WFEGrpcClient>(cf.endpoint, cf.root_certs,
                                           cf.token_path.value());
  } else if (cf.endpoint.type == WFEndpoint::ClientType::GRPCS_MTLS) {
    return std::make_unique<WFEGrpcClient>(cf.endpoint, cf.root_certs,
                                           cf.client_cert_path.value(),
                                           cf.client_key_path.value());
  } else {
    XrdSsiPb::Config config;

    if (getenv("XRDDEBUG")) {
      config.set("log", "all");
    } else {
      config.set("log", "info");
    }

    config.set("request_timeout", "120");
    return std::make_unique<WFEXrdClient>(cf.endpoint.address(), cf.ssi_resource, config);
  }
}
