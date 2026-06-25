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

#ifndef __EOSCOMMON_WFECLIENT__HH__
#define __EOSCOMMON_WFECLIENT__HH__

#include "WFEEndpoint.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "cta_frontend.grpc.pb.h"
#include "cta_frontend.pb.h"
#include "xrootd-ssi-protobuf-interface/eos_cta/include/CtaFrontendApi.hpp"
#include <grpc++/grpc++.h>
#include <grpcpp/security/credentials.h>

class WFEClient {
public:
  virtual cta::xrd::Response::ResponseType send(const cta::xrd::Request& request,
                                                cta::xrd::Response& response) = 0;
  virtual ~WFEClient() = default;
};

class RequestSenderConfig {
public:
  WFEndpoint endpoint;
  std::string ssi_resource;
  std::optional<std::string> root_certs;
  std::optional<std::string> token_path;
  std::optional<std::string> client_cert_path;
  std::optional<std::string> client_key_path;

  RequestSenderConfig(WFEndpoint endpoint_, std::string resource,
                      std::optional<std::string> root_certs,
                      std::optional<std::string> token_path,
                      std::optional<std::string> client_cert_path,
                      std::optional<std::string> client_key_path)
      : endpoint(endpoint_)
      , ssi_resource(resource)
      , root_certs(root_certs)
      , token_path(token_path)
      , client_cert_path(client_cert_path)
      , client_key_path(client_key_path)
  {
  }
};

class WFEGrpcClient : public WFEClient {
public:
  WFEGrpcClient(const WFEndpoint endpoint, const std::optional<std::string> root_certs,
                const std::optional<std::string> token_path_str,
                const std::optional<std::string> cert_path_str,
                const std::optional<std::string> key_path_str);

  // for gRPC the default is to retry a failed request (see GRPC_ARG_ENABLE_RETRIES)
  cta::xrd::Response::ResponseType send(const cta::xrd::Request& request,
                                        cta::xrd::Response& response) override;

private:
  WFEndpoint endpoint;
  std::unique_ptr<cta::xrd::CtaRpc::Stub> client_stub;
  std::optional<std::string> token_path;
  std::optional<std::string> cert_path;
  std::optional<std::string> key_path;
};

class WFEXrdClient : public WFEClient {
public:
  WFEXrdClient(std::string endpoint, std::string resource, XrdSsiPb::Config& config)
      : service(XrdSsiPbServiceType(endpoint, resource, config))
  {
  }
  cta::xrd::Response::ResponseType send(const cta::xrd::Request& request,
                                        cta::xrd::Response& response) override;

private:
  XrdSsiPbServiceType service;
};

std::unique_ptr<WFEClient> CreateRequestSender(const RequestSenderConfig& cf);

#endif // __EOSCOMMON_WFECLIENT__HH__
