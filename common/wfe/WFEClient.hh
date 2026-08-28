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

#pragma once

#include "WFEEndpoint.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "cta_frontend.grpc.pb.h"
#include "cta_frontend.pb.h"
#include "xrootd-ssi-protobuf-interface/eos_cta/include/CtaFrontendApi.hpp"
#include <grpcpp/security/credentials.h>

class WFEClient {
public:
  virtual cta::xrd::Response::ResponseType send(const cta::xrd::Request& request,
                                                cta::xrd::Response& response) = 0;
  virtual ~WFEClient() = default;
};

class RequestSenderConfig {
public:
  WFEndpoint m_endpoint;
  std::string m_ssiResource;
  std::optional<std::string> m_rootCerts;
  std::optional<std::string> m_tokenPath;
  std::optional<std::string> m_clientCertPath;
  std::optional<std::string> m_clientKeyPath;

  RequestSenderConfig(WFEndpoint endpoint_, std::string resource,
                      std::optional<std::string> rootCerts,
                      std::optional<std::string> tokenPath,
                      std::optional<std::string> clientCertPath,
                      std::optional<std::string> clientKeyPath)
      : m_endpoint(endpoint_)
      , m_ssiResource(resource)
      , m_rootCerts(rootCerts)
      , m_tokenPath(tokenPath)
      , m_clientCertPath(clientCertPath)
      , m_clientKeyPath(clientKeyPath)
  {
  }
};

class WFEGrpcClient : public WFEClient {
public:
  // GRPCS_JWT: TLS with root certs and JWT token
  WFEGrpcClient(const WFEndpoint endpoint, const std::optional<std::string>& rootCerts,
                const std::string& tokenPathStr);

  // GRPCS_MTLS: TLS with root certs and client certificates
  WFEGrpcClient(const WFEndpoint endpoint, const std::optional<std::string>& rootCerts,
                const std::string& certPathStr, const std::string& keyPathStr);

  // for gRPC the default is to retry a failed request (see GRPC_ARG_ENABLE_RETRIES)
  cta::xrd::Response::ResponseType send(const cta::xrd::Request& request,
                                        cta::xrd::Response& response) override;

private:
  static std::shared_ptr<grpc::ChannelCredentials>
  buildCredentials(const std::optional<std::string>& rootCerts,
                   const std::optional<std::string>& certPathStr,
                   const std::optional<std::string>& keyPathStr)
  {
    grpc::SslCredentialsOptions sslOptions;

    // load CA root certificates, if they're set
    if (rootCerts.has_value()) {
      std::string pem;
      const char* res =
          eos::common::StringConversion::LoadFileIntoString(rootCerts->c_str(), pem);

      if (res && pem[0] != '\0') {
        sslOptions.pem_root_certs = std::move(pem);

        eos_static_info("loaded a root certificate from %s", rootCerts->c_str());
        eos_static_debug("root certificate contents: %s",
                         sslOptions.pem_root_certs.c_str());
      } else {
        eos_static_warning(
            "failed to load root certs from '%s', falling back to grpc default",
            rootCerts->c_str());
      }
    }

    // sanity check to make sure we have two paths for the client cert (cert + key)
    if (certPathStr.has_value() != keyPathStr.has_value()) {
      eos_static_warning("mTLS requires both cert and key paths - only one was provided "
                         "(cert_path=%s, key_path=%s). Skipping client certificate",
                         certPathStr.has_value() ? certPathStr->c_str() : "<none>",
                         keyPathStr.has_value() ? keyPathStr->c_str() : "<none>");
    } else if (certPathStr.has_value() && keyPathStr.has_value()) {
      std::string cert, key;
      const char* certRes =
          eos::common::StringConversion::LoadFileIntoString(certPathStr->c_str(), cert);
      const char* keyRes =
          eos::common::StringConversion::LoadFileIntoString(keyPathStr->c_str(), key);

      if (certRes && cert[0] != '\0' && keyRes && key[0] != '\0') {
        sslOptions.pem_cert_chain = std::move(cert);
        sslOptions.pem_private_key = std::move(key);
        eos_static_info("Using mTLS. Client cert_path=\"%s\" key_path=\"%s\"",
                        certPathStr->c_str(), keyPathStr->c_str());
      } else {
        eos_static_warning(
            "failed to load client cert/key (cert_path=\"%s\" key_path=\"%s\"). "
            "mTLS will not be used",
            certPathStr->c_str(), keyPathStr->c_str());
      }
    }

    return grpc::SslCredentials(sslOptions);
  }

  WFEndpoint m_endpoint;
  std::unique_ptr<cta::xrd::CtaRpc::Stub> m_clientStub;
  std::optional<std::string> m_tokenPath;
  std::optional<std::string> m_certPath;
  std::optional<std::string> m_keyPath;
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
