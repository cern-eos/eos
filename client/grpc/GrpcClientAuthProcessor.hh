// ----------------------------------------------------------------------
// File: GrpcClientAuthProcessor.hh
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

#pragma once

/*----------------------------------------------------------------------------*/
#include "client/Namespace.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#ifdef EOS_GRPC
#include <grpc++/grpc++.h>

/*----------------------------------------------------------------------------*/

EOSCLIENTNAMESPACE_BEGIN

/**
 * @file   GrpcClientAuthProcessor.hh
 *
 * @brief  This class implements an authentication processor for an EOS GRPC client
 *         allowing to extract the client property name
 *
 */


class GrpcClientProcessor : public grpc::AuthMetadataProcessor
{
public:

  struct Const {

    static const std::string& TokenKeyName()
    {
      static std::string _("token");
      return _;
    }

    static const std::string& PeerIdentityPropertyName()
    {
      static std::string _("username");
      return _;
    }
  };

  grpc::Status Process(const InputMetadata& auth_metadata,
                       grpc::AuthContext* context, OutputMetadata* consumed_auth_metadata,
                       OutputMetadata* response_metadata) override
  {
    // determine intercepted method
    std::string dispatch_keyname = ":path";
    auto dispatch_kv = auth_metadata.find(dispatch_keyname);

    if (dispatch_kv == auth_metadata.end()) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Internal Error");
    }

    // if token metadata not necessary, return early, avoid token checking
    auto dispatch_value = std::string(dispatch_kv->second.data());

    if (dispatch_value == "/MyPackage.MyService/Authenticate") {
      return grpc::Status::OK;
    }

    // determine availability of token metadata
    auto token_kv = auth_metadata.find(Const::TokenKeyName());

    if (token_kv == auth_metadata.end()) {
      return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Missing Token");
    }

    // determine validity of token metadata
    auto token_value = std::string(token_kv->second.data());

    if (tokens.count(token_value) == 0) {
      return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Invalid Token");
    }

    // once verified, mark as consumed and store user for later retrieval
    consumed_auth_metadata->insert(std::make_pair(Const::TokenKeyName(),
                                   token_value)); // required
    context->AddProperty(Const::PeerIdentityPropertyName(),
                         tokens[token_value]); // optional
    context->SetPeerIdentityPropertyName(
      Const::PeerIdentityPropertyName()); // optional
    return grpc::Status::OK;
  }

  std::map<std::string, std::string> tokens;
};

#endif

EOSCLIENTNAMESPACE_END

