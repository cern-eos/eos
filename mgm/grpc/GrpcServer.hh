// ----------------------------------------------------------------------
// File: GrpcServer.hh
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
#include "mgm/Namespace.hh"
#include "common/AssistedThread.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#ifdef EOS_GRPC
#include <grpc++/grpc++.h>
#endif

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/**
 * @file   GrpcServer.hh
 *
 * @brief  This class implements a gRPC server running embedded in the MGM
 *
 */


class GrpcServer
{
private:
  int mPort;
  bool mSSL;
  std::string mSSLCert;
  std::string mSSLKey;
  std::string mSSLCa;
  std::string mSSLCertFile;
  std::string mSSLKeyFile;
  std::string mSSLCaFile;

#ifdef EOS_GRPC
  std::unique_ptr<grpc::Server> mServer;
#endif
  AssistedThread mThread; ///< Thread running GRPC service

public:

  /* Default Constructor - enabling port 50051 by default
   */
  GrpcServer(int port = 50051) : mPort(port), mSSL(false) { }

  virtual ~GrpcServer()
  {
    mThread.join();
  }

  /* Run function */
  void Run(ThreadAssistant& assistant) noexcept;

  /* Startup function */
  void Start()
  {
    mThread.reset(&GrpcServer::Run, this);
  }

#ifdef EOS_GRPC
  /* return client DN*/
  static std::string DN(grpc::ServerContext* context)
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

    return "";
  }
#endif
};



EOSMGMNAMESPACE_END

