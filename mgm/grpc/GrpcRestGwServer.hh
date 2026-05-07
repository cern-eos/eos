// ----------------------------------------------------------------------
// File: GrpcRestGwServer.cc
// Author: Elvin Sindrilaru - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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
#include "mgm/Namespace.hh"
#include "common/AssistedThread.hh"
#include "common/Mapping.hh"
#include "common/Logging.hh"
#include "GrpcRestGwInterface.hh"

#ifdef EOS_GRPC_GATEWAY
#include <grpc++/grpc++.h>
#endif // EOS_GRPC_GATEWAY

EOSMGMNAMESPACE_BEGIN

/**
 * @file   GrpcRestGwServer.hh
 *
 * @brief  This class implements a GRPC server with a grpc-gateway
           for accessing all EOS console commands through HTTP requests
 */
class GrpcRestGwServer: public eos::common::LogId
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
  AssistedThread mThread; // Thread running GRPC service

#ifdef EOS_GRPC_GATEWAY
  std::unique_ptr<grpc::Server> mRestGwServer;
#endif /// EOS_GRPC_GATEWAY

public:

  /* Default Constructor - enabling port 50054 by default
   */
  GrpcRestGwServer(int port = 50054) : mPort(port), mSSL(false) {  }

  ~GrpcRestGwServer()
  {
#ifdef EOS_GRPC_GATEWAY

    if (mRestGwServer) {
      mRestGwServer->Shutdown();
    }

#endif // EOS_GRPC_GATEWAY
    mThread.join();
  }

  /* Run function */
  void Run(ThreadAssistant& assistant) noexcept;

  /* Startup function */
  void Start()
  {
    mThread.reset(&GrpcRestGwServer::Run, this);
  }

#ifdef EOS_GRPC_GATEWAY

  /* return client DN*/
  static std::string DN(grpc::ServerContext* context);
  /* return client IP*/
  static std::string IP(grpc::ServerContext* context, std::string* id = 0,
                        std::string* port = 0);
  /* return VID for a given call */
  static void Vid(grpc::ServerContext* context,
                  eos::common::VirtualIdentity& vid);

#endif // EOS_GRPC_GATEWAY
};

EOSMGMNAMESPACE_END
