//-----------------------------------------------------------------------------
// File: GrpcWncServer.hh
// Author: Branko Blagojevic <branko.blagojevic@comtrade.com>
//-----------------------------------------------------------------------------

#pragma once

//-----------------------------------------------------------------------------
#include "common/AssistedThread.hh"
#include "common/Mapping.hh"
#include "mgm/Namespace.hh"
//-----------------------------------------------------------------------------
#ifdef EOS_GRPC
#include <grpc++/grpc++.h>
#endif
//-----------------------------------------------------------------------------

EOSMGMNAMESPACE_BEGIN

/**
 * @file   GrpcWncServer.hh
 *
 * @brief  This class implements a gRPC server for EOS Windows native client
 * running embedded in the MGM
 *
 */
class GrpcWncServer
{
private:
  int mWncPort; // 50052 by default
  bool mSSL;
  std::string mSSLCert;
  std::string mSSLKey;
  std::string mSSLCa;
  std::string mSSLCertFile;
  std::string mSSLKeyFile;
  std::string mSSLCaFile;
  AssistedThread mThread; // Thread running gRPC service

#ifdef EOS_GRPC
  std::unique_ptr<grpc::Server> mWncServer;
#endif

public:

  // Default Constructor - enabling port 50052 by default
  GrpcWncServer(int port = 50052) : mWncPort(port), mSSL(false) { }

  virtual ~GrpcWncServer()
  {
#ifdef EOS_GRPC
    mWncServer->Shutdown();
#endif
    mThread.join();
  }

  // Run gRPC server for EOS Windows native client
  void RunWnc(ThreadAssistant& assistant) noexcept;

  // Create thread for gRPC server for EOS Windows native client
  void StartWnc()
  {
    mThread.reset(&GrpcWncServer::RunWnc, this);
  }
};

EOSMGMNAMESPACE_END