#pragma once

#include "mgm/Namespace.hh"
#include "common/AssistedThread.hh"
#include "common/Mapping.hh"
#include "common/Logging.hh"
#include "GrpcEchoInterface.hh"

#ifdef EOS_GRPC
#include <grpc++/grpc++.h>
#endif

EOSMGMNAMESPACE_BEGIN

/**
 * @file   GrpcEchoServer.hh
 *
 * @brief  This class implements a simple GRPC server
 */
class GrpcEchoServer: public eos::common::LogId
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

#ifdef EOS_GRPC
  std::unique_ptr<grpc::Server> mEchoServer;
#endif

public:

  /* Default Constructor - enabling port 50054 by default
   */
  GrpcEchoServer(int port = 50054) : mPort(port), mSSL(false) {  }

  ~GrpcEchoServer()
  {
#ifdef EOS_GRPC

    if (mEchoServer) {
      mEchoServer->Shutdown();
    }

#endif
    mThread.join();
  }

  /* Run function */
  void Run(ThreadAssistant& assistant) noexcept;

  /* Startup function */
  void Start()
  {
    mThread.reset(&GrpcEchoServer::Run, this);
  }

#ifdef EOS_GRPC

  /* return client DN*/
  static std::string DN(grpc::ServerContext* context);
  /* return client IP*/
  static std::string IP(grpc::ServerContext* context, std::string* id = 0,
                        std::string* port = 0);
  /* return VID for a given call */
  static void Vid(grpc::ServerContext* context,
                  eos::common::VirtualIdentity& vid,
                  const std::string& authkey);

#endif
};

EOSMGMNAMESPACE_END
