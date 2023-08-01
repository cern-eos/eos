//+build ignore

#include <memory>

#include <gflags/gflags.h>
#include <grpc++/grpc++.h>

#include "proto/go/echo_service.grpc.pb.h"
#include "gateway.h"

DEFINE_string(addr, "0.0.0.0:19000", "addr to listen on");
DEFINE_string(gwaddr, "0.0.0.0:18080", "gateway addr to listen on");

namespace eos {
namespace echo {
namespace service {
using grpc::Status;
using grpc::ServerContext;

class EchoServiceImpl final : public EchoService::Service {
  Status Echo(ServerContext* context, const SimpleMessage* request,
               SimpleMessage* reply) override;
  Status EchoBody(ServerContext* context, const SimpleMessage* request,
                  SimpleMessage* reply) override;
};

Status EchoServiceImpl::Echo(ServerContext* context,
                             const SimpleMessage* request,
                             SimpleMessage* reply) {
  std::cerr << "Got an echo request!" << std::endl;
  reply->CopyFrom(*request);
  return Status::OK;
}

Status EchoServiceImpl::EchoBody(ServerContext* context,
                                 const SimpleMessage* request,
                                 SimpleMessage* reply) {
  reply->CopyFrom(*request);
  return Status::OK;
}
}  // namespace service
}  // namespace echo
}  // namespace eos

int RunService() {
  eos::echo::service::EchoServiceImpl service;
  grpc::ServerBuilder builder;

  builder.AddListeningPort(FLAGS_addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  const auto server(builder.BuildAndStart());
  std::cerr << "Listening on " << FLAGS_addr << std::endl;

  char* const addr = const_cast<char*>(FLAGS_addr.c_str());
  char* const gwaddr = const_cast<char*>(FLAGS_gwaddr.c_str());
  char* path = (char *)"../../../../protos/examplepb";
  const auto gatewayServer = SpawnGrpcGateway(gwaddr, "tcp", addr, path);
  std::cerr << "Done spawning GrpcGateway" << std::endl;
  server->Wait();
  return !WaitForGrpcGateway(gatewayServer);
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RunService();
}
