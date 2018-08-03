#include <string>
#include <iostream>

#include "client/grpc/GrpcClient.hh"

int main(int argc, const char* argv[])
{
  std::string endpoint = "localhost:50051";

  if (argc == 2) {
    endpoint = argv[1];
  }

  eos::client::GrpcClient eosgrpc(grpc::CreateChannel(
                                    endpoint,
                                    grpc::InsecureChannelCredentials()));
  std::string message("ping");
  std::chrono::steady_clock::time_point watch_global =
    std::chrono::steady_clock::now();
  int n_requests = 1000;

  for (auto i = 0; i < n_requests; ++i) {
    std::chrono::steady_clock::time_point watch_local =
      std::chrono::steady_clock::now();
    std::string reply = eosgrpc.Ping(message);

    if (reply != "ping") {
      std::cout << "request: failed/timeout" << std::endl;
    } else {
      std::chrono::microseconds elapsed_local =
        std::chrono::duration_cast<std::chrono::microseconds>
        (std::chrono::steady_clock::now() - watch_local);
      std::cout << "request: " << message << " reply: " << reply << " timing: " <<
                elapsed_local.count() << " micro seconds" << std::endl;
    }
  }

  std::chrono::microseconds elapsed_global =
    std::chrono::duration_cast<std::chrono::microseconds>
    (std::chrono::steady_clock::now() - watch_global);
  std::cout << n_requests << " requests took " << elapsed_global.count() <<
            " micro seconds" << std::endl;
  return 0;
}
