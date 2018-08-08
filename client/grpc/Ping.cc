#include <string>
#include <iostream>
#include "client/grpc/GrpcClient.hh"
#include <stdio.h>
#include "common/StringConversion.hh"

int usage(const char* prog)
{
  fprintf(stderr, "usage: %s [--key <ssl-key-file> "
          "--cert <ssl-cert-file> "
          "--ca <ca-cert-file>] "
          "[--endponit <host:port>] [--token <auth-token>]\n", prog);
  return -1;
}

int main(int argc, const char* argv[])
{
  std::string endpoint = "localhost:50051";
  std::string token = "";
  std::string key;
  std::string cert;
  std::string ca;
  std::string keyfile;
  std::string certfile;
  std::string cafile;

  for (auto i = 1; i < argc; ++i) {
    std::string option = argv[i];

    if (option == "--key") {
      if (argc > i + 1) {
        keyfile = argv[i + 1];
        ++i;
        continue;
      } else {
        return usage(argv[0]);
      }
    }

    if (option == "--cert") {
      if (argc > i + 1) {
        certfile = argv[i + 1];
        ++i;
        continue;
      } else {
        return usage(argv[0]);
      }
    }

    if (option == "--ca") {
      if (argc > i + 1) {
        cafile = argv[i + 1];
        ++i;
        continue;
      } else {
        return usage(argv[0]);
      }
    }

    if (option == "--endpoint") {
      if (argc > i + 1) {
        endpoint = argv[i + 1];
        ++i;
        continue;
      } else {
        return usage(argv[0]);
      }
    }

    if (option == "--token") {
      if (argc > i + 1) {
        token = argv[i + 1];
        ++i;
        continue;
      } else {
        return usage(argv[0]);
      }
    }

    return usage(argv[0]);
  }

  if (keyfile.length() || certfile.length() || cafile.length()) {
    if (!keyfile.length() || !certfile.length() || !cafile.length()) {
      return usage(argv[0]);
    }
  }

  std::unique_ptr<eos::client::GrpcClient> eosgrpc =
    eos::client::GrpcClient::Create(
      endpoint,
      token,
      keyfile,
      certfile,
      cafile);

  if (!eosgrpc) {
    return usage(argv[0]);
  }

  std::string message("ping");
  std::chrono::steady_clock::time_point watch_global =
    std::chrono::steady_clock::now();
  int n_requests = 1000;

  for (auto i = 0; i < n_requests; ++i) {
    std::chrono::steady_clock::time_point watch_local =
      std::chrono::steady_clock::now();
    std::string reply = eosgrpc->Ping(message);

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
