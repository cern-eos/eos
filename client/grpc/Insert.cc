#include <string>
#include <iostream>
#include <fstream>
#include "client/grpc/GrpcClient.hh"
#include <stdio.h>
#include "common/StringConversion.hh"

int usage(const char* prog)
{
  fprintf(stderr, "usage: %s [--key <ssl-key-file> "
          "--cert <ssl-cert-file> "
          "--ca <ca-cert-file>] "
          "[--endpoint <host:port>] [--token <auth-token>] "
          "[--prefix prefix] "
          "[--treefile <treefile>] "
          "[--force-ssl] \n", prog);
  fprintf(stderr,
          "treefile format providing inodes: \n"
          "----------------------------------\n"
          "ino:000000000000ffff:/eos/mydir/\n"
          "ino:000000000000ff01:/eos/mydir/myfile\n\n");
  fprintf(stderr,
          "treefile format without inodes: \n"
          "----------------------------------\n"
          "/eos/mydir/\n"
          "/eos/mydir/myfile\n\n");
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
  std::string prefix = "/grpc";
  std::string treefile = "namespace.txt";
  bool force_ssl = false;

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

    if (option == "--prefix") {
      if (argc > i + 1) {
        prefix = argv[i + 1];
        ++i;
        continue;
      } else {
        return usage(argv[0]);
      }
    }

    if (option == "--treefile") {
      if (argc > i + 1) {
        treefile = argv[i + 1];
        ++i;
        continue;
      } else {
        return usage(argv[0]);
      }
    }

    if (option == "--force-ssl") {
      force_ssl = true;
      continue;
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
      cafile,
      force_ssl);

  if (!eosgrpc) {
    return usage(argv[0]);
  }

  std::cout << "=> settings: prefix=" << prefix << " treefile=" << treefile <<
            std::endl;
  std::ifstream input(treefile);
  size_t n = 0;
  size_t bulk = 1000;
  bool dirmode = true;
  std::vector<std::string> paths;
  std::chrono::steady_clock::time_point watch_global =
    std::chrono::steady_clock::now();

  for (std::string line ; std::getline(input, line);) {
    n++;

    if (line.substr(0, 4) == "ino:") {
      line.insert(21, prefix);
    } else {
      line.insert(0, prefix);
    }

    std::cout << n << " " << line << std::endl;

    if (line.back() == '/') {
      // dir
      if (dirmode) {
        paths.push_back(line);
      } else {
        // SEND OFF DIRS
        int retc = eosgrpc->FileInsert(paths);
        std::cout << "::send::files" << " retc=" << retc << std::endl;
        paths.clear();
        paths.push_back(line);
        dirmode = true;
      }
    } else {
      // file
      if (dirmode) {
        // SEND OFF FILES
        int retc = eosgrpc->ContainerInsert(paths);
        std::cout << "::send::dirs " << " retc=" << retc << std::endl;
        paths.clear();
        paths.push_back(line);
        dirmode = false;
      } else {
        paths.push_back(line);
      }
    }

    if (paths.size() >= bulk) {
      if (dirmode) {
        // SEND OF DIRS
        int retc = eosgrpc->ContainerInsert(paths);
        std::cout << "::send::dirs" << " retc=" << retc << std::endl;
        paths.clear();
      } else {
        // SEND OF FILES
        int retc = eosgrpc->FileInsert(paths);
        std::cout << "::send::files" << " retc=" << retc << std::endl;
        paths.clear();
      }
    }
  }

  std::chrono::microseconds elapsed_global =
    std::chrono::duration_cast<std::chrono::microseconds>
    (std::chrono::steady_clock::now() - watch_global);
  std::cout << n << " requests took " << elapsed_global.count() <<
            " micro seconds" << std::endl;
  return 0;
}
