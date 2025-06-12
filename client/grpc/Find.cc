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
          "[--endpoint <host:port>] [--token <auth-token>] [--export <exportfs>] [--depth <depth>] [--select <filter-string>] [-f | -d] <path>\n",
          prog);
  fprintf(stderr,
          " <filter-string> is setup as \"key1:val1,key2:val2,key3:val3 ... where keyN:valN is one of \n");
  fprintf(stderr, ""
          "                    owner-root:1|0\n"
          "                    group-root:1|0\n"
          "                    owner:<uid>\n"
          "                    group:<gid>\n"
          "                    regex-filename:<regex>\n"
          "                    regex-dirname:<regex>\n"
          "                    zero-size:1|0\n"
          "                    min-size:<min>\n"
          "                    max-size:<max>\n"
          "                    min-children:<min>\n"
          "                    max-children:<max>\n"
          "                    zero-children:1|0\n"
          "                    min-locations:<min>\n"
          "                    max-locations:<max>\n"
          "                    zero-locations:1|0\n"
          "                    min-unlinked_locations:<min>\n"
          "                    max-unlinked_locations:<max\n"
          "                    zero-unlinked_locations:1|0\n"
          "                    min-treesize:<min>\n"
          "                    max-treesize:<max>\n"
          "                    zero-treesize:1|0\n"
          "                    min-ctime:<unixtst>\n"
          "                    max-ctime:<unixtst>\n"
          "                    zero-ctime:1|0\n"
          "                    min-mtime:<unixtst>\n"
          "                    max-mtime:<unixtst>\n"
          "                    zero-mtime:1|0\n"
          "                    min-stime:<unixtst>\n"
          "                    max-stime:<unixtst>\n"
          "                    zero-stime:1|0\n"
          "                    layoutid:<layoudid>\n"
          "                    flags:<flags>\n"
          "                    symlink:1|0\n"
          "                    checksum-type:<cksname>\n"
          "                    checksum-value:<cksvalue>\n"
          "                    xattr:<key>=<val>\n");
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
  std::string path = "";
  std::list<std::string> select;
  bool files = false;
  bool dirs  = false;
  uint64_t depth = 1024;
  std::string exportfs = "";

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

    if (option == "--export") {
      if (argc > i + 1) {
        exportfs = argv[i + 1];
        ++i;
        continue;
      } else {
        return usage(argv[0]);
      }
    }

    if (option == "--depth") {
      if (argc > i + 1) {
        depth = strtoull(argv[i + 1], 0, 10);
        ++i;
        continue;
      } else {
        return usage(argv[0]);
      }
    }

    if (option == "--select") {
      if (argc > i + 1) {
        select.push_back(argv[i + 1]);
        ++i;
        continue;
      } else {
        return usage(argv[0]);
      }
    }

    if (option == "-f") {
      files = true;
      continue;
    }

    if (option == "-d") {
      dirs = true;
      continue;
    }

    path = option;

    if (argc > (i + 1)) {
      return usage(argv[0]);
    }
  }

  if (!files && ! dirs) {
    files = true;
    dirs = true;
  }

  if (keyfile.length() || certfile.length() || cafile.length()) {
    if (!keyfile.length() || !certfile.length() || !cafile.length()) {
      return usage(argv[0]);
    }
  }

  if (path.empty()) {
    return usage(argv[0]);
  }

  if (path.front() != '/') {
    return usage(argv[0]);
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

  std::chrono::steady_clock::time_point watch_global =
    std::chrono::steady_clock::now();
  std::string reply = eosgrpc->Find(path, select, 0, 0, files, dirs, depth, true,
                                    exportfs);
  std::chrono::microseconds elapsed_global =
    std::chrono::duration_cast<std::chrono::microseconds>
    (std::chrono::steady_clock::now() - watch_global);
  std::cout << "request took " << elapsed_global.count() <<
            " micro seconds" << std::endl;
  return 0;
}
