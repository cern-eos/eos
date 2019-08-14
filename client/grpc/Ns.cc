#include <string>
#include <iostream>
#include "client/grpc/GrpcClient.hh"
#include <stdio.h>
#include "common/StringConversion.hh"
#include <google/protobuf/util/json_util.h>

int usage(const char* prog)
{
  fprintf(stderr, "usage: %s [--key <ssl-key-file> "
          "--cert <ssl-cert-file> "
          "--ca <ca-cert-file>] "
          "[--endpoint <host:port>] [--token <auth-token>] [--uid] [--gid] [--norecycle] [-r] [--target <target>] -p <path> <command>\n", prog);
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
  std::string cmd = "";
  std::string path = "";
  std::string target = "";

  uid_t uid = 0;
  gid_t gid = 0;
  bool recursive = false;
  bool norecycle = false;

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

    if (option == "--uid") {
      if (argc > i + 1) {
	uid = strtoul(argv[i + 1],0,10);
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "--gid") {
      if (argc > i + 1) {
	gid = strtoul(argv[i + 1],0,10);
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "-p") {
      if (argc > i + 1) {
	path = argv[i+1];
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "--target") {
      if (argc > i + 1) {
	target = argv[i+1];
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "-r") {
      recursive = true;
      continue;
    }

    if (option == "--norecycle") {
      norecycle = true;
      continue;
    }

    cmd = option;

    if (argc > (i + 1)) {
      return usage(argv[0]);
    }
  }

  if (keyfile.length() || certfile.length() || cafile.length()) {
    if (!keyfile.length() || !certfile.length() || !cafile.length()) {
      return usage(argv[0]);
    }
  }

  if (cmd.empty() || path.empty()) {
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

  eos::rpc::NSRequest request;
  eos::rpc::NSResponse reply;
  request.set_authkey(token);
  if (uid) {
    request.mutable_role()->set_uid(uid);
  }
  if (gid) {
    request.mutable_role()->set_gid(gid);
  }

  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string jsonstring;
  
  if (cmd == "mkdir") {
    request.mutable_mkdir()->mutable_id()->set_path(path);

    if (recursive) {
      request.mutable_mkdir()->set_recursive(true);
    }
    request.mutable_mkdir()->set_mode(755);
  } else if (cmd == "rmdir") {
    request.mutable_rmdir()->mutable_id()->set_path(path);
  } else if (cmd == "touch") {
    request.mutable_touch()->mutable_id()->set_path(path);
  } else if (cmd == "unlink") {
    request.mutable_unlink()->mutable_id()->set_path(path);
    if (norecycle) {
      request.mutable_unlink()->set_norecycle(norecycle);
    }
  } else if (cmd == "rm") {
    request.mutable_rm()->mutable_id()->set_path(path);
    if (norecycle) {
      request.mutable_rm()->set_norecycle(norecycle);
    }

    if (recursive) {
      request.mutable_rm()->set_recursive(recursive);
    }
  } else if (cmd == "rename") {
    request.mutable_rename()->mutable_id()->set_path(path);
    request.mutable_rename()->set_target(target);
  } else if (cmd == "symlink") {
    request.mutable_symlink()->mutable_id()->set_path(path);
    request.mutable_symlink()->set_target(target);
  } else if (cmd == "setxattr") {
    request.mutable_xattr()->mutable_id()->set_path(path);
    (*(request.mutable_xattr()->mutable_xattrs()))["user.test1"] = "sys1";
    (*(request.mutable_xattr()->mutable_xattrs()))["user.test2"] = "user2";
  } else if (cmd == "chown") {
    // run as root
    request.mutable_chown()->mutable_id()->set_path(path);
  } else if (cmd == "chmod") {
    request.mutable_chmod()->mutable_id()->set_path(path);
    request.mutable_chmod()->set_mode(0777);
  }

  google::protobuf::util::MessageToJsonString(request,
					      &jsonstring, options);
  
  std::cout << "request: " << std::endl << jsonstring << std::endl;
  
  if (eosgrpc->Exec(request, reply)) {
    std::cerr << "grpc request failed" << std::endl;
  }
  
  jsonstring = "";
  google::protobuf::util::MessageToJsonString(reply,
					      &jsonstring, options);
  
  std::cout << "reply: " << std::endl << jsonstring << std::endl;
  
  std::chrono::microseconds elapsed_global =
    std::chrono::duration_cast<std::chrono::microseconds>
    (std::chrono::steady_clock::now() - watch_global);
  std::cout << "request took " << elapsed_global.count() <<
            " micro seconds" << std::endl;
  return 0;
}
