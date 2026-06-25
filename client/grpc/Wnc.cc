#include "proto/EosWnc.grpc.pb.h"
#include <cerrno>
#include <grpc++/grpc++.h>
#include <iostream>
#include <memory>
#include <string>

namespace {

int
usage(const char* prog)
{
  std::cerr << "usage: " << prog << " [--endpoint <host:port>] [--token <auth-token>]"
            << " [--uid <uid>] [--gid <gid>] [-p <path>]"
            << " version | quota get | quota set --inodes <n> --volume <n>" << std::endl;
  return EINVAL;
}

} // namespace

int
main(int argc, const char* argv[])
{
  std::string endpoint = "localhost:50052";
  std::string token;
  std::string cmd;
  std::string subcmd;
  std::string path;
  std::string uid;
  std::string gid;
  std::string inodes = "0";
  std::string volume = "0";

  for (int i = 1; i < argc; ++i) {
    std::string option = argv[i];

    if (option == "--endpoint") {
      if (argc <= i + 1) {
        return usage(argv[0]);
      }

      endpoint = argv[++i];
    } else if (option == "--token") {
      if (argc <= i + 1) {
        return usage(argv[0]);
      }

      token = argv[++i];
    } else if (option == "--uid") {
      if (argc <= i + 1) {
        return usage(argv[0]);
      }

      uid = argv[++i];
    } else if (option == "--gid") {
      if (argc <= i + 1) {
        return usage(argv[0]);
      }

      gid = argv[++i];
    } else if (option == "-p") {
      if (argc <= i + 1) {
        return usage(argv[0]);
      }

      path = argv[++i];
    } else if (option == "--inodes") {
      if (argc <= i + 1) {
        return usage(argv[0]);
      }

      inodes = argv[++i];
    } else if (option == "--volume") {
      if (argc <= i + 1) {
        return usage(argv[0]);
      }

      volume = argv[++i];
    } else if (cmd.empty()) {
      cmd = option;
    } else if (subcmd.empty()) {
      subcmd = option;
    } else {
      return usage(argv[0]);
    }
  }

  eos::console::RequestProto request;
  request.mutable_auth()->set_authkey(token);

  if (cmd == "version") {
    request.mutable_version();
  } else if (cmd == "quota") {
    if (subcmd == "get") {
      auto* ls = request.mutable_quota()->mutable_ls();
      ls->set_uid(uid);
      ls->set_gid(gid);
      ls->set_space(path);
    } else if (subcmd == "set") {
      auto* set = request.mutable_quota()->mutable_set();
      set->set_uid(uid);
      set->set_gid(gid);
      set->set_space(path);
      set->set_maxinodes(inodes);
      set->set_maxbytes(volume);
    } else {
      return usage(argv[0]);
    }
  } else {
    return usage(argv[0]);
  }

  auto channel = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
  std::unique_ptr<eos::console::EosWnc::Stub> stub =
      eos::console::EosWnc::NewStub(channel);
  eos::console::ReplyProto reply;
  grpc::ClientContext context;
  grpc::Status status = stub->ProcessSingle(&context, request, &reply);

  if (!status.ok()) {
    std::cerr << "grpc request failed: " << status.error_message() << std::endl;
    return EIO;
  }

  std::cout << "retc: " << reply.retc() << std::endl;

  if (!reply.std_err().empty()) {
    std::cout << "std_err: " << reply.std_err() << std::endl;
  }

  if (!reply.std_out().empty()) {
    std::cout << "std_out: " << reply.std_out() << std::endl;
  }

  return reply.retc();
}
