//------------------------------------------------------------------------------
//! @file ConsoleTestFixture.hh
//! @brief Run console commands against an isolated local fake MGM.
//------------------------------------------------------------------------------

#pragma once

#include "common/SymKeys.hh"
#include "console/CommandFramework.hh"
#include "mgm/proc/ProcInterface.hh"
#include "proto/ConsoleRequest.pb.h"
#include "gtest/gtest.h"
#include <XrdOuc/XrdOucEnv.hh>
#include <atomic>
#include <exception>
#include <future>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <zmq.hpp>

extern XrdOucString app;

namespace {
// Both client_command and ICmdHelper use the real IPC transport here. This
// catches commands accidentally migrated from protobuf to an unsupported CGI
// command, without contacting an EOS instance or executing a server operation.
class ConsoleTestFixture : public ::testing::Test {
protected:
  void
  SetUp() override
  {
    RegisterNativeConsoleCommands();
    savedOpts = gGlobalOpts;
    savedUri = serveruri;
    savedPwd = gPwd;
    savedRetc = global_retc;
    savedComment = global_comment;
    savedApp = app;
    savedUserRole = user_role;
    savedGroupRole = group_role;
    savedJson = json;
    savedSilent = silent;
    global_comment = "";
    app = "";
    user_role = "";
    group_role = "";
    json = false;
    silent = false;
    char directory[] = "/tmp/eos-command-test-XXXXXX";
    ASSERT_NE(mkdtemp(directory), nullptr);
    tempDir = directory;
    const std::string endpoint = "ipc://" + tempDir + "/mgm";
    std::promise<void> ready;
    auto bound = ready.get_future();
    responder = std::thread([this, endpoint, ready = std::move(ready)]() mutable {
      bool ready_fulfilled = false;
      try {
        zmq::context_t zmqContext(1);
        zmq::socket_t socket(zmqContext, ZMQ_REP);
        socket.set(zmq::sockopt::rcvtimeo, 100);
        socket.set(zmq::sockopt::linger, 0);
        socket.bind(endpoint);
        ready.set_value();
        ready_fulfilled = true;
        while (!stopping) {
          zmq::message_t message;
          if (!socket.recv(message, zmq::recv_flags::none)) {
            continue;
          }
          {
            std::lock_guard<std::mutex> lock(mutex);
            requests.emplace_back(static_cast<const char*>(message.data()),
                                  message.size());
          }
          const std::string reply = "mgm.proc.stdout=&mgm.proc.stderr=&mgm.proc.retc=" +
                                    std::to_string(responseRetc.load());
          socket.send(zmq::buffer(reply), zmq::send_flags::none);
        }
      } catch (...) {
        if (ready_fulfilled) {
          responderError = std::current_exception();
        } else {
          ready.set_exception(std::current_exception());
        }
      }
    });
    ASSERT_NO_THROW(bound.get());
    gGlobalOpts = GlobalOptions{};
    gGlobalOpts.mMgmUri = endpoint;
    serveruri = endpoint.c_str();
    gPwd = "/eos/dispatch-test/";
    context.globalOpts = &gGlobalOpts;
    context.clientCommand = &client_command;
    context.outputResult = &output_result;
  }

  void
  TearDown() override
  {
    stopping = true;
    if (responder.joinable()) {
      responder.join();
    }
    if (!tempDir.empty()) {
      unlink((tempDir + "/mgm").c_str());
      rmdir(tempDir.c_str());
    }
    gGlobalOpts = savedOpts;
    serveruri = savedUri;
    gPwd = savedPwd;
    global_retc = savedRetc;
    global_comment = savedComment;
    app = savedApp;
    user_role = savedUserRole;
    group_role = savedGroupRole;
    json = savedJson;
    silent = savedSilent;
    if (responderError) {
      EXPECT_NO_THROW(std::rethrow_exception(responderError));
    }
  }

  void
  run(const std::string& name, const std::vector<std::string>& args)
  {
    {
      std::lock_guard<std::mutex> lock(mutex);
      requests.clear();
    }
    auto* command = CommandRegistry::instance().find(name);
    ASSERT_NE(command, nullptr);
    global_retc = 0;
    command->run(args, context);
  }

  std::string
  request()
  {
    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_EQ(requests.size(), 1u);
    return requests.empty() ? "" : requests.front();
  }

  eos::console::RequestProto
  protobuf(bool admin)
  {
    const std::string wire = request();
    EXPECT_EQ(wire.substr(0, wire.find('?')), admin ? "/proc/admin/" : "/proc/user/");
    XrdOucEnv env(wire.substr(wire.find('?') + 1).c_str());
    const char* encoded = env.Get("mgm.cmd.proto");
    EXPECT_NE(encoded, nullptr) << wire;
    eos::console::RequestProto proto;
    if (encoded) {
      std::string bytes;
      EXPECT_TRUE(eos::common::SymKey::Base64Decode(encoded, bytes));
      EXPECT_TRUE(proto.ParseFromString(bytes));
      auto root = eos::common::VirtualIdentity::Root();
      EXPECT_NE(eos::mgm::ProcInterface::GetProcCommand(
                    "console-dispatch-test", root, admin ? "/proc/admin/" : "/proc/user/",
                    wire.substr(wire.find('?') + 1).c_str()),
                nullptr);
    }
    return proto;
  }

  void
  expectRejected(const std::string& name, const std::vector<std::string>& args)
  {
    run(name, args);
    EXPECT_EQ(global_retc, EINVAL);
    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_TRUE(requests.empty());
  }

  CommandContext context{};
  GlobalOptions savedOpts;
  XrdOucString savedUri, savedPwd;
  XrdOucString savedComment, savedApp, savedUserRole, savedGroupRole;
  bool savedJson = false, savedSilent = false;
  int savedRetc = 0;
  std::string tempDir;
  std::atomic<bool> stopping{false};
  std::atomic<int> responseRetc{ENOENT};
  std::thread responder;
  std::exception_ptr responderError;
  std::mutex mutex;
  std::vector<std::string> requests;
};
} // namespace
