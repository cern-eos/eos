//------------------------------------------------------------------------------
//! @file CommandDispatchTest.cc
//! @brief Exercise registered console commands through a local fake MGM.
//------------------------------------------------------------------------------

#include "common/SymKeys.hh"
#include "console/CommandFramework.hh"
#include "mgm/proc/ProcInterface.hh"
#include "proto/ConsoleRequest.pb.h"
#include "gtest/gtest.h"
#include <XrdOuc/XrdOucEnv.hh>
#include <atomic>
#include <future>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <zmq.hpp>

namespace {
// Both client_command and ICmdHelper use the real IPC transport here. This
// catches commands accidentally migrated from protobuf to an unsupported CGI
// command, without contacting an EOS instance or executing a server operation.
class CommandDispatchTest : public ::testing::Test {
protected:
  void
  SetUp() override
  {
    RegisterNativeConsoleCommands();
    savedOpts = gGlobalOpts;
    savedUri = serveruri;
    savedPwd = gPwd;
    savedRetc = global_retc;
    char directory[] = "/tmp/eos-command-test-XXXXXX";
    ASSERT_NE(mkdtemp(directory), nullptr);
    tempDir = directory;
    const std::string endpoint = "ipc://" + tempDir + "/mgm";
    std::promise<void> ready;
    auto bound = ready.get_future();
    responder = std::thread([this, endpoint, ready = std::move(ready)]() mutable {
      try {
        zmq::context_t zmqContext(1);
        zmq::socket_t socket(zmqContext, ZMQ_REP);
        socket.set(zmq::sockopt::rcvtimeo, 100);
        socket.set(zmq::sockopt::linger, 0);
        socket.bind(endpoint);
        ready.set_value();
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
        ready.set_exception(std::current_exception());
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
  int savedRetc = 0;
  std::string tempDir;
  std::atomic<bool> stopping{false};
  std::atomic<int> responseRetc{ENOENT};
  std::thread responder;
  std::mutex mutex;
  std::vector<std::string> requests;
};
} // namespace

TEST_F(CommandDispatchTest, EvictUsesUserProtobufAndPreservesAllFiles)
{
  run("evict", {"/eos/tape/a b&c=d", "relative", "fid:123", "fxid:ab"});
  EXPECT_EQ(global_retc, ENOENT);
  const auto proto = protobuf(false);
  ASSERT_TRUE(proto.has_evict());
  const auto& evict = proto.evict();
  ASSERT_EQ(evict.file_size(), 4);
  EXPECT_EQ(evict.file(0).path(), "/eos/tape/a b&c=d");
  EXPECT_EQ(evict.file(1).path(), "/eos/dispatch-test/relative");
  EXPECT_EQ(evict.file(2).fid(), 123u);
  EXPECT_EQ(evict.file(3).fid(), 0xabu);
  EXPECT_FALSE(evict.has_evictsinglereplica());
  EXPECT_FALSE(evict.ignoreevictcounter());
  EXPECT_FALSE(evict.ignoreremovalonfst());
}

TEST_F(CommandDispatchTest, EvictSingleReplicaOptions)
{
  run("evict",
      {"--ignore-evict-counter", "--ignore-removal-on-fst", "--fsid", "42", "fid:123"});
  const auto proto = protobuf(false);
  ASSERT_TRUE(proto.has_evict());
  EXPECT_EQ(proto.evict().evictsinglereplica().fsid(), 42u);
  EXPECT_TRUE(proto.evict().ignoreevictcounter());
  EXPECT_TRUE(proto.evict().ignoreremovalonfst());
  run("evict", {"--ignore-evict-counter", "fid:123"});
  const auto force = protobuf(false);
  EXPECT_TRUE(force.evict().ignoreevictcounter());
  EXPECT_FALSE(force.evict().has_evictsinglereplica());
}

TEST_F(CommandDispatchTest, EvictRejectsInvalidOptionsBeforeSending)
{
  for (const auto& args : std::vector<std::vector<std::string>>{
           {},
           {"--help"},
           {"--fsid"},
           {"--fsid", "1", "fid:1"},
           {"--ignore-removal-on-fst", "fid:1"},
           {"--unknown", "fid:1"},
           {"--ignore-evict-counter"},
           {"--ignore-evict-counter", "--fsid=", "fid:1"},
           {"--ignore-evict-counter", "--fsid", "1"}}) {
    SCOPED_TRACE(::testing::PrintToString(args));
    expectRejected("evict", args);
  }
  for (const char* fsid : {"no", "-1", "1x", "1.5", "18446744073709551616"}) {
    SCOPED_TRACE(fsid);
    expectRejected("evict", {"--ignore-evict-counter", "--fsid", fsid, "fid:1"});
  }
}

TEST_F(CommandDispatchTest, RegisterUsesAdminProtobufAndPreservesMetadata)
{
  run("register",
      {"-u", "/eos/register/a b&c=d", "uid=123", "gid=456", "size=12345678901",
       "mode=640", "layoutid=1048850", "location=1", "location=2", "checksum=abcd",
       "attr=sys.acl=u:100:rwx", "xattr=user.md=a b&c", "btime=1670334863.101232",
       "ctime=1670334864.110123", "mtime=1670334865.11234",
       "atimeifnewer=1670334866.101233"});
  EXPECT_EQ(global_retc, ENOENT);
  const auto proto = protobuf(true);
  ASSERT_TRUE(proto.has_record());
  const auto& reg = proto.record();
  EXPECT_EQ(reg.path(), "/eos/register/a b&c=d");
  EXPECT_TRUE(reg.update());
  EXPECT_EQ(reg.owner().uid(), 123u);
  EXPECT_EQ(reg.owner().gid(), 456u);
  EXPECT_EQ(reg.size(), 12345678901u);
  EXPECT_EQ(reg.mode(), 0640);
  EXPECT_EQ(reg.layoutid(), 1048850);
  ASSERT_EQ(reg.locations_size(), 2);
  EXPECT_EQ(reg.locations(0), 1u);
  EXPECT_EQ(reg.locations(1), 2u);
  EXPECT_EQ(reg.checksum(), "abcd");
  EXPECT_EQ(reg.attr().at("sys.acl"), "u:100:rwx");
  EXPECT_EQ(reg.attr().at("user.md"), "a b&c");
  EXPECT_EQ(reg.btime().sec(), 1670334863);
  EXPECT_EQ(reg.btime().nsec(), 101232);
  EXPECT_EQ(reg.ctime().sec(), 1670334864);
  EXPECT_EQ(reg.mtime().sec(), 1670334865);
  EXPECT_EQ(reg.atime().sec(), 1670334866);
  EXPECT_TRUE(reg.atimeifnewer());
}

TEST_F(CommandDispatchTest, RegisterNamedOwnersAndPathTag)
{
  for (const auto& owner : {"uid=testuser", "username=testuser"}) {
    run("register", {"path=/eos/register/test", owner, "groupname=testgroup"});
    const auto proto = protobuf(true);
    EXPECT_EQ(proto.record().path(), "/eos/register/test");
    EXPECT_EQ(proto.record().owner().username(), "testuser");
    EXPECT_EQ(proto.record().owner().groupname(), "testgroup");
    EXPECT_FALSE(proto.record().update());
  }
}

TEST_F(CommandDispatchTest, RegisterRejectsInvalidMetadataBeforeSending)
{
  for (const auto& args : std::vector<std::vector<std::string>>{
           {}, {"--help"}, {"-u"}, {"size=1"}, {"relative"}, {"path="}}) {
    expectRejected("register", args);
  }
  for (const char* tag :
       {"size=no", "size=-1", "size=18446744073709551616", "mode=888", "location=-1",
        "location=4294967296", "layoutid=2147483648", "attr=invalid", "xattr==value",
        "ctime=invalid", "ctime=12junk", "mtime=12.1000000000", "btime=-1", "atime=12.",
        "unknown=value", "--unknown"}) {
    SCOPED_TRACE(tag);
    expectRejected("register", {"/eos/register/test", tag});
  }
}

TEST_F(CommandDispatchTest, GlobalProtobufOptionsReachMgm)
{
  gGlobalOpts.mJsonFormat = true;
  gGlobalOpts.mComment = "dispatch regression";
  gGlobalOpts.mUserRole = "123";
  gGlobalOpts.mGroupRole = "456";
  for (const auto& name : {"evict", "register"}) {
    run(name, {"/eos/dispatch-test/file"});
    const auto wire = request();
    EXPECT_NE(wire.find("&eos.ruid=123"), std::string::npos);
    EXPECT_NE(wire.find("&eos.rgid=456"), std::string::npos);
    const auto proto = protobuf(std::string(name) == "register");
    EXPECT_EQ(proto.format(), eos::console::RequestProto::JSON);
    EXPECT_EQ(proto.comment(), "dispatch regression");
  }
}

TEST_F(CommandDispatchTest, PreservesServerReturnCodes)
{
  for (const auto* name : {"evict", "register"}) {
    for (const int retc : {0, ENOENT, EACCES, EBUSY}) {
      SCOPED_TRACE(name);
      SCOPED_TRACE(retc);
      responseRetc = retc;
      run(name, {"/eos/dispatch-test/file"});
      EXPECT_EQ(global_retc, retc);
      EXPECT_EQ(protobuf(std::string(name) == "register").command_case(),
                std::string(name) == "register" ? eos::console::RequestProto::kRecord
                                                : eos::console::RequestProto::kEvict);
    }
  }
}

TEST_F(CommandDispatchTest, OtherProtobufCommandFamiliesKeepTheirDispatch)
{
  using R = eos::console::RequestProto;
  struct Case {
    const char* name;
    std::vector<std::string> args;
    R::CommandCase command;
    bool admin;
  };
  const std::vector<Case> cases = {
      {"acl", {"-l", "/eos/test"}, R::kAcl, false},
      {"config", {"ls"}, R::kConfig, false},
      {"convert", {"list"}, R::kConvert, false},
      {"debug", {"get"}, R::kDebug, false},
      {"devices", {"ls"}, R::kDevices, true},
      {"df", {}, R::kDf, false},
      {"newfind", {"/eos/test"}, R::kFind, false},
      {"find", {"/eos/test"}, R::kFind, false},
      {"du", {"/eos/test"}, R::kFind, false},
      {"info", {"/eos/test"}, R::kFile, false},
      {"fileinfo", {"/eos/test"}, R::kFile, false},
      {"file", {"info", "/eos/test"}, R::kFile, false},
      {"ln", {"/eos/test/a", "/eos/test/b"}, R::kFile, false},
      {"mv", {"/eos/test/a", "/eos/test/b"}, R::kFile, false},
      {"touch", {"/eos/test"}, R::kFile, false},
      {"fs", {"ls"}, R::kFs, true},
      {"fsck", {"stat"}, R::kFsck, true},
      {"group", {"ls"}, R::kGroup, false},
      {"inspector", {"-m"}, R::kSpace, true},
      {"io", {"stat"}, R::kIo, false},
      {"monit", {"config", "ls"}, R::kMonit, true},
      {"node", {"ls"}, R::kNode, true},
      {"ns", {"stat"}, R::kNs, true},
      {"quota", {"ls"}, R::kQuota, false},
      {"recycle", {"ls"}, R::kRecycle, false},
      {"rm", {"/eos/test/file"}, R::kRm, false},
      {"route", {"ls"}, R::kRoute, false},
      {"sched", {"ls", "default", "tree"}, R::kSched, true},
      {"token", {"--token", "test-token"}, R::kToken, false},
      {"space", {"ls"}, R::kSpace, true},
      {"tracker", {"ls"}, R::kSpace, false}};
  for (const auto& item : cases) {
    SCOPED_TRACE(item.name);
    run(item.name, item.args);
    EXPECT_EQ(protobuf(item.admin).command_case(), item.command);
  }
}

TEST_F(CommandDispatchTest, LegacyCommandFamiliesKeepSupportedMgmRoutes)
{
  struct Case {
    const char* name;
    std::vector<std::string> args;
    const char* command;
    bool admin;
  };
  const std::vector<Case> cases = {
      {"access", {"ls"}, "access", true},
      {"accounting", {"report"}, "accounting", false},
      {"backup",
       {"root://source//eos/test", "root://destination//eos/test"},
       "backup",
       true},
      {"archive", {"list", "/eos/test"}, "archive", false},
      {"attr", {"ls", "/eos/test"}, "attr", false},
      {"cd", {"/eos/test"}, "cd", false},
      {"chmod", {"755", "/eos/test"}, "chmod", false},
      {"chown", {"123", "/eos/test"}, "chown", false},
      {"file", {"check", "/eos/test"}, "file", false},
      {"fusex", {"ls"}, "fusex", true},
      {"geosched", {"show", "state"}, "geosched", true},
      {"ls", {"/eos/test"}, "ls", false},
      {"map", {"ls"}, "map", false},
      {"member", {"testgroup"}, "member", false},
      {"mkdir", {"/eos/test"}, "mkdir", false},
      {"motd", {}, "motd", false},
      {"oldfind", {"/eos/test"}, "find", false},
      {"rmdir", {"/eos/test"}, "rmdir", false},
      {"rtlog", {"."}, "rtlog", true},
      {"version", {}, "version", false},
      {"vid", {"ls"}, "vid", true},
      {"who", {}, "who", false},
      {"whoami", {}, "whoami", false}};
  for (const auto& item : cases) {
    SCOPED_TRACE(item.name);
    run(item.name, item.args);
    const auto wire = request();
    EXPECT_EQ(wire.substr(0, wire.find('?')),
              item.admin ? "/proc/admin/" : "/proc/user/");
    XrdOucEnv env(wire.substr(wire.find('?') + 1).c_str());
    EXPECT_STREQ(env.Get("mgm.cmd"), item.command);
    EXPECT_EQ(env.Get("mgm.cmd.proto"), nullptr);
  }
}
