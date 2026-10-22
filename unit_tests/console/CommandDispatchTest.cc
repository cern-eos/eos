//------------------------------------------------------------------------------
//! @file CommandDispatchTest.cc
//! @brief Exercise registered console commands through a local fake MGM.
//------------------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleColor.hh"
#include "mgm/proc/ProcInterface.hh"
#include "proto/ConsoleRequest.pb.h"
#include "unit_tests/console/ConsoleTestFixture.hh"
#include "gtest/gtest.h"
#include <XrdOuc/XrdOucEnv.hh>
#include <atomic>
#include <future>
#include <google/protobuf/text_format.h>
#include <google/protobuf/util/message_differencer.h>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <zmq.hpp>

using CommandDispatchTest = ConsoleTestFixture;

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
           {}, {"-u"}, {"size=1"}, {"relative"}, {"path="}}) {
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

namespace {
struct RequestCase {
  const char* id;
  const char* command;
  std::vector<std::string> args;
  const char* expected;
  bool admin = false;
};

class ConsoleRequestTest : public CommandDispatchTest,
                           public ::testing::WithParamInterface<RequestCase> {};

// Text protobufs describe the public wire contract independently of the parser.
// Compare the entire request so dropped flags and unexpected fields both fail.
TEST_P(ConsoleRequestTest, SerializesExpectedRequestAndPropagatesErrors)
{
  const auto& item = GetParam();
  eos::console::RequestProto expected;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(item.expected, &expected));
  for (const int retc : {0, EACCES}) {
    SCOPED_TRACE(retc);
    responseRetc = retc;
    run(item.command, item.args);
    EXPECT_EQ(global_retc, retc);
    auto actual = protobuf(item.admin);
    EXPECT_EQ(actual.dontcolor(), !eos::console::ServerColorEnabled());
    actual.clear_dontcolor();
    EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equivalent(expected, actual))
        << "Expected:\n"
        << expected.DebugString() << "Actual:\n"
        << actual.DebugString();
  }
}

TEST_P(ConsoleRequestTest, PreservesGlobalOptions)
{
  const auto& item = GetParam();
  gGlobalOpts.mJsonFormat = true;
  gGlobalOpts.mComment = "audit comment with spaces & equals= and \"quotes\"";
  gGlobalOpts.mUserRole = "123";
  gGlobalOpts.mGroupRole = "456";
  gGlobalOpts.mForceSss = true;
  run(item.command, item.args);
  EXPECT_EQ(global_retc, ENOENT);
  const auto wire = request();
  XrdOucEnv env(wire.substr(wire.find('?') + 1).c_str());
  EXPECT_STREQ(env.Get("eos.ruid"), "123");
  EXPECT_STREQ(env.Get("eos.rgid"), "456");
  EXPECT_STREQ(env.Get("xrd.wantprot"), "sss");
  eos::console::RequestProto expected;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(item.expected, &expected));
  expected.set_format(eos::console::RequestProto::JSON);
  expected.set_comment(gGlobalOpts.mComment);
  auto actual = protobuf(item.admin);
  actual.clear_dontcolor();
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equivalent(expected, actual))
      << "Expected:\n"
      << expected.DebugString() << "Actual:\n"
      << actual.DebugString();
}

INSTANTIATE_TEST_SUITE_P(
    Subcommands, ConsoleRequestTest,
    ::testing::ValuesIn(std::vector<RequestCase>{
        {"ConfigList", "config", {"ls"}, "config { ls {} }"},
        {"ConfigBackupShort",
         "config",
         {"ls", "-b"},
         "config { ls { showbackup: true } }"},
        {"ConfigBackupLong",
         "config",
         {"ls", "--backup"},
         "config { ls { showbackup: true } }"},
        {"ConfigDumpCurrent", "config", {"dump"}, "config { dump {} }"},
        {"ConfigDumpNamed",
         "config",
         {"dump", "test-config"},
         R"(config { dump { file: "test-config" } })"},
        {"ConfigReset", "config", {"reset"}, "config { reset: true }"},
        {"ConfigExport",
         "config",
         {"export", "test-config"},
         R"(config { exp { file: "test-config" } })"},
        {"ConfigExportForce",
         "config",
         {"export", "test-config", "-f"},
         R"(config { exp { file: "test-config" force: true } })"},
        {"ConfigSave",
         "config",
         {"save", "test-config"},
         R"(config { save { file: "test-config" } })"},
        {"ConfigSaveForce",
         "config",
         {"save", "test-config", "-f"},
         R"(config { save { file: "test-config" force: true } })"},
        {"ConfigLoad",
         "config",
         {"load", "test-config"},
         R"(config { load { file: "test-config" } })"},
        {"ConfigChangelogDefault",
         "config",
         {"changelog"},
         "config { changelog { lines: 10 } }"},
        {"ConfigChangelogCount",
         "config",
         {"changelog", "25"},
         "config { changelog { lines: 25 } }"},
        {"GroupList", "group", {"ls"}, "group { ls {} }"},
        {"GroupDepth",
         "group",
         {"ls", "-g", "3", "default.*"},
         R"(group { ls { outdepth: 3 selection: "default.*" } })"},
        {"GroupBriefShort", "group", {"ls", "-b"}, "group { ls { outhost: true } }"},
        {"GroupBriefLong", "group", {"ls", "--brief"}, "group { ls { outhost: true } }"},
        {"GroupMonitor", "group", {"ls", "-m"}, "group { ls { outformat: MONITORING } }"},
        {"GroupListing", "group", {"ls", "-l"}, "group { ls { outformat: LISTING } }"},
        {"GroupIo", "group", {"ls", "--io"}, "group { ls { outformat: IOGROUP } }"},
        {"GroupIoFs", "group", {"ls", "--IO"}, "group { ls { outformat: IOFS } }"},
        {"GroupRemove",
         "group",
         {"rm", "default.1"},
         R"(group { rm { group: "default.1" } })"},
        {"GroupOn",
         "group",
         {"set", "default.1", "on"},
         R"(group { set { group: "default.1" group_state: "on" } })"},
        {"GroupOff",
         "group",
         {"set", "default.1", "off"},
         R"(group { set { group: "default.1" group_state: "off" } })"},
        {"GroupDrain",
         "group",
         {"set", "default.1", "drain"},
         R"(group { set { group: "default.1" group_state: "drain" } })"},
        {"DfDefault", "df", {}, "df { si: true readable: true }"},
        {"DfPath",
         "df",
         {"/eos/test"},
         R"(df { si: true readable: true path: "/eos/test" })"},
        {"DfMonitor",
         "df",
         {"-m", "/eos/test"},
         R"(df { monitoring: true si: true path: "/eos/test" })"},
        {"DfHuman",
         "df",
         {"-H", "/eos/test"},
         R"(df { readable: true path: "/eos/test" })"},
        {"DfBytes", "df", {"-b", "/eos/test"}, R"(df { path: "/eos/test" })"},
        {"RouteListAll", "route", {"ls"}, "route { list {} }"},
        {"RouteListPath",
         "route",
         {"ls", "/eos/test"},
         R"(route { list { path: "/eos/test/" } })"},
        {"RouteUnlink",
         "route",
         {"unlink", "/eos/test/"},
         R"(route { unlink { path: "/eos/test/" } })"},
        {"RouteDefaultPorts",
         "route",
         {"link", "/eos/test", "mgm.example.org"},
         R"(route { link { path: "/eos/test/" endpoints { fqdn: "mgm.example.org" xrd_port: 1094 http_port: 8000 } } })"},
        {"RouteXrdPort",
         "route",
         {"link", "/eos/test", "mgm.example.org:1095"},
         R"(route { link { path: "/eos/test/" endpoints { fqdn: "mgm.example.org" xrd_port: 1095 http_port: 8000 } } })"},
        {"RouteMultipleEndpoints",
         "route",
         {"link", "/eos/test", "mgm.example.org:1095:8001,127.0.0.1"},
         R"(route { link { path: "/eos/test/" endpoints { fqdn: "mgm.example.org" xrd_port: 1095 http_port: 8001 } endpoints { fqdn: "127.0.0.1" xrd_port: 1094 http_port: 8000 } } })"},
        {"QuotaPersonalPath",
         "quota",
         {"/eos/test/"},
         R"(quota { lsuser { space: "/eos/test/" } })"},
        {"QuotaPersonalExisting",
         "quota",
         {"-x", "/eos/test/"},
         R"(quota { lsuser { space: "/eos/test/" exists: true } })"},
        {"QuotaPersonalNode",
         "quota",
         {"-q", "/eos/test/"},
         R"(quota { lsuser { space: "/eos/test/" quotanode: true } })"},
        {"QuotaList",
         "quota",
         {"ls", "-u", "123", "-g", "456", "-m", "-n", "-p", "/eos/test/"},
         R"(quota { ls { uid: "123" gid: "456" format: true printid: true space: "/eos/test/" } })"},
        {"QuotaSetUser",
         "quota",
         {"set", "-u", "123", "-v", "1G", "-i", "100", "-p", "/eos/test/"},
         R"(quota { set { uid: "123" maxbytes: "1G" maxinodes: "100" space: "/eos/test/" } })"},
        {"QuotaSetGroupLong",
         "quota",
         {"set", "--gid", "456", "--volume", "2G", "--path", "/eos/test/"},
         R"(quota { set { gid: "456" maxbytes: "2G" space: "/eos/test/" } })"},
        {"QuotaRemoveVolume",
         "quota",
         {"rm", "-u", "123", "-v", "/eos/test/"},
         R"(quota { rm { uid: "123" type: VOLUME space: "/eos/test/" } })"},
        {"QuotaRemoveInodes",
         "quota",
         {"rm", "-g", "456", "-i", "/eos/test/"},
         R"(quota { rm { gid: "456" type: INODE space: "/eos/test/" } })"},
        {"QuotaRemoveNode",
         "quota",
         {"rmnode", "-p", "/eos/test/"},
         R"(quota { rmnode { space: "/eos/test/" } })"},
        {"RecycleStatus", "recycle", {}, "recycle { ls { Type: ALL } }"},
        {"RecycleMonitorStatus",
         "recycle",
         {"-m"},
         "recycle { ls { MonitorFmt: true } }"},
        {"RecycleList",
         "recycle",
         {"ls", "-m", "-n", "--all"},
         "recycle { ls { FullDetails: true NumericIds: true MonitorFmt: true Type: ALL } "
         "}"},
        {"RecycleDateLimit",
         "recycle",
         {"ls", "2026/10/15", "100"},
         R"(recycle { ls { FullDetails: true Date: "2026/10/15" Maxentries: 100 } })"},
        {"RecycleListId",
         "recycle",
         {"ls", "--rid", "123"},
         R"(recycle { ls { FullDetails: true Type: RID RecycleId: "123" } })"},
        {"RecycleListProject",
         "recycle",
         {"ls", "--project", "/eos/test"},
         R"(recycle { ls { FullDetails: true Path: "/eos/test" } })"},
        {"RecyclePurgeDate",
         "recycle",
         {"purge", "--all", "2026/10/15"},
         R"(recycle { purge { Type: ALL Date: "2026/10/15" } })"},
        {"RecyclePurgeKey",
         "recycle",
         {"purge", "-k", "recycle-key"},
         R"(recycle { purge { Key: "recycle-key" } })"},
        {"RecyclePurgeId",
         "recycle",
         {"purge", "--rid", "123", "2026"},
         R"(recycle { purge { Type: RID RecycleId: "123" Date: "2026" } })"},
        {"RecycleRestore",
         "recycle",
         {"restore", "-p", "-f", "-r", "recycle-key"},
         R"(recycle { restore { MakePath: true ForceOrigName: true RestoreVersions: true Key: "recycle-key" } })"},
        {"RecycleRestoreLong",
         "recycle",
         {"restore", "--force-original-name", "--restore-versions", "recycle-key"},
         R"(recycle { restore { ForceOrigName: true RestoreVersions: true Key: "recycle-key" } })"},
        {"RecycleProject",
         "recycle",
         {"project", "--path", "/eos/test", "--acl", "u:123=rx"},
         R"(recycle { project { path: "/eos/test" acl: "u:123=rx" } })"},
        {"RecycleAddBin",
         "recycle",
         {"config", "--add-bin", "/eos/test"},
         R"(recycle { config { op: ADD_BIN Subtree: "/eos/test" } })"},
        {"RecycleRemoveBin",
         "recycle",
         {"config", "--remove-bin", "/eos/test"},
         R"(recycle { config { op: RM_BIN Subtree: "/eos/test" } })"},
        {"RecycleDump",
         "recycle",
         {"config", "--dump"},
         "recycle { config { op: DUMP } }"},
        {"RecycleEnable",
         "recycle",
         {"config", "--enable", "off"},
         R"(recycle { config { op: ENABLE value: "off" } })"},
        {"RecycleEnforce",
         "recycle",
         {"config", "--enforce", "on"},
         R"(recycle { config { op: ENFORCE value: "on" } })"},
        {"RecycleDryRun",
         "recycle",
         {"config", "--dry-run", "yes"},
         R"(recycle { config { op: DRY_RUN value: "yes" } })"},
        {"RecycleLifetime",
         "recycle",
         {"config", "--lifetime", "3600"},
         "recycle { config { op: LIFETIME LifetimeSec: 3600 } }"},
        {"RecycleRatio",
         "recycle",
         {"config", "--ratio", "0.5"},
         "recycle { config { op: RATIO Ratio: 0.5 } }"},
        {"RecycleCollect",
         "recycle",
         {"config", "--collect-interval", "3600"},
         "recycle { config { op: COLLECT_INTERVAL Size: 3600 } }"},
        {"RecycleRemoveInterval",
         "recycle",
         {"config", "--remove-interval", "60"},
         "recycle { config { op: REMOVE_INTERVAL Size: 60 } }"},
        {"RmPath", "rm", {"relative"}, R"(rm { Path: "/eos/dispatch-test/relative" })"},
        {"RmFlags",
         "rm",
         {"-rF", "--no-workflow", "--no-globbing", "--no-confirmation", "/eos/test/tree"},
         R"(rm { Recursive: true BypassRecycle: true NoWorkflow: true NoGlobbing: true Path: "/eos/test/tree" })"},
        {"RmFileId", "rm", {"-r", "fid:123"}, "rm { Fileid: 123 }"},
        {"RmHexFileId", "rm", {"fxid:ab"}, "rm { Fileid: 171 }"},
        {"RmContainerId",
         "rm",
         {"-r", "cid:123"},
         "rm { Recursive: true Containerid: 123 }"},
        {"RmHexContainerId", "rm", {"cxid:ab"}, "rm { Containerid: 171 }"},
        {"EvictZeroId", "evict", {"fid:0"}, "evict { File { Fid: 0 } }"},
        {"EvictMaxId",
         "evict",
         {"fid:18446744073709551615"},
         "evict { File { Fid: 18446744073709551615 } }"},
        {"EvictMaxHexId",
         "evict",
         {"fxid:ffffffffffffffff"},
         "evict { File { Fid: 18446744073709551615 } }"},
        {"EvictFsidAssignment",
         "evict",
         {"--fsid=42", "--ignore-evict-counter", "/eos/test"},
         R"(evict { File { Path: "/eos/test" } ignoreEvictCounter: true evictSingleReplica { fsid: 42 } })"},
        {"RegisterDefaults",
         "register",
         {"/eos/test"},
         R"(record { path: "/eos/test" })",
         true},
        {"RegisterMaxNumbers",
         "register",
         {"/eos/test", "uid=4294967295", "gid=4294967295", "size=18446744073709551615",
          "layoutid=2147483647", "location=4294967295"},
         R"(record { path: "/eos/test" owner { uid: 4294967295 gid: 4294967295 } size: 18446744073709551615 layoutid: 2147483647 locations: 4294967295 })",
         true},
        {"RegisterEmptyAttribute",
         "register",
         {"/eos/test", "attr=user.key="},
         R"(record { path: "/eos/test" attr { key: "user.key" value: "" } })",
         true},
        {"RegisterAttributeEquals",
         "register",
         {"/eos/test", "xattr=user.key=a=b=c"},
         R"(record { path: "/eos/test" attr { key: "user.key" value: "a=b=c" } })",
         true},
        {"RegisterRepeatedAttribute",
         "register",
         {"/eos/test", "attr=user.key=old", "attr=user.key=new"},
         R"(record { path: "/eos/test" attr { key: "user.key" value: "new" } })",
         true},
        {"RegisterPathEquals",
         "register",
         {"/eos/a=b&c"},
         R"(record { path: "/eos/a=b&c" })",
         true},
        {"RegisterTimes",
         "register",
         {"/eos/test", "btime=0", "ctime=1.0", "mtime=2.999999999", "atime=3.1"},
         R"(record { path: "/eos/test" btime {} ctime { sec: 1 } mtime { sec: 2 nsec: 999999999 } atime { sec: 3 nsec: 1 } })",
         true},
        {"DebugGet", "debug", {"get"}, "debug { get { placeholder: true } }"},
        {"DebugSetFilter",
         "debug",
         {"info", "/eos/test:1095/fst", "--filter", "Mgm,Fst"},
         R"(debug { set { debuglevel: "info" nodename: "/eos/test:1095/fst" filter: "Mgm,Fst" } })"},
    }),
    [](const ::testing::TestParamInfo<RequestCase>& info) { return info.param.id; });

struct RejectedCase {
  const char* id;
  const char* command;
  std::vector<std::string> args;
};
class ConsoleRejectedInputTest : public CommandDispatchTest,
                                 public ::testing::WithParamInterface<RejectedCase> {};

TEST_P(ConsoleRejectedInputTest, ReturnsEinvalWithoutSending)
{
  expectRejected(GetParam().command, GetParam().args);
}

INSTANTIATE_TEST_SUITE_P(
    InvalidArguments, ConsoleRejectedInputTest,
    ::testing::ValuesIn(std::vector<RejectedCase>{
        {"ConfigMissingSubcommand", "config", {}},
        {"ConfigUnknownSubcommand", "config", {"invalid"}},
        {"ConfigListUnknownOption", "config", {"ls", "--invalid"}},
        {"ConfigResetExtra", "config", {"reset", "unexpected"}},
        {"ConfigExportMissingName", "config", {"export"}},
        {"ConfigExportOptionAsName", "config", {"export", "-f"}},
        {"ConfigSaveMissingName", "config", {"save"}},
        {"ConfigSaveUnknownOption", "config", {"save", "test", "--invalid"}},
        {"ConfigLoadMissingName", "config", {"load"}},
        {"ConfigInvalidChangelog", "config", {"changelog", "no"}},
        {"GroupMissingSubcommand", "group", {}},
        {"GroupUnknownSubcommand", "group", {"invalid"}},
        {"GroupMissingDepth", "group", {"ls", "-g"}},
        {"GroupNegativeDepth", "group", {"ls", "-g", "-1"}},
        {"GroupMalformedDepth", "group", {"ls", "-g", "2junk"}},
        {"GroupOverflowDepth", "group", {"ls", "-g", "999999999999999999999"}},
        {"GroupUnknownOption", "group", {"ls", "--invalid"}},
        {"GroupRemoveMissingName", "group", {"rm"}},
        {"GroupSetMissingName", "group", {"set"}},
        {"GroupSetMissingState", "group", {"set", "default.1"}},
        {"GroupSetInvalidState", "group", {"set", "default.1", "invalid"}},
        {"DfRelativePath", "df", {"relative"}},
        {"DfUnknownOption", "df", {"--invalid"}},
        {"DfRelativeOptionPath", "df", {"-m", "relative"}},
        {"DfMultiplePaths", "df", {"-m", "/eos/a", "/eos/b"}},
        {"RouteMissingSubcommand", "route", {}},
        {"RouteUnknownSubcommand", "route", {"invalid"}},
        {"RouteRelativePath", "route", {"ls", "relative"}},
        {"RouteParentTraversal", "route", {"ls", "/eos/../test"}},
        {"RouteDotTraversal", "route", {"ls", "/eos/./test"}},
        {"RouteBackslash", "route", {"ls", "/eos/te\\st"}},
        {"RouteUnlinkMissingPath", "route", {"unlink"}},
        {"RouteLinkMissingEndpoint", "route", {"link", "/eos/test"}},
        {"RouteInvalidHostname", "route", {"link", "/eos/test", "bad!host"}},
        {"RouteNonnumericPort", "route", {"link", "/eos/test", "mgm.example.org:abc"}},
        {"QuotaMissingUid", "quota", {"ls", "-u"}},
        {"QuotaMissingGid", "quota", {"ls", "--gid"}},
        {"QuotaMissingPath", "quota", {"ls", "--path"}},
        {"QuotaSetMissingVolume", "quota", {"set", "--volume"}},
        {"QuotaSetMissingInodes", "quota", {"set", "--inodes"}},
        {"QuotaUnknownOption", "quota", {"set", "--invalid"}},
        {"QuotaRemoveMissingUid", "quota", {"rm", "-u"}},
        {"QuotaRemoveNodeMissingPath", "quota", {"rmnode"}},
        {"RecycleMissingProjectPath", "recycle", {"ls", "--project"}},
        {"RecycleRelativeProjectList", "recycle", {"ls", "--project", "relative"}},
        {"RecyclePurgeMissingId", "recycle", {"purge", "--rid"}},
        {"RecyclePurgeInvalidId", "recycle", {"purge", "--rid", "no"}},
        {"RecyclePurgeMissingKey", "recycle", {"purge", "-k"}},
        {"RecyclePurgeDateAndKey", "recycle", {"purge", "2026", "-k", "key"}},
        {"RecycleRestoreMissingKey", "recycle", {"restore", "-p", "-f"}},
        {"RecycleProjectMissingPathValue", "recycle", {"project", "--path"}},
        {"RecycleConfigMissingOption", "recycle", {"config"}},
        {"RecycleConfigMissingLifetime", "recycle", {"config", "--lifetime"}},
        {"RecycleConfigInvalidRatio", "recycle", {"config", "--ratio", "no"}},
        {"RecycleConfigMissingSize", "recycle", {"config", "--size"}},
        {"RecycleConfigMissingEnable", "recycle", {"config", "--enable"}},
        {"RecycleUnknownSubcommand", "recycle", {"invalid"}},
        {"RmMissingPath", "rm", {}},
        {"RmOptionsWithoutPath", "rm", {"--no-recycle-bin", "--no-workflow"}},
        {"RmUnknownOption", "rm", {"--invalid", "/eos/test"}},
        {"MkdirMissingPath", "mkdir", {"-p"}},
        {"MkdirUnknownOption", "mkdir", {"--invalid", "/eos/test"}},
        {"ChmodMissingPath", "chmod", {"755"}},
        {"ChownMissingPath", "chown", {"123"}},
        {"LnMissingTarget", "ln", {"/eos/test"}},
        {"MvMissingTarget", "mv", {"/eos/test"}},
        {"TouchMissingPath", "touch", {}},
        {"EvictEmptyFile", "evict", {""}},
        {"EvictEmptyFileAmongOthers", "evict", {"/eos/test", ""}},
        {"RegisterEmptyUid", "register", {"/eos/test", "uid="}},
        {"RegisterEmptyGid", "register", {"/eos/test", "gid="}},
        {"RegisterUidOverflow", "register", {"/eos/test", "uid=4294967296"}},
        {"RegisterGidOverflow", "register", {"/eos/test", "gid=4294967296"}},
        {"RegisterModeSuffix", "register", {"/eos/test", "mode=644junk"}},
        {"RegisterEmptySize", "register", {"/eos/test", "size="}},
        {"RegisterTimestampOverflow",
         "register",
         {"/eos/test", "atime=9223372036854775808"}},
        {"RegisterTimestampTwoDots", "register", {"/eos/test", "ctime=1.2.3"}},
        {"RegisterRelativePathTag", "register", {"path=relative"}},
    }),
    [](const ::testing::TestParamInfo<RejectedCase>& info) { return info.param.id; });

struct AliasCase {
  const char* id;
  const char* alias;
  std::vector<std::string> args;
  const char* target;
  std::vector<std::string> forwarded;
};
class ConsoleAliasTest : public CommandDispatchTest,
                         public ::testing::WithParamInterface<AliasCase> {};

TEST_P(ConsoleAliasTest, MatchesCanonicalRequest)
{
  const auto& item = GetParam();
  run(item.target, item.forwarded);
  const auto canonical = protobuf(false);
  const int canonicalRetc = global_retc;
  run(item.alias, item.args);
  EXPECT_EQ(global_retc, canonicalRetc);
  EXPECT_TRUE(
      google::protobuf::util::MessageDifferencer::Equivalent(canonical, protobuf(false)));
}

INSTANTIATE_TEST_SUITE_P(
    Aliases, ConsoleAliasTest,
    ::testing::ValuesIn(std::vector<AliasCase>{
        {"InfoPath", "info", {"/eos/a b&c=d"}, "file", {"info", "/eos/a b&c=d"}},
        {"FileinfoId", "fileinfo", {"fid:123"}, "file", {"info", "fid:123"}},
        {"MovePaths",
         "mv",
         {"/eos/a b", "/eos/c d"},
         "file",
         {"rename", "/eos/a b", "/eos/c d"}},
        {"LinkPaths",
         "ln",
         {"/eos/a b", "/eos/c d"},
         "file",
         {"symlink", "/eos/a b", "/eos/c d"}},
        {"LinkForce",
         "ln",
         {"-f", "/eos/a", "/eos/b"},
         "file",
         {"symlink", "-f", "/eos/a", "/eos/b"}},
        {"TouchFlags", "touch", {"-0", "/eos/a b"}, "file", {"touch", "-0", "/eos/a b"}},
        {"FindAlias", "newfind", {"/eos/test"}, "find", {"/eos/test"}},
    }),
    [](const ::testing::TestParamInfo<AliasCase>& info) { return info.param.id; });

class ConsoleHelpTest : public CommandDispatchTest,
                        public ::testing::WithParamInterface<const char*> {};

TEST_P(ConsoleHelpTest, DoesNotSendRequests)
{
  // Exercise registered commands, including legacy CGI commands, with the same
  // help spellings accepted by the shell. No MGM service should be needed.
  for (const auto* option : {"--help", "-h"}) {
    SCOPED_TRACE(option);
    run(GetParam(), {option});
    EXPECT_TRUE(global_retc == 0 || global_retc == EINVAL);
    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_TRUE(requests.empty());
  }
}

INSTANTIATE_TEST_SUITE_P(
    Commands, ConsoleHelpTest,
    ::testing::Values("access", "accounting", "acl", "archive", "attr", "backup", "cd",
                      "chmod", "chown", "config", "convert", "debug", "devices", "df",
                      "du", "evict", "file", "fileinfo", "find", "fs", "fsck", "fusex",
                      "geosched", "group", "info", "inspector", "io", "ln", "ls", "map",
                      "member", "mkdir", "monit", "motd", "mv", "newfind", "node", "ns",
                      "oldfind", "quota", "recycle", "register", "rm", "rmdir", "route",
                      "rtlog", "sched", "space", "token", "touch", "tracker", "version",
                      "vid", "who", "whoami"),
    [](const ::testing::TestParamInfo<const char*>& info) { return info.param; });
} // namespace

namespace {
struct LegacyFieldsCase {
  const char* id;
  const char* command;
  std::vector<std::string> args;
  std::vector<std::pair<std::string, std::string>> fields;
};
class ConsoleLegacyFieldsTest : public CommandDispatchTest,
                                public ::testing::WithParamInterface<LegacyFieldsCase> {};

TEST_P(ConsoleLegacyFieldsTest, PreservesArgumentsAndServerStatus)
{
  const auto& item = GetParam();
  for (const int retc : {0, ENOENT, EACCES, EBUSY}) {
    SCOPED_TRACE(retc);
    responseRetc = retc;
    run(item.command, item.args);
    EXPECT_EQ(global_retc, retc);
    const auto wire = request();
    EXPECT_EQ(wire.substr(0, wire.find('?')), "/proc/user/");
    XrdOucEnv env(wire.substr(wire.find('?') + 1).c_str());
    EXPECT_STREQ(env.Get("mgm.cmd"), item.command);
    EXPECT_EQ(env.Get("mgm.cmd.proto"), nullptr);
    for (const auto& field : item.fields) {
      SCOPED_TRACE(field.first);
      EXPECT_STREQ(env.Get(field.first.c_str()), field.second.c_str());
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
    LegacyArguments, ConsoleLegacyFieldsTest,
    ::testing::ValuesIn(std::vector<LegacyFieldsCase>{
        {"MkdirRelative",
         "mkdir",
         {"relative"},
         {{"mgm.path", "/#curl#/eos/dispatch-test/relative"}, {"eos.encodepath", "1"}}},
        {"MkdirParents",
         "mkdir",
         {"-p", "/eos/test"},
         {{"mgm.path", "/#curl#/eos/test"}, {"mgm.option", "p"}}},
        {"MkdirEscapedPath",
         "mkdir",
         {"/eos/a b&c=d"},
         {{"mgm.path", "/#curl#/eos/a%20b%26c%3Dd"}, {"eos.encodepath", "1"}}},
        {"ChmodMode",
         "chmod",
         {"640", "/eos/test"},
         {{"mgm.path", "/#curl#/eos/test"},
          {"mgm.chmod.mode", "640"},
          {"eos.encodepath", "1"}}},
        {"ChmodRecursive",
         "chmod",
         {"-r", "755", "relative"},
         {{"mgm.path", "/#curl#/eos/dispatch-test/relative"},
          {"mgm.chmod.mode", "755"},
          {"mgm.option", "r"}}},
        {"ChownUser",
         "chown",
         {"123", "/eos/test"},
         {{"mgm.path", "/#curl#/eos/test"},
          {"mgm.chown.owner", "123"},
          {"eos.encodepath", "1"}}},
        {"ChownGroup", "chown", {":456", "/eos/test"}, {{"mgm.chown.owner", ":456"}}},
        {"ChownRecursiveLink",
         "chown",
         {"-r", "-h", "123:456", "/eos/test"},
         {{"mgm.chown.owner", "123:456"}, {"mgm.chown.option", "rh"}}},
        {"MemberGroup", "member", {"testgroup"}, {{"mgm.egroup", "testgroup"}}},
        {"MemberRefresh",
         "member",
         {"--update", "testgroup"},
         {{"mgm.egroup", "testgroup"}, {"mgm.egroupupdate", "true"}}},
        {"RmdirPath",
         "rmdir",
         {"relative"},
         {{"mgm.path", "/#curl#/eos/dispatch-test/relative"}}},
    }),
    [](const ::testing::TestParamInfo<LegacyFieldsCase>& info) { return info.param.id; });
} // namespace

TEST_F(CommandDispatchTest, DfFormattingFlagsDoNotBecomePaths)
{
  for (const auto* flag : {"-m", "-H", "-b"}) {
    SCOPED_TRACE(flag);
    run("df", {flag});
    EXPECT_EQ(global_retc, ENOENT);
    const auto proto = protobuf(false);
    ASSERT_TRUE(proto.has_df());
    EXPECT_TRUE(proto.df().path().empty());
    EXPECT_EQ(proto.df().monitoring(), std::string(flag) == "-m");
    EXPECT_EQ(proto.df().si(), std::string(flag) == "-m");
    EXPECT_EQ(proto.df().readable(), std::string(flag) == "-H");
  }
}

TEST_F(CommandDispatchTest, DfRejectsMultiplePathsWithoutFormattingFlag)
{
  expectRejected("df", {"/eos/first", "/eos/second"});
}

TEST_F(CommandDispatchTest, RoutePortBounds)
{
  for (const auto* ports : {"1:65535", "65535:1"}) {
    SCOPED_TRACE(ports);
    run("route", {"link", "/eos/test", std::string("mgm.example.org:") + ports});
    EXPECT_EQ(global_retc, ENOENT);
    const auto proto = protobuf(false);
    ASSERT_EQ(proto.route().link().endpoints_size(), 1);
    const auto& endpoint = proto.route().link().endpoints(0);
    EXPECT_EQ(endpoint.xrd_port(), std::string(ports) == "1:65535" ? 1u : 65535u);
    EXPECT_EQ(endpoint.http_port(), std::string(ports) == "1:65535" ? 65535u : 1u);
  }
}

TEST_F(CommandDispatchTest, RouteRejectsMalformedEndpointsBeforeSending)
{
  for (const auto* endpoint : {":",
                               ",",
                               "",
                               "mgm.example.org:0",
                               "mgm.example.org:65536",
                               "mgm.example.org:-1",
                               "mgm.example.org:+1094",
                               "mgm.example.org:1094junk",
                               "mgm.example.org:4294967297",
                               "mgm.example.org:18446744073709551616",
                               "mgm.example.org:1094:0",
                               "mgm.example.org:1094:65536",
                               "mgm.example.org:1094:-1",
                               "mgm.example.org:1094:8000junk",
                               "mgm.example.org:1094:4294967297",
                               "mgm.example.org:",
                               "mgm.example.org::8000",
                               "mgm.example.org:1094:",
                               "mgm.example.org:1094:8000:9000",
                               ":1094:8000",
                               ",mgm.example.org",
                               "mgm.example.org,",
                               "mgm.example.org,,other.example.org"}) {
    SCOPED_TRACE(endpoint);
    expectRejected("route", {"link", "/eos/test", endpoint});
  }
}

TEST_F(CommandDispatchTest, RouteRejectsTrailingArguments)
{
  expectRejected("route", {"ls", "/eos/test", "unexpected"});
  expectRejected("route", {"unlink", "/eos/test", "unexpected"});
  expectRejected("route", {"link", "/eos/test", "mgm.example.org", "unexpected"});
}

TEST_F(CommandDispatchTest, RecycleProjectRequiresAbsolutePath)
{
  expectRejected("recycle", {"project"});
  expectRejected("recycle", {"project", "--path", "relative"});
  expectRejected("recycle", {"project", "--path", ""});
  expectRejected("recycle", {"project", "--acl", "u:123=rx"});
}

TEST_F(CommandDispatchTest, RecycleProjectAcceptsAclBeforePath)
{
  run("recycle", {"project", "--acl", "u:123=rx", "--path", "/eos/test"});
  EXPECT_EQ(global_retc, ENOENT);
  const auto proto = protobuf(false);
  EXPECT_EQ(proto.recycle().project().path(), "/eos/test");
  EXPECT_EQ(proto.recycle().project().acl(), "u:123=rx");
}
