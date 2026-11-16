// ----------------------------------------------------------------------
// File: McpAdminTests.cc
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "mgm/mcp/McpAdmin.hh"
#include "mgm/mcp/McpAdminCommands.hh"
#include "mgm/mcp/McpAdminSource.hh"
#include "mgm/proc/ProcInterface.hh"
#include "common/Mapping.hh"
#include "common/RWMutex.hh"
#include "common/VirtualIdentity.hh"
#include <gtest/gtest.h>
#include <json/json.h>
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <unistd.h>
#include <sys/stat.h>

namespace {

Json::Value
Parse(const std::string& body)
{
  Json::Value root;
  Json::Reader reader;
  EXPECT_TRUE(reader.parse(body, root)) << reader.getFormattedErrorMessages()
                                        << " body=" << body;
  return root;
}

class FakeSource : public eos::mgm::mcp::McpAdminSource
{
public:
  Json::Value Status(const std::string& scope, std::string& error) const override
  {
    ++status_calls;

    if (scope == "bad") {
      error = "unknown scope";
      return Json::Value();
    }

    Json::Value value(Json::objectValue);
    value["scope"] = scope;
    value["nodes"] = 1;
    return value;
  }

  Json::Value ListLogs(std::string& /*error*/) const override
  {
    Json::Value logs(Json::arrayValue);
    Json::Value entry(Json::objectValue);
    entry["name"] = "mgm";
    logs.append(entry);
    Json::Value value(Json::objectValue);
    value["logs"] = logs;
    return value;
  }

  Json::Value ReadLog(const std::string& name, int lines, const std::string& level,
                      std::string& error) const override
  {
    ++read_calls;
    last_name = name;

    if (name != "mgm" && name != "memory") {
      error = "log name is not allowlisted";
      return Json::Value();
    }

    Json::Value value(Json::objectValue);
    value["name"] = name;
    value["lines"] = lines;
    value["level"] = level;
    value["text"] = "hello from " + name + "\n";
    return value;
  }

  eos::console::ReplyProto
  RunCommand(eos::console::RequestProto&& req,
             const eos::common::VirtualIdentity& vid) const override
  {
    ++run_calls;
    last_request = req;
    last_uid = vid.uid;
    eos::console::ReplyProto reply;
    reply.set_std_out("command output\n");
    return reply;
  }

  mutable int status_calls = 0;
  mutable int read_calls = 0;
  mutable int run_calls = 0;
  mutable std::string last_name;
  mutable eos::console::RequestProto last_request;
  mutable uid_t last_uid = 99;
};

eos::mgm::mcp::McpDispatchResult
Call(const std::string& body, const eos::mgm::mcp::McpAdminSource& source)
{
  return eos::mgm::mcp::Dispatch(body, source,
                                 eos::common::VirtualIdentity::Root());
}

Json::Value
CallTool(const std::string& name, const std::string& arguments,
         const eos::mgm::mcp::McpAdminSource& source)
{
  return Parse(Call("{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"tools/call\","
                    "\"params\":{\"name\":\"" + name + "\",\"arguments\":" +
                    arguments + "}}", source).body);
}

bool
ReadOnly(const eos::console::RequestProto& req)
{
  std::string error;
  return eos::mgm::mcp::IsReadOnlyAdminRequest(req, error);
}

std::unique_ptr<eos::common::HttpRequest>
Request(const std::string& method, const std::string& url, const std::string& body,
        eos::common::HttpRequest::HeaderMap headers, size_t& size)
{
  size = body.size();
  eos::common::HttpRequest::HeaderMap cookies;
  return std::make_unique<eos::common::HttpRequest>(headers, method, url, "", body,
         &size, cookies);
}

class ScopedLogDir
{
public:
  ScopedLogDir()
  {
    const char* old = std::getenv("XRDLOGDIR");

    if (old) {
      mOld = old;
      mHad = true;
    }

    char tmpl[] = "/tmp/eos-mcp-log-XXXXXX";
    char* made = mkdtemp(tmpl);

    if (made) {
      mDir = made;
      setenv("XRDLOGDIR", mDir.c_str(), 1);
    }
  }

  ~ScopedLogDir()
  {
    if (mHad) {
      setenv("XRDLOGDIR", mOld.c_str(), 1);
    } else {
      unsetenv("XRDLOGDIR");
    }

    if (!mDir.empty()) {
      ::unlink((mDir + "/mgm/xrdlog.mgm").c_str());
      ::unlink((mDir + "/secret.txt").c_str());
      ::rmdir((mDir + "/mgm").c_str());
      ::rmdir(mDir.c_str());
    }
  }

  const std::string& dir() const
  {
    return mDir;
  }

private:
  std::string mDir;
  std::string mOld;
  bool mHad = false;
};

} // namespace

TEST(McpAdmin, pathMatchesEndpointOnly)
{
  EXPECT_TRUE(eos::mgm::mcp::IsMcpPath("/mcp"));
  EXPECT_TRUE(eos::mgm::mcp::IsMcpPath("/mcp/"));
  EXPECT_TRUE(eos::mgm::mcp::IsMcpPath("/mcp?authz=token"));
  EXPECT_TRUE(eos::mgm::mcp::IsMcpPath("https://mgm.example:8443/mcp"));
  EXPECT_FALSE(eos::mgm::mcp::IsMcpPath("/mcp/extra"));
  EXPECT_FALSE(eos::mgm::mcp::IsMcpPath("/proc/user"));
}

TEST(McpAdmin, initializeAndToolsAreReadOnly)
{
  FakeSource source;
  const auto init = Call(
                      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
                      "\"params\":{\"protocolVersion\":\"2025-06-18\"}}", source);
  ASSERT_EQ(200, init.http_status);
  const Json::Value root = Parse(init.body);
  EXPECT_EQ("2025-06-18", root["result"]["protocolVersion"].asString());
  EXPECT_EQ("eos-mgm-admin", root["result"]["serverInfo"]["name"].asString());
  EXPECT_NE(std::string::npos,
            root["result"]["instructions"].asString().find("Read-only"));
  const auto listed = Call("{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}",
                           source);
  const Json::Value tools = Parse(listed.body)["result"]["tools"];
  std::set<std::string> names;
  Json::Value ns_stat;

  for (const auto& tool : tools) {
    names.insert(tool["name"].asString());

    if (tool["name"].asString() == "eos_ns_stat") {
      ns_stat = tool;
    }
  }

  for (const auto& name : {
         "eos_log_list", "eos_log_read", "eos_status", "eos_ns_stat",
         "eos_space_ls", "eos_node_ls", "eos_fs_status", "eos_group_ls"
       }) {
    EXPECT_TRUE(names.count(name)) << name;
  }

  const Json::Value props = ns_stat["inputSchema"]["properties"]["params"]["properties"];
  EXPECT_TRUE(props.isMember("Summary"));
  EXPECT_FALSE(props.isMember("Reset"));
  EXPECT_TRUE(ns_stat["annotations"]["readOnlyHint"].asBool());
}

TEST(McpAdmin, adminCommandToolsBuildAllowlistedRequests)
{
  FakeSource source;
  const Json::Value stat = CallTool("eos_ns_stat",
                                    "{\"params\":{\"Summary\":true},\"json\":true}", source);
  EXPECT_FALSE(stat["result"].isMember("isError"));
  EXPECT_NE(std::string::npos,
            stat["result"]["content"][0]["text"].asString().find("command output"));
  ASSERT_EQ(1, source.run_calls);
  EXPECT_EQ(0u, source.last_uid);
  EXPECT_TRUE(source.last_request.ns().stat().summary());
  EXPECT_EQ(eos::console::RequestProto::JSON, source.last_request.format());
  EXPECT_TRUE(source.last_request.dontcolor());
  CallTool("eos_space_ls", "{\"params\":{\"outformat\":\"MONITORING\"}}", source);
  EXPECT_EQ(eos::console::SpaceProto_LsProto::MONITORING,
            source.last_request.space().ls().outformat());
  CallTool("eos_ns_tracker_list", "{}", source);
  EXPECT_EQ(eos::console::NsProto_TrackerProto::LIST,
            source.last_request.ns().tracker().op());
  CallTool("eos_space_inspector", "{\"params\":{\"mgmspace\":\"default\"}}", source);
  EXPECT_EQ("m", source.last_request.space().inspector().options());
  CallTool("eos_fsck_stat", "{}", source);
  EXPECT_TRUE(source.last_request.fsck().stat());
  EXPECT_EQ(5, source.run_calls);
}

TEST(McpAdmin, adminCommandToolsRejectWriteFields)
{
  FakeSource source;

  for (const auto& call : std::vector<std::pair<std::string, std::string>> {
  {"eos_ns_stat", "{\"params\":{\"Reset\":true}}"},
  {"eos_ns_tracker_list", "{\"params\":{\"op\":\"CLEAR\"}}"},
  {"eos_ns_behaviour_get", "{\"params\":{\"op\":\"SET\",\"name\":\"x\"}}"},
  {"eos_devices_ls", "{\"params\":{\"refresh\":true}}"},
  {"eos_space_inspector", "{\"params\":{\"options\":\"ce\"}}"},
  {"eos_space_ls", "{\"params\":{\"bogus\":1}}"},
  {"eos_space_ls", "{\"Comment\":\"x\"}"},
  {"eos_space_ls", "{\"params\":[]}"},
  {"eos_space_reset", "{}"}
}) {
    const Json::Value reply = CallTool(call.first, call.second, source);
    EXPECT_EQ(-32602, reply["error"]["code"].asInt()) << call.first << " "
        << call.second;
  }

  EXPECT_EQ(0, source.run_calls);
}

TEST(McpAdminCommands, validatorIsClosedByDefault)
{
  eos::console::RequestProto req;
  req.mutable_ns()->mutable_stat()->set_summary(true);
  EXPECT_TRUE(ReadOnly(req));
  req.mutable_ns()->mutable_stat()->set_reset(true);
  EXPECT_FALSE(ReadOnly(req));
  req.Clear();
  req.mutable_space()->mutable_ls();
  EXPECT_TRUE(ReadOnly(req));
  req.set_comment("audit");
  EXPECT_FALSE(ReadOnly(req));
  req.Clear();
  req.mutable_space()->mutable_ls();
  req.set_format(eos::console::RequestProto::HTTP);
  EXPECT_FALSE(ReadOnly(req));
  req.Clear();
  req.mutable_space()->mutable_reset();
  EXPECT_FALSE(ReadOnly(req));
  req.Clear();
  req.mutable_ns()->mutable_tracker()->set_op(
    eos::console::NsProto_TrackerProto::CLEAR);
  EXPECT_FALSE(ReadOnly(req));
  req.mutable_ns()->mutable_tracker()->set_op(
    eos::console::NsProto_TrackerProto::LIST);
  EXPECT_TRUE(ReadOnly(req));
  req.Clear();
  req.mutable_ns()->mutable_behaviour()->set_op(
    eos::console::NsProto_BehaviourProto::GET);
  req.mutable_ns()->mutable_behaviour()->set_name("rain_min_fsid_entry");
  EXPECT_TRUE(ReadOnly(req));
  req.mutable_ns()->mutable_behaviour()->set_op(
    eos::console::NsProto_BehaviourProto::SET);
  EXPECT_FALSE(ReadOnly(req));
  req.Clear();
  req.mutable_space()->mutable_inspector()->set_options("m");
  EXPECT_TRUE(ReadOnly(req));
  req.mutable_space()->mutable_inspector()->set_options("ce");
  EXPECT_FALSE(ReadOnly(req));
  req.Clear();
  req.mutable_devices()->mutable_ls()->set_refresh(true);
  EXPECT_FALSE(ReadOnly(req));
  req.Clear();
  req.mutable_config()->mutable_changelog();
  EXPECT_FALSE(ReadOnly(req));
  req.Clear();
  req.mutable_space()->mutable_groupdrainer()->mutable_reset();
  EXPECT_FALSE(ReadOnly(req));
  req.Clear();
  req.mutable_sched()->mutable_disabled();
  EXPECT_FALSE(ReadOnly(req));
  req.mutable_sched()->mutable_disabled()->set_op(
    eos::console::SchedProto_DisabledProto::LS);
  EXPECT_TRUE(ReadOnly(req));
  req.Clear();
  req.mutable_quota();
  EXPECT_FALSE(ReadOnly(req));
  req.Clear();
  EXPECT_FALSE(ReadOnly(req));
}

TEST(McpAdminSource, runCommandRefusesNonAllowlistedRequest)
{
  eos::mgm::mcp::EosMcpAdminSource source;
  eos::console::RequestProto req;
  req.mutable_ns()->mutable_stat()->set_reset(true);
  const eos::console::ReplyProto reply =
    source.RunCommand(std::move(req), eos::common::VirtualIdentity::Root());
  EXPECT_EQ(EPERM, reply.retc());
  EXPECT_NE(std::string::npos, reply.std_err().find("not allowed"));
}

TEST(McpAdmin, statusAndLogToolsUseTheSource)
{
  FakeSource source;
  const auto status = Call(
                        "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\","
                        "\"params\":{\"name\":\"eos_status\",\"arguments\":{\"scope\":\"nodes\"}}}",
                        source);
  const Json::Value status_root = Parse(status.body);
  EXPECT_FALSE(status_root["result"].isMember("isError") &&
               status_root["result"]["isError"].asBool());
  EXPECT_NE(std::string::npos,
            status_root["result"]["content"][0]["text"].asString().find("nodes"));
  EXPECT_EQ(1, source.status_calls);
  const auto log = Call(
                     "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/call\","
                     "\"params\":{\"name\":\"eos_log_read\",\"arguments\":{\"name\":\"mgm\",\"lines\":5}}}",
                     source);
  const Json::Value log_root = Parse(log.body);
  EXPECT_NE(std::string::npos,
            log_root["result"]["content"][0]["text"].asString().find("hello from mgm"));
  EXPECT_EQ("mgm", source.last_name);
  const auto resource = Call(
                          "{\"jsonrpc\":\"2.0\",\"id\":8,\"method\":\"resources/read\","
                          "\"params\":{\"uri\":\"eos://logs/memory\"}}", source);
  EXPECT_NE(std::string::npos,
            Parse(resource.body)["result"]["contents"][0]["text"].asString()
            .find("hello from memory"));
  EXPECT_EQ("memory", source.last_name);
}

TEST(McpAdmin, rejectsUnknownAndUnsafeOperations)
{
  FakeSource source;
  const auto unknown = Call(
                         "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"tools/call\","
                         "\"params\":{\"name\":\"eos_node_set\",\"arguments\":{}}}", source);
  EXPECT_EQ(-32602, Parse(unknown.body)["error"]["code"].asInt());
  EXPECT_EQ(0, source.status_calls);
  const auto traversal = Call(
                           "{\"jsonrpc\":\"2.0\",\"id\":6,\"method\":\"resources/read\","
                           "\"params\":{\"uri\":\"eos://logs/../../etc/passwd\"}}", source);
  EXPECT_EQ(-32002, Parse(traversal.body)["error"]["code"].asInt());
  EXPECT_EQ(0, source.read_calls);
  const auto missing = Call(
                         "{\"jsonrpc\":\"2.0\",\"method\":\"tools/list\"}", source);
  EXPECT_EQ(400, missing.http_status);
  const auto note = Call(
                      "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}", source);
  EXPECT_TRUE(note.notification);
  EXPECT_TRUE(note.body.empty());
  EXPECT_EQ(202, note.http_status);
}

TEST(McpAdmin, httpRequiresAdminAndPost)
{
  FakeSource source;
  eos::common::VirtualIdentity nobody;
  eos::common::VirtualIdentity root = eos::common::VirtualIdentity::Root();
  eos::common::HttpRequest::HeaderMap headers;
  headers["Content-Type"] = "application/json";
  headers["Accept"] = "application/json, text/event-stream";
  const std::string body =
    "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}";
  size_t size = 0;
  auto denied = Request("POST", "/mcp", body, headers, size);
  std::unique_ptr<eos::common::HttpResponse> denied_response(
    eos::mgm::mcp::HandleHttp(denied.get(), &nobody, &source));
  ASSERT_EQ(eos::common::HttpResponse::FORBIDDEN, denied_response->GetResponseCode());
  auto allowed = Request("POST", "/mcp", body, headers, size);
  std::unique_ptr<eos::common::HttpResponse> allowed_response(
    eos::mgm::mcp::HandleHttp(allowed.get(), &root, &source));
  ASSERT_EQ(eos::common::HttpResponse::OK, allowed_response->GetResponseCode());
  const Json::Value ping = Parse(allowed_response->GetBody());
  EXPECT_TRUE(ping["result"].isObject());
  EXPECT_EQ(0u, ping["result"].getMemberNames().size());
  auto get = Request("GET", "/mcp", "", headers, size);
  std::unique_ptr<eos::common::HttpResponse> get_response(
    eos::mgm::mcp::HandleHttp(get.get(), &root, &source));
  EXPECT_EQ(eos::common::HttpResponse::METHOD_NOT_ALLOWED,
            get_response->GetResponseCode());
  EXPECT_EQ("POST", get_response->GetHeaders()["Allow"]);
}

class ScopedMcpReader
{
public:
  explicit ScopedMcpReader(uid_t uid): mUid(uid)
  {
    eos::common::RWMutexWriteLock lock(eos::common::Mapping::gMapMutex);
    eos::common::Mapping::gMcpReaders.insert(mUid);
  }

  ~ScopedMcpReader()
  {
    eos::common::RWMutexWriteLock lock(eos::common::Mapping::gMapMutex);
    eos::common::Mapping::gMcpReaders.erase(mUid);
  }

private:
  uid_t mUid;
};

TEST(McpAdmin, mcpReaderIsAcceptedWithoutAdminRights)
{
  FakeSource source;
  eos::common::VirtualIdentity reader;
  reader.uid = 12345;
  reader.gid = 12345;
  reader.allowed_uids.insert(12345);
  reader.allowed_gids.insert(12345);
  EXPECT_FALSE(eos::mgm::ProcInterface::VidIsAdmin(reader));
  EXPECT_FALSE(eos::mgm::mcp::IsMcpCaller(reader));
  eos::common::HttpRequest::HeaderMap headers;
  headers["Content-Type"] = "application/json";
  const std::string body =
    "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}";
  size_t size = 0;
  auto denied = Request("POST", "/mcp", body, headers, size);
  std::unique_ptr<eos::common::HttpResponse> denied_response(
    eos::mgm::mcp::HandleHttp(denied.get(), &reader, &source));
  EXPECT_EQ(eos::common::HttpResponse::FORBIDDEN, denied_response->GetResponseCode());
  ScopedMcpReader marked(reader.uid);
  EXPECT_TRUE(eos::mgm::mcp::IsMcpCaller(reader));
  EXPECT_FALSE(eos::mgm::ProcInterface::VidIsAdmin(reader));
  auto allowed = Request("POST", "/mcp", body, headers, size);
  std::unique_ptr<eos::common::HttpResponse> allowed_response(
    eos::mgm::mcp::HandleHttp(allowed.get(), &reader, &source));
  EXPECT_EQ(eos::common::HttpResponse::OK, allowed_response->GetResponseCode());
}

TEST(McpAdminSource, runCommandRefusesCallerWithoutMcpRole)
{
  eos::mgm::mcp::EosMcpAdminSource source;
  eos::common::VirtualIdentity user;
  user.uid = 12346;
  user.gid = 12346;
  eos::console::RequestProto req;
  req.mutable_ns()->mutable_stat();
  const eos::console::ReplyProto reply = source.RunCommand(std::move(req), user);
  EXPECT_EQ(EPERM, reply.retc());
  EXPECT_NE(std::string::npos, reply.std_err().find("mcp reader"));
}

TEST(McpAdminSource, readsAllowlistedLogAndRefusesSymlink)
{
  ScopedLogDir logs;
  ASSERT_FALSE(logs.dir().empty());
  ASSERT_EQ(0, ::mkdir((logs.dir() + "/mgm").c_str(), 0755));
  {
    std::ofstream out(logs.dir() + "/mgm/xrdlog.mgm");
    ASSERT_TRUE(out.good());
    out << "first\nsecond\nthird\n";
  }
  eos::mgm::mcp::EosMcpAdminSource source;
  std::string error;
  const Json::Value listed = source.ListLogs(error);
  ASSERT_TRUE(error.empty());
  bool saw_mgm = false;

  for (const auto& entry : listed["logs"]) {
    if (entry["name"].asString() == "mgm") {
      saw_mgm = true;
      EXPECT_TRUE(entry["available"].asBool());
      EXPECT_EQ("mgm/xrdlog.mgm", entry["relative"].asString());
    }
  }

  EXPECT_TRUE(saw_mgm);
  const Json::Value text = source.ReadLog("mgm", 2, "info", error);
  ASSERT_TRUE(error.empty()) << error;
  EXPECT_NE(std::string::npos, text["text"].asString().find("second"));
  EXPECT_NE(std::string::npos, text["text"].asString().find("third"));
  EXPECT_EQ(std::string::npos, text["text"].asString().find("first"));
  source.ReadLog("not-a-log", 2, "info", error);
  EXPECT_EQ("log name is not allowlisted", error);
  error.clear();
  ::unlink((logs.dir() + "/mgm/xrdlog.mgm").c_str());
  {
    std::ofstream secret(logs.dir() + "/secret.txt");
    secret << "SECRET-LINE\n";
  }
  ASSERT_EQ(0, ::symlink((logs.dir() + "/secret.txt").c_str(),
                         (logs.dir() + "/mgm/xrdlog.mgm").c_str()));
  const Json::Value refused = source.ReadLog("mgm", 20, "info", error);
  EXPECT_FALSE(error.empty());
  EXPECT_EQ(std::string::npos, refused.toStyledString().find("SECRET-LINE"));
}
