// ----------------------------------------------------------------------
// File: McpAdmin.cc
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
#include "common/Logging.hh"
#include "common/Mapping.hh"
#include "common/RWMutex.hh"
#include "common/http/PlainHttpResponse.hh"
#include <json/json.h>
#include <algorithm>
#include <cctype>
#include <memory>
#include <set>
#include <string>
#include <vector>

EOSMGMNAMESPACE_BEGIN

namespace mcp {

namespace {

constexpr const char* kServerProtocol = "2025-06-18";

const char* kSupportedProtocols[] = {
  "2024-11-05",
  "2025-03-26",
  "2025-06-18"
};

const char* kLogLevels[] = {
  "emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"
};

const char* kStatusScopes[] = {
  "all", "nodes", "spaces", "groups", "filesystems"
};

std::string
WriteJson(const Json::Value& value, bool pretty)
{
  Json::StreamWriterBuilder builder;
  builder["indentation"] = pretty ? "  " : "";
  builder["commentStyle"] = "None";
  return Json::writeString(builder, value);
}

bool
ParseJson(const std::string& body, Json::Value& root, std::string& error)
{
  Json::CharReaderBuilder builder;
  builder["collectComments"] = false;
  std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
  const char* begin = body.data();
  return reader->parse(begin, begin + body.size(), &root, &error);
}

bool
EqualsIgnoreCase(const std::string& left, const std::string& right)
{
  if (left.size() != right.size()) {
    return false;
  }

  for (std::size_t i = 0; i < left.size(); ++i) {
    const unsigned char a = static_cast<unsigned char>(left[i]);
    const unsigned char b = static_cast<unsigned char>(right[i]);

    if (std::tolower(a) != std::tolower(b)) {
      return false;
    }
  }

  return true;
}

std::string
HeaderValue(eos::common::HttpRequest& request, const std::string& name)
{
  for (const auto& header : request.GetHeaders()) {
    if (EqualsIgnoreCase(header.first, name)) {
      return header.second;
    }
  }

  return "";
}

bool
HeaderContains(const std::string& header, const std::string& token)
{
  std::string lower = header;
  std::transform(lower.begin(), lower.end(), lower.begin(),
  [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return lower.find(token) != std::string::npos;
}

bool
OneOf(const std::string& value, const char* const* choices, std::size_t count)
{
  for (std::size_t i = 0; i < count; ++i) {
    if (value == choices[i]) {
      return true;
    }
  }

  return false;
}

Json::Value
RpcError(const Json::Value& id, int code, const std::string& message)
{
  Json::Value error(Json::objectValue);
  error["code"] = code;
  error["message"] = message;
  Json::Value response(Json::objectValue);
  response["jsonrpc"] = "2.0";
  response["id"] = id;
  response["error"] = error;
  return response;
}

Json::Value
RpcResult(const Json::Value& id, Json::Value result)
{
  Json::Value response(Json::objectValue);
  response["jsonrpc"] = "2.0";
  response["id"] = id;
  response["result"] = std::move(result);
  return response;
}

Json::Value
ToolResult(const std::string& text, bool is_error)
{
  Json::Value item(Json::objectValue);
  item["type"] = "text";
  item["text"] = text;
  Json::Value content(Json::arrayValue);
  content.append(item);
  Json::Value result(Json::objectValue);
  result["content"] = content;

  if (is_error) {
    result["isError"] = true;
  }

  return result;
}

Json::Value
TextSchema(const char* description)
{
  Json::Value schema(Json::objectValue);
  schema["type"] = "string";
  schema["description"] = description;
  return schema;
}

Json::Value
ToolDefinition(const char* name, const char* description, Json::Value schema,
               const std::vector<std::string>& required)
{
  Json::Value tool(Json::objectValue);
  tool["name"] = name;
  tool["description"] = description;
  tool["inputSchema"] = std::move(schema);

  if (!required.empty()) {
    Json::Value names(Json::arrayValue);

    for (const auto& item : required) {
      names.append(item);
    }

    tool["inputSchema"]["required"] = names;
  }

  return tool;
}

Json::Value
ToolsList()
{
  Json::Value status_schema(Json::objectValue);
  status_schema["type"] = "object";
  status_schema["additionalProperties"] = false;
  status_schema["properties"]["scope"] = TextSchema(
      "View to return: all, nodes, spaces, groups, or filesystems.");
  Json::Value status = ToolDefinition(
                         "eos_status",
                         "Read node, space, group, and filesystem status. "
                         "Does not modify the instance.",
                         status_schema, std::vector<std::string> {});
  Json::Value list_schema(Json::objectValue);
  list_schema["type"] = "object";
  list_schema["additionalProperties"] = false;
  list_schema["properties"] = Json::Value(Json::objectValue);
  Json::Value list = ToolDefinition(
                       "eos_log_list",
                       "List the MGM in-memory log and the allowlisted log files.",
                       list_schema, std::vector<std::string> {});
  Json::Value read_schema(Json::objectValue);
  read_schema["type"] = "object";
  read_schema["additionalProperties"] = false;
  read_schema["properties"]["name"] = TextSchema(
                                        "Allowlisted log name returned by eos_log_list.");
  Json::Value lines(Json::objectValue);
  lines["type"] = "integer";
  lines["minimum"] = 1;
  lines["maximum"] = kMaxLogLines;
  lines["description"] = "Trailing lines to return. Defaults to 200.";
  read_schema["properties"]["lines"] = lines;
  read_schema["properties"]["level"] = TextSchema(
                                         "Highest in-memory level when name is memory: "
                                         "emerg, alert, crit, err, warning, notice, info, or debug.");
  Json::Value read = ToolDefinition(
                       "eos_log_read",
                       "Read the tail of one allowlisted log. "
                       "The name memory reads the MGM in-memory ring.",
                       read_schema, std::vector<std::string> {"name"});
  Json::Value tools(Json::arrayValue);
  tools.append(status);
  tools.append(list);
  tools.append(read);

  for (const auto& tool : AdminCommandTools()) {
    tools.append(tool);
  }

  Json::Value result(Json::objectValue);
  result["tools"] = tools;
  return result;
}

Json::Value
ResourceEntry(const char* uri, const char* name, const char* mime,
              const char* description)
{
  Json::Value entry(Json::objectValue);
  entry["uri"] = uri;
  entry["name"] = name;
  entry["mimeType"] = mime;
  entry["description"] = description;
  return entry;
}

Json::Value
ResourcesList()
{
  Json::Value resources(Json::arrayValue);
  resources.append(ResourceEntry("eos://status", "status", "application/json",
                                 "Read-only filesystem view status."));
  resources.append(ResourceEntry("eos://logs", "logs", "application/json",
                                 "Allowlisted administration logs."));
  resources.append(ResourceEntry("eos://logs/memory", "memory", "text/plain",
                                 "Recent lines from the MGM in-memory log."));
  resources.append(ResourceEntry("eos://logs/mgm", "mgm", "text/plain",
                                 "Tail of the MGM xrdlog."));
  resources.append(ResourceEntry("eos://logs/fst", "fst", "text/plain",
                                 "Tail of the FST xrdlog."));
  resources.append(ResourceEntry("eos://logs/mq", "mq", "text/plain",
                                 "Tail of the MQ xrdlog."));
  resources.append(ResourceEntry("eos://logs/logbook", "logbook", "text/plain",
                                 "Tail of the MGM logbook."));
  Json::Value result(Json::objectValue);
  result["resources"] = resources;
  return result;
}

std::string
NegotiatedProtocol(const Json::Value& params)
{
  if (params.isObject() && params.isMember("protocolVersion") &&
      params["protocolVersion"].isString()) {
    const std::string requested = params["protocolVersion"].asString();

    if (OneOf(requested, kSupportedProtocols,
              sizeof(kSupportedProtocols) / sizeof(kSupportedProtocols[0]))) {
      return requested;
    }
  }

  return kServerProtocol;
}

Json::Value
InitializeResult(const std::string& protocol)
{
  Json::Value result(Json::objectValue);
  result["protocolVersion"] = protocol;
  result["capabilities"]["tools"]["listChanged"] = false;
  result["capabilities"]["resources"]["subscribe"] = false;
  result["capabilities"]["resources"]["listChanged"] = false;
  result["serverInfo"]["name"] = "eos-mgm-admin";
  result["serverInfo"]["version"] = "1";
  result["instructions"] =
    "Read-only EOS administration endpoint. Use eos_status to inspect nodes, "
    "spaces, groups, and filesystems. Use eos_log_list and eos_log_read to "
    "read the in-memory MGM log and allowlisted log files. The eos_<command> "
    "tools run read-only admin commands such as ns stat, space ls, node ls "
    "and fs status. No tool can change configuration, write data, or read "
    "arbitrary paths.";
  return result;
}

bool
KnownArguments(const Json::Value& arguments, const std::set<std::string>& allowed,
               std::string& error)
{
  if (!arguments.isObject()) {
    error = "arguments must be an object";
    return false;
  }

  for (const auto& name : arguments.getMemberNames()) {
    if (!allowed.count(name)) {
      error = "unexpected argument: " + name;
      return false;
    }
  }

  return true;
}

McpDispatchResult
Finish(int status, Json::Value body, const std::string& protocol)
{
  McpDispatchResult result;
  result.http_status = status;
  result.protocol_version = protocol;
  result.body = WriteJson(body, false);
  return result;
}

McpDispatchResult
Fail(const Json::Value& id, int http_status, int code, const std::string& message,
     const std::string& protocol)
{
  return Finish(http_status, RpcError(id, code, message), protocol);
}

std::string
LogText(const Json::Value& value)
{
  if (value.isMember("text") && value["text"].isString() &&
      !value["text"].asString().empty()) {
    return value["text"].asString();
  }

  return "(no log lines)";
}

std::string
CommandText(const eos::console::ReplyProto& reply)
{
  std::string text = reply.std_out();

  if (!reply.std_err().empty()) {
    if (!text.empty() && text.back() != '\n') {
      text += '\n';
    }

    text += reply.std_err();
  }

  if (text.size() > kMaxCommandOutputBytes) {
    text.resize(kMaxCommandOutputBytes);
    text += "\n... output truncated\n";
  }

  if (reply.retc()) {
    text = "retc=" + std::to_string(reply.retc()) + "\n" + text;
  }

  return text.empty() ? "(no output)" : text;
}

McpDispatchResult
CallTool(const Json::Value& id, const Json::Value& params,
         const McpAdminSource& source, const eos::common::VirtualIdentity& vid,
         const std::string& protocol)
{
  if (!params.isObject() || !params.isMember("name") || !params["name"].isString()) {
    return Fail(id, 200, -32602, "tools/call requires a tool name", protocol);
  }

  const std::string name = params["name"].asString();
  Json::Value arguments(Json::objectValue);

  if (params.isMember("arguments")) {
    arguments = params["arguments"];
  }

  std::string error;

  if (name == "eos_status") {
    if (!KnownArguments(arguments, {"scope"}, error)) {
      return Fail(id, 200, -32602, error, protocol);
    }

    std::string scope = "all";

    if (arguments.isMember("scope")) {
      if (!arguments["scope"].isString()) {
        return Fail(id, 200, -32602, "scope must be a string", protocol);
      }

      scope = arguments["scope"].asString();
    }

    if (!OneOf(scope, kStatusScopes,
               sizeof(kStatusScopes) / sizeof(kStatusScopes[0]))) {
      return Fail(id, 200, -32602, "unknown status scope", protocol);
    }

    const Json::Value status = source.Status(scope, error);
    const std::string text = error.empty() ? WriteJson(status, true) : error;
    return Finish(200, RpcResult(id, ToolResult(text, !error.empty())), protocol);
  }

  if (name == "eos_log_list") {
    if (!KnownArguments(arguments, {}, error)) {
      return Fail(id, 200, -32602, error, protocol);
    }

    const Json::Value logs = source.ListLogs(error);
    const std::string text = error.empty() ? WriteJson(logs, true) : error;
    return Finish(200, RpcResult(id, ToolResult(text, !error.empty())), protocol);
  }

  if (name == "eos_log_read") {
    if (!KnownArguments(arguments, {"name", "lines", "level"}, error)) {
      return Fail(id, 200, -32602, error, protocol);
    }

    if (!arguments.isMember("name") || !arguments["name"].isString()) {
      return Fail(id, 200, -32602, "name must be a string", protocol);
    }

    const std::string log_name = arguments["name"].asString();

    if (!IsSafeLogName(log_name)) {
      return Fail(id, 200, -32602, "log name is not allowlisted", protocol);
    }

    int lines = kDefaultLogLines;

    if (arguments.isMember("lines")) {
      if (!arguments["lines"].isInt()) {
        return Fail(id, 200, -32602, "lines must be an integer", protocol);
      }

      lines = arguments["lines"].asInt();

      if (lines < 1 || lines > kMaxLogLines) {
        return Fail(id, 200, -32602, "lines is outside the allowed range", protocol);
      }
    }

    std::string level = "info";

    if (arguments.isMember("level")) {
      if (!arguments["level"].isString()) {
        return Fail(id, 200, -32602, "level must be a string", protocol);
      }

      level = arguments["level"].asString();

      if (!OneOf(level, kLogLevels, sizeof(kLogLevels) / sizeof(kLogLevels[0]))) {
        return Fail(id, 200, -32602, "unknown log level", protocol);
      }
    }

    const Json::Value log = source.ReadLog(log_name, lines, level, error);
    const std::string text = error.empty() ? LogText(log) : error;
    return Finish(200, RpcResult(id, ToolResult(text, !error.empty())), protocol);
  }

  if (IsAdminCommandTool(name)) {
    eos::console::RequestProto req;

    if (!BuildAdminRequest(name, arguments, req, error)) {
      return Fail(id, 200, -32602, error, protocol);
    }

    const eos::console::ReplyProto reply = source.RunCommand(std::move(req), vid);
    return Finish(200, RpcResult(id, ToolResult(CommandText(reply),
                                 reply.retc() != 0)), protocol);
  }

  return Fail(id, 200, -32602, "unknown tool", protocol);
}

McpDispatchResult
ReadResource(const Json::Value& id, const Json::Value& params,
             const McpAdminSource& source, const std::string& protocol)
{
  if (!params.isObject() || !params.isMember("uri") || !params["uri"].isString()) {
    return Fail(id, 200, -32602, "resources/read requires a uri", protocol);
  }

  const std::string uri = params["uri"].asString();
  std::string error;
  Json::Value item(Json::objectValue);
  item["uri"] = uri;

  if (uri == "eos://status") {
    const Json::Value status = source.Status("all", error);

    if (!error.empty()) {
      return Fail(id, 200, -32603, error, protocol);
    }

    item["mimeType"] = "application/json";
    item["text"] = WriteJson(status, true);
  } else if (uri == "eos://logs") {
    const Json::Value logs = source.ListLogs(error);

    if (!error.empty()) {
      return Fail(id, 200, -32603, error, protocol);
    }

    item["mimeType"] = "application/json";
    item["text"] = WriteJson(logs, true);
  } else if (uri.compare(0, 11, "eos://logs/") == 0) {
    const std::string name = uri.substr(11);

    if (!IsSafeLogName(name)) {
      return Fail(id, 200, -32002, "log name is not allowlisted", protocol);
    }

    const Json::Value log = source.ReadLog(name, kDefaultLogLines, "info", error);

    if (!error.empty()) {
      return Fail(id, 200, -32002, error, protocol);
    }

    item["mimeType"] = "text/plain";
    item["text"] = LogText(log);
  } else {
    return Fail(id, 200, -32002, "resource not found", protocol);
  }

  Json::Value contents(Json::arrayValue);
  contents.append(item);
  Json::Value result(Json::objectValue);
  result["contents"] = contents;
  return Finish(200, RpcResult(id, result), protocol);
}

eos::common::HttpResponse*
JsonHttp(int code, const std::string& body, const std::string& protocol,
         const std::string& allow)
{
  auto* response = new eos::common::PlainHttpResponse();
  response->SetResponseCode(code);

  if (!body.empty()) {
    response->SetBody(body);
    response->AddHeader("Content-Type", "application/json");
  }

  response->AddHeader("Cache-Control", "no-store");

  if (!protocol.empty()) {
    response->AddHeader("MCP-Protocol-Version", protocol);
  }

  if (!allow.empty()) {
    response->AddHeader("Allow", allow);
  }

  return response;
}

} // namespace

bool
IsMcpPath(const std::string& url)
{
  std::string path = url;
  const auto scheme = path.find("://");

  if (scheme != std::string::npos) {
    const auto slash = path.find('/', scheme + 3);
    path = slash == std::string::npos ? std::string("/") : path.substr(slash);
  }

  const auto query = path.find('?');

  if (query != std::string::npos) {
    path.erase(query);
  }

  const auto fragment = path.find('#');

  if (fragment != std::string::npos) {
    path.erase(fragment);
  }

  while (path.size() > 1 && path.back() == '/') {
    path.pop_back();
  }

  return path == "/mcp";
}

bool
IsMcpReader(const eos::common::VirtualIdentity& vid)
{
  if (vid.uid == eos::common::VirtualIdentity::kNobodyUid) {
    return false;
  }

  eos::common::RWMutexReadLock lock(eos::common::Mapping::gMapMutex);
  return eos::common::Mapping::gMcpReaders.count(vid.uid) != 0;
}

bool
IsMcpCaller(const eos::common::VirtualIdentity& vid)
{
  return ProcInterface::VidIsAdmin(vid) || IsMcpReader(vid);
}

McpDispatchResult
Dispatch(const std::string& body, const McpAdminSource& source,
         const eos::common::VirtualIdentity& vid)
{
  const std::string protocol = kServerProtocol;

  if (body.size() > kMaxRequestBytes) {
    return Fail(Json::Value(), 413, -32600, "MCP request body is too large",
                protocol);
  }

  Json::Value root;
  std::string parse_error;

  if (!ParseJson(body, root, parse_error) || !root.isObject()) {
    return Fail(Json::Value(), 400, -32700, "Parse error", protocol);
  }

  Json::Value id;

  if (root.isMember("id")) {
    id = root["id"];
  }

  if (!root.isMember("jsonrpc") || !root["jsonrpc"].isString() ||
      root["jsonrpc"].asString() != "2.0" || !root.isMember("method") ||
      !root["method"].isString()) {
    return Fail(id.isNull() && !root.isMember("id") ? Json::Value() : id,
                400, -32600, "Invalid Request", protocol);
  }

  const std::string method = root["method"].asString();
  const bool notification = !root.isMember("id") ||
                            method.compare(0, 14, "notifications/") == 0;
  Json::Value params(Json::objectValue);

  if (root.isMember("params")) {
    params = root["params"];
  }

  const std::string selected = method == "initialize" ? NegotiatedProtocol(params)
                               : protocol;

  if (method.compare(0, 14, "notifications/") == 0) {
    McpDispatchResult result;
    result.notification = true;
    result.http_status = 202;
    result.protocol_version = selected;
    return result;
  }

  if (notification) {
    return Fail(Json::Value(), 400, -32600, "Invalid Request", selected);
  }

  if (method == "initialize") {
    return Finish(200, RpcResult(id, InitializeResult(selected)), selected);
  }

  if (method == "ping") {
    return Finish(200, RpcResult(id, Json::Value(Json::objectValue)), selected);
  }

  if (method == "tools/list") {
    return Finish(200, RpcResult(id, ToolsList()), selected);
  }

  if (method == "tools/call") {
    return CallTool(id, params, source, vid, selected);
  }

  if (method == "resources/list") {
    return Finish(200, RpcResult(id, ResourcesList()), selected);
  }

  if (method == "resources/templates/list") {
    Json::Value result(Json::objectValue);
    result["resourceTemplates"] = Json::Value(Json::arrayValue);
    return Finish(200, RpcResult(id, result), selected);
  }

  if (method == "resources/read") {
    return ReadResource(id, params, source, selected);
  }

  return Fail(id, 200, -32601, "Method not found", selected);
}

McpDispatchResult
DispatchForAdmin(const std::string& body,
                 const eos::common::VirtualIdentity& vid)
{
  try {
    return Dispatch(body, EosMcpAdminSource::Instance(), vid);
  } catch (const std::exception& e) {
    eos_static_err("msg=\"mcp dispatch failed\" error=\"%s\"", e.what());
    return Fail(Json::Value(), 500, -32603, "internal error", kServerProtocol);
  }
}

eos::common::HttpResponse*
HandleHttp(eos::common::HttpRequest* request,
           const eos::common::VirtualIdentity* vid,
           const McpAdminSource* source)
{
  if (!request) {
    return JsonHttp(500, "", kServerProtocol, "");
  }

  if (!vid || !IsMcpCaller(*vid)) {
    eos_static_warning("msg=\"mcp admin denied\" uid=%u", vid ? vid->uid : 0);
    const std::string body = WriteJson(
                               RpcError(Json::Value(), -32001,
                                        "admin or mcp reader identity required"), false);
    return JsonHttp(403, body, kServerProtocol, "");
  }

  if (!EqualsIgnoreCase(request->GetMethod(), "POST")) {
    const std::string body = WriteJson(
                               RpcError(Json::Value(), -32600, "MCP endpoint accepts POST only"), false);
    return JsonHttp(405, body, kServerProtocol, "POST");
  }

  const std::string content_type = HeaderValue(*request, "Content-Type");

  if (!HeaderContains(content_type, "application/json")) {
    const std::string body = WriteJson(
                               RpcError(Json::Value(), -32600, "Content-Type must be application/json"),
                               false);
    return JsonHttp(415, body, kServerProtocol, "");
  }

  const std::string accept = HeaderValue(*request, "Accept");

  if (!accept.empty() && !HeaderContains(accept, "application/json") &&
      !HeaderContains(accept, "*/*")) {
    const std::string body = WriteJson(
                               RpcError(Json::Value(), -32600, "Accept must include application/json"),
                               false);
    return JsonHttp(406, body, kServerProtocol, "");
  }

  McpDispatchResult result;

  try {
    result = source ? Dispatch(request->GetBody(), *source, *vid)
             : DispatchForAdmin(request->GetBody(), *vid);
  } catch (const std::exception& e) {
    eos_static_err("msg=\"mcp http failed\" error=\"%s\"", e.what());
    result = Fail(Json::Value(), 500, -32603, "internal error", kServerProtocol);
  }

  return JsonHttp(result.http_status, result.body, result.protocol_version, "");
}

} // namespace mcp

EOSMGMNAMESPACE_END
