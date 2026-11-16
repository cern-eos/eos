// ----------------------------------------------------------------------
// File: McpAdminCommands.cc
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

#include "mgm/mcp/McpAdminCommands.hh"
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/util/message_differencer.h>
#include <memory>
#include <set>
#include <vector>

EOSMGMNAMESPACE_BEGIN

namespace mcp {

namespace {

using google::protobuf::Descriptor;
using google::protobuf::FieldDescriptor;
using google::protobuf::Message;
using google::protobuf::Reflection;

//------------------------------------------------------------------------------
//! One read-only subcommand. Every entry was audited for side effects: a
//! field is only listed if no value of it changes state, and values that
//! select an operation are pinned in fixed.
//------------------------------------------------------------------------------
struct CommandSpec {
  const char* tool;
  const char* command;
  //! Field path in RequestProto selecting the subcommand
  const char* root;
  //! Field paths, relative to root, that the caller may set
  std::vector<std::string> params;
  //! JSON merged into root after the caller's params
  const char* fixed;
};

const std::vector<CommandSpec>&
Specs()
{
  static const std::vector<CommandSpec> specs = {
    {
      "eos_ns_stat", "ns stat", "ns.stat",
      {"Groupids", "Monitor", "NumericIds", "Summary", "Apps"}, ""
    },
    {"eos_ns_master_log", "ns master --log", "ns.master", {}, R"({"op":"LOG"})"},
    {"eos_ns_drain_list", "ns drain list", "ns.drain", {}, ""},
    {
      "eos_ns_tracker_list", "ns tracker list", "ns.tracker", {"name"},
      R"({"op":"LIST"})"
    },
    {
      "eos_ns_behaviour_list", "ns behaviour list", "ns.behaviour", {},
      R"({"op":"LIST"})"
    },
    {
      "eos_ns_behaviour_get", "ns behaviour get", "ns.behaviour", {"name"},
      R"({"op":"GET"})"
    },
    {"eos_fs_ls", "fs ls", "fs.ls", {"display", "Brief", "Matchlist"}, ""},
    {
      "eos_fs_status", "fs status", "fs.status",
      {"Fsid", "NodeQueue", "LongFormat", "RiskAssessment"}, ""
    },
    {
      "eos_fs_dumpmd", "fs dumpmd", "fs.dumpmd",
      {"Fsid", "display", "ShowFid", "ShowPath", "ShowSize", "ShowFxid", "ShowCount"},
      ""
    },
    {"eos_fs_compare", "fs compare", "fs.compare", {"SourceId", "TargetId"}, ""},
    {"eos_node_ls", "node ls", "node.ls", {"outhost", "outformat", "selection"}, ""},
    {"eos_node_status", "node status", "node.status", {"node"}, ""},
    {
      "eos_group_ls", "group ls", "group.ls",
      {"outdepth", "outhost", "outformat", "selection"}, ""
    },
    {"eos_space_ls", "space ls", "space.ls", {"outdepth", "outformat", "selection"}, ""},
    {"eos_space_status", "space status", "space.status", {"mgmspace", "outformat_m"}, ""},
    {
      "eos_space_node_get", "space node-get", "space.nodeGet",
      {"mgmspace", "nodeget_key"}, ""
    },
    {"eos_space_tracker", "space tracker", "space.tracker", {"mgmspace"}, ""},
    // Only the monitoring dump: the other formats re-apply the space config
    // and may enable or disable the inspector.
    {
      "eos_space_inspector", "space inspector -m", "space.inspector", {"mgmspace"},
      R"({"options":"m"})"
    },
    {
      "eos_space_groupbalancer_status", "space groupbalancer status",
      "space.groupbalancer", {"mgmspace", "status.options"}, R"({"status":{}})"
    },
    {
      "eos_space_groupdrainer_status", "space groupdrainer status",
      "space.groupdrainer", {"mgmspace", "status.outformat"}, R"({"status":{}})"
    },
    {"eos_df", "df", "df", {"monitoring", "si", "readable", "path"}, ""},
    {
      "eos_io_stat", "io stat", "io.stat",
      {
        "details", "monitoring", "numerical", "top", "domain", "apps", "summary",
        "sample_stat", "time_ago", "time_interval"
      }, ""
    },
    {
      "eos_io_ns", "io ns", "io.ns",
      {"monitoring", "rank_by_byte", "rank_by_access", "last_week", "hotfiles", "count"},
      ""
    },
    {
      "eos_io_shaping_list", "io shaping ls", "io.shaping.list",
      {
        "show_apps", "show_users", "show_groups", "show_nodes", "json_output",
        "time_window_seconds", "system_stats", "show_fs", "show_all", "resolve_ids"
      }, ""
    },
    {
      "eos_io_shaping_policy_list", "io shaping policy ls", "io.shaping.policy.list",
      {
        "filter_apps", "filter_users", "filter_groups", "json_output",
        "show_controller_limits"
      }, ""
    },
    {
      "eos_io_shaping_config_list", "io shaping config ls", "io.shaping.config.list",
      {"json_output"}, ""
    },
    {
      "eos_io_shaping_pressure_list", "io shaping pressure ls",
      "io.shaping.pressure.list", {"json_output"}, ""
    },
    {"eos_fsck_stat", "fsck stat", "fsck", {}, R"({"stat":true})"},
    {
      "eos_fsck_report", "fsck report", "fsck.report",
      {"display_per_fs", "display_fxid", "display_lfn", "display_json", "tags"}, ""
    },
    {"eos_devices_ls", "devices ls", "devices.ls", {"outformat"}, ""},
    {"eos_monit_config_ls", "monit config ls", "monit.config", {}, R"({"ls":{}})"},
    {"eos_config_ls", "config ls", "config.ls", {"showbackup"}, ""},
    {"eos_config_dump", "config dump", "config.dump", {"file"}, ""},
    {"eos_access_ls", "access ls", "access.ls", {"monitoring", "id2name"}, ""},
    {"eos_debug_get", "debug get", "debug.get", {}, ""},
    {"eos_convert_list", "convert list", "convert.list", {}, ""},
    {"eos_convert_config", "convert config list", "convert.config", {}, ""},
    {"eos_sched_ls", "sched ls", "sched.ls", {"option", "spacename"}, ""},
    {
      "eos_sched_show", "sched configure show", "sched.config",
      {"show.option", "show.spacename"}, R"({"show":{}})"
    },
    {
      "eos_sched_disabled_ls", "sched disabled ls", "sched.disabled", {"spacename"},
      R"({"op":"LS"})"
    },
    {"eos_route_list", "route ls", "route.list", {"path"}, ""},
    {
      "eos_recycle_ls", "recycle ls", "recycle.ls",
      {
        "FullDetails", "NumericIds", "MonitorFmt", "Date", "Maxentries", "Type",
        "RecycleId", "Path"
      }, ""
    }
  };
  return specs;
}

const CommandSpec*
FindSpec(const std::string& tool)
{
  for (const auto& spec : Specs()) {
    if (tool == spec.tool) {
      return &spec;
    }
  }

  return nullptr;
}

std::string
Join(const std::string& prefix, const std::string& name)
{
  return prefix.empty() ? name : prefix + "." + name;
}

std::vector<std::string>
SplitPath(const std::string& path)
{
  std::vector<std::string> parts;
  std::string::size_type start = 0;

  while (true) {
    const auto dot = path.find('.', start);
    parts.push_back(path.substr(start, dot - start));

    if (dot == std::string::npos) {
      break;
    }

    start = dot + 1;
  }

  return parts;
}

void
AddWithPrefixes(std::set<std::string>& paths, const std::string& path)
{
  for (auto dot = path.find('.'); dot != std::string::npos;
       dot = path.find('.', dot + 1)) {
    paths.insert(path.substr(0, dot));
  }

  paths.insert(path);
}

const FieldDescriptor*
SubmessageField(const Descriptor* desc, const std::string& name)
{
  const FieldDescriptor* field = desc->FindFieldByName(name);

  if (!field || field->is_repeated() ||
      field->cpp_type() != FieldDescriptor::CPPTYPE_MESSAGE) {
    return nullptr;
  }

  return field;
}

Message*
MutableRoot(Message& msg, const std::string& path)
{
  Message* current = &msg;

  for (const auto& name : SplitPath(path)) {
    const FieldDescriptor* field = SubmessageField(current->GetDescriptor(), name);

    if (!field) {
      return nullptr;
    }

    current = current->GetReflection()->MutableMessage(current, field);
  }

  return current;
}

bool
HasRoot(const Message& msg, const std::string& path)
{
  const Message* current = &msg;

  for (const auto& name : SplitPath(path)) {
    const FieldDescriptor* field = SubmessageField(current->GetDescriptor(), name);
    const Reflection* refl = current->GetReflection();

    if (!field || !refl->HasField(*current, field)) {
      return false;
    }

    current = &refl->GetMessage(*current, field);
  }

  return true;
}

void
CollectSetPaths(const Message& msg, const std::string& prefix,
                std::vector<std::string>& paths)
{
  const Reflection* refl = msg.GetReflection();

  if (refl->GetUnknownFields(msg).field_count()) {
    paths.push_back(Join(prefix, "<unknown>"));
  }

  std::vector<const FieldDescriptor*> fields;
  refl->ListFields(msg, &fields);

  for (const FieldDescriptor* field : fields) {
    const std::string path = Join(prefix, field->name());
    paths.push_back(path);

    if (field->cpp_type() != FieldDescriptor::CPPTYPE_MESSAGE) {
      continue;
    }

    if (field->is_repeated()) {
      for (int i = 0; i < refl->FieldSize(msg, field); ++i) {
        CollectSetPaths(refl->GetRepeatedMessage(msg, field, i), path, paths);
      }
    } else {
      CollectSetPaths(refl->GetMessage(msg, field), path, paths);
    }
  }
}

bool
ParseInto(const std::string& json, Message& msg, std::string& error)
{
  google::protobuf::util::JsonParseOptions options;
  options.ignore_unknown_fields = false;
  const auto status = google::protobuf::util::JsonStringToMessage(json, &msg,
                      options);

  if (!status.ok()) {
    error = "invalid params: " + status.ToString();
    return false;
  }

  return true;
}

std::unique_ptr<Message>
NewRootMessage(const CommandSpec& spec)
{
  eos::console::RequestProto req;
  Message* root = MutableRoot(req, spec.root);
  return std::unique_ptr<Message>(root ? root->New() : nullptr);
}

std::unique_ptr<Message>
FixedMessage(const CommandSpec& spec, std::string& error)
{
  std::unique_ptr<Message> fixed = NewRootMessage(spec);

  if (!fixed) {
    error = "invalid command root";
    return nullptr;
  }

  if (*spec.fixed && !ParseInto(spec.fixed, *fixed, error)) {
    return nullptr;
  }

  return fixed;
}

bool
Validate(const eos::console::RequestProto& req, const CommandSpec& spec,
         std::string& error)
{
  if (!HasRoot(req, spec.root)) {
    error = std::string("request is not '") + spec.command + "'";
    return false;
  }

  const std::unique_ptr<Message> fixed = FixedMessage(spec, error);

  if (!fixed) {
    return false;
  }

  std::set<std::string> allowed = {"format", "DontColor"};
  AddWithPrefixes(allowed, spec.root);

  for (const auto& param : spec.params) {
    AddWithPrefixes(allowed, Join(spec.root, param));
  }

  std::vector<std::string> fixed_paths;
  CollectSetPaths(*fixed, spec.root, fixed_paths);
  allowed.insert(fixed_paths.begin(), fixed_paths.end());
  std::vector<std::string> paths;
  CollectSetPaths(req, "", paths);

  for (const auto& path : paths) {
    if (!allowed.count(path)) {
      error = std::string("field is not allowed for '") + spec.command + "': " + path;
      return false;
    }
  }

  if (req.format() != eos::console::RequestProto::DEFAULT &&
      req.format() != eos::console::RequestProto::JSON) {
    error = "output format is not allowed";
    return false;
  }

  eos::console::RequestProto expected(req);
  MutableRoot(expected, spec.root)->MergeFrom(*fixed);

  if (!google::protobuf::util::MessageDifferencer::Equals(req, expected)) {
    error = std::string("operation is not allowed for '") + spec.command + "'";
    return false;
  }

  return true;
}

Json::Value
MessageSchema(const Descriptor* desc, const std::string& prefix,
              const std::set<std::string>& allowed);

Json::Value
FieldSchema(const FieldDescriptor* field, const std::string& path,
            const std::set<std::string>& allowed)
{
  Json::Value schema(Json::objectValue);

  switch (field->cpp_type()) {
  case FieldDescriptor::CPPTYPE_BOOL:
    schema["type"] = "boolean";
    break;

  case FieldDescriptor::CPPTYPE_UINT32:
  case FieldDescriptor::CPPTYPE_UINT64:
    schema["type"] = "integer";
    schema["minimum"] = 0;
    break;

  case FieldDescriptor::CPPTYPE_INT32:
  case FieldDescriptor::CPPTYPE_INT64:
    schema["type"] = "integer";
    break;

  case FieldDescriptor::CPPTYPE_FLOAT:
  case FieldDescriptor::CPPTYPE_DOUBLE:
    schema["type"] = "number";
    break;

  case FieldDescriptor::CPPTYPE_STRING:
    schema["type"] = "string";
    break;

  case FieldDescriptor::CPPTYPE_ENUM: {
    schema["type"] = "string";
    Json::Value values(Json::arrayValue);

    for (int i = 0; i < field->enum_type()->value_count(); ++i) {
      values.append(field->enum_type()->value(i)->name());
    }

    schema["enum"] = values;
    break;
  }

  case FieldDescriptor::CPPTYPE_MESSAGE:
    schema = MessageSchema(field->message_type(), path, allowed);
    break;
  }

  if (field->is_repeated()) {
    Json::Value array(Json::objectValue);
    array["type"] = "array";
    array["items"] = schema;
    return array;
  }

  return schema;
}

Json::Value
MessageSchema(const Descriptor* desc, const std::string& prefix,
              const std::set<std::string>& allowed)
{
  Json::Value schema(Json::objectValue);
  schema["type"] = "object";
  schema["additionalProperties"] = false;
  schema["properties"] = Json::Value(Json::objectValue);

  for (int i = 0; i < desc->field_count(); ++i) {
    const FieldDescriptor* field = desc->field(i);
    const std::string path = Join(prefix, field->name());

    if (allowed.count(path)) {
      schema["properties"][field->name()] = FieldSchema(field, path, allowed);
    }
  }

  return schema;
}

std::set<std::string>
AllowedParams(const CommandSpec& spec)
{
  std::set<std::string> allowed;

  for (const auto& param : spec.params) {
    AddWithPrefixes(allowed, param);
  }

  return allowed;
}

std::string
WriteCompactJson(const Json::Value& value)
{
  Json::StreamWriterBuilder builder;
  builder["indentation"] = "";
  builder["commentStyle"] = "None";
  return Json::writeString(builder, value);
}

} // namespace

Json::Value
AdminCommandTools()
{
  Json::Value tools(Json::arrayValue);

  for (const auto& spec : Specs()) {
    const std::unique_ptr<Message> root = NewRootMessage(spec);

    if (!root) {
      continue;
    }

    Json::Value schema(Json::objectValue);
    schema["type"] = "object";
    schema["additionalProperties"] = false;
    schema["properties"]["params"] = MessageSchema(root->GetDescriptor(), "",
                                     AllowedParams(spec));
    schema["properties"]["params"]["description"] =
      "Command fields, as in the eos-protobuf-spec message.";
    schema["properties"]["json"]["type"] = "boolean";
    schema["properties"]["json"]["description"] =
      "Return JSON output, like eos --json.";
    Json::Value tool(Json::objectValue);
    tool["name"] = spec.tool;
    tool["description"] = std::string("Run the read-only admin command 'eos ") +
                          spec.command + "' with the caller's identity.";
    tool["inputSchema"] = schema;
    tool["annotations"]["readOnlyHint"] = true;
    tools.append(tool);
  }

  return tools;
}

bool
IsAdminCommandTool(const std::string& name)
{
  return FindSpec(name) != nullptr;
}

bool
BuildAdminRequest(const std::string& tool, const Json::Value& arguments,
                  eos::console::RequestProto& req, std::string& error)
{
  const CommandSpec* spec = FindSpec(tool);

  if (!spec) {
    error = "unknown tool";
    return false;
  }

  if (!arguments.isObject()) {
    error = "arguments must be an object";
    return false;
  }

  for (const auto& name : arguments.getMemberNames()) {
    if (name != "params" && name != "json") {
      error = "unexpected argument: " + name;
      return false;
    }
  }

  req.Clear();
  req.set_dontcolor(true);

  if (arguments.isMember("json")) {
    if (!arguments["json"].isBool()) {
      error = "json must be a boolean";
      return false;
    }

    if (arguments["json"].asBool()) {
      req.set_format(eos::console::RequestProto::JSON);
    }
  }

  Message* root = MutableRoot(req, spec->root);

  if (!root) {
    error = "invalid command root";
    return false;
  }

  if (arguments.isMember("params")) {
    if (!arguments["params"].isObject()) {
      error = "params must be an object";
      return false;
    }

    std::unique_ptr<Message> params(root->New());

    if (!ParseInto(WriteCompactJson(arguments["params"]), *params, error)) {
      return false;
    }

    const std::set<std::string> allowed = AllowedParams(*spec);
    std::vector<std::string> paths;
    CollectSetPaths(*params, "", paths);

    for (const auto& path : paths) {
      if (!allowed.count(path)) {
        error = "parameter is not allowed: " + path;
        return false;
      }
    }

    root->MergeFrom(*params);
  }

  const std::unique_ptr<Message> fixed = FixedMessage(*spec, error);

  if (!fixed) {
    return false;
  }

  root->MergeFrom(*fixed);
  return Validate(req, *spec, error);
}

bool
IsReadOnlyAdminRequest(const eos::console::RequestProto& req,
                       std::string& error)
{
  error = "command is not in the read-only allowlist";

  for (const auto& spec : Specs()) {
    if (!HasRoot(req, spec.root)) {
      continue;
    }

    std::string spec_error;

    if (Validate(req, spec, spec_error)) {
      error.clear();
      return true;
    }

    error = spec_error;
  }

  return false;
}

} // namespace mcp

EOSMGMNAMESPACE_END
