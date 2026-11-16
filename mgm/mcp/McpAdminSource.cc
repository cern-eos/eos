// ----------------------------------------------------------------------
// File: McpAdminSource.cc
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

#include "mgm/mcp/McpAdminSource.hh"
#include "mgm/mcp/McpAdminCommands.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/proc/IProcCommand.hh"
#include "mgm/proc/ProcInterface.hh"
#include "common/Constants.hh"
#include "common/FileSystem.hh"
#include "common/Logging.hh"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <sstream>
#include <vector>

EOSMGMNAMESPACE_BEGIN

namespace mcp {

namespace {

constexpr std::size_t kMaxFilesystemEntries = 4096;
constexpr off_t kMaxLogBytes = 1 << 20;

struct AllowedLog {
  const char* name;
  bool memory;
  const char* relative[2];
};

const AllowedLog kLogs[] = {
  {"memory", true, {nullptr, nullptr}},
  {"mgm", false, {"mgm/xrdlog.mgm", "xrdlog.mgm"}},
  {"fst", false, {"fst/xrdlog.fst", "xrdlog.fst"}},
  {"mq", false, {"mq/xrdlog.mq", "xrdlog.mq"}},
  {"logbook", false, {"mgm/logbook.log", nullptr}},
  {"eosxd-stacktraces", false, {"mgm/eosxd-stacktraces.log", nullptr}},
  {"eosxd-logtraces", false, {"mgm/eosxd-logtraces.log", nullptr}},
};

const AllowedLog*
FindLog(const std::string& name)
{
  for (const auto& log : kLogs) {
    if (name == log.name) {
      return &log;
    }
  }

  return nullptr;
}

bool
SafeLogRoot(std::string& root, std::string& error)
{
  const char* env = std::getenv("XRDLOGDIR");
  root = (env && *env) ? env : "/var/log/eos";

  if (root.empty() || root.front() != '/' || root.find("..") != std::string::npos) {
    error = "log directory is not available";
    return false;
  }

  while (root.size() > 1 && root.back() == '/') {
    root.pop_back();
  }

  return true;
}

bool
StatRegular(const std::string& path, off_t& size)
{
  struct stat st;

  if (lstat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
    return false;
  }

  size = st.st_size;
  return true;
}

class FileDescriptor
{
public:
  explicit FileDescriptor(int fd) : mFd(fd) {}

  ~FileDescriptor()
  {
    if (mFd >= 0) {
      ::close(mFd);
    }
  }

  FileDescriptor(const FileDescriptor&) = delete;
  FileDescriptor& operator=(const FileDescriptor&) = delete;

  int get() const
  {
    return mFd;
  }

private:
  int mFd;
};

bool
ReadTail(int fd, int lines, std::string& text, bool& truncated, std::string& error)
{
  struct stat st;

  if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0) {
    error = "log file cannot be read";
    return false;
  }

  off_t start = 0;

  if (st.st_size > kMaxLogBytes) {
    start = st.st_size - kMaxLogBytes;
    truncated = true;
  }

  if (lseek(fd, start, SEEK_SET) < 0) {
    error = "log file cannot be read";
    return false;
  }

  std::string data;
  data.resize(static_cast<std::size_t>(st.st_size - start));
  std::size_t got = 0;

  while (got < data.size()) {
    const ssize_t n = ::read(fd, &data[got], data.size() - got);

    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }

      error = "log file cannot be read";
      return false;
    }

    if (n == 0) {
      data.resize(got);
      break;
    }

    got += static_cast<std::size_t>(n);
  }

  std::size_t begin = 0;

  if (truncated) {
    const auto newline = data.find('\n');

    if (newline != std::string::npos) {
      begin = newline + 1;
    }
  }

  std::vector<std::string> collected;
  std::size_t cursor = begin;

  while (cursor < data.size()) {
    const auto newline = data.find('\n', cursor);

    if (newline == std::string::npos) {
      collected.emplace_back(data.substr(cursor));
      break;
    }

    std::string line = data.substr(cursor, newline - cursor);

    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }

    collected.emplace_back(std::move(line));
    cursor = newline + 1;
  }

  while (!collected.empty() && collected.back().empty()) {
    collected.pop_back();
  }

  if (static_cast<int>(collected.size()) > lines) {
    collected.erase(collected.begin(),
                    collected.end() - static_cast<std::ptrdiff_t>(lines));
    truncated = true;
  }

  std::ostringstream out;

  for (const auto& line : collected) {
    out << line << '\n';
  }

  if (truncated) {
    out << "# truncated\n";
  }

  text = out.str();
  return true;
}

Json::Value
ReadMemory(int lines, const std::string& level, std::string& error)
{
  eos::common::Logging& logging = eos::common::Logging::GetInstance();
  const int max_priority = logging.GetPriorityByString(level.c_str());

  if (max_priority < 0) {
    error = "unknown log level";
    return Json::Value();
  }

  std::vector<std::string> collected;
  bool truncated = false;
  {
    XrdSysMutexHelper lock(logging.gMutex);
    const unsigned long ring = logging.gCircularIndexSize;

    if (ring > 0) {
      for (int priority = max_priority;
           priority >= 0 && static_cast<int>(collected.size()) < lines;
           --priority) {
        if (static_cast<std::size_t>(priority) >= logging.gLogMemory.size()) {
          continue;
        }

        const auto& slots = logging.gLogMemory[priority];

        if (slots.size() < ring) {
          continue;
        }

        const unsigned long next = logging.gLogCircularIndex[priority];

        for (int i = 1; i <= lines && static_cast<int>(collected.size()) < lines;
             ++i) {
          const unsigned long slot = (next + ring - static_cast<unsigned long>(i)) %
                                     ring;
          std::string line = slots[slot].c_str();

          if (line.empty()) {
            break;
          }

          if (!line.empty() && line.back() == '\n') {
            line.pop_back();
          }

          collected.emplace_back(std::move(line));
        }
      }
    }
  }
  std::ostringstream out;
  out << "# memory level=" << level << '\n';
  std::size_t bytes = 0;

  for (const auto& line : collected) {
    if (bytes >= static_cast<std::size_t>(kMaxLogBytes)) {
      truncated = true;
      break;
    }

    out << line << '\n';
    bytes += line.size() + 1;
  }

  if (truncated) {
    out << "# truncated\n";
  }

  Json::Value value(Json::objectValue);
  value["name"] = "memory";
  value["level"] = level;
  value["truncated"] = truncated;
  value["text"] = out.str();
  return value;
}

Json::Value
ViewEntry(const std::string& name, const std::string& status, std::size_t filesystems)
{
  Json::Value item(Json::objectValue);
  item["name"] = name;
  item["status"] = status;
  item["filesystems"] = static_cast<Json::UInt64>(filesystems);
  return item;
}

} // namespace

Json::Value
EosMcpAdminSource::Status(const std::string& scope, std::string& error) const
{
  const bool all = scope == "all";
  const bool want_nodes = all || scope == "nodes";
  const bool want_spaces = all || scope == "spaces";
  const bool want_groups = all || scope == "groups";
  const bool want_filesystems = all || scope == "filesystems";

  if (!want_nodes && !want_spaces && !want_groups && !want_filesystems) {
    error = "unknown status scope";
    return Json::Value();
  }

  Json::Value root(Json::objectValue);
  root["scope"] = scope;

  if (gOFS) {
    root["mgm"] = std::string(gOFS->MgmOfsQueue.c_str());
  }

  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  root["counts"]["nodes"] = static_cast<Json::UInt64>(FsView::gFsView.mNodeView.size());
  root["counts"]["spaces"] = static_cast<Json::UInt64>(FsView::gFsView.mSpaceView.size());
  root["counts"]["groups"] = static_cast<Json::UInt64>(FsView::gFsView.mGroupView.size());
  root["counts"]["filesystems"] = static_cast<Json::UInt64>(
                                    std::distance(FsView::gFsView.mIdView.begin(),
                                        FsView::gFsView.mIdView.end()));

  if (want_nodes) {
    Json::Value nodes(Json::arrayValue);

    for (const auto& entry : FsView::gFsView.mNodeView) {
      FsNode* node = entry.second;

      if (!node) {
        continue;
      }

      Json::Value item = ViewEntry(node->mName, node->GetStatus(), node->size());
      item["queue"] = entry.first;
      item["active"] = eos::common::FileSystem::GetActiveStatusAsString(
                         node->GetActiveStatus());
      item["heartbeat"] = node->HasHeartbeat();
      nodes.append(std::move(item));
    }

    root["nodes"] = nodes;
  }

  if (want_spaces) {
    Json::Value spaces(Json::arrayValue);

    for (const auto& entry : FsView::gFsView.mSpaceView) {
      FsSpace* space = entry.second;

      if (!space) {
        continue;
      }

      spaces.append(ViewEntry(space->mName, space->GetStatus(), space->size()));
    }

    root["spaces"] = spaces;
  }

  if (want_groups) {
    Json::Value groups(Json::arrayValue);

    for (const auto& entry : FsView::gFsView.mGroupView) {
      FsGroup* group = entry.second;

      if (!group) {
        continue;
      }

      groups.append(ViewEntry(group->mName, group->GetStatus(), group->size()));
    }

    root["groups"] = groups;
  }

  if (want_filesystems) {
    Json::Value filesystems(Json::arrayValue);
    bool truncated = false;
    std::size_t total = 0;

    for (const auto& entry : FsView::gFsView.mIdView) {
      ++total;
      FileSystem* fs = entry.second;

      if (!fs) {
        continue;
      }

      if (filesystems.size() >= kMaxFilesystemEntries) {
        truncated = true;
        continue;
      }

      Json::Value item(Json::objectValue);
      item["id"] = static_cast<Json::UInt64>(fs->GetId());
      item["host"] = fs->GetString("host");
      item["path"] = fs->GetPath();
      item["boot"] = fs->GetString("stat.boot");
      item["configstatus"] = fs->GetString("configstatus");
      item["schedgroup"] = fs->GetString("schedgroup");
      item["active"] = eos::common::FileSystem::GetActiveStatusAsString(
                         fs->GetActiveStatus());
      filesystems.append(std::move(item));
    }

    root["filesystems"] = filesystems;
    root["filesystems_total"] = static_cast<Json::UInt64>(total);
    root["filesystems_truncated"] = truncated;
  }

  return root;
}

Json::Value
EosMcpAdminSource::ListLogs(std::string& /*error*/) const
{
  std::string root;
  std::string root_error;
  const bool have_root = SafeLogRoot(root, root_error);
  Json::Value logs(Json::arrayValue);

  for (const auto& spec : kLogs) {
    Json::Value item(Json::objectValue);
    item["name"] = spec.name;

    if (spec.memory) {
      item["kind"] = "memory";
      item["available"] = true;
      item["description"] = "MGM in-memory log ring";
    } else {
      item["kind"] = "file";
      item["relative"] = spec.relative[0] ? spec.relative[0] : "";
      off_t size = 0;
      bool available = false;

      if (have_root) {
        for (const char* relative : spec.relative) {
          if (!relative) {
            continue;
          }

          if (StatRegular(root + "/" + relative, size)) {
            item["relative"] = relative;
            available = true;
            break;
          }
        }
      }

      item["available"] = available;

      if (available) {
        item["bytes"] = static_cast<Json::UInt64>(size);
      }
    }

    logs.append(std::move(item));
  }

  Json::Value result(Json::objectValue);
  result["logs"] = logs;

  if (!have_root) {
    result["log_directory"] = root_error;
  }

  return result;
}

Json::Value
EosMcpAdminSource::ReadLog(const std::string& name, int lines,
                           const std::string& level, std::string& error) const
{
  if (!IsSafeLogName(name)) {
    error = "log name is not allowlisted";
    return Json::Value();
  }

  if (lines < 1 || lines > kMaxLogLines) {
    error = "lines is outside the allowed range";
    return Json::Value();
  }

  const AllowedLog* spec = FindLog(name);

  if (!spec) {
    error = "log name is not allowlisted";
    return Json::Value();
  }

  const std::string selected_level = level.empty() ? "info" : level;

  if (spec->memory) {
    return ReadMemory(lines, selected_level, error);
  }

  std::string root;

  if (!SafeLogRoot(root, error)) {
    return Json::Value();
  }

  int opened = -1;
  bool refused = false;

  for (const char* relative : spec->relative) {
    if (!relative) {
      continue;
    }

    const std::string path = root + "/" + relative;
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);

    if (fd >= 0) {
      opened = fd;
      break;
    }

    if (errno != ENOENT) {
      refused = true;
      break;
    }
  }

  if (opened < 0) {
    error = refused ? "log file cannot be read" : "log file is not available";
    return Json::Value();
  }

  FileDescriptor descriptor(opened);
  std::string text;
  bool truncated = false;

  if (!ReadTail(descriptor.get(), lines, text, truncated, error)) {
    return Json::Value();
  }

  Json::Value value(Json::objectValue);
  value["name"] = name;
  value["truncated"] = truncated;
  value["text"] = text;
  return value;
}

eos::console::ReplyProto
EosMcpAdminSource::RunCommand(eos::console::RequestProto&& req,
                              const eos::common::VirtualIdentity& vid) const
{
  eos::console::ReplyProto reply;
  std::string error;

  if (!IsReadOnlyAdminRequest(req, error)) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: " + error);
    return reply;
  }

  if (!IsMcpCaller(vid)) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: admin or mcp reader identity required");
    return reply;
  }

  eos::common::VirtualIdentity caller = vid;

  if (!ProcInterface::VidIsAdmin(caller)) {
    // MCP readers are not admins. Grant the admin group for this one
    // allowlisted command only, uid and name stay those of the caller.
    caller.allowed_gids.insert(eos::common::ADM_GID);
    eos_static_info("msg=\"mcp reader runs admin command\" uid=%u name=\"%s\" "
                    "cmd=\"%s\"", caller.uid, caller.name.c_str(),
                    ProcInterface::ToLogJson(req).c_str());
  }

  std::unique_ptr<IProcCommand> cmd =
    ProcInterface::HandleProtobufRequest(req, caller);

  if (!cmd) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: command refused");
    return reply;
  }

  return cmd->ProcessRequest();
}

const EosMcpAdminSource&
EosMcpAdminSource::Instance()
{
  static const EosMcpAdminSource source;
  return source;
}

} // namespace mcp

EOSMGMNAMESPACE_END
