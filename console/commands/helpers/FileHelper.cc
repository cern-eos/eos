//------------------------------------------------------------------------------
//! @file FileHelper.cc
//! @author Octavian-Mihai Matei - CERN
//------------------------------------------------------------------------------

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

#include "console/commands/helpers/FileHelper.hh"
#include "common/StringConversion.hh"
#include <XrdOuc/XrdOucString.hh>
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <vector>

namespace {
//------------------------------------------------------------------------------
//! Extract a file id given as "fid:<dec>" or "fxid:<hex>".
//!
//! Equivalent to ConsoleMain's Path2FileDenominator(), reproduced here so that
//! this helper stays linkable outside the console (eos-grpc-ns links it
//! without ConsoleMain)
//!
//! @param raw input, either a path or an id specifier
//! @param id extracted id, only valid if the function returns true
//!
//! @return true if raw holds an id specifier, otherwise false
//------------------------------------------------------------------------------
bool
ExtractFileId(const std::string& raw, unsigned long long& id)
{
  auto try_prefix = [&raw, &id](const std::string& prefix, int base) {
    auto is_digit = [base](char c) {
      const auto uc = static_cast<unsigned char>(c);
      return (base == 16) ? (std::isxdigit(uc) != 0) : (std::isdigit(uc) != 0);
    };

    for (size_t pos = raw.find(prefix); pos != std::string::npos;
         pos = raw.find(prefix, pos + 1)) {
      size_t start = pos + prefix.size();
      size_t end = start;

      while ((end < raw.size()) && is_digit(raw[end])) {
        ++end;
      }

      // At least one digit, and the run must reach the end of the string or
      // of the line - this is what the trailing '$' anchors under REG_NEWLINE
      if ((end == start) || ((end != raw.size()) && (raw[end] != '\n'))) {
        continue;
      }

      id = strtoull(raw.substr(start, end - start).c_str(), nullptr, base);
      return true;
    }

    return false;
  };
  // 'fxid:' is tried first, matching ConsoleMain's ordering
  return try_prefix("fxid:", 16) || try_prefix("fid:", 10);
}
} // namespace

//------------------------------------------------------------------------------
// Apply the path resolver, if any
//------------------------------------------------------------------------------
std::string
FileHelper::AbsPath(const char* in) const
{
  return mPathResolver ? mPathResolver(in) : std::string(in ? in : "");
}

//------------------------------------------------------------------------------
// Detect whether token is a path/fid/fxid-style specifier (as opposed to a
// flag like "-f")
//------------------------------------------------------------------------------
bool
FileHelper::IsPathOrId(const std::string& s)
{
  return !s.empty() &&
         (s[0] == '/' || s.find("fid:") == 0 || s.find("fxid:") == 0 ||
          s.find("pid:") == 0 || s.find("pxid:") == 0 || s.find("inode:") == 0 ||
          s.find("cid:") == 0 || s.find("cxid:") == 0 || s[0] != '-');
}

//------------------------------------------------------------------------------
// Populate the shared eos::console::Metadata target (path or numeric file id)
// from a raw token that may be a plain path, "fid:<dec>" or "fxid:<hex>".
//------------------------------------------------------------------------------
void
FileHelper::SetPathOrId(eos::console::Metadata* md, const std::string& raw) const
{
  unsigned long long id = 0ull;

  if (ExtractFileId(raw, id)) {
    md->set_id(id);
  } else {
    md->set_path(AbsPath(raw.c_str()));
  }
}

//------------------------------------------------------------------------------
// Like SetPathOrId, but also understands the container/inode specifiers
// accepted by 'file info': pid:<dec>, pxid:<hex> (container id) and
// inode:<dec> (fuse inode)
//------------------------------------------------------------------------------
void
FileHelper::SetInfoPathOrId(eos::console::Metadata* md, const std::string& raw) const
{
  if (raw.find("inode:") == 0) {
    try {
      md->set_ino(std::stoull(raw.substr(6)));
      return;
    } catch (const std::exception&) {
      // fall through and treat as a path
    }
  }

  if (raw.find("pid:") == 0 || raw.find("pxid:") == 0) {
    bool isHex = (raw.find("pxid:") == 0);
    std::string num = raw.substr(isHex ? 5 : 4);

    try {
      md->set_id(std::stoull(num, nullptr, isHex ? 16 : 10));
      md->set_type(eos::console::CONTAINER);
      return;
    } catch (const std::exception&) {
      // fall through and treat as a path
    }
  }

  SetPathOrId(md, raw);
}

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool
FileHelper::ParseCommand(const char* arg)
{
  eos::console::FileProto* file = mReq.mutable_file();
  static const char kBackslashSentinel = '\x01';
  std::string sanitized = arg;
  std::replace(sanitized.begin(), sanitized.end(), '\\', kBackslashSentinel);
  std::vector<std::string> all_tokens;
  eos::common::StringConversion::TokenizeQuoted(sanitized, all_tokens, " ");

  for (auto& tok : all_tokens) {
    std::replace(tok.begin(), tok.end(), kBackslashSentinel, '\\');
  }

  if (all_tokens.empty()) {
    return false;
  }

  const std::string& token = all_tokens[0];
  std::vector<std::string> rest(all_tokens.begin() + 1, all_tokens.end());

  if (token == "rename") {
    if (rest.size() < 2) {
      return false;
    }
    eos::console::FileRenameProto* rename = file->mutable_rename();
    SetPathOrId(file->mutable_md(), rest[0]);
    rename->set_new_path(AbsPath(rest[1].c_str()));
  } else if (token == "rename_with_symlink") {
    if (rest.size() < 2) {
      return false;
    }
    eos::console::FileRenameWithSymlinkProto* rws = file->mutable_rename_with_symlink();
    SetPathOrId(file->mutable_md(), rest[0]);
    rws->set_destination_dir(AbsPath(rest[1].c_str()));
  } else if (token == "symlink") {
    std::vector<std::string> positionals;
    bool force = false;

    for (const auto& a : rest) {
      if (a == "-f") {
        force = true;
      } else {
        positionals.push_back(a);
      }
    }

    if (positionals.size() < 2) {
      return false;
    }

    eos::console::FileSymlinkProto* symlink = file->mutable_symlink();
    // positionals[0]=link (symlink name), positionals[1]=target
    SetPathOrId(file->mutable_md(), positionals[0]);
    symlink->set_target_path(positionals[1]);
    symlink->set_force(force);
  } else if (token == "drop") {
    if (rest.size() < 2) {
      return false;
    }

    eos::console::FileDropProto* drop = file->mutable_drop();
    SetPathOrId(file->mutable_md(), rest[0]);

    if (rest[1] == "cache") {
      drop->set_dropcache(true);
    } else {
      try {
        drop->set_fsid(std::stoul(rest[1]));
      } catch (...) {
        return false;
      }

      if (rest.size() > 2 && rest[2] == "-f") {
        drop->set_force(true);
      }
    }
  } else if (token == "touch") {
    eos::console::TouchProto* touch = file->mutable_touch();
    std::string option;
    size_t idx = 0;

    for (; idx < rest.size(); ++idx) {
      if (!rest[idx].empty() && rest[idx][0] == '-') {
        std::string t = rest[idx];
        t.erase(std::remove(t.begin(), t.end(), '-'), t.end());
        option += t;
      } else {
        break;
      }
    }

    if (idx >= rest.size()) {
      return false;
    }

    SetPathOrId(file->mutable_md(), rest[idx]);
    std::string fsid1 = (idx + 1 < rest.size()) ? rest[idx + 1] : "";
    std::string fsid2 = (idx + 2 < rest.size()) ? rest[idx + 2] : "";

    if (option.find('n') != std::string::npos) {
      touch->set_nolayout(true);
    }
    if (option.find('0') != std::string::npos) {
      touch->set_truncate(true);
    }
    if (option.find('a') != std::string::npos) {
      touch->set_absorb(true);
    }
    if (option.find('l') != std::string::npos) {
      touch->set_lockop("lock");
      if (!fsid1.empty()) {
        touch->set_lockop_lifetime(fsid1);
        fsid1.clear();
      }
      if (!fsid2.empty()) {
        if ((fsid2 != "app") && (fsid2 != "user")) {
          return false;
        }
        if (fsid2 == "app") {
          touch->set_wildcard("user");
        } else {
          touch->set_wildcard("app");
        }
        fsid2.clear();
      }
    }
    if (option.find('u') != std::string::npos) {
      touch->set_lockop("unlock");
      fsid1.clear();
      fsid2.clear();
    }
    if (!fsid1.empty()) {
      if (fsid1[0] == '/') {
        touch->set_hardlinkpath(fsid1);
      } else {
        try {
          touch->set_size(std::stoull(fsid1));
        } catch (const std::exception&) {
          return false;
        }
      }
    }
    if (!fsid2.empty()) {
      touch->set_checksuminfo(fsid2);
    }
  } else if (token == "move") {
    if (rest.size() < 3) {
      return false;
    }

    eos::console::FileMoveProto* move = file->mutable_move();
    SetPathOrId(file->mutable_md(), rest[0]);

    try {
      move->set_fsid1(std::stoul(rest[1]));
      move->set_fsid2(std::stoul(rest[2]));
    } catch (...) {
      return false;
    }
  } else if (token == "copy") {
    std::string option;
    size_t idx = 0;

    for (; idx < rest.size(); ++idx) {
      if (!rest[idx].empty() && rest[idx][0] == '-') {
        std::string t = rest[idx];
        t.erase(std::remove(t.begin(), t.end(), '-'), t.end());
        option += t;
      } else {
        break;
      }
    }

    if (idx + 1 >= rest.size()) {
      return false;
    }

    eos::console::FileCopyProto* copy = file->mutable_copy();
    SetPathOrId(file->mutable_md(), rest[idx]);

    if (!option.empty()) {
      std::string checkoption = option;
      checkoption.erase(std::remove(checkoption.begin(), checkoption.end(), 'f'),
                        checkoption.end());
      checkoption.erase(std::remove(checkoption.begin(), checkoption.end(), 's'),
                        checkoption.end());
      checkoption.erase(std::remove(checkoption.begin(), checkoption.end(), 'c'),
                        checkoption.end());

      if (!checkoption.empty()) {
        return false;
      }

      if (option.find('f') != std::string::npos) {
        copy->set_force(true);
      }
      if (option.find('s') != std::string::npos) {
        copy->set_silent(true);
      }
      if (option.find('c') != std::string::npos) {
        copy->set_clone(true);
      }
    }

    copy->set_dst(AbsPath(rest[idx + 1].c_str()));
  } else if (token == "replicate") {
    if (rest.size() < 3) {
      return false;
    }

    eos::console::FileReplicateProto* replicate = file->mutable_replicate();
    SetPathOrId(file->mutable_md(), rest[0]);

    try {
      replicate->set_fsid1(std::stoul(rest[1]));
      replicate->set_fsid2(std::stoul(rest[2]));
    } catch (...) {
      return false;
    }
  } else if (token == "purge" || token == "version") {
    if (rest.empty()) {
      return false;
    }

    int32_t purge_version = -1;

    if (rest.size() > 1) {
      try {
        purge_version = std::stoi(rest[1]);
      } catch (...) {
        return false;
      }
    }

    file->mutable_md()->set_path(AbsPath(rest[0].c_str()));

    if (token == "purge") {
      file->mutable_purge()->set_purge_version(purge_version);
    } else {
      file->mutable_version()->set_purge_version(purge_version);
    }
  } else if (token == "versions") {
    if (rest.empty()) {
      return false;
    }

    eos::console::FileVersionsProto* versions = file->mutable_versions();
    SetPathOrId(file->mutable_md(), rest[0]);
    versions->set_grab_version(rest.size() > 1 ? rest[1] : "-1");
  } else if (token == "layout") {
    if (rest.size() < 2) {
      return false;
    }

    eos::console::FileLayoutProto* layout = file->mutable_layout();
    SetPathOrId(file->mutable_md(), rest[0]);

    if (rest[1] == "-stripes" && rest.size() > 2) {
      try {
        layout->set_stripes(std::stoul(rest[2]));
      } catch (...) {
        return false;
      }
    } else if (rest[1] == "-checksum" && rest.size() > 2) {
      layout->set_checksum(rest[2]);
    } else if (rest[1] == "-type" && rest.size() > 2) {
      layout->set_type(rest[2]);
    } else {
      return false;
    }
  } else if (token == "tag") {
    if (rest.size() < 2) {
      return false;
    }

    eos::console::FileTagProto* tag = file->mutable_tag();
    SetPathOrId(file->mutable_md(), rest[0]);

    // rest[1] format: +|-|~<fsid>
    std::string spec = rest[1];

    if (spec.empty()) {
      return false;
    }

    char prefix = spec[0];
    std::string fsid_str = spec.substr(1);

    if (prefix == '+') {
      tag->set_add(true);
    } else if (prefix == '-') {
      tag->set_remove(true);
    } else if (prefix == '~') {
      tag->set_unlink(true);
    } else {
      fsid_str = spec;
    }

    try {
      tag->set_fsid(std::stoul(fsid_str));
    } catch (...) {
      return false;
    }
  } else if (token == "convert") {
    bool rewrite = false;
    std::vector<std::string> positionals;

    for (const auto& a : rest) {
      if (a == "--rewrite") {
        rewrite = true;
      } else if (a == "--sync") {
        std::cerr << "error: --sync is currently not supported" << std::endl;
        return false;
      } else {
        positionals.push_back(a);
      }
    }

    if (positionals.empty()) {
      return false;
    }

    eos::console::FileConvertProto* convert = file->mutable_convert();
    SetPathOrId(file->mutable_md(), positionals[0]);

    if (positionals.size() > 1) {
      convert->set_layout(positionals[1]);
    }
    if (positionals.size() > 2) {
      convert->set_target_space(positionals[2]);
    }
    if (positionals.size() > 3) {
      convert->set_placement_policy(positionals[3]);
    }
    if (positionals.size() > 4) {
      convert->set_checksum(positionals[4]);
    }

    convert->set_rewrite(rewrite);
  } else if (token == "verify") {
    std::string path;
    std::string filter_fsid;
    std::string rate_val;
    eos::console::FileVerifyProto* verify = file->mutable_verify();

    for (size_t i = 0; i < rest.size(); ++i) {
      const std::string& opt = rest[i];

      if (opt == "-checksum") {
        verify->set_checksum(true);
      } else if (opt == "-commitchecksum") {
        verify->set_commitchecksum(true);
      } else if (opt == "-commitsize") {
        verify->set_commitsize(true);
      } else if (opt == "-commitfmd") {
        verify->set_commitfmd(true);
      } else if (opt == "-rate") {
        if (i + 1 < rest.size()) {
          rate_val = rest[++i];

          try {
            verify->set_rate(std::stoul(rate_val));
          } catch (...) {
            return false;
          }
        } else {
          return false;
        }
      } else if (opt == "-resync") {
        verify->set_resync(true);
      } else if (!path.empty() && !opt.empty() &&
                 std::isdigit(static_cast<unsigned char>(opt[0]))) {
        filter_fsid = opt;

        try {
          verify->set_fsid(std::stoul(filter_fsid));
        } catch (...) {
          return false;
        }
      } else if (IsPathOrId(opt)) {
        if (path.empty()) {
          path = opt;
        } else {
          return false;
        }
      } else {
        return false;
      }
    }

    if (path.empty()) {
      return false;
    }

    SetPathOrId(file->mutable_md(), path);
  } else if (token == "adjustreplica") {
    if (rest.empty()) {
      return false;
    }

    eos::console::FileAdjustreplicaProto* adjust = file->mutable_adjustreplica();
    SetPathOrId(file->mutable_md(), rest[0]);

    std::vector<std::string> args(rest.begin() + 1, rest.end());
    int positional_index = 0;

    for (size_t i = 0; i < args.size(); ++i) {
      if (args[i] == "--exclude-fs") {
        if (i + 1 < args.size()) {
          adjust->set_exclude_fs(args[i + 1]);
          i++;
        } else {
          return false;
        }
      } else if (args[i] == "--nodrop") {
        adjust->set_nodrop(true);
      } else {
        if (positional_index == 0) {
          adjust->set_space(args[i]);
          positional_index++;
        } else if (positional_index == 1) {
          adjust->set_subgroup(args[i]);
          positional_index++;
        } else {
          return false;
        }
      }
    }
  } else if (token == "share") {
    if (rest.empty()) {
      return false;
    }

    eos::console::FileShareProto* share = file->mutable_share();
    file->mutable_md()->set_path(AbsPath(rest[0].c_str()));
    unsigned long long expires = (28ull * 86400ull);

    if (rest.size() > 1) {
      try {
        expires = std::stoull(rest[1]);
      } catch (...) {
        return false;
      }
    }

    share->set_expires(expires);
  } else if (token == "workflow") {
    if (rest.size() < 3) {
      return false;
    }

    eos::console::FileWorkflowProto* workflow = file->mutable_workflow();
    file->mutable_md()->set_path(AbsPath(rest[0].c_str()));
    workflow->set_workflow(rest[1]);
    workflow->set_event(rest[2]);
  } else if (token == "info") {
    eos::console::FileinfoProto* info = file->mutable_fileinfo();
    std::string path;

    for (const auto& arg : rest) {
      if (IsPathOrId(arg)) {
        if (!path.empty()) {
          return false;
        }

        path = arg;
      } else if (arg == "s" || arg == "-s" || arg == "--silent") {
        mIsSilent = true;
      } else if (arg == "--path") {
        info->set_path(true);
      } else if (arg == "--fid") {
        info->set_fid(true);
      } else if (arg == "--fxid") {
        info->set_fxid(true);
      } else if (arg == "--size") {
        info->set_size(true);
      } else if (arg == "--checksum") {
        info->set_checksum(true);
      } else if (arg == "--fullpath") {
        info->set_fullpath(true);
      } else if (arg == "--proxy") {
        info->set_proxy(true);
      } else if (arg == "-m") {
        info->set_monitoring(true);
      } else if (arg == "--env") {
        info->set_env(true);
      } else {
        return false;
      }
    }

    if (path.empty()) {
      return false;
    }

    SetInfoPathOrId(info->mutable_md(), path);
  } else {
    return false;
  }

  return true;
}
