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
#include "console/ConsoleCli.hh"
#include "console/commands/helpers/PathIdentifier.hh"
#include <algorithm>
#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <limits>
#include <map>

namespace {
using eos::console::IdentifierType;
using eos::console::ParsePathIdentifier;

template <typename Number>
bool
ParseUnsigned(const std::string& text, Number& value, int base = 10)
{
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), value, base);
  return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}

CLI::Validator
TargetValidator(bool info = false, bool pathOnly = false)
{
  return CLI::Validator(
      [](const std::string& text) {
        if (text.empty()) {
          return std::string("target must not be empty");
        }
        const auto identifier = ParsePathIdentifier(text);
        if (identifier.type == IdentifierType::Invalid) {
          return std::string("malformed or out-of-range identifier");
        }
        return std::string{};
      },
      info       ? "PATH_OR_IDENTIFIER"
      : pathOnly ? "PATH"
                 : "PATH_OR_FILE_ID");
}

const CLI::Validator LocationSpec{
    [](const std::string& text) {
      const bool prefixed = !text.empty() && (text.front() == '+' ||
                                              text.front() == '-' || text.front() == '~');
      uint32_t fsid = 0;
      return ParseUnsigned(text.substr(prefixed ? 1 : 0), fsid)
                 ? std::string{}
                 : std::string(
                       "location must be a uint32 filesystem id prefixed by +, -, or ~");
    },
    "[+|-|~]FSID"};

const CLI::Validator VersionName{
    [](const std::string& value) {
      return !value.empty() && value != "." && value != ".." &&
                     value.find('/') == std::string::npos
                 ? std::string{}
                 : std::string("version name must be a non-empty basename");
    },
    "VERSION_NAME"};

const CLI::Validator DecimalDuration{
    [](const std::string& text) {
      size_t offset = 0;
      bool digits = false;
      while (offset < text.size() && text[offset] >= '0' && text[offset] <= '9') {
        ++offset;
        digits = true;
      }
      if (offset < text.size() && text[offset] == '.') {
        ++offset;
        while (offset < text.size() && text[offset] >= '0' && text[offset] <= '9') {
          ++offset;
          digits = true;
        }
      }
      while (offset < text.size() &&
             std::isspace(static_cast<unsigned char>(text[offset]))) {
        ++offset;
      }
      for (; offset < text.size(); ++offset) {
        if (!std::isalpha(static_cast<unsigned char>(text[offset]))) {
          return std::string(
              "expected a non-negative decimal duration with an optional unit");
        }
      }
      return digits ? std::string{}
                    : std::string("duration must contain a decimal number");
    },
    "DECIMAL_DURATION"};

struct FileOptions {
  CLI::App app{"File handling", "file"};
  std::map<std::string, CLI::App*> commands;
  std::string source, destination, dropLocation, tagLocation, first, second;
  std::string layoutChecksum, layoutType, grabVersion = "-1";
  std::string conversionLayout, space, placement, checksum, workflow, event;
  uint32_t fsid1 = 0, fsid2 = 0, stripes = 0, rate = 0, excludeFs = 0;
  int32_t purgeVersion = -1, subgroup = -1;
  double lifetime = 28 * 86400;
  bool force = false, silent = false, clone = false, noLayout = false;
  bool truncate = false, absorb = false, lock = false, unlock = false;
  bool rewrite = false, compress = false, uncompress = false;
  bool createFolder = false, noDrop = false;
  bool verifyChecksum = false, commitChecksum = false, commitSize = false;
  bool commitFmd = false, resync = false;
  bool infoPath = false, infoFid = false, infoFxid = false, infoSize = false;
  bool infoChecksum = false, fullPath = false, proxy = false, monitoring = false;
  bool env = false;

  CLI::App*
  Add(const std::string& name, const std::string& description, bool info = false,
      bool pathOnly = false)
  {
    auto* command = app.add_subcommand(name, description);
    commands.emplace(name, command);
    command->add_option("source", source)
        ->required()
        ->check(TargetValidator(info, pathOnly));
    return command;
  }

  FileOptions()
  {
    app.require_subcommand(1, 1);
    auto* rename = Add("rename", "Rename a file or directory");
    rename->add_option("new-path", destination)->required()->check(TargetValidator());
    auto* renameLink = Add("rename_with_symlink",
                           "Move a file and create a symlink at its original path");
    renameLink->add_option("destination-dir", destination)
        ->required()
        ->check(TargetValidator());
    auto* symlink = Add("symlink", "Create a symlink");
    symlink->add_flag("-f", force, "Overwrite the destination");
    symlink->add_option("target", destination)->required()->check(TargetValidator());
    auto* drop = Add("drop", "Drop a disk replica or read-through cache location");
    drop->add_option("location", dropLocation)
        ->required()
        ->check(CLI::Validator(
            [](const std::string& value) {
              uint32_t fsid = 0;
              return value == "cache" || ParseUnsigned(value, fsid)
                         ? std::string{}
                         : std::string(
                               "location must be a uint32 filesystem id or cache");
            },
            "FSID_OR_CACHE"));
    drop->add_flag("-f", force, "Force replica removal");
    auto* touch = Add("touch", "Create or touch a file, or manage an attribute lock");
    touch->add_flag("-n", noLayout, "Disable placement logic");
    touch->add_flag("-0", truncate, "Truncate the file");
    touch->add_flag("-a", absorb, "Absorb a file from its hardlink path");
    auto* lockFlag = touch->add_flag("-l", lock, "Create an attribute lock");
    auto* unlockFlag = touch->add_flag("-u", unlock, "Remove an attribute lock");
    lockFlag->excludes(unlockFlag);
    auto* firstArgument = touch->add_option("size-or-link-or-lifetime", first)
                              ->check(eos::console::NonEmptyArgument);
    auto* secondArgument = touch->add_option("checksum-or-audience", second)
                               ->check(eos::console::NonEmptyArgument)
                               ->needs(firstArgument);
    unlockFlag->excludes(firstArgument)->excludes(secondArgument);
    for (const auto* name : {"move", "replicate"}) {
      auto* transfer = Add(name, "Transfer a file replica between filesystems");
      transfer->add_option("source-fsid", fsid1)
          ->required()
          ->transform(eos::console::DecimalNumber);
      transfer->add_option("destination-fsid", fsid2)
          ->required()
          ->transform(eos::console::DecimalNumber);
    }
    auto* copy = Add("copy", "Copy a file or directory");
    copy->add_flag("-f", force, "Overwrite the destination");
    copy->add_flag("-s", silent, "Suppress copy output");
    copy->add_flag("-c", clone, "Preserve ctime and mtime");
    copy->add_option("destination", destination)->required()->check(TargetValidator());
    for (const auto* name : {"purge", "version"}) {
      auto* version = Add(name, "Manage file version retention", false, true);
      auto* count = version->add_option("purge-version", purgeVersion)
                        ->transform(eos::console::SignedDecimalNumber)
                        ->check(CLI::Range(-1, std::numeric_limits<int32_t>::max()));
      if (std::string(name) == "version") {
        count->check(CLI::Validator(
            [](const std::string& value) {
              return value == "0" ? std::string("version count must be -1 or positive")
                                  : std::string{};
            },
            "NONZERO"));
        version
            ->add_flag("--create-folder", createFolder,
                       "Only create the version directory")
            ->excludes(count);
      }
    }
    auto* versions = Add("versions", "List versions or stage a version");
    versions->add_option("grab-version", grabVersion)->check(VersionName);
    auto* layout = Add("layout", "Change one layout property");
    layout->allow_non_standard_option_names();
    auto* layoutSelector =
        layout->add_option_group("selector", "Choose exactly one layout property");
    layoutSelector->allow_non_standard_option_names();
    layoutSelector->require_option(1, 1);
    layoutSelector->add_option("-stripes", stripes)
        ->transform(eos::console::DecimalNumber)
        ->check(CLI::Range(1, 255));
    layoutSelector->add_option("-checksum", layoutChecksum)
        ->check(eos::console::NonEmptyArgument);
    layoutSelector->add_option("-type", layoutType)
        ->check(CLI::Validator(
            [](const std::string& text) {
              const std::string value = text.substr(0, 2) == "0x" ? text.substr(2) : text;
              uint32_t parsed = 0;
              return ParseUnsigned(value, parsed, 16)
                         ? std::string{}
                         : std::string("layout type must be a hexadecimal uint32 value");
            },
            "HEX_LAYOUT"));
    auto* tag = Add("tag", "Add, remove, or unlink a filesystem location");
    tag->add_option("location", tagLocation)->required()->check(LocationSpec);
    auto* convert = Add("convert", "Convert a file layout");
    convert->add_option("layout", conversionLayout)
        ->check(eos::console::NonEmptyArgument);
    convert->add_option("target-space", space)->check(eos::console::NonEmptyArgument);
    convert->add_option("placement-policy", placement)
        ->check(eos::console::NonEmptyArgument);
    convert->add_option("checksum", checksum)->check(eos::console::NonEmptyArgument);
    convert->add_flag("--rewrite", rewrite, "Rewrite all replicas");
    auto* compressFlag = convert->add_flag(
                           "--compress", compress,
                           "Mark the file for at-rest ZSTD compression");
    auto* uncompressFlag = convert->add_flag(
                             "--uncompress", uncompress,
                             "Rewrite the file back to plain bytes");
    compressFlag->excludes(uncompressFlag);
    uncompressFlag->excludes(compressFlag);
    auto* verify = Add("verify", "Verify a file against its disk replicas");
    verify->allow_non_standard_option_names();
    verify->add_option("fsid", fsid1)->transform(eos::console::DecimalNumber);
    verify->add_flag("-checksum", verifyChecksum);
    verify->add_flag("-commitchecksum", commitChecksum);
    verify->add_flag("-commitsize", commitSize);
    verify->add_flag("-commitfmd", commitFmd);
    verify->add_flag("-resync", resync);
    verify->add_option("-rate", rate)->transform(eos::console::DecimalNumber);
    auto* adjust = Add("adjustreplica", "Adjust the number of file replicas");
    adjust->add_option("space", space)->check(eos::console::NonEmptyArgument);
    adjust->add_option("subgroup", subgroup)
        ->transform(eos::console::SignedDecimalNumber)
        ->check(CLI::Range(-1, std::numeric_limits<int32_t>::max()));
    adjust->add_flag("--nodrop", noDrop);
    adjust->add_option("--exclude-fs", excludeFs)->transform(eos::console::DecimalNumber);
    auto* share = Add("share", "Create a share link", false, true);
    share
        ->add_option("lifetime", lifetime,
                     "Lifetime in seconds, s, min, h, d, w, mo, or y (default: 28d)")
        ->transform(
            CLI::AsNumberWithUnit(std::map<std::string, double>{{"s", 1},
                                                                {"min", 60},
                                                                {"h", 3600},
                                                                {"d", 86400},
                                                                {"w", 7 * 86400},
                                                                {"mo", 31 * 86400},
                                                                {"y", 365 * 86400}}))
        ->transform(DecimalDuration)
        ->check(
            CLI::Range(0.0, static_cast<double>(std::numeric_limits<uint32_t>::max())));
    auto* trigger = Add("workflow", "Trigger a workflow event");
    trigger->add_option("workflow", workflow)
        ->required()
        ->check(eos::console::NonEmptyArgument);
    trigger->add_option("event", event)
        ->required()
        ->check(eos::console::NonEmptyArgument);
    auto* info = Add("info", "Show file, container, or inode information", true);
    info->add_flag("-s,--silent", silent);
    info->add_flag("--path", infoPath);
    info->add_flag("--fid", infoFid);
    info->add_flag("--fxid", infoFxid);
    info->add_flag("--size", infoSize);
    info->add_flag("--checksum", infoChecksum);
    info->add_flag("--fullpath", fullPath);
    info->add_flag("--proxy", proxy);
    info->add_flag("-m", monitoring);
    info->add_flag("--env", env);
  }
};
} // namespace

std::string
FileHelper::AbsPath(const char* in) const
{
  return mPathResolver ? mPathResolver(in) : std::string(in ? in : "");
}

void
FileHelper::SetPathOrId(eos::console::Metadata* md, const std::string& raw, bool info,
                        bool pathOnly) const
{
  const auto identifier = ParsePathIdentifier(raw);
  if (!pathOnly && (identifier.type == IdentifierType::File ||
                    (info && identifier.type == IdentifierType::Container))) {
    md->set_id(identifier.value);
    md->set_type(identifier.type == IdentifierType::File ? eos::console::FILE
                                                         : eos::console::CONTAINER);
  } else if (info && identifier.type == IdentifierType::Inode) {
    md->set_ino(identifier.value);
  } else {
    md->set_path(AbsPath(raw.c_str()));
  }
}

bool
FileHelper::ParseCommand(const char* arg)
{
  std::vector<std::string> args;
  if (!eos::console::TokenizeArguments(arg, args)) {
    mReq.clear_file();
    mIsSilent = false;
    return false;
  }
  return ParseArguments(args);
}

bool
FileHelper::ParseArguments(const std::vector<std::string>& args)
{
  mReq.clear_file();
  mIsSilent = false;
  FileOptions options;
  if (!eos::console::ParseCliArguments(options.app, args)) {
    return false;
  }
  eos::console::FileProto file;
  const std::string command = options.app.get_subcommands().front()->get_name();
  SetPathOrId(command == "info" ? file.mutable_fileinfo()->mutable_md()
                                : file.mutable_md(),
              options.source, command == "info",
              command == "purge" || command == "version" || command == "share");
  if (command == "rename") {
    file.mutable_rename()->set_new_path(AbsPath(options.destination.c_str()));
  } else if (command == "rename_with_symlink") {
    file.mutable_rename_with_symlink()->set_destination_dir(
        AbsPath(options.destination.c_str()));
  } else if (command == "symlink") {
    auto* symlink = file.mutable_symlink();
    symlink->set_target_path(options.destination);
    symlink->set_force(options.force);
  } else if (command == "drop") {
    auto* drop = file.mutable_drop();
    if (options.dropLocation == "cache") {
      if (options.force) {
        std::cerr << "error: force removal does not apply to cache locations\n";
        return false;
      }
      drop->set_dropcache(true);
    } else {
      uint32_t fsid = 0;
      ParseUnsigned(options.dropLocation, fsid);
      drop->set_fsid(fsid);
      drop->set_force(options.force);
    }
  } else if (command == "touch") {
    auto* touch = file.mutable_touch();
    touch->set_nolayout(options.noLayout);
    touch->set_truncate(options.truncate);
    touch->set_absorb(options.absorb);
    if (options.lock) {
      touch->set_lockop("lock");
      if (!options.first.empty()) {
        uint32_t lifetime = 0;
        if (!ParseUnsigned(options.first, lifetime) ||
            lifetime > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
          std::cerr
              << "error: lock lifetime must be a non-negative int32 number of seconds\n";
          return false;
        }
        touch->set_lockop_lifetime(options.first);
      }
      if (!options.second.empty()) {
        if (options.second != "app" && options.second != "user") {
          std::cerr << "error: lock audience must be app or user\n";
          return false;
        }
        touch->set_wildcard(options.second == "app" ? "user" : "app");
      }
    } else if (options.unlock) {
      touch->set_lockop("unlock");
    } else {
      if (!options.first.empty()) {
        if (options.first.front() == '/') {
          touch->set_hardlinkpath(options.first);
        } else {
          uint64_t size = 0;
          if (!ParseUnsigned(options.first, size)) {
            std::cerr << "error: touch size must be a uint64 number of bytes\n";
            return false;
          }
          touch->set_size(size);
        }
      }
      if (!options.second.empty() &&
          (options.second.size() > 64 || options.second.size() % 2 != 0 ||
           !std::all_of(options.second.begin(), options.second.end(),
                        [](unsigned char ch) { return std::isxdigit(ch) != 0; }))) {
        std::cerr << "error: checksum must contain at most 64 hexadecimal digits in "
                     "complete bytes\n";
        return false;
      }
      touch->set_checksuminfo(options.second);
    }
  } else if (command == "move") {
    file.mutable_move()->set_fsid1(options.fsid1);
    file.mutable_move()->set_fsid2(options.fsid2);
  } else if (command == "replicate") {
    file.mutable_replicate()->set_fsid1(options.fsid1);
    file.mutable_replicate()->set_fsid2(options.fsid2);
  } else if (command == "copy") {
    auto* copy = file.mutable_copy();
    copy->set_dst(AbsPath(options.destination.c_str()));
    copy->set_force(options.force);
    copy->set_silent(options.silent);
    copy->set_clone(options.clone);
  } else if (command == "purge") {
    file.mutable_purge()->set_purge_version(options.purgeVersion);
  } else if (command == "version") {
    file.mutable_version()->set_purge_version(options.purgeVersion);
    file.mutable_version()->set_create_folder(options.createFolder);
  } else if (command == "versions") {
    file.mutable_versions()->set_grab_version(options.grabVersion);
  } else if (command == "layout") {
    auto* layout = file.mutable_layout();
    layout->set_stripes(options.stripes);
    layout->set_checksum(options.layoutChecksum);
    layout->set_type(options.layoutType);
  } else if (command == "tag") {
    auto* tag = file.mutable_tag();
    const char prefix = options.tagLocation.front();
    const bool prefixed = prefix == '+' || prefix == '-' || prefix == '~';
    uint32_t fsid = 0;
    ParseUnsigned(options.tagLocation.substr(prefixed ? 1 : 0), fsid);
    tag->set_fsid(fsid);
    tag->set_add(prefix == '+');
    tag->set_remove(prefix == '-');
    tag->set_unlink(prefix == '~');
  } else if (command == "convert") {
    auto* convert = file.mutable_convert();
    convert->set_layout(options.conversionLayout);
    convert->set_target_space(options.space);
    convert->set_placement_policy(options.placement);
    convert->set_checksum(options.checksum);
    convert->set_rewrite(options.rewrite);
    convert->set_compress(options.compress);
    convert->set_uncompress(options.uncompress);
  } else if (command == "verify") {
    auto* verify = file.mutable_verify();
    verify->set_fsid(options.fsid1);
    verify->set_rate(options.rate);
    verify->set_checksum(options.verifyChecksum);
    verify->set_commitchecksum(options.commitChecksum);
    verify->set_commitsize(options.commitSize);
    verify->set_commitfmd(options.commitFmd);
    verify->set_resync(options.resync);
  } else if (command == "adjustreplica") {
    auto* adjust = file.mutable_adjustreplica();
    adjust->set_space(options.space);
    adjust->set_nodrop(options.noDrop);
    const auto* parsed = options.commands.at(command);
    if (parsed->get_option("subgroup")->count()) {
      adjust->set_subgroup(std::to_string(options.subgroup));
    }
    if (parsed->get_option("--exclude-fs")->count()) {
      adjust->set_exclude_fs(std::to_string(options.excludeFs));
    }
  } else if (command == "share") {
    const std::time_t now = std::time(nullptr);
    if (now < 0 || !std::isfinite(options.lifetime) ||
        options.lifetime > static_cast<double>(std::numeric_limits<uint32_t>::max()) -
                               static_cast<double>(now)) {
      std::cerr << "error: share expiration exceeds the uint32 epoch range\n";
      return false;
    }
    file.mutable_share()->set_expires(static_cast<uint32_t>(now + options.lifetime));
  } else if (command == "workflow") {
    file.mutable_workflow()->set_workflow(options.workflow);
    file.mutable_workflow()->set_event(options.event);
  } else if (command == "info") {
    auto* info = file.mutable_fileinfo();
    mIsSilent = options.silent;
    info->set_path(options.infoPath);
    info->set_fid(options.infoFid);
    info->set_fxid(options.infoFxid);
    info->set_size(options.infoSize);
    info->set_checksum(options.infoChecksum);
    info->set_fullpath(options.fullPath);
    info->set_proxy(options.proxy);
    info->set_monitoring(options.monitoring);
    info->set_env(options.env);
  }
  *mReq.mutable_file() = std::move(file);
  return true;
}
