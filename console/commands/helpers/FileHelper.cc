//------------------------------------------------------------------------------
//! @file FileHelper.cc
//! @author Octavian Matei - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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
#include "console/ConsoleMain.hh"
#include "proto/File.pb.h"
#include "common/SymKeys.hh"
#include "common/LayoutId.hh"
#include "json/json.h"

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool
FileHelper::ParseCommand(const char* arg)
{
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  const char* temp = tokenizer.GetToken(false);

  if (!temp) {
    std::cerr << "error: no subcommand specified" << std::endl;
    return false;
  }

  std::string subcommand(temp);
  bool isOk;

  if (subcommand == "info") {
    isOk = ParseInfo(tokenizer);
  } else if (subcommand == "touch") {
    isOk = ParseTouch(tokenizer);
  } else if (subcommand == "adjustreplica") {
    isOk = ParseAdjustreplica(tokenizer);
  } else if (subcommand == "check") {
    isOk = ParseCheck(tokenizer);
  } else if (subcommand == "convert") {
    isOk = ParseConvert(tokenizer);
  } else if (subcommand == "copy") {
    isOk = ParseCopy(tokenizer);
  } else if (subcommand == "drop") {
    isOk = ParseDrop(tokenizer);
  } else if (subcommand == "layout") {
    isOk = ParseLayout(tokenizer);
  } else if (subcommand == "move") {
    isOk = ParseMove(tokenizer);
  } else if (subcommand == "purge") {
    isOk = ParsePurge(tokenizer);
  } else if (subcommand == "rename") {
    isOk = ParseRename(tokenizer);
  } else if (subcommand == "rename_with_symlink") {
    isOk = ParseRenameWithSymlink(tokenizer);
  } else if (subcommand == "replicate") {
    isOk = ParseReplicate(tokenizer);
  } else if (subcommand == "share") {
    isOk = ParseShare(tokenizer);
  } else if (subcommand == "symlink") {
    isOk = ParseSymlink(tokenizer);
  } else if (subcommand == "tag") {
    isOk = ParseTag(tokenizer);
  } else if (subcommand == "verify") {
    isOk = ParseVerify(tokenizer);
  } else if (subcommand == "version") {
    isOk = ParseVersion(tokenizer);
  } else if (subcommand == "versions") {
    isOk = ParseVersions(tokenizer);
  } else if (subcommand == "workflow") {
    isOk = ParseWorkflow(tokenizer);
  } else {
    std::cerr << "error: unknown subcommand '" << subcommand << "'" << std::endl;
    isOk = false;
  }

  return isOk;
}


//------------------------------------------------------------------------------
//! Return Fmd from a remote filesystem
//!
//! @param manager host:port of the server to contact
//! @param shexfid hex string of the file id
//! @param sfsid string of filesystem id
//! @param fmd reference to the Fmd struct to store Fmd
//------------------------------------------------------------------------------
int
FileHelper::GetRemoteFmdFromLocalDb(const char* manager, const char* shexfid,
                                    const char* sfsid, eos::common::FmdHelper& fmd)
{
  if ((!manager) || (!shexfid) || (!sfsid)) {
    return EINVAL;
  }

  int rc = 0;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status;
  XrdOucString fmdquery = "/?fst.pcmd=getfmd&fst.getfmd.fid=";
  fmdquery += shexfid;
  fmdquery += "&fst.getfmd.fsid=";
  fmdquery += sfsid;
  XrdOucString address = "root://";
  address += manager;
  address += "//dummy";
  XrdCl::URL url(address.c_str());

  if (!url.IsValid()) {
    eos_static_err("error=URL is not valid: %s", address.c_str());
    return EINVAL;
  }

  std::unique_ptr<XrdCl::FileSystem> fs(new XrdCl::FileSystem(url));

  if (!fs) {
    eos_static_err("error=failed to get new FS object");
    return EINVAL;
  }

  arg.FromString(fmdquery.c_str());
  status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg, response);

  if (status.IsOK()) {
    rc = 0;
    eos_static_debug("got replica file meta data from server %s for fxid=%s fsid=%s",
                     manager, shexfid, sfsid);
  } else {
    rc = ECOMM;
    eos_static_err("Unable to retrieve meta data from server %s for fxid=%s fsid=%s",
                   manager, shexfid, sfsid);
  }

  if (rc) {
    delete response;
    return EIO;
  }

  if (!strncmp(response->GetBuffer(), "ERROR", 5)) {
    // remote side couldn't get the record
    eos_static_info("Unable to retrieve meta data on remote server %s for fxid=%s fsid=%s",
                    manager, shexfid, sfsid);
    delete response;
    return ENODATA;
  }

  // get the remote file meta data into an env hash
  XrdOucEnv fmdenv(response->GetBuffer());

  if (!eos::common::EnvToFstFmd(fmdenv, fmd)) {
    int envlen;
    eos_static_err("Failed to unparse file meta data %s", fmdenv.Env(envlen));
    delete response;
    return EIO;
  }

  // very simple check
  if (fmd.mProtoFmd.fid() != eos::common::FileId::Hex2Fid(shexfid)) {
    eos_static_err("Uups! Received wrong meta data from remote server - fid "
                   "is %lu instead of %lu !", fmd.mProtoFmd.fid(),
                   eos::common::FileId::Hex2Fid(shexfid));
    delete response;
    return EIO;
  }

  delete response;
  return 0;
}

//------------------------------------------------------------------------------
// Execute command with special handling for check
//------------------------------------------------------------------------------
int
FileHelper::Execute(bool print_err, bool add_route)
{
  // Check if this is a check command - needs special formatting
  if (mReq.file().has_check()) {
    int retc = ICmdHelper::ExecuteWithoutPrint(add_route);

    if (retc == 0 && !mOutcome.result.empty()) {
      std::string json_result = mOutcome.result;
      size_t json_pos = json_result.find("mgm.proc.json=");

      if (json_pos != std::string::npos) {
        json_result = json_result.substr(json_pos + 14);
        size_t retc_pos = json_result.find("&mgm.proc.retc=");

        if (retc_pos != std::string::npos) {
          json_result = json_result.substr(0, retc_pos);
        }

        XrdOucString sealed = json_result.c_str();
        eos::common::StringConversion::UnSeal(sealed);
        json_result = sealed.c_str();
      }

      if (mGlobalOpts.mJsonFormat) {
        std::cout << json_result << std::endl;
        return retc;
      }

      std::string options = mReq.file().check().options();
      FormatCheckOutput(json_result, options);
    }

    if (print_err && !mOutcome.error.empty()) {
      std::cerr << GetError();
    }

    return retc;
  }

  // Standard execution for other commands
  return ICmdHelper::Execute(print_err, add_route);
}

//------------------------------------------------------------------------------
// Format check command output from server response
//------------------------------------------------------------------------------
void
FileHelper::FormatCheckOutput(const std::string& response,
                              const std::string& options)
{
  // Parse JSON response
  Json::Value json;
  Json::CharReaderBuilder builder;
  std::istringstream stream(response);
  std::string errs;

  if (!Json::parseFromStream(builder, stream, &json, &errs)) {
    std::cerr << "error: failed to parse JSON response\n";
    return;
  }

  // Check for error response
  if (json.isMember("errc")) {
    std::cerr << "error: " << json.get("errmsg",
                                       "unknown error").asString() << "\n";
    return;
  }

  // Extract required fields with validation
  if (!json.isMember("path") || !json.isMember("checksum") ||
      !json.isMember("size") || !json.isMember("nrep")) {
    std::vector<std::string> missing;

    if (!json.isMember("path")) {
      missing.push_back("path");
    }

    if (!json.isMember("checksum")) {
      missing.push_back("checksum");
    }

    if (!json.isMember("size")) {
      missing.push_back("size");
    }

    if (!json.isMember("nrep")) {
      missing.push_back("nrep");
    }

    std::cerr << "error: incomplete metadata from server - missing: ";

    for (size_t i = 0; i < missing.size(); ++i) {
      if (i > 0) {
        std::cerr << ", ";
      }

      std::cerr << missing[i];
    }

    std::cerr << "\n";
    return;
  }

  std::string ns_path = json.get("path", "").asString();
  std::string checksum = json.get("checksum", "").asString();
  std::string checksumtype = json.get("checksumtype", "unknown").asString();
  uint64_t mgm_size = json.get("size", 0).asUInt64();
  int nrep_count = json.get("nrep", 0).asInt();
  int stripes = json.get("nstripes", 0).asInt();
  std::string fid = json.get("fid", "unknown").asString();
  bool silent_cmd = (options.find("%silent") != std::string::npos) || mIsSilent;

  // Print header
  if (!silent_cmd) {
    fprintf(stdout, "path=\"%s\" fxid=\"%s\" size=\"%llu\" nrep=\"%d\" "
            "checksumtype=\"%s\" checksum=\"%s\"\n",
            ns_path.c_str(), fid.c_str(),
            (unsigned long long)mgm_size, nrep_count,
            checksumtype.c_str(), checksum.c_str());
  }

  std::string err_label;
  std::set<std::string> set_errors;
  int nrep_online = 0;

  // Iterate through replicas
  if (!json.isMember("replicas") || !json["replicas"].isArray()) {
    std::cerr << "error: no replica information in response\n";
    return;
  }

  const Json::Value& replicas = json["replicas"];

  for (Json::ArrayIndex i = 0; i < replicas.size(); ++i) {
    err_label = "none";
    const Json::Value& replica = replicas[i];
    std::string repurl = replica.get("hostport", "").asString();
    std::string repfid = replica.get("fid", "").asString();
    int repfsid = replica.get("fsid", 0).asInt();
    std::string repbootstat = replica.get("bootstat", "").asString();
    std::string repfstpath = replica.get("fstpath", "").asString();

    if (repurl.empty()) {
      continue;
    }

    // Query the FSTs for stripe info
    XrdCl::StatInfo* stat_info = 0;
    XrdCl::XRootDStatus status;
    std::ostringstream oss;
    oss << "root://" << repurl << "//dummy";
    XrdCl::URL url(oss.str());

    if (!url.IsValid()) {
      std::cerr << "error: URL is not valid: " << oss.str() << std::endl;
      continue;
    }

    std::unique_ptr<XrdCl::FileSystem> fs(new XrdCl::FileSystem(url));

    if (!fs) {
      std::cerr << "error: failed to get new FS object" << std::endl;
      continue;
    }

    bool down = (repbootstat != "booted");

    if (down && (options.find("%force") == std::string::npos)) {
      err_label = "DOWN";
      set_errors.insert(err_label);

      if (!silent_cmd) {
        fprintf(stderr, "error: unable to retrieve file meta data from %s "
                "[ status=%s ]\n", repurl.c_str(), repbootstat.c_str());
      }

      continue;
    }

    // Do a remote stat using XrdCl::FileSystem
    uint64_t stat_size = std::numeric_limits<uint64_t>::max();
    std::string statpath = repfstpath;

    if (!statpath.empty() && statpath[0] != '/') {
      // base 64 encode this path
      XrdOucString statpath_xrd = statpath.c_str();
      XrdOucString statpath64;
      eos::common::SymKey::Base64(statpath_xrd, statpath64);
      statpath = "/#/";
      statpath += statpath64.c_str();
    }

    status = fs->Stat(statpath.c_str(), stat_info);

    if (!status.IsOK()) {
      err_label = "STATFAILED";
      set_errors.insert(err_label);
    } else {
      stat_size = stat_info->GetSize();
    }

    delete stat_info;
    int retc = 0;
    eos::common::FmdHelper fmd;

    if ((retc = GetRemoteFmdFromLocalDb(repurl.c_str(), repfid.c_str(),
                                        std::to_string(repfsid).c_str(), fmd))) {
      if (!silent_cmd) {
        fprintf(stderr, "error: unable to retrieve file meta data from %s [%d]\n",
                repurl.c_str(), retc);
      }

      err_label = "NOFMD";
      set_errors.insert(err_label);
    } else {
      const auto& proto_fmd = fmd.mProtoFmd;
      std::string cx = proto_fmd.checksum();

      // Pad checksum to 32 bytes (64 hex chars)
      while (cx.length() < 64) {
        cx += "00";
      }

      std::string disk_cx = proto_fmd.diskchecksum();

      while (disk_cx.length() < 64) {
        disk_cx += "00";
      }

      if (eos::common::LayoutId::IsRain(proto_fmd.lid()) == false) {
        // These checks make sense only for non-rain layouts
        if (proto_fmd.size() != mgm_size) {
          err_label = "SIZE";
          set_errors.insert(err_label);
        } else {
          if (proto_fmd.size() != stat_size) {
            err_label = "FSTSIZE";
            set_errors.insert(err_label);
          }
        }

        if (cx != checksum) {
          err_label = "CHECKSUM";
          set_errors.insert(err_label);
        }

        uint64_t disk_cx_val = 0ull;

        try {
          disk_cx_val = std::stoull(disk_cx.substr(0, 8), nullptr, 16);
        } catch (...) {
          // error during conversion
        }

        if ((disk_cx.length() > 0) && disk_cx_val &&
            ((disk_cx.length() < 8) ||
             (cx.substr(0, disk_cx.length()) != disk_cx))) {
          err_label = "DISK_CHECKSUM";
          set_errors.insert(err_label);
        }

        if (!silent_cmd) {
          fprintf(stdout, "nrep=\"%02d\" fsid=\"%d\" host=\"%s\" fstpath=\"%s\" "
                  "size=\"%llu\" statsize=\"%llu\" checksum=\"%s\" diskchecksum=\"%s\" "
                  "error_label=\"%s\"\n",
                  (int)i, repfsid, repurl.c_str(), repfstpath.c_str(),
                  (unsigned long long)proto_fmd.size(),
                  (unsigned long long)(stat_size),
                  cx.c_str(), disk_cx.c_str(), err_label.c_str());
        }
      } else {
        // For RAIN layouts we only check for block-checksum errors
        if (proto_fmd.blockcxerror()) {
          err_label = "BLOCK_XS";
          set_errors.insert(err_label);
        }

        if (!silent_cmd) {
          fprintf(stdout, "nrep=\"%02d\" fsid=\"%d\" host=\"%s\" fstpath=\"%s\" "
                  "size=\"%llu\" statsize=\"%llu\" error_label=\"%s\"\n",
                  (int)i, repfsid, repurl.c_str(), repfstpath.c_str(),
                  (unsigned long long)proto_fmd.size(),
                  (unsigned long long)(stat_size), err_label.c_str());
        }
      }

      ++nrep_online;
    }
  }

  if (nrep_count != stripes) {
    if (set_errors.find("NOFMD") == set_errors.end()) {
      err_label = "NUM_REPLICAS";
      set_errors.insert(err_label);
    }
  }

  if (set_errors.size()) {
    if (options.find("%output") != std::string::npos) {
      fprintf(stdout, "INCONSISTENCY %s path=%-32s fxid=%s size=%llu "
              "stripes=%d nrep=%d nrepstored=%d nreponline=%d "
              "checksumtype=%s checksum=%s\n", set_errors.begin()->c_str(),
              ns_path.c_str(), fid.c_str(),
              (unsigned long long)mgm_size, stripes, nrep_count, nrep_online, nrep_online,
              checksumtype.c_str(), checksum.c_str());
    }
  }
}

//------------------------------------------------------------------------------
// Parse touch subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseTouch(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  auto* touch = file->mutable_touch();
  std::string token;
  const char* temp;
  std::string path;
  std::string options;

  // Parse options
  while ((temp = tokenizer.GetToken(false)) != 0) {
    token = std::string(temp);

    if (token[0] == '-') {
      // Remove leading dashes
      size_t pos = 0;

      while (pos < token.length() && token[pos] == '-') {
        pos++;
      }

      options += token.substr(pos);
    } else {
      path = token;
    }
  }

  if (path.empty()) {
    std::cerr << "error: touch requires a path" << std::endl;
    return false;
  }

  if (!SetPath(path)) {
    std::cerr << "error: invalid path" << std::endl;
    return false;
  }

  // Process option flags
  if (options.find('n') != std::string::npos) {
    touch->set_nolayout(true);
  }

  if (options.find('0') != std::string::npos) {
    touch->set_truncate(true);
  }

  if (options.find('a') != std::string::npos) {
    touch->set_absorb(true);
  }

  if (options.find('l') != std::string::npos) {
    touch->set_lockop("lock");

    // Check for lock lifetime
    if ((temp = tokenizer.GetToken(false)) != 0) {
      token = std::string(temp);
      touch->set_lockop_lifetime(token);

      // Check for wildcard (app/user)
      if ((temp = tokenizer.GetToken(false)) != 0) {
        token = std::string(temp);

        if (token == "app") {
          // Inverted logic: set wildcard to "user"
          touch->set_wildcard("user");
        } else if (token == "user") {
          // Inverted logic: set wildcard to "app"
          touch->set_wildcard("app");
        } else {
          std::cerr << "error: lock wildcard must be 'app' or 'user'" << std::endl;
          return false;
        }
      }
    }
  }

  if (options.find('u') != std::string::npos) {
    touch->set_lockop("unlock");
    return true; // No further arguments needed for unlock
  }

  // Parse additional arguments (size/hardlinkpath and checksuminfo)
  if ((temp = tokenizer.GetToken(false)) != 0) {
    token = std::string(temp);

    if (token[0] == '/') {
      // It's a hardlink path
      touch->set_hardlinkpath(token);
    } else {
      // It's a size
      try {
        uint64_t size = std::stoull(token);
        touch->set_size(size);
      } catch (const std::exception& e) {
        std::cerr << "error: touch size must be a valid integer" << std::endl;
        return false;
      }
    }
  }

  // Parse checksum info
  if ((temp = tokenizer.GetToken(false)) != 0) {
    token = std::string(temp);
    touch->set_checksuminfo(token);
  }

  return true;
}

//------------------------------------------------------------------------------
// Parse info subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseInfo(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  auto* fileinfo = file->mutable_fileinfo();
  std::string token;
  const char* temp;
  std::string path;

  // Parse options
  while ((temp = tokenizer.GetToken(false)) != 0) {
    token = std::string(temp);

    if (token == "--fullpath") {
      fileinfo->set_fullpath(true);
    } else if (token == "--checksum") {
      fileinfo->set_checksum(true);
    } else if (token[0] == '-') {
      // Remove leading dashes
      size_t pos = 0;

      while (pos < token.length() && token[pos] == '-') {
        pos++;
      }

      std::string option = token.substr(pos);

      // Parse individual option flags
      for (char c : option) {
        if (c == 'p') {
          fileinfo->set_path(true);
        } else if (c == 'f') {
          fileinfo->set_fid(true);
        } else if (c == 'x') {
          fileinfo->set_fxid(true);
        } else if (c == 's') {
          fileinfo->set_size(true);
        } else if (c == 'c') {
          fileinfo->set_checksum(true);
        } else if (c == 'm') {
          fileinfo->set_monitoring(true);
        } else if (c == 'e') {
          fileinfo->set_env(true);
        } else  {
          std::cerr << "error: unrecognized info option: -" << c << std::endl;
          return false;
        }
      }
    } else {
      path = token;
    }
  }

  if (path.empty()) {
    std::cerr << "error: info requires a path" << std::endl;
    return false;
  }

  if (!SetPath(path)) {
    std::cerr << "error: invalid path" << std::endl;
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Parse adjustreplica subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseAdjustreplica(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  std::string token;
  const char* temp;
  bool nodrop = false;

  while ((temp = tokenizer.GetToken(false)) != 0) {
    token = std::string(temp);

    if (token == "--nodrop") {
      nodrop = true;
    } else if (token.at(0) == '-') {
      std::cerr << "error: unrecognized adjustreplica option: " << token << std::endl;
      return false;
    } else {
      break;
    }
  }

  std::string path;

  if (!token.empty()) {
    path = token;

    if (!SetPath(path)) {
      std::cerr << "error: invalid path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: adjustreplica requires a path" << std::endl;
    return false;
  }

  auto* adjustreplica = file->mutable_adjustreplica();
  adjustreplica->set_nodrop(nodrop);
  // Collect remaining arguments
  std::vector<std::string> args;

  while ((temp = tokenizer.GetToken(false)) != 0) {
    args.push_back(std::string(temp));
  }

  // Parse remaining arguments (positional and named)
  size_t positional_index = 0;

  for (size_t i = 0; i < args.size(); ++i) {
    if (args[i] == "--exclude-fs") {
      if (i + 1 < args.size()) {
        adjustreplica->set_exclude_fs(args[i + 1]);
        i++;
      } else {
        std::cerr << "error: --exclude-fs requires a value" << std::endl;
        return false;
      }
    } else {
      // Positional arguments
      if (positional_index == 0) {
        adjustreplica->set_space(args[i]);
        positional_index++;
      } else if (positional_index == 1) {
        adjustreplica->set_subgroup(args[i]);
        positional_index++;
      } else {
        std::cerr << "error: too many positional arguments for adjustreplica" <<
                  std::endl;
        return false;
      }
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Parse check subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseCheck(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  const char* temp;
  std::string path;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    path = std::string(temp);

    if (!SetPath(path)) {
      std::cerr << "error: invalid path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: check requires a path" << std::endl;
    return false;
  }

  // Get optional options string
  std::string options;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    options = std::string(temp);
    // Validate options contain only allowed characters
    static const std::string allowed = "%sizechecksumnrepdiskforceoutputsilent";

    if (options.find_first_not_of(allowed) != std::string::npos) {
      std::cerr << "error: invalid check option" << std::endl;
      return false;
    }
  }

  auto* check = file->mutable_check();

  if (!options.empty()) {
    check->set_options(options);
  }

  return true;
}


//------------------------------------------------------------------------------
// Parse convert subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseConvert(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  std::string token;
  const char* temp;
  bool sync_mode = false;
  bool rewrite_mode = false;

  while ((temp = tokenizer.GetToken(false)) != 0) {
    token = std::string(temp);

    if (token == "--sync") {
      sync_mode = true;
    } else if (token == "--rewrite") {
      rewrite_mode = true;
    } else if (token.at(0) == '-') {
      std::cerr << "error: unrecognized convert option: " << token << std::endl;
      return false;
    } else {
      break;
    }
  }

  std::string path;

  if (!token.empty()) {
    path = token;

    if (!SetPath(path)) {
      std::cerr << "error: invalid path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: convert requires a path" << std::endl;
    return false;
  }

  auto* convert = file->mutable_convert();
  convert->set_sync(sync_mode);
  convert->set_rewrite(rewrite_mode);

  if ((temp = tokenizer.GetToken(false)) != 0) {
    convert->set_layout(std::string(temp));
  }

  if ((temp = tokenizer.GetToken(false)) != 0) {
    convert->set_target_space(std::string(temp));
  }

  if ((temp = tokenizer.GetToken(false)) != 0) {
    convert->set_placement_policy(std::string(temp));
  }

  if ((temp = tokenizer.GetToken(false)) != 0) {
    std::string checksum = std::string(temp);
    convert->set_checksum(checksum);
  }

  return true;
}

//------------------------------------------------------------------------------
// Parse copy subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseCopy(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  std::string token;
  const char* temp;
  bool force = false;
  bool silent = false;
  bool clone = false;

  while ((temp = tokenizer.GetToken(false)) != 0) {
    token = std::string(temp);

    if (token == "-f") {
      force = true;
    } else if (token == "-s") {
      silent = true;
    } else if (token == "-c") {
      clone = true;
    } else if (token.at(0) == '-') {
      std::cerr << "error: unrecognized copy option: " << token << std::endl;
      return false;
    } else {
      break;
    }
  }

  std::string source_path;

  if (!token.empty()) {
    source_path = token;

    if (!SetPath(source_path)) {
      std::cerr << "error: invalid source path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: copy requires a source path" << std::endl;
    return false;
  }

  std::string dest_path;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    dest_path = std::string(temp);

    if (dest_path.at(0) != '/') {
      dest_path = abspath(dest_path.c_str());
    }
  } else {
    std::cerr << "error: copy requires a destination path" << std::endl;
    return false;
  }

  auto* copy = file->mutable_copy();
  copy->set_dst(dest_path);
  copy->set_force(force);
  copy->set_clone(clone);
  copy->set_silent(silent);
  return true;
}

//------------------------------------------------------------------------------
// Parse drop subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseDrop(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  std::string token;
  const char* temp;
  std::string path;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    path = std::string(temp);

    if (!SetPath(path)) {
      std::cerr << "error: invalid path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: drop requires a path" << std::endl;
    return false;
  }

  if ((temp = tokenizer.GetToken(false)) != 0) {
    token = std::string(temp);

    try {
      uint32_t fsid = std::stoul(token);
      file->mutable_drop()->set_fsid(fsid);
    } catch (const std::exception& e) {
      std::cerr << "error: drop fsid must be a valid integer" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: drop requires an fsid" << std::endl;
    return false;
  }

  bool force = false;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    token = std::string(temp);

    if (token == "-f") {
      force = true;
    } else {
      std::cerr << "error: unrecognized drop argument: " << token << std::endl;
      return false;
    }
  }

  file->mutable_drop()->set_force(force);
  return true;
}

//------------------------------------------------------------------------------
// Parse layout subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseLayout(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  const char* temp;
  std::string path;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    path = std::string(temp);

    if (!SetPath(path)) {
      std::cerr << "error: invalid path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: layout requires a path" << std::endl;
    return false;
  }

  std::string layout_param;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    layout_param = std::string(temp);
  } else {
    std::cerr <<
              "error: layout requires a parameter (-stripes, -checksum, or -type)" <<
              std::endl;
    return false;
  }

  std::string layout_value;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    layout_value = std::string(temp);
  } else {
    std::cerr << "error: layout parameter requires a value" << std::endl;
    return false;
  }

  auto* layout = file->mutable_layout();

  if (layout_param == "-stripes") {
    try {
      uint32_t stripes = std::stoul(layout_value);
      layout->set_stripes(stripes);
    } catch (const std::exception& e) {
      std::cerr << "error: stripes must be a valid integer" << std::endl;
      return false;
    }
  } else if (layout_param == "-checksum") {
    layout->set_checksum(layout_value);
  } else if (layout_param == "-type") {
    layout->set_type(layout_value);
  } else {
    std::cerr << "error: invalid layout parameter '" << layout_param << "'" <<
              std::endl;
    std::cerr << "       valid parameters are: -stripes, -checksum, -type" <<
              std::endl;
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Parse move subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseMove(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  std::string token;
  const char* temp;
  std::string path;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    path = std::string(temp);

    if (!SetPath(path)) {
      std::cerr << "error: invalid path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: move requires a path" << std::endl;
    return false;
  }

  if ((temp = tokenizer.GetToken(false)) != 0) {
    token = std::string(temp);

    try {
      uint32_t fsid1 = std::stoul(token);
      file->mutable_move()->set_fsid1(fsid1);
    } catch (const std::exception& e) {
      std::cerr << "error: move fsid1 must be a valid integer" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: move requires fsid1" << std::endl;
    return false;
  }

  if ((temp = tokenizer.GetToken(false)) != 0) {
    token = std::string(temp);

    try {
      uint32_t fsid2 = std::stoul(token);
      file->mutable_move()->set_fsid2(fsid2);
    } catch (const std::exception& e) {
      std::cerr << "error: move fsid2 must be a valid integer" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: move requires fsid2" << std::endl;
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Parse purge subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParsePurge(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  const char* temp;
  std::string path;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    path = std::string(temp);

    if (!SetPath(path)) {
      std::cerr << "error: invalid path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: purge requires a path" << std::endl;
    return false;
  }

  // Get optional purge_version parameter
  int32_t purge_version = -1;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    try {
      purge_version = std::stoi(std::string(temp));
    } catch (const std::exception& e) {
      std::cerr << "error: purge_version must be an integer" << std::endl;
      return false;
    }
  }

  auto* version = file->mutable_version();
  version->set_purge_version(purge_version);
  return true;
}

//------------------------------------------------------------------------------
// Parse rename subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseRename(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  const char* temp;
  // Get the old path (source)
  std::string oldpath;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    oldpath = std::string(temp);

    if (!SetPath(oldpath)) {
      std::cerr << "error: invalid source path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: rename requires source path" << std::endl;
    return false;
  }

  // Get the new path (destination)
  std::string newpath;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    newpath = std::string(temp);

    if (newpath.at(0) != '/') {
      newpath = abspath(newpath.c_str());
    }
  } else {
    std::cerr << "error: rename requires destination path" << std::endl;
    return false;
  }

  auto* rename = file->mutable_rename();
  rename->set_new_path(newpath);
  return true;
}

//------------------------------------------------------------------------------
// Parse rename_with_symlink subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseRenameWithSymlink(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  const char* temp;
  // Get the source file path
  std::string source_path;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    source_path = std::string(temp);

    if (!SetPath(source_path)) {
      std::cerr << "error: invalid source path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: rename_with_symlink requires source file path" <<
              std::endl;
    return false;
  }

  // Get the destination directory
  std::string dest_dir;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    dest_dir = std::string(temp);

    if (dest_dir.at(0) != '/') {
      dest_dir = abspath(dest_dir.c_str());
    }
  } else {
    std::cerr << "error: rename_with_symlink requires destination directory" <<
              std::endl;
    return false;
  }

  auto* rename_symlink = file->mutable_rename_with_symlink();
  rename_symlink->set_destination_dir(dest_dir);
  return true;
}

//------------------------------------------------------------------------------
// Parse replicate subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseReplicate(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  std::string token;
  const char* temp;
  std::string path;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    path = std::string(temp);

    if (!SetPath(path)) {
      std::cerr << "error: invalid path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: replicate requires a path" << std::endl;
    return false;
  }

  if ((temp = tokenizer.GetToken(false)) != 0) {
    token = std::string(temp);

    try {
      uint32_t fsid1 = std::stoul(token);
      file->mutable_replicate()->set_fsid1(fsid1);
    } catch (const std::exception& e) {
      std::cerr << "error: replicate fsid1 must be a valid integer" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: replicate requires source fsid" << std::endl;
    return false;
  }

  if ((temp = tokenizer.GetToken(false)) != 0) {
    token = std::string(temp);

    try {
      uint32_t fsid2 = std::stoul(token);
      file->mutable_replicate()->set_fsid2(fsid2);
    } catch (const std::exception& e) {
      std::cerr << "error: replicate fsid2 must be a valid integer" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: replicate requires target fsid" << std::endl;
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Parse share subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseShare(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  const char* temp;
  std::string path;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    path = std::string(temp);

    if (!SetPath(path)) {
      std::cerr << "error: invalid path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: share requires a path" << std::endl;
    return false;
  }

  // Get optional expires parameter (in seconds)
  uint32_t expires = 0;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    try {
      expires = std::stoul(std::string(temp));
    } catch (const std::exception& e) {
      std::cerr << "error: expires must be a valid integer" << std::endl;
      return false;
    }
  }

  auto* share = file->mutable_share();

  if (expires > 0) {
    share->set_expires(expires);
  }

  return true;
}

//------------------------------------------------------------------------------
// Parse symlink subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseSymlink(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  std::string token;
  const char* temp;
  bool force = false;

  while ((temp = tokenizer.GetToken(false)) != 0) {
    token = std::string(temp);

    if (token == "-f") {
      force = true;
    } else if (token.at(0) == '-') {
      std::cerr << "error: unrecognized symlink option: " << token << std::endl;
      return false;
    } else {
      break;
    }
  }

  std::string source_path;

  if (!token.empty()) {
    source_path = token;

    if (!SetPath(source_path)) {
      std::cerr << "error: invalid source path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: symlink requires a source path" << std::endl;
    return false;
  }

  // Get the target path (what the symlink points to)
  std::string target_path;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    target_path = std::string(temp);
  } else {
    std::cerr << "error: symlink requires a target path" << std::endl;
    return false;
  }

  auto* symlink = file->mutable_symlink();
  symlink->set_target_path(target_path);
  symlink->set_force(force);
  return true;
}

//------------------------------------------------------------------------------
// Parse tag subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseTag(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  std::string token;
  const char* temp;
  bool add_tag = false;
  bool remove_tag = false;
  bool unlink_tag = false;

  while ((temp = tokenizer.GetToken(false)) != 0) {
    token = std::string(temp);

    if (token == "+") {
      add_tag = true;
    } else if (token == "-") {
      remove_tag = true;
    } else if (token == "~") {
      unlink_tag = true;
    } else if (token.at(0) == '-' || token.at(0) == '+' || token.at(0) == '~') {
      std::cerr << "error: unrecognized tag option: " << token << std::endl;
      return false;
    } else {
      break;
    }
  }

  std::string path;

  if (!token.empty()) {
    path = token;

    if (!SetPath(path)) {
      std::cerr << "error: invalid path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: tag requires a path" << std::endl;
    return false;
  }

  // Get optional fsid
  uint32_t fsid = 0;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    try {
      fsid = std::stoul(std::string(temp));
    } catch (const std::exception& e) {
      std::cerr << "error: fsid must be a valid integer" << std::endl;
      return false;
    }
  }

  auto* tag = file->mutable_tag();
  tag->set_add(add_tag);
  tag->set_remove(remove_tag);
  tag->set_unlink(unlink_tag);

  if (fsid > 0) {
    tag->set_fsid(fsid);
  }

  return true;
}

//------------------------------------------------------------------------------
// Parse verify subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseVerify(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  std::string token;
  const char* temp;
  std::string path;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    path = std::string(temp);

    if (!SetPath(path)) {
      std::cerr << "error: invalid path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: verify requires a path" << std::endl;
    return false;
  }

  auto* verify = file->mutable_verify();

  while ((temp = tokenizer.GetToken(false)) != 0) {
    token = std::string(temp);

    if (token == "-checksum") {
      verify->set_checksum(true);
    } else if (token == "-commitchecksum") {
      verify->set_commitchecksum(true);
    } else if (token == "-commitsize") {
      verify->set_commitsize(true);
    } else if (token == "-commitfmd") {
      verify->set_commitfmd(true);
    } else if (token == "-resync") {
      verify->set_resync(true);
    } else if (token == "-rate") {
      if ((temp = tokenizer.GetToken(false)) != 0) {
        try {
          uint32_t rate = std::stoul(std::string(temp));
          verify->set_rate(rate);
        } catch (const std::exception& e) {
          std::cerr << "error: rate must be a valid integer" << std::endl;
          return false;
        }
      } else {
        std::cerr << "error: -rate requires a value" << std::endl;
        return false;
      }
    } else if (token.at(0) != '-') {
      try {
        uint32_t fsid = std::stoul(token);
        verify->set_fsid(fsid);
      } catch (const std::exception& e) {
        std::cerr << "error: fsid must be a valid integer" << std::endl;
        return false;
      }
    } else {
      std::cerr << "error: unrecognized verify option: " << token << std::endl;
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Parse version subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseVersion(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  const char* temp;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    std::string path = std::string(temp);

    if (!SetPath(path)) {
      std::cerr << "error: invalid path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: version requires a path" << std::endl;
    return false;
  }

  int32_t purge_version = -1;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    try {
      purge_version = std::stoi(std::string(temp));
    } catch (const std::exception& e) {
      std::cerr << "error: purge_version must be an integer" << std::endl;
      return false;
    }
  }

  auto* version = file->mutable_version();
  version->set_purge_version(purge_version);
  return true;
}

//------------------------------------------------------------------------------
// Parse versions subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseVersions(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  const char* temp;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    std::string path = std::string(temp);

    if (!SetPath(path)) {
      std::cerr << "error: invalid path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: versions requires a path" << std::endl;
    return false;
  }

  std::string grab_version = "-1";

  if ((temp = tokenizer.GetToken(false)) != 0) {
    grab_version = std::string(temp);
  }

  auto* versions = file->mutable_versions();
  versions->set_grab_version(grab_version);
  return true;
}

//------------------------------------------------------------------------------
// Parse workflow subcommand
//------------------------------------------------------------------------------
bool
FileHelper::ParseWorkflow(eos::common::StringTokenizer& tokenizer)
{
  using eos::console::FileProto;
  FileProto* file = mReq.mutable_file();
  const char* temp;
  std::string path;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    path = std::string(temp);

    if (!SetPath(path)) {
      std::cerr << "error: invalid path" << std::endl;
      return false;
    }
  } else {
    std::cerr << "error: workflow requires a path" << std::endl;
    return false;
  }

  std::string workflow_name;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    workflow_name = std::string(temp);
  } else {
    std::cerr << "error: workflow requires a workflow name" << std::endl;
    return false;
  }

  std::string event_name;

  if ((temp = tokenizer.GetToken(false)) != 0) {
    event_name = std::string(temp);
  } else {
    std::cerr << "error: workflow requires an event name" << std::endl;
    return false;
  }

  auto* workflow = file->mutable_workflow();
  workflow->set_workflow(workflow_name);
  workflow->set_event(event_name);
  return true;
}

//------------------------------------------------------------------------------
// Helper method to set path in proto message
//------------------------------------------------------------------------------
bool
FileHelper::SetPath(const std::string& in_path)
{
  eos::console::FileProto* file = mReq.mutable_file();
  eos::console::Metadata* md = file->mutable_md();

  if (in_path.empty()) {
    return false;
  }

  if (in_path.find("fid:") == 0 || in_path.find("fxid:") == 0 ||
      in_path.find("pid:") == 0 || in_path.find("pxid:") == 0 ||
      in_path.find("inode:") == 0) {
    md->set_path(in_path);
  } else if (in_path.at(0) == '/') {
    md->set_path(in_path);
  } else {
    md->set_path(abspath(in_path.c_str()));
  }

  return true;
}