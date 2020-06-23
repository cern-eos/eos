//------------------------------------------------------------------------------
// File: ConvertCmd.cc
// Author: Mihai Patrascoiu - CERN
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

#include "mgm/XrdMgmOfs.hh"
#include "mgm/Scheduler.hh"
#include "mgm/FsView.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IContainerMD.hh"
#include "ConvertCmd.hh"

#include <json/json.h>

EOSMGMNAMESPACE_BEGIN

// Helper function forward declaration
static int CheckValidPath(const char*, eos::common::VirtualIdentity&,
                          std::string&, XrdSfsFileExistence = XrdSfsFileExistNo);

static std::string BuildConversionId(const std::string&,
                                     eos::common::LayoutId::eChecksum,
                                     int,
                                     eos::common::FileId::fileid_t file_id,
                                     const std::string&,
                                     const std::string& = "");

//------------------------------------------------------------------------------
// Method implementing the specific behaviour of the command executed
//------------------------------------------------------------------------------
eos::console::ReplyProto
ConvertCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::ConvertProto convert = mReqProto.convert();
  const auto& subcmd = convert.subcmd_case();
  bool jsonOutput =
    (mReqProto.format() == eos::console::RequestProto::JSON);

  if (!gOFS->mConverterDriver) {
    reply.set_std_err("error: ConverterEngine service is not enabled");
    reply.set_retc(ENOTSUP);
    return reply;
  }

  if (subcmd == eos::console::ConvertProto::kAction) {
    ActionSubcmd(convert.action(), reply);
  } else if (subcmd == eos::console::ConvertProto::kStatus) {
    StatusSubcmd(convert.status(), reply, jsonOutput);
  } else if (subcmd == eos::console::ConvertProto::kConfig) {
    ConfigSubcmd(convert.config(), reply, jsonOutput);
  } else if (subcmd == eos::console::ConvertProto::kFile) {
    FileSubcmd(convert.file(), reply, jsonOutput);
  } else if (subcmd == eos::console::ConvertProto::kRule) {
    RuleSubcmd(convert.rule(), reply, jsonOutput);
  } else if (subcmd == eos::console::ConvertProto::kList) {
    ListSubcmd(convert.list(), reply, jsonOutput);
  } else if (subcmd == eos::console::ConvertProto::kClear) {
    ClearSubcmd(convert.clear(), reply);
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: command not supported");
  }

  return reply;
}

//------------------------------------------------------------------------------
// Execute action subcommand
//------------------------------------------------------------------------------
void ConvertCmd::ActionSubcmd(
  const eos::console::ConvertProto_ActionProto& action,
  eos::console::ReplyProto& reply)
{
  std::ostringstream out;
  auto converter_action = action.action();

  if (converter_action == eos::console::ConvertProto_ActionProto::ENABLE) {
    gOFS->mConverterDriver->Start();
    out << "converter engine started";
  } else {
    gOFS->mConverterDriver->Stop();
    out << "converter engine stopped";
  }

  reply.set_std_out(out.str());
}

//------------------------------------------------------------------------------
// Execute status subcommand
//------------------------------------------------------------------------------
void ConvertCmd::StatusSubcmd(
  const eos::console::ConvertProto_StatusProto& status,
  eos::console::ReplyProto& reply,
  bool jsonOutput)
{
  std::ostringstream out;
  // Lambda function to parse threadpool information
  auto parseKeyValueString = [](std::string skeyvalue) -> Json::Value {
    using eos::common::StringConversion;
    std::map<std::string, std::string> map;
    Json::Value json;

    if (StringConversion::GetKeyValueMap(skeyvalue.c_str(),
    map, "=", " "))
    {
      for (const auto& it : map) {
        json[it.first] = map[it.first];
      }
    }

    return json;
  };
  // Extract Converter Driver parameters
  std::string threadpool = gOFS->mConverterDriver->GetThreadPoolInfo();
  std::string config =
    SSTR("maxthreads=" << gOFS->mConverterDriver->GetMaxThreadPoolSize() <<
         " interval=" << gOFS->mConverterDriver->GetRequestIntervalSec());
  uint64_t running = gOFS->mConverterDriver->NumRunningJobs();
  uint64_t failed = gOFS->mConverterDriver->NumFailedJobs();
  int64_t pending = gOFS->mConverterDriver->NumQdbPendingJobs();
  int64_t failed_qdb = gOFS->mConverterDriver->NumQdbFailedJobs();
  auto state = gOFS->mConverterDriver->IsRunning() ? "enabled" : "disabled";

  if (jsonOutput) {
    Json::Value json;
    json["threadpool"] = parseKeyValueString(threadpool.c_str());
    json["config"] = parseKeyValueString(config.c_str());
    json["status"] = state;
    json["running"] = (Json::Value::UInt64) running;
    json["pending"] = (Json::Value::UInt64) pending;
    json["failed"] = (Json::Value::UInt64) failed;
    json["failed_qdb"] = (Json::Value::UInt64) failed_qdb;
    out << Json::StyledWriter().write(json);
  } else {
    out << "Threadpool: " << threadpool << std::endl
        << "Config: " << config << std::endl
        << "Status: " << state << std::endl
        << "Running jobs: " << running << std::endl
        << "Pending jobs: " << pending << std::endl
        << "Failed jobs: " << failed << std::endl
        << "Failed jobs (QDB): " << failed_qdb;
  }

  reply.set_std_out(out.str());
}

//------------------------------------------------------------------------------
// Execute config subcommand
//------------------------------------------------------------------------------
void ConvertCmd::ConfigSubcmd(
  const eos::console::ConvertProto_ConfigProto& config,
  eos::console::ReplyProto& reply,
  bool jsonOutput)
{
  using output_map = std::map<std::string, std::string>;
  std::ostringstream out;
  std::ostringstream err;
  output_map output;
  int retc = 0;

  if (config.maxthreads() != 0) {
    if (config.maxthreads() > 5000) {
      err << "error: maxthreads value " << config.maxthreads()
          << " above 5000 limit" << std::endl;
      retc = EINVAL;
    } else {
      gOFS->mConverterDriver->SetMaxThreadPoolSize(config.maxthreads());
      output["maxthreads"] = std::to_string(config.maxthreads());
    }
  }

  if (config.interval() != 0) {
    if (config.interval() > 3600 * 24) {
      err << "error: interval value " << config.interval()
          << " above 1 day limit" << std::endl;
      retc = EINVAL;
    } else {
      gOFS->mConverterDriver->SetRequestIntervalSec(config.interval());
      output["interval"] = std::to_string(config.interval());
    }
  }

  if (output.empty()) {
    err << "error: no config values given" << std::endl;
    retc = ENODATA;
  } else if (jsonOutput) {
    Json::Value json;

    for (auto it = output.begin(); it != output.end(); it++) {
      json[it->first] = it->second;
    }

    out << Json::StyledWriter().write(json);
  } else {
    out << "Config values updated:" << std::endl;

    for (auto it = output.begin(); it != output.end(); it++) {
      out << it->first << "=" << it->second << std::endl;
    }
  }

  reply.set_std_out(out.str());
  reply.set_std_err(err.str());
  reply.set_retc(retc);
}

//------------------------------------------------------------------------------
// Execute file subcommand
//------------------------------------------------------------------------------
void ConvertCmd::FileSubcmd(const eos::console::ConvertProto_FileProto& file,
                            eos::console::ReplyProto& reply,
                            bool jsonOutput)
{
  using eos::common::LayoutId;
  auto conversion = file.conversion();
  std::ostringstream out;
  std::ostringstream err;
  std::string errmsg;
  std::string path;
  int retc = 0;
  auto enforce_file = XrdSfsFileExistence::XrdSfsFileExistIsFile;
  path = PathFromIdentifierProto(file.identifier(), errmsg);

  if (!path.length()) {
    reply.set_std_err(errmsg);
    reply.set_retc(errno);
    return;
  }

  if ((retc = CheckValidPath(path.c_str(), mVid, errmsg, enforce_file))) {
    reply.set_std_err(errmsg);
    reply.set_retc(retc);
    return;
  }

  if ((retc = CheckConversionProto(conversion, errmsg))) {
    reply.set_std_err(errmsg);
    reply.set_retc(retc);
    return;
  }

  // Extract file id, layout id and replica location
  eos::IFileMD::id_t file_id = 0;
  eos::IFileMD::layoutId_t file_layoutid = 0;
  eos::IFileMD::location_t replica_location = 0;

  try {
    eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosView->getFile(path).get();
    file_id = fmd->getId();
    file_layoutid = fmd->getLayoutId();
    replica_location = fmd->getLocations().at(0);
  } catch (eos::MDException& e) {
    eos_debug("msg=\"exception retrieving file metadata\" path=%s "
              "ec=%d emsg=\"%s\"", path.c_str(), e.getErrno(),
              e.getMessage().str().c_str());
    err << "error: failed to retrieve file metadata '" << path << "'";
    reply.set_std_err(err.str());
    reply.set_retc(e.getErrno());
    return;
  }

  // Handle conversion space
  std::string space = conversion.space();

  if (space.empty()) {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    auto filesystem = FsView::gFsView.mIdView.lookupByID(replica_location);

    if (filesystem) {
      space = filesystem->GetString("schedgroup");
    } else {
      err << "error: unable to retrieve "
          << "filesystem location for '" << path << "'";
      reply.set_std_err(err.str());
      reply.set_retc(EINVAL);
      return;
    }
  }

  // Handle checksum
  LayoutId::eChecksum echecksum;

  if (conversion.checksum().length()) {
    echecksum = static_cast<LayoutId::eChecksum>(
                  LayoutId::GetChecksumFromString(conversion.checksum()));
  } else {
    echecksum = static_cast<LayoutId::eChecksum>(
                  LayoutId::GetChecksum(file_layoutid));
  }

  // Schedule conversion job
  std::string conversion_id = BuildConversionId(conversion.layout(), echecksum,
                              conversion.replica(), file_id,
                              space, conversion.placement());
  eos_info("msg=\"scheduling conversion job\" path=%s conversion_id=%s",
           path.c_str(), conversion_id.c_str());

  if (!gOFS->mConverterDriver->ScheduleJob(file_id, conversion_id)) {
    err << "error: unable to push conversion job '" << conversion_id.c_str()
        << "' to QuarkDB";
    reply.set_std_err(err.str());
    reply.set_retc(EIO);
    return;
  }

  if (jsonOutput) {
    Json::Value json;
    json["conversion_id"] = conversion_id;
    json["path"] = path;
    json["space"] = space;
    json["checksum"] = LayoutId::GetChecksumString(echecksum);
    out << Json::StyledWriter().write(json);
  } else {
    out << "Scheduled conversion job: " << conversion_id;
  }

  reply.set_std_out(out.str());
}

//------------------------------------------------------------------------------
// Execute rule subcommand
//------------------------------------------------------------------------------
void ConvertCmd::RuleSubcmd(const eos::console::ConvertProto_RuleProto& rule,
                            eos::console::ReplyProto& reply,
                            bool jsonOutput)
{
  using eos::common::LayoutId;
  auto conversion = rule.conversion();
  XrdOucErrInfo errInfo;
  std::ostringstream out;
  std::ostringstream err;
  std::string errmsg;
  std::string path;
  int retc = 0;
  auto enforce_dir = XrdSfsFileExistence::XrdSfsFileExistIsDirectory;
  path = PathFromIdentifierProto(rule.identifier(), errmsg);

  if (!path.length()) {
    reply.set_std_err(errmsg);
    reply.set_retc(errno);
    return;
  }

  if ((retc = CheckValidPath(path.c_str(), mVid, errmsg, enforce_dir))) {
    reply.set_std_err(errmsg);
    reply.set_retc(retc);
    return;
  }

  if ((retc = CheckConversionProto(conversion, errmsg))) {
    reply.set_std_err(errmsg);
    reply.set_retc(retc);
    return;
  }

  if (conversion.checksum().empty()) {
    err << "error: no conversion checksum provided";
    reply.set_std_err(err.str());
    reply.set_retc(EINVAL);
    return;
  }

  // Handle space default scenario
  std::string space = conversion.space().empty() ?
                      "default.0" : conversion.space();
  // Handle checksum
  LayoutId::eChecksum echecksum = static_cast<LayoutId::eChecksum>(
                                    LayoutId::GetChecksumFromString(conversion.checksum()));
  //------------------------------------------
  // This part acts as a placeholder
  //------------------------------------------
  // Build conversion rule
  std::string conversion_rule = BuildConversionId(conversion.layout(), echecksum,
                                conversion.replica(), 0, space,
                                conversion.placement());
  size_t pos = conversion_rule.find(":");

  if (pos != std::string::npos) {
    conversion_rule.erase(0, pos + 1);
  }

  // Set rule as extended attribute
  eos_info("msg=\"placing conversion rule\" path=%s conversion_rule=%s",
           path.c_str(), conversion_rule.c_str());

  if (gOFS->_attr_set(path.c_str(), errInfo, mVid, 0, "sys.eos.convert.rule",
                      conversion_rule.c_str())) {
    err << "error: could not set conversion rule '" << conversion_rule
        << "' on path '" << path << "' -- emsg=" << errInfo.getErrText();
    reply.set_retc(errInfo.getErrInfo());
    return;
  }

  if (jsonOutput) {
    Json::Value json;
    json["conversion_rule"] = conversion_rule;
    json["path"] = path;
    out << Json::StyledWriter().write(json);
  } else {
    out << "Set conversion rule '" << conversion_rule
        << "' on path '" << path << "'";
  }

  reply.set_std_out(out.str());
}

//------------------------------------------------------------------------------
// List jobs subcommand
//------------------------------------------------------------------------------
void
ConvertCmd::ListSubcmd(const eos::console::ConvertProto_ListProto& list,
                       eos::console::ReplyProto& reply, bool jsonOutput)
{
}

//------------------------------------------------------------------------------
// Clear jobs subcommand
//------------------------------------------------------------------------------
void
ConvertCmd::ClearSubcmd(const eos::console::ConvertProto_ClearProto& clear,
                        eos::console::ReplyProto& reply)
{
}

//------------------------------------------------------------------------------
// Translate the identifier proto into a namespace path
//------------------------------------------------------------------------------
std::string ConvertCmd::PathFromIdentifierProto(
  const eos::console::ConvertProto_IdentifierProto& identifier,
  std::string& err_msg)
{
  using eos::console::ConvertProto;
  const auto& type = identifier.Identifier_case();
  std::string path = "";

  if (type == ConvertProto::IdentifierProto::kPath) {
    path = identifier.path().c_str();
  } else if (type == ConvertProto::IdentifierProto::kFileId) {
    GetPathFromFid(path, identifier.fileid(), err_msg);
  } else {
    err_msg = "error: received empty string path";
  }

  return path;
}

//------------------------------------------------------------------------------
// Check that the given proto conversion is valid
//------------------------------------------------------------------------------
int ConvertCmd::CheckConversionProto(
  const eos::console::ConvertProto_ConversionProto& conversion,
  std::string& err_msg)
{
  using eos::common::LayoutId;

  if (LayoutId::GetLayoutFromString(conversion.layout()) == -1) {
    err_msg = "error: invalid conversion layout";
    return EINVAL;
  }

  if (conversion.replica() < 1 || conversion.replica() > 32) {
    err_msg = "error: invalid replica number (must be between 1 and 32)";
    return EINVAL;
  }

  if (conversion.checksum().length()) {
    auto xs_id = LayoutId::GetChecksumFromString(conversion.checksum());

    if ((xs_id == -1) || (xs_id == LayoutId::eChecksum::kNone)) {
      err_msg = "error: invalid conversion checksum";
      return EINVAL;
    }
  }

  if (conversion.placement().length() &&
      eos::mgm::Scheduler::PlctPolicyFromString(conversion.placement()) == -1) {
    err_msg = "error: invalid conversion placement policy";
    return EINVAL;
  }

  return 0;
}

//------------------------------------------------------------------------------
//! Check that the given path points to a valid entry
//!
//! @param path the path to check
//! @param vid virtual identity of the client
//! @param err_msg string to place error message
//! @param enforce_type expected entry type (file or directory)
//!
//! @return 0 if path is valid, error code otherwise
//------------------------------------------------------------------------------
static int CheckValidPath(const char* path,
                          eos::common::VirtualIdentity& vid,
                          std::string& err_msg,
                          XrdSfsFileExistence enforce_type)
{
  XrdSfsFileExistence fileExists;
  XrdOucErrInfo errInfo;

  // Check for path existence
  if (gOFS->_exists(path, fileExists, errInfo, vid)) {
    err_msg = "error: unable to check for path existence";
    return errInfo.getErrInfo();
  }

  if (fileExists == XrdSfsFileExistNo) {
    err_msg = "error: path does not point to a valid entry";
    return EINVAL;
  } else if ((fileExists != XrdSfsFileExistIsFile) &&
             (fileExists != XrdSfsFileExistIsDirectory)) {
    err_msg = "error: path does not point to a file or container";
    return EINVAL;
  }

  if ((enforce_type != XrdSfsFileExistNo) && (fileExists != enforce_type)) {
    std::string type = (fileExists == XrdSfsFileExistIsFile) ?
                       "file" : "directory";
    err_msg = "error: path must point to a " + type;
    return EINVAL;
  }

  return 0;
}

//------------------------------------------------------------------------------
//! Build and return a conversion id from the provided arguments.
//!
//! @param layout the conversion layout
//! @param echecksum the conversion checksum
//! @param stripes the conversion number of stripes
//! @param file_id the conversion file id
//! @paran space the conversion target space
//! @param placement the conversion placement type
//!
//! @return 0 if path is valid, error code otherwise
//------------------------------------------------------------------------------
static std::string BuildConversionId(const std::string& layout,
                                     eos::common::LayoutId::eChecksum echecksum,
                                     int stripes,
                                     eos::common::FileId::fileid_t file_id,
                                     const std::string& space,
                                     const std::string& placement)
{
  using eos::common::LayoutId;
  unsigned long layoutid = 0;
  std::ostringstream ssid;
  layoutid = LayoutId::GetId(LayoutId::GetLayoutFromString(layout),
                             echecksum,
                             stripes,
                             LayoutId::eBlockSize::k4M,
                             LayoutId::eChecksum::kCRC32C,
                             LayoutId::GetRedundancyFromLayoutString(layout));
  ssid << std::hex << std::setw(16) << std::setfill('0') << file_id
       << ":" << space
       << "#" << std::setw(8) << std::setfill('0') << layoutid;

  if (placement.length()) {
    ssid << "~" << placement;
  }

  return ssid.str();
}

EOSMGMNAMESPACE_END
