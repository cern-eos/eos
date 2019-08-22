//------------------------------------------------------------------------------
// File: QoSCmd.cc
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

#include "common/LayoutId.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Scheduler.hh"
#include "mgm/qos/QoSClass.hh"
#include "mgm/qos/QoSConfig.hh"
#include "QoSCmd.hh"

EOSMGMNAMESPACE_BEGIN

// Helper function forward declaration
static int CheckIsFile(const char*, eos::common::VirtualIdentity&,
                       std::string&);

//------------------------------------------------------------------------------
// Method implementing the specific behaviour of the command executed
//------------------------------------------------------------------------------
eos::console::ReplyProto
QoSCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::QoSProto qos = mReqProto.qos();
  const auto& subcmd = qos.subcmd_case();
  bool jsonOutput =
    (mReqProto.format() == eos::console::RequestProto::JSON);

  if (subcmd == eos::console::QoSProto::kList) {
    ListSubcmd(qos.list(), reply, jsonOutput);
  } else if (subcmd == eos::console::QoSProto::kGet) {
    GetSubcmd(qos.get(), reply, jsonOutput);
  } else if (subcmd == eos::console::QoSProto::kSet) {
    SetSubcmd(qos.set(), reply, jsonOutput);
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: command not supported");
  }

  return reply;
}

//------------------------------------------------------------------------------
// Execute list subcommand
//------------------------------------------------------------------------------
void QoSCmd::ListSubcmd(const eos::console::QoSProto_ListProto& list,
                        eos::console::ReplyProto& reply,
                        bool jsonOutput)
{
  std::ostringstream out;

  if (!gOFS->MgmQoSEnabled) {
    reply.set_std_err("error: QoS support is disabled");
    reply.set_retc(ENOTSUP);
    return;
  }

  if (list.classname().empty()) {
    // List available QoS classes
    if (!jsonOutput) {
      if (gOFS->mQoSClassMap.empty()) {
        out << "No QoS classes defined";
      } else {
        out << "Available QoS classes: [";
        for (const auto& it: gOFS->mQoSClassMap) {
          out << " " << it.first << ",";
        }
        out.seekp(-1, std::ios_base::end);
        out << " ]";
      }
    } else {
      Json::Value json = Json::arrayValue;
      for (const auto& it: gOFS->mQoSClassMap) {
        json.append(it.first);
      }

      out << Json::StyledWriter().write(json);
    }
  } else {
    // List properties of the given QoS class
    if (!gOFS->mQoSClassMap.count(list.classname())) {
      reply.set_std_err("error: QoS class not found");
      reply.set_retc(EINVAL);
      return;
    }

    auto qos = gOFS->mQoSClassMap.at(list.classname());
    out <<
        (jsonOutput ? Json::StyledWriter().write(QoSConfig::QoSClassToJson(qos))
                    : QoSConfig::QoSClassToString(qos));
  }

  reply.set_std_out(out.str());
}

//------------------------------------------------------------------------------
// Execute get subcommand
//------------------------------------------------------------------------------
void QoSCmd::GetSubcmd(const eos::console::QoSProto_GetProto& get,
                       eos::console::ReplyProto& reply,
                       bool jsonOutput)
{
  std::ostringstream out;
  std::ostringstream err;
  XrdOucString spath;
  int retc = 0;

  spath = PathFromIdentifierProto(get.identifier());

  if (!spath.length()) {
    reply.set_std_err(stdErr.c_str());
    reply.set_retc(ENOENT);
    return;
  }

  XrdOucErrInfo errInfo;
  std::string errmsg;

  // Check path points to a valid file
  if ((retc = CheckIsFile(spath.c_str(), mVid, errmsg))) {
    reply.set_std_err(errmsg);
    reply.set_retc(retc);
    return;
  }

  // Check for access permission
  if (gOFS->_access(spath.c_str(), R_OK, errInfo, mVid, 0)) {
    err << "error: " << errInfo.getErrText();
    reply.set_std_err(err.str().c_str());
    reply.set_retc(errInfo.getErrInfo());
    return;
  }

  // Keep a key set to avoid processing duplicates
  std::set<std::string> qosKeys;
  eos::IFileMD::QoSAttrMap qosMap;

  for (const auto& key: get.key()) {
    if (key == "class") {
      qosKeys.insert({"current_qos", "target_qos"});
      continue;
    } else if (key == "all") {
      qosKeys.clear();
      break;
    }

    qosKeys.insert(key);
  }

  // Process specified keys
  for (const auto& key: qosKeys) {
    if (key == "cdmi") {
      eos::IFileMD::QoSAttrMap cdmiMap;

      if (gOFS->_qos_ls(spath.c_str(), errInfo, mVid, cdmiMap, true)) {
        err << "error: " << errInfo.getErrText() << std::endl;
        retc = errInfo.getErrInfo();
        continue;
      }

      qosMap.insert(cdmiMap.begin(), cdmiMap.end());
    } else {
      XrdOucString value;

      if (gOFS->_qos_get(spath.c_str(), errInfo, mVid, key.c_str(), value)) {
        err << "error: " << errInfo.getErrText() << std::endl;
        retc = errInfo.getErrInfo();
        continue;
      }

      qosMap[key] = value.c_str();
    }
  }

  // No keys specified -- extract all
  if (qosKeys.empty()) {
    if (gOFS->_qos_ls(spath.c_str(), errInfo, mVid, qosMap)) {
      err << "error: " << errInfo.getErrText() << std::endl;
      retc = errInfo.getErrInfo();
    }
  }

  // Avoid showing an empty target QoS field
  if ((qosMap.count("target_qos")) &&
      (qosMap.at("target_qos") == "null")) {
    qosMap.erase("target_qos");
  }

  // Format QoS properties map to desired output
  out << (jsonOutput ? MapToJSONOutput(qosMap)
                     : MapToDefaultOutput(qosMap));

  reply.set_retc(retc);
  reply.set_std_out(out.str());
  reply.set_std_err(err.str());
}

//------------------------------------------------------------------------------
// Execute set subcommand
//------------------------------------------------------------------------------
void QoSCmd::SetSubcmd(const eos::console::QoSProto_SetProto& set,
                       eos::console::ReplyProto& reply,
                       bool jsonOutput)
{
  using eos::common::LayoutId;
  std::ostringstream out;
  std::ostringstream err;
  XrdOucString spath;
  int retc = 0;

  spath = PathFromIdentifierProto(set.identifier());

  if (!spath.length()) {
    reply.set_std_err(stdErr.c_str());
    reply.set_retc(ENOENT);
    return;
  }

  XrdOucErrInfo errInfo;
  std::string errmsg;

  // Check path points to a valid file
  if ((retc = CheckIsFile(spath.c_str(), mVid, errmsg))) {
    reply.set_std_err(errmsg);
    reply.set_retc(retc);
    return;
  }

  if (!gOFS->MgmQoSEnabled) {
    reply.set_std_err("error: QoS support is disabled");
    reply.set_retc(ENOTSUP);
    return;
  }

  if (!gOFS->mQoSClassMap.count(set.classname())) {
    reply.set_std_err(SSTR("error: unrecognized QoS class name '"
                             << set.classname() << "'"));
    reply.set_retc(EINVAL);
    return;
  }

  auto qos = gOFS->mQoSClassMap.at(set.classname());
  std::string conversion_id = "";

  if (gOFS->_qos_set(spath.c_str(), errInfo, mVid, qos, conversion_id)) {
    err << "error: " << errInfo.getErrText() << std::endl;
    retc = errInfo.getErrInfo();
  }

  if (!retc && !jsonOutput) {
    out << "scheduled QoS conversion job: " << conversion_id;
  } else if (jsonOutput) {
    Json::Value jsonOut;
    jsonOut["retc"] = (Json::Value::UInt64) retc;
    jsonOut["conversionid"] = (retc) ? "null" : conversion_id;
    out << jsonOut;
  }

  reply.set_retc(retc);
  reply.set_std_out(out.str());
  reply.set_std_err(err.str());
}

//------------------------------------------------------------------------------
// Translate the identifier proto into a namespace path
//------------------------------------------------------------------------------
XrdOucString QoSCmd::PathFromIdentifierProto(
  const eos::console::QoSProto_IdentifierProto& identifier)
{
  using eos::console::QoSProto;
  const auto& type = identifier.Identifier_case();
  XrdOucString path = "";

  if (type == QoSProto::IdentifierProto::kPath) {
    path = identifier.path().c_str();
  } else if (type == QoSProto::IdentifierProto::kFileId) {
    GetPathFromFid(path, identifier.fileid(), "error: ");
  } else {
    stdErr = "error: received empty string path";
  }

  return path;
}

//------------------------------------------------------------------------------
// Process a QoS properties map into a default printable output
//------------------------------------------------------------------------------
std::string QoSCmd::MapToDefaultOutput(const eos::IFileMD::QoSAttrMap& map)
{
  std::ostringstream out;

  for (const auto& it: map) {
    out << it.first << "=" << it.second << std::endl;
  }

  return out.str();
}

//------------------------------------------------------------------------------
// Process a QoS properties map into a default printable output
//------------------------------------------------------------------------------
std::string QoSCmd::MapToJSONOutput(const eos::IFileMD::QoSAttrMap& map)
{
  Json::Value jsonOut, jsonCDMI;

  for (const auto& it: map) {
    XrdOucString key = it.first.c_str();

    if (key.beginswith("cdmi_")) {
      jsonCDMI[it.first] = it.second;
    } else {
      jsonOut[it.first] = it.second;
    }
  }

  if (!jsonCDMI.empty()) {
    jsonOut["metadata"] = jsonCDMI;
  }

  return static_cast<std::ostringstream&>(
    std::ostringstream() << jsonOut).str();
}

//------------------------------------------------------------------------------
//! Check that the given path points to a valid file.
//!
//! @param path the path to check
//! @param vid virtual identity of the client
//! @param err_msg string to place error message
//!
//! @return 0 if path points to file, error code otherwise
//------------------------------------------------------------------------------
static int CheckIsFile(const char* path,
                       eos::common::VirtualIdentity& vid,
                       std::string& err_msg)
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
  } else if (fileExists != XrdSfsFileExistIsFile) {
    err_msg = "error: path does not point to a file";
    return EINVAL;
  }

  return 0;
}

EOSMGMNAMESPACE_END
