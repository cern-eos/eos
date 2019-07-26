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

#include "QoSCmd.hh"
#include "mgm/XrdMgmOfs.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Method implementing the specific behaviour of the command executed
//------------------------------------------------------------------------------
eos::console::ReplyProto
QoSCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::QoSProto qos = mReqProto.qos();
  const auto& subcmd = qos.subcmd_case();

  if (subcmd == eos::console::QoSProto::kGet) {
    bool jsonOutput =
        (mReqProto.format() == eos::console::RequestProto::JSON);
    GetSubcmd(qos.get(), reply, jsonOutput);
  } else if(subcmd == eos::console::QoSProto::kSet) {
    SetSubcmd(qos.set(), reply);
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: command not supported");
  }

  return reply;
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

  XrdSfsFileExistence fileExists;
  XrdOucErrInfo errInfo;

  // Check for path existence
  if (gOFS->_exists(spath.c_str(), fileExists, errInfo, mVid)) {
    reply.set_std_err("error: unable to check for path existence");
    reply.set_retc(errno);
    return;
  }

  if (fileExists == XrdSfsFileExistNo) {
    reply.set_std_err("error: path does not point to a file or directory");
    reply.set_retc(EINVAL);
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
    if (qosKeys.find(key) == qosKeys.end()) {
      qosKeys.insert(key);

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
  }

  // No keys specified -- extract all
  if (get.key_size() == 0) {
    if (gOFS->_qos_ls(spath.c_str(), errInfo, mVid, qosMap)) {
      err << "error: " << errInfo.getErrText() << std::endl;
      retc = errInfo.getErrInfo();
    }
  }

  // Format QoS properties map to desired output
  out << (jsonOutput ?  mapToJSONOutput(qosMap)
                     :  mapToDefaultOutput(qosMap));

  reply.set_retc(retc);
  reply.set_std_out(out.str().c_str());
  reply.set_std_err(err.str().c_str());
}

//------------------------------------------------------------------------------
// Execute set subcommand
//------------------------------------------------------------------------------
void QoSCmd::SetSubcmd(const eos::console::QoSProto_SetProto& set,
                       eos::console::ReplyProto& reply)
{

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
  } else if (type == QoSProto::IdentifierProto::kFileId ) {
    GetPathFromFid(path, identifier.fileid(), "error: ");
  } else {
    stdErr = "error: received empty string path";
  }

  return path;
}

//------------------------------------------------------------------------------
// Process a QoS properties map into a default printable output
//------------------------------------------------------------------------------
std::string QoSCmd::mapToDefaultOutput(const eos::IFileMD::QoSAttrMap& map)
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
std::string QoSCmd::mapToJSONOutput(const eos::IFileMD::QoSAttrMap& map)
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

EOSMGMNAMESPACE_END
