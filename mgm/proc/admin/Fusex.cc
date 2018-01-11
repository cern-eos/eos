// ----------------------------------------------------------------------
// File: proc/admin/Fusex.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "common/ZMQ.hh"
#include "mgm/ZMQ.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Fusex()
{
  if (pVid->uid == 0) {
    if (mSubCmd == "ls") {
      std::string option = pOpaque->Get("mgm.option") ? pOpaque->Get("mgm.option") :
                           "";
      std::string out;
      gOFS->zMQ->gFuseServer.Print(out, option, false);
      stdOut += out.c_str();
      retc = 0;
    } else if (mSubCmd == "hb") {
      std::string hb = pOpaque->Get("mgm.fusex.hb") ? pOpaque->Get("mgm.fusex.hb") :
                       "";
      int i_hb = atoi(hb.c_str());

      if ((i_hb > 0) && (i_hb <= 15)) {
        gOFS->zMQ->gFuseServer.Client().SetHeartbeatInterval(i_hb);
        stdOut += "info: configured FUSEX heartbeat interval to ";
        stdOut += hb.c_str();
        stdOut += " seconds";
        retc = 0;
      } else {
        stdErr += "error: hearbeat interval must be [1..15] seconds";
        retc = EINVAL;
      }
    } else if (mSubCmd == "evict") {
      std::string s_reason;
      std::string uuid = pOpaque->Get("mgm.fusex.uuid") ?
                         pOpaque->Get("mgm.fusex.uuid") : "";
      XrdOucString reason64 = pOpaque->Get("mgm.fusex.reason") ?
                              pOpaque->Get("mgm.fusex.reason") : "evicted via EOS shell";
      XrdOucString reason;
      eos::common::SymKey::DeBase64(reason64, reason);
      s_reason = reason.c_str();

      if (gOFS->zMQ->gFuseServer.Client().Evict(uuid, s_reason) == ENOENT) {
        stdErr += "error: no such client '";
        stdErr += uuid.c_str();
        retc = ENOENT;
        stdErr += "'";
      } else {
        stdOut += "info: evicted client '";
        stdOut += uuid.c_str();
        stdOut += "'";
        retc = 0;
      }
    } else if (mSubCmd == "dropcaps") {
      std::string uuid = pOpaque->Get("mgm.fusex.uuid") ?
                         pOpaque->Get("mgm.fusex.uuid") : "";
      std::string out;

      if (gOFS->zMQ->gFuseServer.Client().Dropcaps(uuid, out)) {
        stdErr += "error: no such client '";
        stdErr += uuid.c_str();
        retc = ENOENT;
        stdErr += "'";
      } else {
        retc = 0;
        stdOut += out.c_str();
      }
    } else if (mSubCmd == "droplocks") {
      std::string sinode = pOpaque->Get("mgm.inode") ?
                           pOpaque->Get("mgm.inode") : "";
      std::string spid = pOpaque->Get("mgm.fusex.pid") ?
                         pOpaque->Get("mgm.fusex.pid") : "";
      uint64_t inode = strtoull(sinode.c_str(), 0, 16);
      uint64_t pid   = strtoull(spid.c_str(), 0, 10);

      if (gOFS->zMQ->gFuseServer.Locks().dropLocks(inode, pid)) {
        stdErr += "error: no such lock for inode '";
        stdErr += sinode.c_str();
        stdErr += "'";
        stdErr += " and process '";
        stdErr += spid.c_str();
        stdErr += "'";
        retc = ENOENT;
      } else {
        retc = 0;
        stdOut += "success: removed locks for inode '";
        stdOut += sinode.c_str();
        stdOut += "'";
        stdOut += " and process '";
        stdOut += spid.c_str();
        stdOut += "'";
      }
    } else if (mSubCmd == "caps") {
      std::string option = pOpaque->Get("mgm.option") ? pOpaque->Get("mgm.option") :
                           "t";
      std::string filter = pOpaque->Get("mgm.filter") ? pOpaque->Get("mgm.filter") :
                           "";
      filter = eos::common::StringConversion::curl_unescaped(filter.c_str());
      stdOut += gOFS->zMQ->gFuseServer.Cap().Print(option, filter).c_str();
      retc = 0;
    } else {
      stdErr += "error: subcmd not implemented";
      retc = EINVAL;
    }
  } else {
    stdErr += "error: you have to be root to list VSTs";
    retc = EPERM;
  }

  return SFS_OK;
}

EOSMGMNAMESPACE_END
