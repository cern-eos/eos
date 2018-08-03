// ----------------------------------------------------------------------
// File: Remover.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#include "fst/storage/Storage.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/Deletion.hh"

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Storage::Remover()
{
  static time_t lastAskedForDeletions = 0;
  static int deletionInterval = 300;
  std::string nodeconfigqueue =
    eos::fst::Config::gConfig.getFstNodeConfigQueue("Remover").c_str();
  std::unique_ptr<Deletion> to_del {};

  if (getenv("EOS_FST_DELETE_QUERY_INTERVAL")) {
    try {
      deletionInterval = stoi(getenv("EOS_FST_DELETE_QUERY_INTERVAL"));
    } catch (...) {}
  }

  // Thread that unlinks stored files
  while (true) {
    while ((to_del = GetDeletion())) {
      eos_static_debug("%u files to delete", GetNumDeletions());

      for (unsigned int j = 0; j < to_del->fileVector.size(); ++j) {
        eos_static_debug("Deleting file_id=%llu on fs_id=%u",
                         to_del->fileVector[j].fId, to_del->fsId);
        XrdOucString hexstring = "";
        eos::common::FileId::Fid2Hex(to_del->fileVector[j].fId, hexstring);
        XrdOucErrInfo error;
        XrdOucString capOpaqueString = "/?mgm.pcmd=drop";
        XrdOucString OpaqueString = "";
        OpaqueString += "&mgm.fsid=";
        OpaqueString += (int) to_del->fsId;
        OpaqueString += "&mgm.fid=";
        OpaqueString += hexstring;
        OpaqueString += "&mgm.localprefix=";
        OpaqueString += to_del->localPrefix;
        if (to_del->fileVector[j].logicalPath.length()) {
          OpaqueString += "&mgm.lpath=";
          OpaqueString += to_del->fileVector[j].logicalPath;
          OpaqueString += "&mgm.ctime=";
          OpaqueString += to_del->fileVector[j].cTime;
        }
        XrdOucEnv Opaque(OpaqueString.c_str());
        capOpaqueString += OpaqueString;

        if ((gOFS._rem("/DELETION", error, (const XrdSecEntity*) 0, &Opaque,
                       0, 0, 0, true) != SFS_OK)) {
          std::string errMsg = "unable to remove fid %s fsid %lu localprefix=%s";
          if (to_del->fileVector[j].logicalPath.length()) {
            errMsg += " logicalpath=%s creation_time=%s";
          }
          eos_static_err(errMsg.c_str(), hexstring.c_str(), to_del->fsId, to_del->localPrefix.c_str(),
                         to_del->fileVector[j].logicalPath.c_str(),
                         to_del->fileVector[j].cTime.c_str());
        }

        // Update the manager
        int rc = gOFS.CallManager(&error, 0, 0, capOpaqueString);

        if (rc) {
          eos_static_err("unable to drop file id %s fsid %u at manager %s",
                         hexstring.c_str(), to_del->fsId, to_del->managerId.c_str());
        }
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    time_t now = time(NULL);

    // Ask to schedule deletions regularly (default is every 5 minutes)
    if ((now - lastAskedForDeletions) > deletionInterval) {
      // get some global variables
      gOFS.ObjectManager.HashMutex.LockRead();
      XrdMqSharedHash* confighash = gOFS.ObjectManager.GetHash(
                                      nodeconfigqueue.c_str());
      std::string manager = confighash ? confighash->Get("manager") : "unknown";
      eos_static_debug("manager=%s", manager.c_str());
      gOFS.ObjectManager.HashMutex.UnLockRead();
      // ---------------------------------------
      lastAskedForDeletions = now;
      eos_static_debug("asking for new deletions");
      XrdOucString managerQuery = "/?";
      managerQuery += "mgm.pcmd=schedule2delete";
      managerQuery += "&mgm.target.nodename=";
      managerQuery += Config::gConfig.FstQueue;
      // the log ID to the schedule2delete call
      managerQuery += "&mgm.logid=";
      managerQuery += logId;
      XrdOucErrInfo error;
      XrdOucString response = "";
      int rc = gOFS.CallManager(&error, "/", 0, managerQuery, &response);

      if (rc) {
        eos_static_err("manager returned errno=%d", rc);
      } else {
        if (response == "submitted") {
          eos_static_debug("manager scheduled deletions for us!");
          // We wait 30 seconds to receive our deletions
          std::this_thread::sleep_for(std::chrono::seconds(30));
        } else {
          eos_static_debug("manager returned no deletion to schedule [ENODATA]");
        }
      }
    }
  }
}

EOSFSTNAMESPACE_END
