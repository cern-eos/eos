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

/*----------------------------------------------------------------------------*/
#include "fst/storage/Storage.hh"
#include "fst/XrdFstOfs.hh"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Storage::Remover ()
{
  static time_t lastAskedForDeletions = 0;

  std::string nodeconfigqueue = "";
  const char* val = 0;
  // we have to wait that we know our node config queue
  while (!(val = eos::fst::Config::gConfig.FstNodeConfigQueue.c_str()))
  {
    XrdSysTimer sleeper;
    sleeper.Snooze(5);
    eos_static_info("Snoozing ...");
  }

  nodeconfigqueue = eos::fst::Config::gConfig.FstNodeConfigQueue.c_str();

  // this thread unlinks stored files
  while (1)
  {
    // since we use vector and erase from the beginning, this is not really a perfect choice, but we don't have any performance issues here
    deletionsMutex.Lock();
    while (deletions.size())
    {
      eos_static_debug("%u files to delete", deletions.size());
      Deletion todelete = deletions[0];
      deletionsMutex.UnLock();

      for (unsigned int j = 0; j < todelete.fIdVector.size(); j++)
      {
        eos_static_debug("Deleting File Id=%llu on Fs=%u", todelete.fIdVector[j], todelete.fsId);
        // delete the file
        XrdOucString hexstring = "";
        eos::common::FileId::Fid2Hex(todelete.fIdVector[j], hexstring);
        static XrdOucErrInfo error;

        XrdOucString capOpaqueString = "/?mgm.pcmd=drop";
        XrdOucString OpaqueString = "";
        OpaqueString += "&mgm.fsid=";
        OpaqueString += (int) todelete.fsId;
        OpaqueString += "&mgm.fid=";
        OpaqueString += hexstring;
        OpaqueString += "&mgm.localprefix=";
        OpaqueString += todelete.localPrefix;
        XrdOucEnv Opaque(OpaqueString.c_str());
        capOpaqueString += OpaqueString;

        if ((gOFS._rem("/DELETION", error, (const XrdSecEntity*) 0, &Opaque, 0, 0, 0, true) != SFS_OK))
        {
          eos_static_warning("unable to remove fid %s fsid %lu localprefix=%s", hexstring.c_str(), todelete.fsId, todelete.localPrefix.c_str());
        }

        // update the manager
        int rc = gOFS.CallManager(&error, 0, todelete.managerId.c_str(), capOpaqueString);
        if (rc)
        {
          eos_static_err("unable to drop file id %s fsid %u at manager %s", hexstring.c_str(), todelete.fsId, todelete.managerId.c_str());
        }
      }
      deletionsMutex.Lock();
      deletions.erase(deletions.begin());
    }

    deletionsMutex.UnLock();
    XrdSysTimer msSleep;
    msSleep.Wait(100);

    time_t now = time(NULL);

    // ask to schedule deletions every 5 minutes
    if ((now - lastAskedForDeletions) > 300)
    {
      // ---------------------------------------
      // get some global variables
      // ---------------------------------------
      gOFS.ObjectManager.HashMutex.LockRead();

      XrdMqSharedHash* confighash = gOFS.ObjectManager.GetHash(nodeconfigqueue.c_str());
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

      XrdOucString response = "";
      int rc = gOFS.CallManager(&error, "/", manager.c_str(), managerQuery, &response);
      if (rc)
      {
        eos_static_err("manager returned errno=%d", rc);
      }
      else
      {
        if (response == "submitted")
        {
          eos_static_debug("manager scheduled deletions for us!");
          // we wait 30 seconds to receive our deletions
          XrdSysTimer Sleeper;
          Sleeper.Snooze(30);
        }
        else
        {
          eos_static_debug("manager returned no deletion to schedule [ENODATA]");
        }
      }
    }
  }
}

EOSFSTNAMESPACE_END


