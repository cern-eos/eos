// ----------------------------------------------------------------------
// File: Schedule2Delete.cc
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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

{
  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  EXEC_TIMING_BEGIN("Scheduled2Delete");
  gOFS->MgmStats.Add("Schedule2Delete", 0, 0, 1);

  XrdOucString nodename = env.Get("mgm.target.nodename");

  eos_static_debug("nodename=%s", nodename.c_str() ? nodename.c_str() : "-none-");
  std::vector <unsigned int> fslist;
  // get a list of file Ids

  std::map<eos::common::FileSystem::fsid_t, eos::mgm::FileSystem*>::const_iterator it;

  {
    std::set<eos::common::FileSystem::fsid_t>::const_iterator set_it;

    // get all the filesystem's of that node
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    std::string snodename = nodename.c_str() ? nodename.c_str() : "-none-";
    if (!FsView::gFsView.mNodeView.count(snodename))
    {
      eos_static_warning("msg=\"node is not configured\" name=%s", snodename.c_str());
      return Emsg(epname, error, EINVAL, "unable to schedule - node is not existing");
    }

    for (set_it = FsView::gFsView.mNodeView[snodename]->begin(); set_it != FsView::gFsView.mNodeView[snodename]->end(); ++set_it)
    {
      fslist.push_back(*set_it);
    }
  }

  size_t totaldeleted = 0;

  for (unsigned int i = 0; i < fslist.size(); i++)
  {
    // ---------------------------------------------------------------------
    // loop over all file systems
    // ---------------------------------------------------------------------
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    eos::FileSystemView::FileIterator it;
    std::vector<eos::common::FileId::fileid_t> lIdVector;

    {
      // reduce lock contention
      eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
      std::pair<eos::FileSystemView::FileIterator, eos::FileSystemView::FileIterator> unlinkpair;
      try
      {
	unlinkpair = eosFsView->getUnlinkedFiles(fslist[i]);
	for (it = unlinkpair.first; it != unlinkpair.second; ++it) 
	  lIdVector.push_back(*it);
      }
      catch (...)
      {
	eos_static_debug("nothing to delete in fs %lu", (unsigned long) fslist[i]);
	continue;
      }

    }

    {
      XrdMqMessage message("deletion");
      eos::FileSystemView::FileIterator it;
      int ndeleted = 0;

      eos::mgm::FileSystem* fs = 0;
      XrdOucString receiver = "";
      XrdOucString msgbody = "mgm.cmd=drop";
      XrdOucString capability = "";
      XrdOucString idlist = "";
      for (size_t n=0; n < lIdVector.size(); n++)
      {
        eos_static_info("msg=\"add to deletion message\" fxid=%08llx fsid=%lu",
                        lIdVector[n], (unsigned long) fslist[i]);

        // loop over all files and emit a deletion message
        if (!fs)
        {
          // set the file system only for the first file to relax the mutex contention
          if (!fslist[i])
          {
            eos_err("no filesystem in deletion list");
            continue;
          }

          if (FsView::gFsView.mIdView.count(fslist[i]))
          {
            fs = FsView::gFsView.mIdView[fslist[i]];
          }
          else
          {
            fs = 0;
          }

          if (fs)
          {
            eos::common::FileSystem::fsstatus_t bootstatus = fs->GetStatus();
            // check the state of the filesystem (if it can actually delete in this moment!)
            if ((fs->GetConfigStatus() <= eos::common::FileSystem::kOff) ||
                (bootstatus != eos::common::FileSystem::kBooted))
            {
              // we don't need to send messages, this one is anyway down or currently booting
              break;
            }

            if ((fs->GetActiveStatus() == eos::common::FileSystem::kOffline))
            {
              break;
            }

            capability += "&mgm.access=delete";
            capability += "&mgm.manager=";
            capability += gOFS->ManagerId.c_str();
            capability += "&mgm.fsid=";
            capability += (int) fs->GetId();
            capability += "&mgm.localprefix=";
            capability += fs->GetPath().c_str();
            capability += "&mgm.fids=";
            receiver = fs->GetQueue().c_str();
          }
        }

        ndeleted++;
        totaldeleted++;

        XrdOucString sfid = "";
        XrdOucString hexfid = "";
        eos::common::FileId::Fid2Hex(lIdVector[n], hexfid);
        idlist += hexfid;
        idlist += ",";

        if (ndeleted > 1024)
        {
          XrdOucString refcapability = capability;
          refcapability += idlist;
          XrdOucEnv incapability(refcapability.c_str());
          XrdOucEnv* capabilityenv = 0;
          eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

          int caprc = 0;
          if ((caprc = gCapabilityEngine.Create(&incapability, capabilityenv, symkey)))
          {
            eos_static_err("unable to create capability - errno=%u", caprc);
          }
          else
          {
            int caplen = 0;
            msgbody += capabilityenv->Env(caplen);
            // we send deletions in bunches of max 1024 for efficiency
            message.SetBody(msgbody.c_str());

            if (!Messaging::gMessageClient.SendMessage(message, receiver.c_str()))
            {
              eos_static_err("unable to send deletion message to %s", receiver.c_str());
            }
          }
          idlist = "";
          ndeleted = 0;
          msgbody = "mgm.cmd=drop";
          if (capabilityenv)
            delete capabilityenv;
        }
      }

      // send the remaining ids
      if (idlist.length())
      {
        XrdOucString refcapability = capability;
        refcapability += idlist;
        XrdOucEnv incapability(refcapability.c_str());
        XrdOucEnv* capabilityenv = 0;
        eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

        int caprc = 0;
        if ((caprc = gCapabilityEngine.Create(&incapability, capabilityenv, symkey)))
        {
          eos_static_err("unable to create capability - errno=%u", caprc);
        }
        else
        {
          int caplen = 0;
          msgbody += capabilityenv->Env(caplen);
          // we send deletions in bunches of max 1000 for efficiency
          message.SetBody(msgbody.c_str());
          if (!Messaging::gMessageClient.SendMessage(message, receiver.c_str()))
          {
            eos_static_err("unable to send deletion message to %s", receiver.c_str());
          }
        }
        if (capabilityenv)
          delete capabilityenv;
      }
    }
  }
  // -----------------------------------------------------------------------
  if (totaldeleted)
  {
    EXEC_TIMING_END("Scheduled2Delete");
    gOFS->MgmStats.Add("Scheduled2Delete", 0, 0, totaldeleted);
    error.setErrInfo(0, "submitted");
    return SFS_DATA;
  }
  else
  {
    error.setErrInfo(0, "");
    return SFS_DATA;
  }
}
