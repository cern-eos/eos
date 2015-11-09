// ----------------------------------------------------------------------
// File: Drop.cc
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

  EXEC_TIMING_BEGIN("Drop");
  // drops a replica
  int envlen;
  eos_thread_info("drop request for %s", env.Env(envlen));
  char* afid = env.Get("mgm.fid");
  char* afsid = env.Get("mgm.fsid");

  if (afid && afsid)
  {
    unsigned long fsid = strtoul(afsid, 0, 10);

    // ---------------------------------------------------------------------
    eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
    eos::IFileMD* fmd = 0;
    eos::IContainerMD* container = 0;
    eos::IQuotaNode* ns_quota = 0;

    try
    {
      fmd = eosFileService->getFileMD(eos::common::FileId::Hex2Fid(afid));
    }
    catch (...)
    {
      eos_thread_warning("no meta record exists anymore for fid=%s", afid);
      fmd = 0;
    }

    if (fmd)
    {
      try
      {
        container = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
      }
      catch (eos::MDException &e)
      {
        container = 0;
      }
    }

    if (container)
    {
      try
      {
        ns_quota = gOFS->eosView->getQuotaNode(container);

        if (ns_quota)
          ns_quota->removeFile(fmd);
      }
      catch (eos::MDException &e)
      {
        ns_quota = 0;
      }
    }

    if (fmd)
    {
      try
      {
        // If mgm.dropall flag is set then it means we got a deleteOnClose
        // at the gateway node and we need to delete all replicas
        char* drop_all = env.Get("mgm.dropall");
        std::vector<unsigned int> drop_fsid;
        bool updatestore = false;

        if (drop_all)
        {
          for (unsigned int i = 0; i < fmd->getNumLocation(); i++)
            drop_fsid.push_back(fmd->getLocation(i));
        }
        else
        {
          drop_fsid.push_back(fsid);
        }

        // Drop the selected replicas
        for (auto id = drop_fsid.begin(); id != drop_fsid.end(); id++)
        {
          eos_thread_debug("removing location %u of fid=%s", *id, afid);
          updatestore = false;

          if (fmd->hasLocation(*id))
          {
            fmd->unlinkLocation(*id);
            updatestore = true;
          }

          if (fmd->hasUnlinkedLocation(*id))
          {
            fmd->removeLocation(*id);
            updatestore = true;
          }

          if (updatestore)
          {
            gOFS->eosView->updateFileStore(fmd);
            // After update we have to get the new address - who knows ...
            fmd = eosFileService->getFileMD(eos::common::FileId::Hex2Fid(afid));
          }

          if (ns_quota)
            ns_quota->addFile(fmd);
        }

        // Finally delete the record if all replicas are dropped
        if ((!fmd->getNumUnlinkedLocation()) && (!fmd->getNumLocation()))
        {
          if (ns_quota)
          {
            // If we were still attached to a container, we can now detach
            // and count the file as removed
            ns_quota->removeFile(fmd);
          }

          gOFS->eosView->removeFile(fmd);
        }
      }
      catch (...)
      {
        eos_thread_warning("no meta record exists anymore for fid=%s", afid);
      };
    }

    gOFS->MgmStats.Add("Drop", vid.uid, vid.gid, 1);

    const char* ok = "OK";
    error.setErrInfo(strlen(ok) + 1, ok);
    EXEC_TIMING_END("Drop");
    return SFS_DATA;
  }
}
