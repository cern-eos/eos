// ----------------------------------------------------------------------
// File: Fsck.cc
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
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "common/StringConversion.hh"
#include "common/Mapping.hh"
#include "mgm/Fsck.hh"
#include "mgm/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/


#include <iostream>
#include <fstream>
#include <vector>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

        const char* Fsck::gFsckEnabled = "fsck";
const char* Fsck::gFsckInterval = "fsckinterval";

/*----------------------------------------------------------------------------*/
Fsck::Fsck ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Constructor
 *
 * By default FSCK is enabled and the interval is set to 30 minutes.
 */
{
  mRunning = false;
  mInterval = 30; // in minutes !
  mEnabled = "false";
}

/*----------------------------------------------------------------------------*/
bool
Fsck::Start (int interval)
/*----------------------------------------------------------------------------*/
/**
 * @brief Start FSCK thread
 * @param interval FSCK in minutes
 */
/*----------------------------------------------------------------------------*/
{
  if (interval)
  {
    mInterval = interval;
  }

  if (!mRunning)
  {
    XrdSysThread::Run(&mThread, Fsck::StaticCheck, static_cast<void *> (this), XRDSYSTHREAD_HOLD, "Fsck Thread");
    mRunning = true;
    mEnabled = "true";
    return StoreFsckConfig();
  }
  else
  {
    return false;
  }
}

/*----------------------------------------------------------------------------*/
bool
Fsck::Stop (bool store)
/*----------------------------------------------------------------------------*/
/**
 * @brief Stop the FSCK thread.
 */
/*----------------------------------------------------------------------------*/
{
  if (mRunning)
  {
    eos_static_info("cancel fsck thread");
    // -------------------------------------------------------------------------
    // cancel the FSCK thread
    // -------------------------------------------------------------------------
    XrdSysThread::Cancel(mThread);

    // -------------------------------------------------------------------------
    // join the master thread
    // -------------------------------------------------------------------------
    XrdSysThread::Detach(mThread);
    XrdSysThread::Join(mThread, NULL);
    eos_static_info("joined fsck thread");
    mRunning = false;
    mEnabled = false;
    Log(false, "disabled check");
    if (store)
      return StoreFsckConfig();
    else
      return true;
  }
  else
  {
    return false;
  }
}

/*----------------------------------------------------------------------------*/
Fsck::~Fsck ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Destructor
 *
 * Stops a running FSCK thread.
 */
/*----------------------------------------------------------------------------*/
{
  if (mRunning)
    Stop(false);
}

/*----------------------------------------------------------------------------*/
void
Fsck::ApplyFsckConfig ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Apply the FSCK configuration stored in the configuration engine
 */
/*----------------------------------------------------------------------------*/
{
  std::string enabled = FsView::gFsView.GetGlobalConfig(gFsckEnabled);
  if (enabled.length())
  {
    mEnabled = enabled.c_str();
  }
  std::string interval = FsView::gFsView.GetGlobalConfig(gFsckInterval);
  if (interval.length())
  {
    mInterval = atoi(interval.c_str());
    if (mInterval < 0)
    {
      mInterval = 30;
    }
  }

  Log(false, "enabled=%s", mEnabled.c_str());
  Log(false, "check interval=%d minutes", mInterval);

  if (mEnabled == "true")
  {
    Start();
  }
  else
  {
    Stop();
  }
}

/*----------------------------------------------------------------------------*/
bool
Fsck::StoreFsckConfig ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Store the current running FSCK configuration to the config engine
 */
/*----------------------------------------------------------------------------*/
{
  bool ok = 1;
  XrdOucString sInterval = "";
  sInterval += (int) mInterval;
  ok &= FsView::gFsView.SetGlobalConfig(gFsckEnabled, mEnabled.c_str());
  ok &= FsView::gFsView.SetGlobalConfig(gFsckInterval, sInterval.c_str());
  return ok;
}

/*----------------------------------------------------------------------------*/
void*
Fsck::StaticCheck (void* arg)
/*----------------------------------------------------------------------------*/
/**
 * @brief Static thread-startup function
 */
/*----------------------------------------------------------------------------*/
{
  return reinterpret_cast<Fsck*> (arg)->Check();
}

/*----------------------------------------------------------------------------*/
void*
Fsck::Check (void)
/*----------------------------------------------------------------------------*/
/**
 * @brief Looping thread function collecting FSCK results
 */
/*----------------------------------------------------------------------------*/
{
  XrdSysThread::SetCancelOn();
  XrdSysThread::SetCancelDeferred();

  XrdSysTimer sleeper;

  int bccount = 0;

  ClearLog();

  bool go = false;
  do
  {
    // --------------------------------------------------------
    // wait that the namespace is booted
    // --------------------------------------------------------
    {
      XrdSysMutexHelper(gOFS->InitializationMutex);
      if (gOFS->Initialized == gOFS->kBooted)
      {
        go = true;
      }
    }
    if (!go)
    {
      sleeper.Snooze(10);
    }
  }
  while (!go);

  while (1)
  {
    XrdSysThread::SetCancelOff();
    sleeper.Snooze(1);
    eos_static_debug("Started consistency checker thread");
    ClearLog();
    Log(false, "started check");

    // -------------------------------------------------------------------------
    // don't run fsck if we are not a master
    // -------------------------------------------------------------------------
    bool IsMaster = false;
    while (!IsMaster)
    {
      IsMaster = gOFS->MgmMaster.IsMaster();
      if (!IsMaster)
      {
        XrdSysThread::SetCancelOn();
        sleeper.Snooze(60);
      }
    }
    XrdSysThread::SetCancelOff();

    // -------------------------------------------------------------------------
    // run through the fst's
    // compare files on disk with files in the namespace
    // -------------------------------------------------------------------------

    size_t max = 0;
    {
      {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        max = FsView::gFsView.mIdView.size();
      }
      Log(false, "Filesystems to check: %lu", max);
      eos_static_debug("filesystems to check: %lu", max);
    }

    std::map<eos::common::FileSystem::fsid_t, FileSystem*>::const_iterator it;

    XrdOucString broadcastresponsequeue = gOFS->MgmOfsBrokerUrl;
    broadcastresponsequeue += "-fsck-";
    broadcastresponsequeue += bccount;
    XrdOucString broadcasttargetqueue = gOFS->MgmDefaultReceiverQueue;

    XrdOucString msgbody;
    msgbody = "mgm.cmd=fsck&mgm.fsck.tags=*";

    XrdOucString stdOut = "";
    XrdOucString stdErr = "";

    if (!gOFS->MgmOfsMessaging->BroadCastAndCollect(broadcastresponsequeue, broadcasttargetqueue, msgbody, stdOut, 10))
    {
      eos_static_err("failed to broad cast and collect fsck from [%s]:[%s]", broadcastresponsequeue.c_str(), broadcasttargetqueue.c_str());
      stdErr = "error: broadcast failed\n";
    }

    ResetErrorMaps();

    std::vector<std::string> lines;

    // -------------------------------------------------------------------------
    // convert into a lines-wise seperated array
    // -------------------------------------------------------------------------

    eos::common::StringConversion::StringToLineVector((char*) stdOut.c_str(), lines);

    for (size_t nlines = 0; nlines < lines.size(); nlines++)
    {
      std::set<unsigned long long> fids;
      unsigned long fsid = 0;
      std::string errortag;
      if (eos::common::StringConversion::ParseStringIdSet((char*) lines[nlines].c_str(), errortag, fsid, fids))
      {
        std::set<unsigned long long>::const_iterator it;
        if (fsid)
        {
          XrdSysMutexHelper lock(eMutex);
          for (it = fids.begin(); it != fids.end(); it++)
          {
            // -----------------------------------------------------------------
            // sort the fids into the error maps
            // -----------------------------------------------------------------
            eFsMap[errortag][fsid].insert(*it);
            eMap[errortag].insert(*it);
            eCount[errortag]++;
          }
        }
      }
      else
      {
        eos_static_err("Can not parse fsck response: %s", lines[nlines].c_str());
      }
    }

    // -------------------------------------------------------------------------
    // grab all files which are damaged because filesystems are down
    // -------------------------------------------------------------------------
    {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      std::map<eos::common::FileSystem::fsid_t, FileSystem*>::const_iterator it;
      // loop over all filesystems and check their status
      for (it = FsView::gFsView.mIdView.begin(); it != FsView::gFsView.mIdView.end(); it++)
      {
        eos::common::FileSystem::fsid_t fsid = it->first;
        eos::common::FileSystem::fsactive_t fsactive = it->second->GetActiveStatus();
        eos::common::FileSystem::fsstatus_t fsconfig = it->second->GetConfigStatus();
        eos::common::FileSystem::fsstatus_t fsstatus = it->second->GetStatus();
        if ((fsstatus == eos::common::FileSystem::kBooted) &&
            (fsconfig >= eos::common::FileSystem::kDrain) &&
            (fsactive))
        {
          // -------------------------------------------------------------------
          // this is healthy, don't need to do anything
          // -------------------------------------------------------------------
        }
        else
        {
          // -------------------------------------------------------------------
          // this is not ok and contributes to replica offline errors
          // -------------------------------------------------------------------
          try
          {
            eos::common::RWMutexReadLock nslock(gOFS->eosViewRWMutex);
            eos::FileMD* fmd = 0;
            eos::FileSystemView::FileList filelist = gOFS->eosFsView->getFileList(fsid);
            eos::FileSystemView::FileIterator it;
            for (it = filelist.begin(); it != filelist.end(); ++it)
            {
              fmd = gOFS->eosFileService->getFileMD(*it);
              if (fmd)
              {
                XrdSysMutexHelper lock(eMutex);
                eFsUnavail[fsid]++;
                eFsMap["rep_offline"][fsid].insert(*it);
                eMap["rep_offline"].insert(*it);
                eCount["rep_offline"]++;
              }
            }
          }
          catch (eos::MDException &e)
          {
            errno = e.getErrno();
            eos_static_debug("caught exception %d %s\n",
                             e.getErrno(),
                             e.getMessage().str().c_str());
          }
        }
      }
    }

    // -------------------------------------------------------------------------
    // grab all files with have no replicas at all
    // -------------------------------------------------------------------------
    {
      try
      {
        eos::common::RWMutexReadLock nslock(gOFS->eosViewRWMutex);
        eos::FileMD* fmd = 0;
        eos::FileSystemView::FileList filelist =
                gOFS->eosFsView->getNoReplicasFileList();

        eos::FileSystemView::FileIterator it;
        for (it = filelist.begin(); it != filelist.end(); ++it)
        {
          fmd = gOFS->eosFileService->getFileMD(*it);
          std::string path = gOFS->eosView->getUri(fmd);
          XrdOucString fullpath = path.c_str();
          if (fullpath.beginswith(gOFS->MgmProcPath))
          {
            // -----------------------------------------------------------------
            // don't report eos /proc files
            // -----------------------------------------------------------------
            continue;
          }

          if (fmd)
          {
            XrdSysMutexHelper lock(eMutex);
            eMap["zero_replica"].insert(*it);
            eCount["zero_replica"]++;
          }
        }
      }
      catch (eos::MDException &e)
      {
        errno = e.getErrno();
        eos_static_debug("caught exception %d %s\n",
                         e.getErrno(),
                         e.getMessage().str().c_str());
      }
    }

    std::map<std::string,
            std::set <eos::common::FileId::fileid_t> >::const_iterator emapit;

    // look over unavailable filesystems
    std::map<eos::common::FileSystem::fsid_t,
            unsigned long long >::const_iterator unavailit;

    for (unavailit = eFsUnavail.begin();
            unavailit != eFsUnavail.end();
            unavailit++)
    {
      std::string host = "not configured";
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      if (FsView::gFsView.mIdView.count(unavailit->first))
      {
        host = FsView::gFsView.mIdView[unavailit->first]->GetString("hostport");

      }
      Log(false,
          "host=%s fsid=%lu  replica_offline=%llu ",
          host.c_str(),
          unavailit->first,
          unavailit->second);
    }

    {
      // -----------------------------------------------------------------------
      // loop over all replica_offline and layout error files to assemble a
      // file offline list
      // -----------------------------------------------------------------------
      std::set <eos::common::FileId::fileid_t>::const_iterator it;
      std::set <eos::common::FileId::fileid_t> fid2check;
      for (it = eMap["rep_offline"].begin();
              it != eMap["rep_offline"].end();
              it++)
      {
        fid2check.insert(*it);
      }
      for (it = eMap["rep_diff_n"].begin();
              it != eMap["rep_diff_n"].end();
              it++)
      {
        fid2check.insert(*it);
      }

      for (it = fid2check.begin(); it != fid2check.end(); it++)
      {
        eos::FileMD* fmd = 0;

        // ---------------------------------------------------------------------
        // check if locations are online
        // ---------------------------------------------------------------------
        eos::FileMD::LocationVector::const_iterator lociter;
        try
        {
          eos::common::RWMutexReadLock nslock(gOFS->eosViewRWMutex);
          fmd = gOFS->eosFileService->getFileMD(*it);
        }
        catch (eos::MDException &e)
        {
          // -------------------------------------------------------------------
          // nothing to catch
          // -------------------------------------------------------------------
        }
        if (!fmd)
          continue;

        eos::FileMD fmdCopy(*fmd);
        fmd = &fmdCopy;

        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        size_t nlocations = fmd->getNumLocation();
        size_t offlinelocations = 0;

        for (lociter = fmd->locationsBegin(); lociter != fmd->locationsEnd(); ++lociter)
        {
          if (*lociter)
          {
            if (FsView::gFsView.mIdView.count(*lociter))
            {
              eos::common::FileSystem::fsstatus_t bootstatus =
                      (FsView::gFsView.mIdView[*lociter]->GetStatus(true));

              eos::common::FileSystem::fsstatus_t configstatus =
                      (FsView::gFsView.mIdView[*lociter]->GetConfigStatus());

              bool conda =
                      (FsView::gFsView.mIdView[*lociter]->GetActiveStatus(true) == eos::common::FileSystem::kOffline);

              bool condb =
                      (bootstatus != eos::common::FileSystem::kBooted);

              bool condc =
                      (configstatus == eos::common::FileSystem::kDrainDead);

              if (conda || condb || condc)
              {
                offlinelocations++;
              }
            }
          }
        }

        // ---------------------------------------------------------------------
        // TODO: this condition has to be adjusted for RAIN layouts
        // ---------------------------------------------------------------------
        if (offlinelocations == nlocations)
        {
          XrdSysMutexHelper lock(eMutex);
          eMap["file_offline"].insert(*it);
          eCount["file_offline"]++;
        }
        if (offlinelocations && (offlinelocations != nlocations))
        {
          XrdSysMutexHelper lock(eMutex);
          eMap["adjust_replica"].insert(*it);
          eCount["adjust_replica"]++;
        }
      }
    }

    for (emapit = eMap.begin(); emapit != eMap.end(); emapit++)
    {
      Log(false, "%-30s : %llu (%llu)",
          emapit->first.c_str(),
          emapit->second.size(),
          eCount[emapit->first]);
    }

    {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      // -----------------------------------------------------------------------
      // look for dark MD entries e.g. filesystem ids which have MD entries,
      // but have not configured file system
      // -----------------------------------------------------------------------

      eos::common::RWMutexReadLock nslock(gOFS->eosViewRWMutex);
      size_t nfilesystems = gOFS->eosFsView->getNumFileSystems();
      for (size_t nfsid = 1; nfsid < nfilesystems; nfsid++)
      {
        try
        {
          eos::FileSystemView::FileList filelist =
                  gOFS->eosFsView->getFileList(nfsid);

          if (filelist.size())
          {
            // -----------------------------------------------------------------
            // check if this exists in the gFsView
            // -----------------------------------------------------------------
            if (!FsView::gFsView.mIdView.count(nfsid))
            {
              XrdSysMutexHelper lock(eMutex);
              eFsDark[nfsid] += filelist.size();
              Log(false,
                  "shadow fsid=%lu shadow_entries=%llu ",
                  nfsid,
                  filelist.size());
            }
          }
        }
        catch (eos::MDException &e)
        {
        }
      }
    }

    Log(false, "stopping check");
    Log(false, "=> next run in %d minutes", mInterval);
    XrdSysThread::SetCancelOn();

    // -------------------------------------------------------------------------
    // Wait for next FSCK round ...
    // -------------------------------------------------------------------------
    sleeper.Snooze(mInterval * 60);
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
void
Fsck::PrintOut (XrdOucString &out, XrdOucString option)
/*----------------------------------------------------------------------------*/
/**
 * @brief Print the current log output
 * @param out return of the log output
 * @param option not used
 */
/*----------------------------------------------------------------------------*/

{
  XrdSysMutexHelper lock(mLogMutex);
  out = mLog;
}

/*----------------------------------------------------------------------------*/
bool
Fsck::Usage (XrdOucString &out, XrdOucString &err)
/*----------------------------------------------------------------------------*/
/**
 * @brief return usage information
 * @param out return of the usage information
 * @param err return of the STDERR output
 */
/*----------------------------------------------------------------------------*/

{
  err += "error: invalid option specified\n";
  return false;
}

/*----------------------------------------------------------------------------*/
bool
Fsck::Report (XrdOucString &out, XrdOucString &err, XrdOucString option, XrdOucString selection)
/*----------------------------------------------------------------------------*/
/**
 * @brief Return the current FSCK report
 * @param out return of the report
 * @param err return of STDERR
 * @param option output format selection
 * @param selection selection of the error to report
 */
/*----------------------------------------------------------------------------*/

{
  bool printfid = (option.find("i") != STR_NPOS);
  bool printlfn = (option.find("l") != STR_NPOS);

  XrdSysMutexHelper lock(eMutex);

  XrdOucString checkoption = option;
  checkoption.replace("h", "");
  checkoption.replace("json", "");
  checkoption.replace("i", "");
  checkoption.replace("l", "");
  checkoption.replace("a", "");

  if (checkoption.length())
  {
    return Fsck::Usage(out, err);
  }

  char stimestamp[1024];
  snprintf(stimestamp,
           sizeof (stimestamp) - 1,
           "%lu",
           (unsigned long) eTimeStamp);

  if ((option.find("json") != STR_NPOS) || (option.find("j") != STR_NPOS))
  {
    //--------------------------------------------------------------------------
    // json output format
    //--------------------------------------------------------------------------
    out += "{\n";
    // put the check timestamp
    out += "  \"timestamp\": ";
    out += stimestamp;
    out += ",\n";

    if (!(option.find("a") != STR_NPOS))
    {
      //--------------------------------------------------------------------------
      // dump global table
      //--------------------------------------------------------------------------
      std::map<std::string, std::set <eos::common::FileId::fileid_t> >::const_iterator emapit;
      for (emapit = eMap.begin(); emapit != eMap.end(); emapit++)
      {
        if (selection.length() && (selection.find(emapit->first.c_str()) == STR_NPOS)) continue; // skip unselected
        char sn[1024];
        snprintf(sn,
                 sizeof (sn) - 1,
                 "%llu",
                 (unsigned long long) emapit->second.size());

        out += "  \"";
        out += emapit->first.c_str();
        out += "\": {\n";
        out += "    \"n\":\"";
        out += sn;
        out += "\",\n";
        if (printfid)
        {
          out += "    \"fxid\": [";
          std::set <eos::common::FileId::fileid_t>::const_iterator fidit;
          for (fidit = emapit->second.begin();
                  fidit != emapit->second.end();
                  fidit++)
          {
            XrdOucString hexstring;
            eos::common::FileId::Fid2Hex(*fidit, hexstring);
            out += hexstring.c_str();
            out += ",";
          }
          if (out.endswith(","))
          {
            out.erase(out.length() - 1);
          }
          out += "]\n";
        }
        if (printlfn)
        {
          out += "    \"lfn\": [";
          std::set <eos::common::FileId::fileid_t>::const_iterator fidit;
          for (fidit = emapit->second.begin();
                  fidit != emapit->second.end();
                  fidit++)
          {
            eos::FileMD* fmd = 0;
            eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
            try
            {
              fmd = gOFS->eosFileService->getFileMD(*fidit);
              std::string fullpath = gOFS->eosView->getUri(fmd);
              out += "\"";
              out += fullpath.c_str();
              out += "\"";
            }
            catch (eos::MDException &e)
            {
              out += "\"undefined\"";
            }
            out += ",";
          }
          if (out.endswith(","))
          {
            out.erase(out.length() - 1);
          }
          out += "]\n";
        }
        if (out.endswith(",\n"))
        {
          out.erase(out.length() - 2);
          out += "\n";
        }
        out += "  },\n";
      }
    }
    else
    {
      // do output per filesystem
      std::map<std::string,
              std::set <eos::common::FileId::fileid_t> >::const_iterator emapit;
      for (emapit = eMap.begin(); emapit != eMap.end(); emapit++)
      {
        if (selection.length() &&
            (selection.find(emapit->first.c_str()) == STR_NPOS))
          continue; // skip unselected
        //----------------------------------------------------------------------
        // loop over errors
        //----------------------------------------------------------------------
        char sn[1024];
        snprintf(sn, sizeof (sn) - 1, "%llu",
                 (unsigned long long) emapit->second.size());
        out += "  \"";
        out += emapit->first.c_str();
        out += "\": {\n";
        out += "    \"n\":\"";
        out += sn;
        out += "\",\n";
        out += "    \"fsid\":";
        out += " {\n";
        std::map < eos::common::FileSystem::fsid_t,
                std::set < eos::common::FileId::fileid_t >> ::const_iterator efsmapit;

        for (efsmapit = eFsMap[emapit->first].begin();
                efsmapit != eFsMap[emapit->first].end();
                efsmapit++)
        {
          if (emapit->first == "zero_replica")
          {
            //------------------------------------------------------------------
            // this we cannot break down by filesystem id
            //------------------------------------------------------------------
            continue;
          }
          //--------------------------------------------------------------------
          // loop over filesystems
          //--------------------------------------------------------------------
          out += "      \"";
          out += (int) efsmapit->first;
          out += "\": {\n";
          snprintf(sn, sizeof (sn) - 1,
                   "%llu",
                   (unsigned long long) efsmapit->second.size());
          out += "        \"n\": ";
          out += sn;
          out += ",\n";
          if (printfid)
          {
            out += "        \"fxid\": [";
            std::set <eos::common::FileId::fileid_t>::const_iterator fidit;
            for (fidit = efsmapit->second.begin();
                    fidit != efsmapit->second.end();
                    fidit++)
            {
              XrdOucString hexstring;
              eos::common::FileId::Fid2Hex(*fidit, hexstring);
              out += hexstring.c_str();
              out += ",";
            }
            if (out.endswith(","))
            {
              out.erase(out.length() - 1);
            }
            out += "]\n";
          }
          if (printlfn)
          {
            out += "        \"lfn\": [";
            std::set <eos::common::FileId::fileid_t>::const_iterator fidit;
            for (fidit = efsmapit->second.begin();
                    fidit != efsmapit->second.end();
                    fidit++)
            {
              eos::FileMD* fmd = 0;
              eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
              try
              {
                fmd = gOFS->eosFileService->getFileMD(*fidit);
                std::string fullpath = gOFS->eosView->getUri(fmd);
                out += "\"";
                out += fullpath.c_str();
                out += "\"";
              }
              catch (eos::MDException &e)
              {
                out += "\"undefined\"";
              }
              out += ",";
            }
            if (out.endswith(","))
            {
              out.erase(out.length() - 1);
            }
            out += "]\n";
          }
          if (out.endswith(",\n"))
          {
            out.erase(out.length() - 2);
            out += "\n";
          }
          out += "      },\n";
        }
        out += "    },\n";
      }
    }

    // list shadow filesystems
    std::map<eos::common::FileSystem::fsid_t,
            unsigned long long >::const_iterator fsit;

    out += "  \"shadow_fsid\": [";

    for (fsit = eFsDark.begin(); fsit != eFsDark.end(); fsit++)
    {
      char sfsid[1024];
      snprintf(sfsid, sizeof (sfsid) - 1,
               "%lu",
               (unsigned long) fsit->first);
      out += sfsid;
      out += ",";
    }
    if (out.endswith(","))
    {
      out.erase(out.length() - 1);
    }

    out += "  ]\n";
    out += "}\n";

  }
  else
  {
    // greppable format
    if (!(option.find("a") != STR_NPOS))
    {
      // give global table
      std::map<std::string,
              std::set <eos::common::FileId::fileid_t> >::const_iterator emapit;

      for (emapit = eMap.begin(); emapit != eMap.end(); emapit++)
      {
        if (selection.length() &&
            (selection.find(emapit->first.c_str()) == STR_NPOS))
          continue; // skip unselected
        char sn[1024];
        snprintf(sn, sizeof (sn) - 1,
                 "%llu",
                 (unsigned long long) emapit->second.size());

        out += "timestamp=";
        out += stimestamp;
        out += " ";
        out += "tag=\"";
        out += emapit->first.c_str();
        out += "\"";
        out += " n=";
        out += sn;
        if (printfid)
        {
          out += " fxid=";
          std::set <eos::common::FileId::fileid_t>::const_iterator fidit;
          for (fidit = emapit->second.begin();
                  fidit != emapit->second.end();
                  fidit++)
          {
            XrdOucString hexstring;
            eos::common::FileId::Fid2Hex(*fidit, hexstring);
            out += hexstring.c_str();
            out += ",";
          }
          if (out.endswith(","))
          {
            out.erase(out.length() - 1);
          }
          out += "\n";
        }
        if (printlfn)
        {
          out += " lfn=";
          std::set <eos::common::FileId::fileid_t>::const_iterator fidit;
          for (fidit = emapit->second.begin();
                  fidit != emapit->second.end();
                  fidit++)
          {
            eos::FileMD* fmd = 0;
            eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
            try
            {
              fmd = gOFS->eosFileService->getFileMD(*fidit);
              std::string fullpath = gOFS->eosView->getUri(fmd);
              out += "\"";
              out += fullpath.c_str();
              out += "\"";
            }
            catch (eos::MDException &e)
            {
              out += "\"undefined\"";
            }
            out += ",";
          }
          if (out.endswith(","))
          {
            out.erase(out.length() - 1);
          }
          out += "\n";
        }

        // list shadow filesystems
        std::map<eos::common::FileSystem::fsid_t,
                unsigned long long >::const_iterator fsit;

        out += " shadow_fsid=";
        for (fsit = eFsDark.begin(); fsit != eFsDark.end(); fsit++)
        {
          char sfsid[1024];
          snprintf(sfsid, sizeof (sfsid) - 1,
                   "%lu",
                   (unsigned long) fsit->first);
          out += sfsid;
          out += ",";
        }
        if (out.endswith(","))
        {
          out.erase(out.length() - 1);
        }
        out += "\n";
      }
    }
    else
    {
      //------------------------------------------------------------------------
      // do output per filesystem
      //------------------------------------------------------------------------
      std::map<std::string,
              std::set <eos::common::FileId::fileid_t> >::const_iterator emapit;
      std::map < eos::common::FileSystem::fsid_t, std::set < eos::common::FileId::fileid_t >> ::const_iterator efsmapit;

      for (emapit = eMap.begin(); emapit != eMap.end(); emapit++)
      {
        if (selection.length() &&
            (selection.find(emapit->first.c_str()) == STR_NPOS))
          continue; // skip unselected
        //----------------------------------------------------------------------
        // loop over filesystems
        //----------------------------------------------------------------------
        for (efsmapit = eFsMap[emapit->first].begin();
                efsmapit != eFsMap[emapit->first].end();
                efsmapit++)
        {
          if (emapit->first == "zero_replica")
          {
            //------------------------------------------------------------------
            // this we cannot break down by filesystem id
            //------------------------------------------------------------------
            continue;
          }

          char sn[1024];
          out += "timestamp=";
          out += stimestamp;
          out += " ";
          out += "tag=\"";
          out += emapit->first.c_str();
          out += "\"";
          out += " ";
          out += "fsid=";
          out += (int) efsmapit->first;
          snprintf(sn, sizeof (sn) - 1,
                   "%llu",
                   (unsigned long long) efsmapit->second.size());
          out += " n=";
          out += sn;
          if (printfid)
          {
            out += " fxid=";
            std::set <eos::common::FileId::fileid_t>::const_iterator fidit;
            for (fidit = efsmapit->second.begin();
                    fidit != efsmapit->second.end();
                    fidit++)
            {
              XrdOucString hexstring;
              eos::common::FileId::Fid2Hex(*fidit, hexstring);
              out += hexstring.c_str();
              out += ",";
            }
            if (out.endswith(","))
            {
              out.erase(out.length() - 1);
            }
            out += "\n";
          }
          else
          {
            if (printlfn)
            {
              out += " lfn=";
              std::set <eos::common::FileId::fileid_t>::const_iterator fidit;
              for (fidit = efsmapit->second.begin();
                      fidit != efsmapit->second.end();
                      fidit++)
              {
                eos::FileMD* fmd = 0;
                eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
                try
                {
                  fmd = gOFS->eosFileService->getFileMD(*fidit);
                  std::string fullpath = gOFS->eosView->getUri(fmd);
                  out += "\"";
                  out += fullpath.c_str();
                  out += "\"";
                }
                catch (eos::MDException &e)
                {
                  out += "\"undefined\"";
                }
                out += ",";
              }
              if (out.endswith(","))
              {
                out.erase(out.length() - 1);
              }
              out += "\n";
            }
            else
            {
              out += "\n";
            }
          }
        }
      }
    }
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool
Fsck::Repair (XrdOucString &out, XrdOucString &err, XrdOucString option)
/*----------------------------------------------------------------------------*/
/**
 * @brief Run a repair action
 * @param out return of the action output
 * @param err return of STDERR
 * @param option selection of repair action (see code or command help)
 */
/*----------------------------------------------------------------------------*/
{
  XrdSysMutexHelper lock(eMutex);

  // check for a valid action in option
  if ((option != "checksum") &&
      (option != "checksum-commit") &&
      (option != "resync") &&
      (option != "unlink-unregistered") &&
      (option != "unlink-orphans") &&
      (option != "adjust-replicas") &&
      (option != "adjust-replicas-nodrop") &&
      (option != "drop-missing-replicas") &&
      (option != "unlink-zero-replicas"))
  {
    err += "error: illegal option <";
    err += option;
    err += ">\n";
    return false;
  }

  if (option.beginswith("checksum"))
  {
    out += "# repair checksum -------------------------------------------------------------------------\n";
    std::map < eos::common::FileSystem::fsid_t,
            std::set < eos::common::FileId::fileid_t >> ::const_iterator efsmapit;
    std::map < eos::common::FileSystem::fsid_t,
            std::set < eos::common::FileId::fileid_t >> fid2check;

    // -------------------------------------------------------------------------
    // loop over all filesystems
    // -------------------------------------------------------------------------
    for (efsmapit = eFsMap["m_cx_diff"].begin();
            efsmapit != eFsMap["m_cx_diff"].end();
            efsmapit++)
    {
      std::set <eos::common::FileId::fileid_t>::const_iterator it;

      // -----------------------------------------------------------------------
      // loop over all fids
      // -----------------------------------------------------------------------
      for (it = efsmapit->second.begin();
              it != efsmapit->second.end();
              it++)
      {
        fid2check[efsmapit->first].insert(*it);
      }
    }

    // -------------------------------------------------------------------------
    // loop over all filesystems
    // -------------------------------------------------------------------------
    for (efsmapit = eFsMap["d_cx_diff"].begin();
            efsmapit != eFsMap["d_cx_diff"].end();
            efsmapit++)
    {
      std::set <eos::common::FileId::fileid_t>::const_iterator it;

      // -----------------------------------------------------------------------
      // loop over all fids
      // -----------------------------------------------------------------------
      for (it = efsmapit->second.begin(); it != efsmapit->second.end(); it++)
      {
        fid2check[efsmapit->first].insert(*it);
      }
    }

    // -------------------------------------------------------------------------
    // loop over all filesystems
    // -------------------------------------------------------------------------
    for (efsmapit = fid2check.begin(); efsmapit != fid2check.end(); efsmapit++)
    {
      std::set <eos::common::FileId::fileid_t>::const_iterator it;
      for (it = efsmapit->second.begin(); it != efsmapit->second.end(); it++)
      {
        eos::FileMD* fmd = 0;
        std::string path = "";
        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
        try
        {
          fmd = gOFS->eosFileService->getFileMD(*it);
          path = gOFS->eosView->getUri(fmd);
        }
        catch (eos::MDException &e)
        {
        }

        // ---------------------------------------------------------------------
        // issue verify operations on that particular filesystem
        // ---------------------------------------------------------------------
        eos::common::Mapping::VirtualIdentity vid;
        eos::common::Mapping::Root(vid);
        XrdOucErrInfo error;
        int lretc = 1;
        if (path.length())
        {
          if (option == "checksum-commit")
          {
            // -----------------------------------------------------------------
            // verify & commit
            lretc = gOFS->_verifystripe(path.c_str(),
                                        error,
                                        vid,
                                        efsmapit->first,
                                        "&mgm.verify.compute.checksum=1&mgm.verify.commit.checksum=1&mgm.verify.commit.size=1");
          }
          else
          {
            // -----------------------------------------------------------------
            // verify only
            // -----------------------------------------------------------------
            lretc = gOFS->_verifystripe(path.c_str(),
                                        error,
                                        vid,
                                        efsmapit->first,
                                        "&mgm.verify.compute.checksum=1");
          }
          if (!lretc)
          {
            out += "success: sending verify to fsid=";
            out += (int) efsmapit->first;
            out += " for path=";
            out += path.c_str();
            out += "\n";
          }
          else
          {
            out += "error: sending verify to fsid=";
            out += (int) efsmapit->first;
            out += " failed for path=";
            out += path.c_str();
            out += "\n";
          }
        }
      }
    }
    return true;
  }

  if (option.beginswith("resync"))
  {
    out += "# resycnc         -------------------------------------------------------------------------\n";
    std::map < eos::common::FileSystem::fsid_t,
            std::set < eos::common::FileId::fileid_t >> ::const_iterator efsmapit;

    std::map < eos::common::FileSystem::fsid_t,
            std::set < eos::common::FileId::fileid_t >> fid2check;

    std::map<std::string, std::set <eos::common::FileId::fileid_t> >::const_iterator emapit;

    for (emapit = eMap.begin(); emapit != eMap.end(); emapit++)
    {
      // -----------------------------------------------------------------------
      // we don't sync offline replicas
      // -----------------------------------------------------------------------
      if (emapit->first == "rep_offline")
      {
        continue;
      }

      // -----------------------------------------------------------------------
      // loop over all filesystems
      // -----------------------------------------------------------------------
      for (efsmapit = eFsMap[emapit->first].begin(); efsmapit != eFsMap[emapit->first].end(); efsmapit++)
      {
        std::set <eos::common::FileId::fileid_t>::const_iterator it;

        // ---------------------------------------------------------------------
        // loop over all fids
        // ---------------------------------------------------------------------
        for (it = efsmapit->second.begin(); it != efsmapit->second.end(); it++)
        {
          fid2check[efsmapit->first].insert(*it);
        }
      }
    }

    // -------------------------------------------------------------------------
    // loop over all filesystems
    // -------------------------------------------------------------------------
    for (efsmapit = fid2check.begin(); efsmapit != fid2check.end(); efsmapit++)
    {
      std::set <eos::common::FileId::fileid_t>::const_iterator it;
      for (it = efsmapit->second.begin(); it != efsmapit->second.end(); it++)
      {
        eos::FileMD* fmd = 0;
        std::string path = "";
        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
        try
        {
          fmd = gOFS->eosFileService->getFileMD(*it);
        }
        catch (eos::MDException &e)
        {
          fmd = 0;
        }
        if (fmd)
        {
          int lretc = 0;
          // -------------------------------------------------------------------
          // issue a resync command for a filesystem/fid pair
          // -------------------------------------------------------------------
          lretc = gOFS->SendResync(*it, efsmapit->first);
          if (lretc)
          {
            char outline[1024];
            snprintf(outline,
                     sizeof (outline) - 1,
                     "success: sending resync to fsid=%u fxid=%llx\n",
                     efsmapit->first, *it);
            out += outline;
          }
          else
          {
            char outline[1024];
            snprintf(outline,
                     sizeof (outline) - 1,
                     "error: sending resync to fsid=%u failed for fxid=%llx\n",
                     efsmapit->first, *it);
            out += outline;
          }
        }
        else
        {
          char outline[1024];
          snprintf(outline,
                   sizeof (outline) - 1,
                   "error: no file meta data for fsid=%u failed for fxid=%llx\n",
                   efsmapit->first, *it);
          out += outline;
        }
      }
    }
    return true;
  }

  if (option == "unlink-unregistered")
  {
    out += "# unlink unregistered ---------------------------------------------------------------------\n";
    // unlink all unregistered files
    std::map < eos::common::FileSystem::fsid_t,
            std::set < eos::common::FileId::fileid_t >> ::const_iterator efsmapit;

    std::map < eos::common::FileSystem::fsid_t,
            std::set < eos::common::FileId::fileid_t >> fid2check;

    eos::common::Mapping::VirtualIdentity vid;
    eos::common::Mapping::Root(vid);
    XrdOucErrInfo error;

    // -------------------------------------------------------------------------
    // loop over all filesystems
    // -------------------------------------------------------------------------
    for (efsmapit = eFsMap["unreg_n"].begin();
            efsmapit != eFsMap["unreg_n"].end();
            efsmapit++)
    {
      std::set <eos::common::FileId::fileid_t>::const_iterator it;

      // -----------------------------------------------------------------------
      // loop over all fids
      // -----------------------------------------------------------------------
      for (it = efsmapit->second.begin(); it != efsmapit->second.end(); it++)
      {
        eos::FileMD* fmd = 0;
        bool haslocation = false;
        std::string spath = "";
        // ---------------------------------------------------------------------
        // crosscheck if the location really is not attached
        // ---------------------------------------------------------------------
        try
        {
          eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
          fmd = gOFS->eosFileService->getFileMD(*it);
          spath = gOFS->eosView->getUri(fmd);
          if (fmd->hasLocation(efsmapit->first))
          {
            haslocation = true;
          }
        }
        catch (eos::MDException &e)
        {
        }

        // ---------------------------------------------------------------------
        // send external deletion
        // ---------------------------------------------------------------------

        if (gOFS->DeleteExternal(efsmapit->first, *it))
        {
          char outline[1024];
          snprintf(outline,
                   sizeof (outline) - 1,
                   "success: send unlink to fsid=%u fxid=%llx\n",
                   efsmapit->first, *it);

          out += outline;
        }
        else
        {
          char errline[1024];
          snprintf(errline,
                   sizeof (errline) - 1,
                   "err: unable to send unlink to fsid=%u fxid=%llx\n",
                   efsmapit->first, *it);

          out += errline;
        }

        if (fmd && haslocation)
        {
          // -------------------------------------------------------------------
          // drop in the namespace
          // -------------------------------------------------------------------
          if (gOFS->_dropstripe(spath.c_str(), error, vid, efsmapit->first, false))
          {
            char outline[1024];
            snprintf(outline,
                     sizeof (outline) - 1,
                     "error: unable to drop stripe on fsid=%u fxid=%llx\n",
                     efsmapit->first, *it);
            out += outline;
          }
          else
          {
            char outline[1024];
            snprintf(outline,
                     sizeof (outline) - 1,
                     "success: send dropped stripe on fsid=%u fxid=%llx\n",
                     efsmapit->first, *it);
            out += outline;
          }
        }
      }
    }
    return true;
  }

  if (option == "unlink-orphans")
  {
    out += "# unlink orphans  -------------------------------------------------------------------------\n";
    // -------------------------------------------------------------------------
    // unlink all orphaned files
    // -------------------------------------------------------------------------
    std::map < eos::common::FileSystem::fsid_t,
            std::set < eos::common::FileId::fileid_t >> ::const_iterator efsmapit;

    std::map < eos::common::FileSystem::fsid_t,
            std::set < eos::common::FileId::fileid_t >> fid2check;

    // -------------------------------------------------------------------------
    // loop over all filesystems
    // -------------------------------------------------------------------------
    for (efsmapit = eFsMap["orphans_n"].begin();
            efsmapit != eFsMap["orphans_n"].end();
            efsmapit++)
    {
      std::set <eos::common::FileId::fileid_t>::const_iterator it;

      // -----------------------------------------------------------------------
      // loop over all fids
      // -----------------------------------------------------------------------
      for (it = efsmapit->second.begin(); it != efsmapit->second.end(); it++)
      {
        eos::FileMD* fmd = 0;
        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
        bool haslocation = false;
        // ---------------------------------------------------------------------
        // crosscheck if the location really is not attached
        // ---------------------------------------------------------------------
        try
        {
          fmd = gOFS->eosFileService->getFileMD(*it);
          if (fmd->hasLocation(efsmapit->first))
          {
            haslocation = true;
          }
        }
        catch (eos::MDException &e)
        {
        }

        if (!haslocation)
        {
          if (gOFS->DeleteExternal(efsmapit->first, *it))
          {
            char outline[1024];
            snprintf(outline,
                     sizeof (outline) - 1,
                     "success: send unlink to fsid=%u fxid=%llx\n",
                     efsmapit->first, *it);
            out += outline;
          }
          else
          {
            char errline[1024];
            snprintf(errline,
                     sizeof (errline) - 1,
                     "err: unable to send unlink to fsid=%u fxid=%llx\n",
                     efsmapit->first, *it);
            out += errline;
          }
        }
        else
        {
          char errline[1024];
          snprintf(errline,
                   sizeof (errline) - 1,
                   "err: not sending unlink to fsid=%u fxid=%llx - location exists!\n",
                   efsmapit->first, *it);
          out += errline;
        }
      }
    }
    return true;
  }

  if (option.beginswith("adjust-replicas"))
  {
    out += "# adjust replicas -------------------------------------------------------------------------\n";
    // -------------------------------------------------------------------------
    // adjust all layout errors e.g. missing replicas where possible
    // -------------------------------------------------------------------------
    std::map < eos::common::FileSystem::fsid_t,
            std::set < eos::common::FileId::fileid_t >> ::const_iterator efsmapit;

    std::map < eos::common::FileSystem::fsid_t,
            std::set < eos::common::FileId::fileid_t >> fid2check;

    // -------------------------------------------------------------------------
    // loop over all filesystems
    // -------------------------------------------------------------------------
    for (efsmapit = eFsMap["rep_diff_n"].begin();
            efsmapit != eFsMap["rep_diff_n"].end();
            efsmapit++)
    {
      std::set <eos::common::FileId::fileid_t>::const_iterator it;

      // -----------------------------------------------------------------------
      // loop over all fids
      // -----------------------------------------------------------------------
      for (it = efsmapit->second.begin(); it != efsmapit->second.end(); it++)
      {
        eos::FileMD* fmd = 0;
        std::string path = "";
        try
        {
          {
            eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
            fmd = gOFS->eosFileService->getFileMD(*it);
            path = gOFS->eosView->getUri(fmd);
          }
          // -------------------------------------------------------------------
          // execute adjust replica
          // -------------------------------------------------------------------
          eos::common::Mapping::VirtualIdentity vid;
          eos::common::Mapping::Root(vid);
          XrdOucErrInfo error;

          // -------------------------------------------------------------------
          // execute a proc command
          // -------------------------------------------------------------------
          ProcCommand Cmd;
          XrdOucString info = "mgm.cmd=file&mgm.subcmd=adjustreplica&mgm.path=";
          info += path.c_str();
          info += "&mgm.format=fuse";
          if (option == "adjust-replicas-nodrop")
          {
            info += "&mgm.file.option=nodrop";
          }
          Cmd.open("/proc/user", info.c_str(), vid, &error);
          Cmd.AddOutput(out, err);
          if (!out.endswith("\n"))
          {
            out += "\n";
          }
          if (!err.endswith("\n"))
          {
            err += "\n";
          }
          Cmd.close();
        }
        catch (eos::MDException &e)
        {
        }
      }
    }
    return true;
  }

  if (option == "drop-missing-replicas")
  {
    out += "# drop missing replicas -------------------------------------------------------------------\n";
    // -------------------------------------------------------------------------
    // drop replicas which are in the namespace but have no 'image' on disk
    // -------------------------------------------------------------------------

    // -------------------------------------------------------------------------
    // unlink all orphaned files
    // -------------------------------------------------------------------------
    std::map < eos::common::FileSystem::fsid_t,
            std::set < eos::common::FileId::fileid_t >> ::const_iterator efsmapit;

    std::map < eos::common::FileSystem::fsid_t,
            std::set < eos::common::FileId::fileid_t >> fid2check;

    eos::common::Mapping::VirtualIdentity vid;
    eos::common::Mapping::Root(vid);
    XrdOucErrInfo error;

    // -------------------------------------------------------------------------
    // loop over all filesystems
    // -------------------------------------------------------------------------
    for (efsmapit = eFsMap["rep_missing_n"].begin();
            efsmapit != eFsMap["rep_missing_n"].end();
            efsmapit++)
    {
      std::set <eos::common::FileId::fileid_t>::const_iterator it;

      // -----------------------------------------------------------------------
      // loop over all fids
      // -----------------------------------------------------------------------
      for (it = efsmapit->second.begin(); it != efsmapit->second.end(); it++)
      {
        eos::FileMD* fmd = 0;
        bool haslocation = false;
        std::string path = "";
        // ---------------------------------------------------------------------
        // crosscheck if the location really is not attached
        // ---------------------------------------------------------------------
        try
        {
          eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
          fmd = gOFS->eosFileService->getFileMD(*it);
          path = gOFS->eosView->getUri(fmd);
          if (fmd->hasLocation(efsmapit->first))
          {
            haslocation = true;
          }
        }
        catch (eos::MDException &e)
        {
        }

        if (!haslocation)
        {
          if (gOFS->DeleteExternal(efsmapit->first, *it))
          {
            char outline[1024];
            snprintf(outline,
                     sizeof (outline) - 1,
                     "success: send unlink to fsid=%u fxid=%llx\n",
                     efsmapit->first, *it);
            out += outline;
          }
          else
          {
            char errline[1024];
            snprintf(errline,
                     sizeof (errline) - 1,
                     "err: unable to send unlink to fsid=%u fxid=%llx\n",
                     efsmapit->first, *it);
            out += errline;
          }
        }
        else
        {
          if (fmd)
          {
            // -----------------------------------------------------------------
            // drop in the namespace
            // -----------------------------------------------------------------
            if (gOFS->_dropstripe(path.c_str(),
                                  error,
                                  vid,
                                  efsmapit->first,
                                  false))
            {
              char outline[1024];
              snprintf(outline,
                       sizeof (outline) - 1,
                       "error: unable to drop stripe on fsid=%u fxid=%llx\n",
                       efsmapit->first, *it);
              out += outline;
            }
            else
            {
              char outline[1024];
              snprintf(outline,
                       sizeof (outline) - 1,
                       "success: send dropped stripe on fsid=%u fxid=%llx\n",
                       efsmapit->first, *it);
              out += outline;
            }

            // -----------------------------------------------------------------
            // execute a proc command
            // -----------------------------------------------------------------
            ProcCommand Cmd;
            XrdOucString info = "mgm.cmd=file&mgm.subcmd=adjustreplica&mgm.path=";
            info += path.c_str();
            info += "&mgm.format=fuse";
            Cmd.open("/proc/user", info.c_str(), vid, &error);
            Cmd.AddOutput(out, err);
            if (!out.endswith("\n"))
            {
              out += "\n";
            }
            if (!err.endswith("\n"))
            {
              err += "\n";
            }
            Cmd.close();
          }
        }
      }
    }
    return true;
  }

  if (option == "unlink-zero-replicas")
  {
    out += "# unlink zero replicas --------------------------------------------------------------------\n";
    // -------------------------------------------------------------------------
    // drop all namespace entries which are older than 48 hours and have no
    // files attached
    // -------------------------------------------------------------------------
    std::set <eos::common::FileId::fileid_t>::const_iterator it;

    // -------------------------------------------------------------------------
    // loop over all fids
    // -------------------------------------------------------------------------

    for (it = eMap["zero_replica"].begin();
            it != eMap["zero_replica"].end();
            it++)
    {
      eos::FileMD* fmd = 0;
      std::string path = "";
      time_t now = time(NULL);
      out += "progress: checking fid=";
      out += (int) *it;
      out += "\n";
      eos::FileMD::ctime_t ctime;
      ctime.tv_sec = 0;
      ctime.tv_nsec = 0;
      try
      {
        {
          eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
          fmd = gOFS->eosFileService->getFileMD(*it);
          path = gOFS->eosView->getUri(fmd);
          fmd->getCTime(ctime);
        }
      }
      catch (eos::MDException &e)
      {
      }
      if (fmd)
      {
        if ((ctime.tv_sec + (24 * 3600)) < now)
        {
          // -------------------------------------------------------------------
          // if the file is older than 48 hours, we do the cleanup
          // execute adjust replica
          // -------------------------------------------------------------------
          eos::common::Mapping::VirtualIdentity vid;
          eos::common::Mapping::Root(vid);
          XrdOucErrInfo error;

          if (!gOFS->_rem(path.c_str(), error, vid))
          {
            char outline[1024];
            snprintf(outline,
                     sizeof (outline) - 1,
                     "success: removed path=%s fxid=%llx\n",
                     path.c_str(), *it);
            out += outline;
          }
          else
          {
            char errline[1024];
            snprintf(errline,
                     sizeof (errline) - 1,
                     "err: unable to remove path=%s fxid=%llx\n",
                     path.c_str(), *it);
            out += errline;
          }
        }
        else
        {
          out += "skipping fid=";
          out += (int) *it;
          out += " - file is younger than 48 hours\n";
        }
      }
    }
    return true;
  }

  err = "error: unavailable option";
  return false;
}

/*----------------------------------------------------------------------------*/
void
Fsck::ClearLog ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Clear the current FSCK log
 */
/*----------------------------------------------------------------------------*/
{
  XrdSysMutexHelper lock(mLogMutex);
  mLog = "";
}

/*----------------------------------------------------------------------------*/
void
Fsck::Log (bool overwrite, const char* msg, ...)
/*----------------------------------------------------------------------------*/
/**
 * @brief Write a log message to the current in-memory log
 * @param overwrite if true overwrites the last message
 * @param msg variable length list of printf like format string and args
 */
/*----------------------------------------------------------------------------*/
{
  static time_t current_time;
  static struct timeval tv;
  static struct timezone tz;
  static struct tm *tm;

  va_list args;
  va_start(args, msg);
  char buffer[16384];
  char* ptr;
  time(&current_time);
  gettimeofday(&tv, &tz);

  tm = localtime(&current_time);
  sprintf(buffer, "%02d%02d%02d %02d:%02d:%02d %lu.%06lu ", tm->tm_year - 100, tm->tm_mon + 1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, current_time, (unsigned long) tv.tv_usec);
  ptr = buffer + strlen(buffer);

  vsprintf(ptr, msg, args);
  XrdSysMutexHelper lock(mLogMutex);
  if (overwrite)
  {
    int spos = mLog.rfind("\n", mLog.length() - 2);
    if (spos > 0)
    {
      mLog.erase(spos + 1);
    }
  }
  mLog += buffer;
  mLog += "\n";
  va_end(args);
}
EOSMGMNAMESPACE_END
