// ----------------------------------------------------------------------
// File: LRU.cc
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
#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/Mapping.hh"
#include "common/RWMutex.hh"
#include "mgm/LRU.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysTimer.hh"
/*----------------------------------------------------------------------------*/
const char* LRU::gLRUPolicyPrefix = "sys.lru.*"; //< the attribute name defining any LRU policy

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
bool
LRU::Start ()
{
  // run an asynchronous LRU thread
  eos_static_info("constructor");
  mThread = 0;
  XrdSysThread::Run(&mThread, LRU::StartLRUThread, static_cast<void *> (this), XRDSYSTHREAD_HOLD, "LRU engine Thread");
  return (mThread ? true : false);
}

/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
void
LRU::Stop ()
{
  // cancel the asynchronous LRU thread
  if (mThread)
  {
    XrdSysThread::Cancel(mThread);
    XrdSysThread::Join(mThread, 0);
  }
  mThread = 0;
}

void*
LRU::StartLRUThread (void* arg)
{
  return reinterpret_cast<LRU*> (arg)->LRUr();
}

/*----------------------------------------------------------------------------*/
void*
LRU::LRUr ()
{
  //----------------------------------------------------------------------------
  // Eternal thread doing LRU scans
  //----------------------------------------------------------------------------

  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);
  XrdOucErrInfo lError;
  time_t snoozetime = 60;

  eos_static_info("msg=\"async LRU thread started\"");
  while (1)
  {
    // -------------------------------------------------------------------------
    // every now and then we wake up
    // -------------------------------------------------------------------------

    eos_static_info("snooze-time=%llu", snoozetime);
    XrdSysThread::SetCancelOn();
    XrdSysTimer sleeper;
    sleeper.Snooze(snoozetime);

    // -------------------------------------------------------------------------
    // do a slow find
    // -------------------------------------------------------------------------

    unsigned long long ndirs =
      (unsigned long long) gOFS->eosDirectoryService->getNumContainers();

    time_t ms = 1;

    if (ndirs > 10000000)
    {
      ms = 0;
    }

    if (mMs)
    {
      // we have a forced setting
      ms = GetMs();
    }
    eos_static_info("msg=\"start policy scan\" ndir=%llu ms=%u", ndirs, ms);

    std::map<std::string, std::set<std::string> > lrudirs;

    XrdOucString stdErr;

    // -------------------------------------------------------------------------
    // find all directories defining an LRU policy
    // -------------------------------------------------------------------------
    gOFS->MgmStats.Add("LRUFind", 0, 0, 1);

    EXEC_TIMING_BEGIN("LRUFind");

    if (!gOFS->_find("/",
                     lError,
                     stdErr,
                     rootvid,
                     lrudirs,
                     gLRUPolicyPrefix,
                     "*",
                     true,
                     ms
                     )
        )
    {
      eos_static_info("msg=\"finished LRU scan\" LRU-dirs=%llu", lrudirs.size());
      for (auto it = lrudirs.begin(); it != lrudirs.end(); it++)
      {
        
      }
    }
   
    EXEC_TIMING_END("LRUFind");
    XrdSysThread::SetCancelOff();
  };
  return 0;
}

EOSMGMNAMESPACE_END
