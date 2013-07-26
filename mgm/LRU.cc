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
/*----------------------------------------------------------------------------*/
/**
 * @brief asynchronous LRU thread startup function
 */
/*----------------------------------------------------------------------------*/
{
  // run an asynchronous LRU thread
  mThread = 0;
  XrdSysThread::Run(&mThread,
                    LRU::StartLRUThread,
                    static_cast<void *> (this),
                    XRDSYSTHREAD_HOLD,
                    "LRU engine Thread");

  return (mThread ? true : false);
}

/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
void
LRU::Stop ()
/*----------------------------------------------------------------------------*/
/**
 * @brief asynchronous LRU thread stop function
 */
/*----------------------------------------------------------------------------*/
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
/*----------------------------------------------------------------------------*/
/**
 * @brief LRU method doing the actual policy scrubbing
 * 
 * This thread method loops in regular intervals over all directories which have
 * a LRU policy attribute set (sys.lru.*) and applies the defined policy.
 */
/*----------------------------------------------------------------------------*/
{
  //----------------------------------------------------------------------------
  // Eternal thread doing LRU scans
  //----------------------------------------------------------------------------

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
                     mError,
                     stdErr,
                     mRootVid,
                     lrudirs,
                     gLRUPolicyPrefix,
                     "*",
                     true,
                     ms
                     )
        )
    {
      eos_static_info("msg=\"finished LRU scan\" LRU-dirs=%llu",
                      lrudirs.size()
                      );

      // scan backwards ... in this way we get rid of empty directories in one go ...
      for (auto it = lrudirs.rbegin(); it != lrudirs.rend(); it++)
      {
        // ---------------------------------------------------------------------
        // get the attributes
        // ---------------------------------------------------------------------
        eos_static_info("lru-dir=\"%s\"", it->first.c_str());
        eos::ContainerMD::XAttrMap map;
        if (!gOFS->_attr_ls(it->first.c_str(),
                            mError,
                            mRootVid,
                            (const char *) 0,
                            map)
            )
        {
          // -------------------------------------------------------------------
          // sort out the individual LRU policies 
          // -------------------------------------------------------------------

          if (map.count("sys.lru.expire.empty") && !it->second.size())
          {
            // ----------------------------------------------------------------- 
            // remove empty directories older than <age>
            // -----------------------------------------------------------------
            AgeExpireEmtpy(it->first.c_str(), map["sys.lru.expire.empty"]);
          }

          if (map.count("sys.lru.expire.suffix"))
          {
            // -----------------------------------------------------------------
            // files with a given suffix will be removed after expiration time
            // -----------------------------------------------------------------
            AgeExpire(it->first.c_str(), map["sys.lru.expire.suffix"]);
          }

          if (map.count("sys.lru.lowwatermark") &&
              map.count("sys.lru.highwatermark"))
          {
            // -----------------------------------------------------------------
            // if the space in this directory reaches highwatermark, files are
            // cleaned up according to the LRU policy
            // -----------------------------------------------------------------
            CacheExpire(it->first.c_str(),
                        map["sys.lru.lowwatermark"],
                        map["sys.lru.highwatermark"]
                        );
          }

          if (map.count("sys.lru.convert.atime") &&
              map.count("sys.conversion.atime"))
          {
            // -----------------------------------------------------------------
            // files which havn't been accessed longer than atime ago will be
            // converted into a new layout
            // -----------------------------------------------------------------
            ConvertAtime(it->first.c_str(), map["sys.lru.convert.atime"]);
          }

          if (map.count("sys.lru.convert.suffix"))
          {
            // -----------------------------------------------------------------
            // files with a given suffix will be automatically converted
            // -----------------------------------------------------------------
            ConvertSuffix(it->first.c_str(), map);
          }
        }
      }
    }

    EXEC_TIMING_END("LRUFind");
    XrdSysThread::SetCancelOff();
  };
  return 0;
}

/*----------------------------------------------------------------------------*/
void
LRU::AgeExpireEmtpy (const char* dir, std::string& policy)
/*----------------------------------------------------------------------------*/
/**
 * @brief remove empty directories if they are older than age given in policy
 * @param dir directory to proces
 * @param policy minimum age to expire
 */
/*----------------------------------------------------------------------------*/
{
  struct stat buf;

  eos_static_debug("dir=%s",dir);
  
  if (!gOFS->_stat(dir, &buf, mError, mRootVid, ""))
  {
    // check if there is any child in that directory
    if (buf.st_nlink>1)
    {
      eos_static_debug("dir=%s children=%d", dir,buf.st_nlink);
      return;
    }
    else
    {
      time_t now = time(NULL);
      XrdOucString sage = policy.c_str();
      time_t age = eos::common::StringConversion::GetSizeFromString(sage);
      eos_static_debug("ctime=%u age=%u now=%u", buf.st_ctime, age, now);
      if ((buf.st_ctime + age) < now)
      {
        eos_static_info("msg=\"delete empty directory\" path=\"%s\"", dir);
        if (gOFS->_remdir(dir, mError, mRootVid, ""))
        {
          eos_static_err("msg=\"failed to delete empty directory\" "
                         "path=\"%s\"", dir);
        }
      }
    }
  }
}

/*----------------------------------------------------------------------------*/
void
LRU::AgeExpire (const char* dir,
                std::string& policy)
/*----------------------------------------------------------------------------*/
/**
 * @brief remove all files older than the policy defines
 * @param dir directory to process
 * @param policy minimum age to expire
 */
/*----------------------------------------------------------------------------*/
{
  eos_static_info("msg=\"applying age deletion policy\" dir=\"%s\" age=\"%s\"",
                  dir,
                  policy.c_str());

  std::set<std::string> lDeletionList;
  {
    // check the directory contents
    eos::ContainerMD* cmd = 0;
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    try
    {
      cmd = gOFS->eosView->getContainer(dir);

    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
      cmd = 0;
      eos_static_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                       e.getErrno(), e.getMessage().str().c_str());
    }
  }
}

/*----------------------------------------------------------------------------*/
void
LRU::CacheExpire (const char* dir,
                  std::string& lowmark,
                  std::string & highmark)
/*----------------------------------------------------------------------------*/
/**
 * @brief expire the oldest files to go under the low watermark
 * @param dir directory to process
 * @param policy high water mark when to start expiration
 */
/*----------------------------------------------------------------------------*/
{
  eos_static_info("msg=\"applying volume deletion policy\" "
                  "dir=\"%s\" low-mark=\"%s\" high-mark=\"%s\"",
                  dir,
                  lowmark.c_str(),
                  highmark.c_str());
}

/*----------------------------------------------------------------------------*/
void
LRU::ConvertAtime (const char* dir,
                   std::string & policy)
/*----------------------------------------------------------------------------*/
/**
 * @brief convert all files which have not been accessed longer than atime
 * @param dir directory to process
 * @param policy minimum age policy
 */
/*----------------------------------------------------------------------------*/
{
  eos_static_info("msg=\"applying age conversion policy\" dir=\"%s\" age=\"%s\"",
                  dir,
                  policy.c_str());
}

/*----------------------------------------------------------------------------*/
void
LRU::ConvertSuffix (const char* dir,
                    eos::ContainerMD::XAttrMap & map)
/*----------------------------------------------------------------------------*/
/**
 * @brief convert all files matching a suffix
 * @param dir directory to process
 * @param map storing all the 'sys.conversion.<suffix>' policies
 */
{
  eos_static_info("msg=\"applying suffix policy\" dir=\"%s\" suffix=\"%s\"",
                  dir,
                  map["sys.lru.convert.suffix"].c_str());
}
EOSMGMNAMESPACE_END
