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
#include "mgm/Quota.hh"
#include "mgm/LRU.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysTimer.hh"
/*----------------------------------------------------------------------------*/
const char* LRU::gLRUPolicyPrefix = "sys.lru.*"; //< the attribute name defining any LRU policy

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

  using namespace eos::common;

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

  // ---------------------------------------------------------------------------
  // wait that the namespace is initialized
  // ---------------------------------------------------------------------------
  bool go = false;
  do
  {
    XrdSysThread::SetCancelOff();
    {
      XrdSysMutexHelper(gOFS->InitializationMutex);
      if (gOFS->Initialized == gOFS->kBooted)
      {
        go = true;
      }
    }
    XrdSysThread::SetCancelOn();
    XrdSysTimer sleeper;
    sleeper.Wait(1000);
  }
  while (!go);

  XrdSysTimer sleeper;
  sleeper.Snooze(10);

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

    XrdSysThread::SetCancelOff();

    // only a master needs to run LRU
    if (gOFS->MgmMaster.IsMaster())
    {
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
      eos_static_info("msg=\"start LRU scan\" ndir=%llu ms=%u", ndirs, ms);

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
        eos_static_info("msg=\"finished LRU find\" LRU-dirs=%llu",
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

            if (map.count("sys.lru.expire.match"))
            {
              // -----------------------------------------------------------------
              // files with a given match will be removed after expiration time
              // -----------------------------------------------------------------
              AgeExpire(it->first.c_str(), map["sys.lru.expire.match"]);
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

            if (map.count("sys.lru.convert.match"))
            {
              // -----------------------------------------------------------------
              // files with a given match/age will be automatically converted
              // -----------------------------------------------------------------
              ConvertMatch(it->first.c_str(), map);
            }
          }
        }
      }
      EXEC_TIMING_END("LRUFind");

      eos_static_info("msg=\"finished LRU application\" LRU-dirs=%llu",
                      lrudirs.size()
                      );
    }
    eos_static_info("snooze-time=%llu", snoozetime);
    XrdSysThread::SetCancelOn();
    XrdSysTimer sleeper;
    sleeper.Snooze(snoozetime);

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

  eos_static_debug("dir=%s", dir);

  if (!gOFS->_stat(dir, &buf, mError, mRootVid, ""))
  {
    // check if there is any child in that directory
    if (buf.st_nlink > 1)
    {
      eos_static_debug("dir=%s children=%d", dir, buf.st_nlink);
      return;
    }
    else
    {
      time_t now = time(NULL);
      XrdOucString sage = policy.c_str();
      time_t age = StringConversion::GetSizeFromString(sage);
      eos_static_debug("ctime=%u age=%u now=%u", buf.st_ctime, age, now);
      if ((buf.st_ctime + age) < now)
      {
        eos_static_notice("msg=\"delete empty directory\" path=\"%s\"", dir);
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

  std::map < std::string, std::string> lMatchMap;
  std::map < std::string, time_t> lMatchAgeMap;

  time_t now = time(NULL);

  if (!StringConversion::GetKeyValueMap(policy.c_str(),
                                        lMatchMap,
                                        ":")
      )
  {
    eos_static_err("msg=\"LRU match attribute is illegal\" val=\"%s\"",
                   policy.c_str());
    return;
  }

  for (auto it = lMatchMap.begin(); it != lMatchMap.end(); it++)
  {
    time_t t = strtoul(it->second.c_str(), 0, 10);
    if (errno)
    {
      eos_static_err("msg=\"LRU match attribute has illegal age\" "
                     "match=\"%s\", age=\"%s\"",
                     it->first.c_str(),
                     it->second.c_str());
    }
    else
    {
      lMatchAgeMap[it->first] = t;
      eos_static_info("rule=\"%s %u\"", it->first.c_str(), t);
    }
  }

  std::vector<std::string> lDeleteList;
  {
    // -------------------------------------------------------------------------
    // check the directory contents
    // -------------------------------------------------------------------------
    eos::ContainerMD* cmd = 0;
    RWMutexReadLock lock(gOFS->eosViewRWMutex);
    try
    {
      cmd = gOFS->eosView->getContainer(dir);
      eos::ContainerMD::FileMap::iterator fit;
      for (fit = cmd->filesBegin(); fit != cmd->filesEnd(); ++fit)
      {
        std::string fullpath = dir;
        fullpath += fit->first;
        eos_static_debug("%s", fullpath.c_str());
        // ---------------------------------------------------------------------
        // loop over the match map
        // ---------------------------------------------------------------------
        for (auto mit = lMatchAgeMap.begin(); mit != lMatchAgeMap.end(); mit++)
        {

          XrdOucString fname = fit->second->getName().c_str();
          eos_static_debug("%s %d", mit->first.c_str(), fname.matches(mit->first.c_str()));
          if (fname.matches(mit->first.c_str()))
          {
            // -----------------------------------------------------------------
            // full match check the age policy
            // ----------------------------------------------------------------
            eos::FileMD::ctime_t ctime;
            fit->second->getCTime(ctime);
            time_t age = mit->second;
            if ((ctime.tv_sec + age) < now)
            {
              // ---------------------------------------------------------------
              // this entry can be deleted
              // ---------------------------------------------------------------
              eos_static_notice("msg=\"delete expired file\" path=\"%s\" ctime=%u policy-age=%u age=%u",
                                fullpath.c_str(),
                                ctime.tv_sec,
                                age,
                                now - ctime.tv_sec
                                );

              lDeleteList.push_back(fullpath);
              break;
            }
          }
        }
      }
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
      cmd = 0;
      eos_static_err("msg=\"exception\" ec=%d emsg=\"%s\"",
                     e.getErrno(), e.getMessage().str().c_str());
    }
  }

  for (auto it = lDeleteList.begin(); it != lDeleteList.end(); it++)
  {
    if (gOFS->_rem(it->c_str(), mError, mRootVid, ""))
    {
      eos_static_err("msg=\"failed to expire file\" "
                     "path=\"%s\"", it->c_str());
    }
  }
}

/*----------------------------------------------------------------------------*/
void
LRU::CacheExpire (const char* dir,
                  std::string& lowmark,
                  std::string& highmark)
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

  // ---------------------------------------------------------------------------
  // get the quota space 
  // ---------------------------------------------------------------------------
  RWMutexReadLock gLock(Quota::gQuotaMutex);

  SpaceQuota* space = Quota::GetResponsibleSpaceQuota(dir);
  if (!space)
  {
    // there is no quota here - do nothing
    return;
  }

  if (strcmp(space->GetSpaceName(), dir))
  {
    // this is not the quota node itself - do nothing
    return;
  }

  // ---------------------------------------------------------------------------
  // update the quota information
  // ---------------------------------------------------------------------------
  space->UpdateFromQuotaNode(0, 0, true);

  // ---------------------------------------------------------------------------
  // check for project quota 
  // ---------------------------------------------------------------------------

  long long target_volume = space->GetQuota(SpaceQuota::kGroupBytesTarget,
                                            Quota::gProjectId,
                                            false);
  long long is_volume = space->GetQuota(SpaceQuota::kGroupBytesIs,
                                        Quota::gProjectId,
                                        false);

  if (target_volume <= 0)
    return;

  errno = 0;
  double lwm = strtod(lowmark.c_str(), 0);
  if (!lwm || errno || (lwm >= 100))
  {
    eos_static_err("msg=\"low watermark value is illegal - "
                   "must be 0 < lw < 100\" low-watermark=\"%s\"",
                   lowmark.c_str());
    return;
  }
  errno = 0;
  double hwm = strtod(highmark.c_str(), 0);
  if (!hwm || errno || (hwm < lwm) || (hwm >= 100))
  {
    eos_static_err("msg = \"high watermark value is illegal - "
                   "must be 0 < lw < hw < 100\" "
                   "low_watermark=\"%s\" high-watermark=\"%s\"",
                   lowmark.c_str(), highmark.c_str());
    return;
  }


  double cwm = 100.0 * is_volume / target_volume;

  eos_static_debug("cwm=%.02f hwm=%.02f", cwm, hwm);
  // ---------------------------------------------------------------------------
  // check if we have to do cache cleanup e.g. current is over high water mark
  // ---------------------------------------------------------------------------
  if (cwm < hwm)
    return;

  unsigned long long bytes_to_free = is_volume - (lwm * target_volume / 100.0);
  XrdOucString sizestring;
  eos_static_notice("low-mark=%.02f high-mark=%.02f current-mark=%.02f "
                    "deletion-bytes=%s",
                    lwm,
                    hwm,
                    cwm,
                    StringConversion::GetReadableSizeString(sizestring, bytes_to_free, "B")
                    );

  // ---------------------------------------------------------------------------
  // build the LRU list 
  // ---------------------------------------------------------------------------
  std::map<std::string, std::set<std::string> > cachedirs;

  XrdOucString stdErr;

  time_t ms = 0;

  if (mMs)
  {
    // we have a forced setting
    ms = GetMs();
  }

  // map with path/mtime pairs
  std::set<lru_entry_t> lru_map;
  unsigned long long lru_size = 0;

  if (!gOFS->_find(dir,
                   mError,
                   stdErr,
                   mRootVid,
                   cachedirs,
                   "",
                   "",
                   false,
                   ms
                   ))
  {
    // -------------------------------------------------------------------------
    // loop through the result and build an LRU list
    // we just keep as many entries in the LRU list to have the required
    // number of bytes to free available.
    // -------------------------------------------------------------------------
    for (auto dit = cachedirs.begin(); dit != cachedirs.end(); dit++)
    {
      eos_static_debug("path=%s", dit->first.c_str());
      for (auto fit = dit->second.begin(); fit != dit->second.end(); fit++)
      {
        // build the full path name
        std::string fpath = dit->first;
        fpath += *fit;
        struct stat buf;
        eos_static_debug("path=%s", fpath.c_str());
        // get the current ctime & size information
        if (!gOFS->_stat(fpath.c_str(), &buf, mError, mRootVid, ""))
        {
          if (lru_map.size())
            if ((lru_size > bytes_to_free) &&
                lru_map.size() &&
                ((--lru_map.end())->ctime < buf.st_ctime))
            {
              // this entry is newer than all the rest
              continue;
            }

          // add LRU entry in front
          lru_entry_t lru;
          lru.path = fpath;
          lru.ctime = buf.st_ctime;
          lru.size = buf.st_blocks * buf.st_blksize;
          lru_map.insert(lru);
          lru_size += lru.size;
          eos_static_debug("msg=\"adding\" file=\"%s\" "
                           "bytes-free=\"%llu\" lru-size=\"%llu\"",
                           fpath.c_str(),
                           bytes_to_free,
                           lru_size);

          // check if we can shrink the LRU map
          if (lru_map.size() && (lru_size > bytes_to_free))
          {
            while (lru_map.size() &&
                   ((lru_size - (--lru_map.end())->size) > bytes_to_free))
            {
              // remove the last element  of the map 
              auto it = lru_map.end();
              it--;
              // substract the size
              lru_size -= it->size;
              eos_static_info("msg=\"clean-up\" path=\"%s\"", it->path.c_str());
              lru_map.erase(it);

            }
          }
        }
      }
    }
  }
  else
  {
    eos_static_err("msg=\"%s\"", stdErr.c_str());
  }

  eos_static_notice("msg=\"cleaning LRU cache\" files-to-delete=%llu",
                    lru_map.size());
  // ---------------------------------------------------------------------------
  // delete starting with the 'oldest' entry until we have freed enough space
  // to go under the low watermark
  // ---------------------------------------------------------------------------

  for (auto it = lru_map.begin(); it != lru_map.end(); it++)
  {
    eos_static_notice("msg=\"delete LRU file\" path=\"%s\" ctime=%lu size=%llu",
                      it->path.c_str(),
                      it->ctime,
                      it->size);

    if (gOFS->_rem(it->path.c_str(), mError, mRootVid, ""))
    {
      eos_static_err("msg=\"failed to expire file\" "
                     "path=\"%s\"", it->path.c_str());
    }
  }
}

/*----------------------------------------------------------------------------*/
void
LRU::ConvertMatch (const char* dir,
                   eos::ContainerMD::XAttrMap & map)
/*----------------------------------------------------------------------------*/
/**
 * @brief convert all files matching
 * @param dir directory to process
 * @param map storing all the 'sys.conversion.<match>' policies
 */
{
  eos_static_info("msg=\"applying match policy\" dir=\"%s\" match=\"%s\"",
                  dir,
                  map["sys.lru.convert.match"].c_str());

  std::map < std::string, std::string> lMatchMap;
  std::map < std::string, time_t> lMatchAgeMap;

  time_t now = time(NULL);

  if (!StringConversion::GetKeyValueMap(map["sys.lru.convert.match"].c_str(),
                                        lMatchMap,
                                        ":")
      )
  {
    eos_static_err("msg=\"LRU match attribute is illegal\" val=\"%s\"",
                   map["sys.lru.convert.match"].c_str());
    return;
  }

  for (auto it = lMatchMap.begin(); it != lMatchMap.end(); it++)
  {
    time_t t = strtoul(it->second.c_str(), 0, 10);
    if (errno)
    {
      eos_static_err("msg=\"LRU match attribute has illegal age\" "
                     "match=\"%s\", age=\"%s\"",
                     it->first.c_str(),
                     it->second.c_str());
    }
    else
    {
      std::string conv_attr = "sys.conversion.";
      conv_attr += it->first;
      if (map.count(conv_attr))
      {
        lMatchAgeMap[it->first] = t;
        eos_static_info("rule=\"%s %u\"", it->first.c_str(), t);
      }
      else
      {
        eos_static_err("msg=\"LRU match attribute has no conversion "
                       "attribute defined\" "
                       " attr-missing=\"%s\"", conv_attr.c_str());
      }
    }
  }

  std::vector < std::pair<FileId::fileid_t, std::string> > lConversionList;
  {
    // -------------------------------------------------------------------------
    // check the directory contents
    // -------------------------------------------------------------------------
    eos::ContainerMD* cmd = 0;
    RWMutexReadLock lock(gOFS->eosViewRWMutex);
    try
    {
      cmd = gOFS->eosView->getContainer(dir);
      eos::ContainerMD::FileMap::iterator fit;
      for (fit = cmd->filesBegin(); fit != cmd->filesEnd(); ++fit)
      {
        std::string fullpath = dir;
        fullpath += fit->first;
        eos_static_debug("%s", fullpath.c_str());
        // ---------------------------------------------------------------------
        // loop over the match map
        // ---------------------------------------------------------------------
        for (auto mit = lMatchAgeMap.begin(); mit != lMatchAgeMap.end(); mit++)
        {

          XrdOucString fname = fit->second->getName().c_str();
          eos_static_debug("%s %d", mit->first.c_str(), fname.matches(mit->first.c_str()));
          if (fname.matches(mit->first.c_str()))
          {
            // -----------------------------------------------------------------
            // full match check the age policy
            // ----------------------------------------------------------------
            eos::FileMD::ctime_t ctime;
            fit->second->getCTime(ctime);
            time_t age = mit->second;
            if ((ctime.tv_sec + age) < now)
            {
              std::string conv_attr = "sys.conversion.";
              conv_attr += mit->first;

              // ---------------------------------------------------------------
              // check if this file has already the proper layout
              // ---------------------------------------------------------------
              unsigned long long lid = strtoll(map[conv_attr].c_str(), 0, 16);
              if (fit->second->getLayoutId() == lid)
              {
                eos_static_debug("msg=\"skipping conversion - file has already"
                                 "the desired target layout\" fid=%llu", fit->second->getId());
                continue;
              }

              // ---------------------------------------------------------------
              // this entry can be converted
              // ---------------------------------------------------------------
              eos_static_notice("msg=\"convert expired file\" path=\"%s\" "
                                "ctime=%u policy-age=%u age=%u fid=%llu "
                                "layout=\"%s\"",
                                fullpath.c_str(),
                                ctime.tv_sec,
                                age,
                                now - ctime.tv_sec,
                                (unsigned long long) fit->second->getId(),
                                map[conv_attr].c_str()
                                );

              lConversionList.push_back(std::make_pair(fit->second->getId(),
                                                       map[conv_attr]));
              break;
            }
          }
        }
      }
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
      cmd = 0;
      eos_static_err("msg=\"exception\" ec=%d emsg=\"%s\"",
                     e.getErrno(), e.getMessage().str().c_str());
    }
  }

  for (auto it = lConversionList.begin(); it != lConversionList.end(); it++)
  {
    char conversiontagfile[1024];
    std::string space;

    if (map.count("user.forced.space"))
      space = map["user.forced.space"];

    if (map.count("sys.forced.space"))
      space = map["sys.forced.space"];

    if (map.count("sys.lru.conversion.space"))
      space = map["sys.lru.conversion.space"];

    // the conversion value can be directory an layout env representation like
    // "eos.space=...&eos.layout ..."

    XrdOucEnv cenv(it->second.c_str());

    if (cenv.Get("eos.space"))
    {
      space = cenv.Get("eos.space");
    }

    snprintf(conversiontagfile,
             sizeof (conversiontagfile) - 1,
             "%s/%016llx:%s#%s",
             gOFS->MgmProcConversionPath.c_str(),
             it->first,
             space.c_str(),
             it->second.c_str());

    eos_static_notice("msg=\"creating conversion tag file\" tag-file=%s",
                      conversiontagfile);

    if (gOFS->_touch(conversiontagfile, mError, mRootVid, 0))
    {
      eos_static_err("msg=\"unable to create conversion job\" job-file=\"%s\"",
                     conversiontagfile);
    }
  }
}


EOSMGMNAMESPACE_END
