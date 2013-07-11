// ----------------------------------------------------------------------
// File: Recycle.cc
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
#include "mgm/Recycle.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Quota.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysTimer.hh"
/*----------------------------------------------------------------------------*/
std::string Recycle::gRecyclingPrefix = "/recycle/"; // MgmOfsConfigure prepends the proc directory path e.g. the bin is /eos/<instance/proc/recycle/
std::string Recycle::gRecyclingAttribute = "sys.recycle";
std::string Recycle::gRecyclingTimeAttribute = "sys.recycle.keeptime";
std::string Recycle::gRecyclingPostFix = ".d";
int Recycle::gRecyclingPollTime = 30;

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
bool
Recycle::Start ()
{
  // run an asynchronous recyling thread
  eos_static_info("constructor");
  mThread = 0;
  XrdSysThread::Run(&mThread, Recycle::StartRecycleThread, static_cast<void *> (this), XRDSYSTHREAD_HOLD, "Recycle garbage collection Thread");
  return (mThread ? true : false);
}

/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
void
Recycle::Stop ()
{
  // cancel the asynchronous recycle thread
  if (mThread)
  {
    XrdSysThread::Cancel(mThread);
    XrdSysThread::Join(mThread, 0);
  }
  mThread = 0;
}

void*
Recycle::StartRecycleThread (void* arg)
{
  return reinterpret_cast<Recycle*> (arg)->Recycler();
}

/*----------------------------------------------------------------------------*/
void*
Recycle::Recycler ()
{
  //.............................................................................
  // Eternal thread doing garbage clean-up in the garbeg bin
  // - default garbage directory is '<instance-proc>/recycle/'
  // - one should define an attribute like 'sys.recycle.keeptime' on this dir
  //   to define the time in seconds how long files stay in the recycle bin
  //.............................................................................

  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);
  XrdOucErrInfo lError;
  time_t lKeepTime = 0;

  std::multimap<time_t, std::string> lDeletionMap;
  time_t snoozetime = 10;

  bool show_attribute_missing = true;

  eos_static_info("msg=\"async recycling thread started\"");
  while (1)
  {
    //...........................................................................
    // every now and then we wake up
    //..........................................................................
    eos_static_info("snooze-time=%llu", snoozetime);
    XrdSysThread::SetCancelOn();
    XrdSysTimer sleeper;
    sleeper.Snooze(snoozetime);

    snoozetime = gRecyclingPollTime; // this will be reconfigured to an appropriate value later

    XrdSysThread::SetCancelOff();
    //...........................................................................
    // read our current policy setting
    //...........................................................................
    eos::ContainerMD::XAttrMap attrmap;

    //...........................................................................
    // check if this path has a recycle attribute
    //...........................................................................
    if (gOFS->_attr_ls(Recycle::gRecyclingPrefix.c_str(), lError, rootvid, "", attrmap))
    {
      eos_static_err("msg=\"unable to get attribute on recycle path\" recycle-path=%s", Recycle::gRecyclingPrefix.c_str());
    }
    else
    {
      if (attrmap.count(Recycle::gRecyclingTimeAttribute))
      {
        lKeepTime = strtoull(attrmap[Recycle::gRecyclingTimeAttribute].c_str(), 0, 10);
        eos_static_info("keep-time=%llu deletion-map=%llu", lKeepTime, lDeletionMap.size());
        if (lKeepTime > 0)
        {
          if (!lDeletionMap.size())
          {
            //...................................................................
            //  the deletion map is filled if there is nothing inside with files/
            //  directories found previouslyin the garbage bin
            //...................................................................
            std::string subdirs;
            XrdMgmOfsDirectory dirl1;
            XrdMgmOfsDirectory dirl2;
            XrdMgmOfsDirectory dirl3;
            int listrc = dirl1.open(Recycle::gRecyclingPrefix.c_str(), rootvid, (const char*) 0);
            if (listrc)
            {
              eos_static_err("msg=\"unable to list the garbage directory level-1\" recycle-path=%s", Recycle::gRecyclingPrefix.c_str());
            }
            else
            {
              // loop over all directories = group directories
              const char* dname1;
              while ((dname1 = dirl1.nextEntry()))
              {
                {
                  std::string sdname = dname1;
                  if ((sdname == ".") || (sdname == ".."))
                  {
                    continue;
                  }
                }
                std::string l2 = Recycle::gRecyclingPrefix;
                l2 += dname1;
                // list level-2 user directories
                listrc = dirl2.open(l2.c_str(), rootvid, (const char*) 0);
                if (listrc)
                {
                  eos_static_err("msg=\"unable to list the garbage directory level-2\" recycle-path=%s l2-path=%s", Recycle::gRecyclingPrefix.c_str(), l2.c_str());
                }
                else
                {
                  const char* dname2;
                  while ((dname2 = dirl2.nextEntry()))
                  {
                    {
                      std::string sdname = dname2;
                      if ((sdname == ".") || (sdname == ".."))
                      {
                        continue;
                      }
                    }
                    std::string l3 = l2;
                    l3 += "/";
                    l3 += dname2;
                    // list the level-3 entries
                    listrc = dirl3.open(l3.c_str(), rootvid, (const char*) 0);
                    if (listrc)
                    {
                      eos_static_err("msg=\"unable to list the garbage directory level-2\" recycle-path=%s l2-path=%s l3-path=%s", Recycle::gRecyclingPrefix.c_str(), l2.c_str(), l3.c_str());
                    }
                    else
                    {
                      const char* dname3;
                      while ((dname3 = dirl3.nextEntry()))
                      {
                        {
                          std::string sdname = dname3;
                          if ((sdname == ".") || (sdname == ".."))
                          {
                            continue;
                          }
                        }
                        std::string l4 = l3;
                        l4 += "/";
                        l4 += dname3;
                        eos_static_info("path=%s", l4.c_str());
                        //.......................................................
                        // stat the directory to get the mtime
                        //.......................................................
                        struct stat buf;
                        if (gOFS->_stat(l4.c_str(), &buf, lError, rootvid, ""))
                        {
                          eos_static_err("msg=\"unable to stat a garbage directory entry\" recycle-path=%s l2-path=%s l3-path=%s", Recycle::gRecyclingPrefix.c_str(), l2.c_str(), l3.c_str());

                        }
                        else
                        {
                          //.....................................................
                          // add to the garbage fifo deletion multimap
                          //.....................................................
                          lDeletionMap.insert(std::pair<time_t, std::string > (buf.st_ctime, l4));
                        }
                      }
                      dirl3.close();
                    }
                  }
                  dirl2.close();
                }
              }
              dirl1.close();
            }
          }
          else
          {
            auto it = lDeletionMap.begin();
            time_t now = time(NULL);
            while (it != lDeletionMap.end())
            {
              // take the first element and see if it is exceeding the keep time
              if ((it->first + lKeepTime) < now)
              {
                //...............................................................
                // this entry can be removed
                //...............................................................
                XrdOucString delpath = it->second.c_str();
                if ((it->second.length()) && (delpath.endswith(Recycle::gRecyclingPostFix.c_str())))
                {
                  //.............................................................
                  // do a directory deletion - first find all subtree children
                  //.............................................................
                  std::map<std::string, std::set<std::string> > found;
                  std::map<std::string, std::set<std::string> >::const_reverse_iterator rfoundit;
                  std::set<std::string>::const_iterator fileit;
                  XrdOucString stdErr;
                  if (gOFS->_find(it->second.c_str(), lError, stdErr, rootvid, found))
                  {
                    eos_static_err("msg=\"unable to do a find in subtree\" path=%s stderr=\"%s\"", it->second.c_str(), stdErr.c_str());
                  }
                  else
                  {
                    //...........................................................
                    // standard way to delete files recursively
                    //...........................................................
                    // delete files starting at the deepest level
                    for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++)
                    {
                      for (fileit = rfoundit->second.begin(); fileit != rfoundit->second.end(); fileit++)
                      {
                        std::string fspath = rfoundit->first;
                        fspath += *fileit;
                        if (gOFS->_rem(fspath.c_str(), lError, rootvid, (const char*) 0))
                        {
                          eos_static_err("msg=\"unable to remove file\" path=%s", fspath.c_str());
                        }
                        else
                        {
                          eos_static_info("msg=\"permanently deleted file from recycle bin\" path=%s keep-time=%llu", fspath.c_str(), lKeepTime);
                        }
                      }
                    }
                    //...........................................................
                    // delete directories starting at the deepest level
                    //...........................................................
                    for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++)
                    {
                      //.........................................................
                      // don't even try to delete the root directory
                      //.........................................................
                      std::string fspath = rfoundit->first.c_str();
                      if (fspath == "/")
                        continue;
                      if (gOFS->_remdir(rfoundit->first.c_str(), lError, rootvid, (const char*) 0))
                      {
                        eos_static_err("msg=\"unable to remove directory\" path=%s", fspath.c_str());
                      }
                      else
                      {
                        eos_static_info("msg=\"permanently deleted directory from recycle bin\" path=%s keep-time=%llu", fspath.c_str(), lKeepTime);
                      }
                    }
                  }
                  lDeletionMap.erase(it);
                  it = lDeletionMap.begin();
                }
                else
                {
                  //...........................................................
                  // do a single file deletion
                  //...........................................................
                  if (gOFS->_rem(it->second.c_str(), lError, rootvid, (const char*) 0))
                  {
                    eos_static_err("msg=\"unable to remove file\" path=%s", it->second.c_str());
                  }
                  lDeletionMap.erase(it);
                  it = lDeletionMap.begin();
                }
              }
              else
              {
                //...............................................................
                // this entry has still to be kept
                //...............................................................
                snoozetime = (it->first + lKeepTime) - now;
                if (snoozetime < gRecyclingPollTime)
                {
                  //.............................................................
                  // avoid to activate this thread too many times, 5 minutess
                  // resolution is perfectly fine
                  //.............................................................
                  snoozetime = gRecyclingPollTime;
                }
                if (snoozetime > lKeepTime)
                {
                  eos_static_warning("msg=\"snooze time exceeds keeptime\" snooze-time=%llu keep-time=%llu", snoozetime, lKeepTime);
                  //.............................................................
                  // that is sort of strange but let's have a fix for that
                  //.............................................................
                  snoozetime = lKeepTime;
                }
                it++;
              }
            }
          }
        }
        else
        {
          eos_static_warning("msg=\"parsed '%s' attribute as keep-time of %llu seconds - ignoring!\" recycle-path=%s", Recycle::gRecyclingTimeAttribute.c_str(), Recycle::gRecyclingPrefix.c_str());
        }
      }
      else
      {
        if (show_attribute_missing)
        {
          eos_static_warning("msg=\"unable to read '%s' attribute on recycle path - undefined!\" recycle-path=%s", Recycle::gRecyclingTimeAttribute.c_str(), Recycle::gRecyclingPrefix.c_str());
          show_attribute_missing = false;
        }
      }
    }
  };
  return 0;
}

/*----------------------------------------------------------------------------*/

int
Recycle::ToGarbage (const char* epname, XrdOucErrInfo & error)
{
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);

  char srecyclegroup[4096];
  char srecycleuser[4096];
  char srecyclepath[4096];

  bool isdir = false; // if path ends with '/' we recycle a full directory tree aka directory

  // rewrite the file name /a/b/c as #:#a#:#b#:#c
  XrdOucString contractedpath = mPath.c_str();

  if (contractedpath.endswith("/"))
  {
    isdir = true;
    mPath.erase(mPath.length() - 1);
    // remove the '/' indicating a recursive directory recycling
    contractedpath.erase(contractedpath.length() - 1);
  }

  if (mRecycleDir.length() > 1)
  {
    if (mRecycleDir[mRecycleDir.length() - 1] == '/')
    {
      mRecycleDir.erase(mRecycleDir.length() - 1);
    }
  }
  while (contractedpath.replace("/", "#:#"))
  {
  }

  std::string lPostFix = ""; // for dir's we add a '.d' in the end of the recycle path
  if (isdir)
  {
    lPostFix = Recycle::gRecyclingPostFix;
  }


  snprintf(srecyclegroup, sizeof (srecyclegroup) - 1, "%s/%u", mRecycleDir.c_str(), mOwnerGid);
  snprintf(srecycleuser, sizeof (srecycleuser) - 1, "%s/%u/%u", mRecycleDir.c_str(), mOwnerGid, mOwnerUid);
  snprintf(srecyclepath, sizeof (srecyclepath) - 1, "%s/%u/%u/%s.%016llx%s", mRecycleDir.c_str(), mOwnerGid, mOwnerUid, contractedpath.c_str(), mId, lPostFix.c_str());


  mRecyclePath = srecyclepath;

  // verify/create group/user directory


  if (gOFS->_mkdir(srecycleuser, S_IRUSR | S_IXUSR | SFS_O_MKPTH, error, rootvid, ""))
  {
    return gOFS->Emsg(epname,
                      error,
                      EIO,
                      "remove existing file - the recycle space user directory couldn't be created");
  }

  // check the user recycle directory
  struct stat buf;
  if (gOFS->_stat(srecycleuser, &buf, error, rootvid, ""))
  {
    // check the ownership of the user directory
    return gOFS->Emsg(epname,
                      error,
                      EIO,
                      "remove existing file - could not determine ownership of the recycle space user directory", srecycleuser);
  }

  if ((buf.st_uid != mOwnerUid) || (buf.st_gid != mOwnerGid))
  {
    // set the correct ownership
    if (gOFS->_chown(srecycleuser, mOwnerUid, mOwnerGid, error, rootvid, ""))
    {
      return gOFS->Emsg(epname,
                        error,
                        EIO,
                        "remove existing file - could not change ownership of the recycle space user directory", srecycleuser);

    }
  }

  // check the group recycle directory
  if (gOFS->_stat(srecyclegroup, &buf, error, rootvid, ""))
  {
    // check the ownership of the group directory
    return gOFS->Emsg(epname,
                      error,
                      EIO,
                      "remove existing file - could not determine ownership of the recycle space group directory", srecyclegroup);
  }

  if ((buf.st_uid != mOwnerUid) || (buf.st_gid != mOwnerGid))
  {
    // set the correct ownership
    if (gOFS->_chown(srecycleuser, mOwnerUid, mOwnerGid, error, rootvid, ""))
    {
      return gOFS->Emsg(epname,
                        error,
                        EIO,
                        "remove existing file - could not change ownership of the recycle space group directory",
                        srecycleuser);

    }
  }

  // finally do the rename
  if (gOFS->_rename(mPath.c_str(), srecyclepath, error, rootvid, "", "", true, true))
  {
    return gOFS->Emsg(epname,
                      error,
                      EIO,
                      "rename file/directory",
                      srecyclepath);
  }
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
void
Recycle::Print (XrdOucString &stdOut, XrdOucString &stdErr, eos::common::Mapping::VirtualIdentity_t &vid, bool monitoring, bool translateids, bool details)
{
  XrdOucString uids;
  XrdOucString gids;

  std::map<gid_t, std::map<uid_t, bool> > printmap;

  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);

  if ((!vid.uid) ||
      (eos::common::Mapping::HasUid(3, vid.uid_list)) ||
      (eos::common::Mapping::HasGid(4, vid.gid_list)))
  {
    // add everything found in the recycle directory structure to the printmap
    std::string subdirs;
    XrdMgmOfsDirectory dirl1;
    XrdMgmOfsDirectory dirl2;

    int listrc = dirl1.open(Recycle::gRecyclingPrefix.c_str(), rootvid, (const char*) 0);
    if (listrc)
    {
      eos_static_err("msg=\"unable to list the garbage directory level-1\" recycle-path=%s", Recycle::gRecyclingPrefix.c_str());
    }
    else
    {
      // loop over all directories = group directories
      const char* dname1;
      while ((dname1 = dirl1.nextEntry()))
      {
        std::string sdname = dname1;
        if ((sdname == ".") || (sdname == ".."))
        {
          continue;
        }
        gid_t gid = strtoull(dname1, 0, 10);
        std::string l2 = Recycle::gRecyclingPrefix;
        l2 += dname1;
        // list level-2 user directories
        listrc = dirl2.open(l2.c_str(), rootvid, (const char*) 0);
        if (listrc)
        {
          eos_static_err("msg=\"unable to list the garbage directory level-2\" recycle-path=%s l2-path=%s", Recycle::gRecyclingPrefix.c_str(), l2.c_str());
        }
        else
        {
          const char* dname2;
          while ((dname2 = dirl2.nextEntry()))
          {
            std::string sdname = dname2;
            if ((sdname == ".") || (sdname == ".."))
            {
              continue;
            }
            uid_t uid = strtoull(dname2, 0, 10);
            printmap[gid][uid] = true;
          }
          dirl2.close();
        }
      }
      dirl1.close();
    }
  }
  else
  {
    // add only the virtual user to the printmap
    printmap[vid.gid][vid.uid] = true;
  }

  if (details)
  {
    size_t count = 0;

    for (auto itgid = printmap.begin(); itgid != printmap.end(); itgid++)
    {
      for (auto ituid = itgid->second.begin(); ituid != itgid->second.end(); ituid++)
      {
        XrdMgmOfsDirectory dirl;
        char sdir[4096];
        snprintf(sdir, sizeof (sdir) - 1, "%s/%u/%u/", Recycle::gRecyclingPrefix.c_str(), (unsigned int) itgid->first, (unsigned int) ituid->first);
        int retc = dirl.open(sdir, vid, "");
        if (!retc)
        {
          const char* dname;
          while ((dname = dirl.nextEntry()))
          {
            std::string sdname = dname;
            if ((sdname == ".") || (sdname == ".."))
            {
              continue;
            }
            std::string fullpath = sdir;
            fullpath += dname;
            XrdOucString originode;

            XrdOucString origpath = dname;

            // demangle the original pathname
            while (origpath.replace("#:#", "/"))
            {
            }

            XrdOucString type = "file";

            struct stat buf;
            XrdOucErrInfo error;
            if (!gOFS->_stat(fullpath.c_str(), &buf, error, vid, ""))
            {
              if (translateids)
              {
                int errc = 0;
                uids = eos::common::Mapping::UidToUserName(buf.st_uid, errc).c_str();
                if (errc) uids = eos::common::Mapping::UidAsString(buf.st_uid).c_str();
                gids = eos::common::Mapping::GidToGroupName(buf.st_gid, errc).c_str();
                if (errc) gids = eos::common::Mapping::GidAsString(buf.st_gid).c_str();
              }
              else
              {
                uids = eos::common::Mapping::UidAsString(buf.st_uid).c_str();
                gids = eos::common::Mapping::GidAsString(buf.st_gid).c_str();
              }

              if (origpath.endswith(Recycle::gRecyclingPostFix.c_str()))
              {
                type = "recursive-dir";
                origpath.erase(origpath.length() - Recycle::gRecyclingPostFix.length());
              }

              originode = origpath;
              originode.erase(0, origpath.length() - 16);
              origpath.erase(origpath.length() - 17);

              if (monitoring)
              {
                stdOut += "recycle=ls ";
                stdOut += " recycle-bin=";
                stdOut += Recycle::gRecyclingPrefix.c_str();
                stdOut += " uid=";
                stdOut += uids.c_str();
                stdOut += " gid=";
                stdOut += gids.c_str();
                stdOut += " deletion-time=";
                char deltime[256];
                snprintf(deltime, sizeof (deltime) - 1, "%llu", (unsigned long long) buf.st_ctime);
                stdOut += deltime;
                stdOut += " restore-path=";
                stdOut += origpath.c_str();
                stdOut += " restore-key=";
                stdOut += originode.c_str();
                stdOut += "\n";
              }
              else
              {
                char sline[4096];

                if (count == 0)
                {
                  // print a header
                  snprintf(sline, sizeof (sline) - 1, "# %-24s %-8s %-8s %-13s %-16s %-64s\n", "Deletion Time", "UID", "GID", "TYPE", "RESTORE-KEY", "RESTORE-PATH");
                  stdOut += sline;
                  stdOut += "# ==============================================================================================================================\n";
                }

                char tdeltime[4096];
                std::string deltime = ctime_r(&buf.st_ctime, tdeltime);
                deltime.erase(deltime.length() - 1);
                snprintf(sline, sizeof (sline) - 1, "%-26s %-8s %-8s %-13s %-16s %-64s", deltime.c_str(), uids.c_str(), gids.c_str(), type.c_str(), originode.c_str(), origpath.c_str());
                stdOut += sline;
                stdOut += "\n";
              }
              count++;
              if (count > 100000)
              {
                stdOut += "... (truncated)\n";
                retc = E2BIG;
                stdErr += "warning: list too long - truncated after 100000 entries!\n";
              }
            }
          }
        }
      }
    }
  }
  else
  {
    eos::common::RWMutexReadLock lock(Quota::gQuotaMutex);
    SpaceQuota* spacequota = Quota::GetResponsibleSpaceQuota((Recycle::gRecyclingPrefix + "/").c_str());
    if (spacequota)
    {
      unsigned long long usedbytes = spacequota->GetQuota(SpaceQuota::kGroupBytesIs, Quota::gProjectId);
      unsigned long long maxbytes = spacequota->GetQuota(SpaceQuota::kGroupBytesTarget, Quota::gProjectId);
      unsigned long long usedfiles = spacequota->GetQuota(SpaceQuota::kGroupFilesIs, Quota::gProjectId);
      unsigned long long maxfiles = spacequota->GetQuota(SpaceQuota::kGroupBytesTarget, Quota::gProjectId);
      char sline[1024];
      XrdOucString sizestring1;
      XrdOucString sizestring2;
      eos::ContainerMD::XAttrMap attrmap;
      XrdOucErrInfo error;
      //...........................................................................
      // check if this path has a recycle attribute
      //...........................................................................
      if (gOFS->_attr_ls(Recycle::gRecyclingPrefix.c_str(), error, rootvid, "", attrmap))
      {
        eos_static_err("msg=\"unable to get attribute on recycle path\" recycle-path=%s", Recycle::gRecyclingPrefix.c_str());
      }
      if (!monitoring)
      {
        stdOut += "# _______________________________________________________________________________________________\n";
        snprintf(sline, sizeof (sline) - 1, "# used %s out of %s (%.02f%% volume / %.02f%% inodes used) Object-Lifetime %s [s]",
                 eos::common::StringConversion::GetReadableSizeString(sizestring1, usedbytes, "B"),
                 eos::common::StringConversion::GetReadableSizeString(sizestring2, maxbytes, "B"),
                 usedbytes * 100.0 / maxbytes,
                 usedfiles * 100.0 / maxfiles,
                 attrmap.count(Recycle::gRecyclingTimeAttribute) ? attrmap[Recycle::gRecyclingTimeAttribute].c_str() : "not configured");
        stdOut += sline;
        stdOut += "\n";
        stdOut += "# _______________________________________________________________________________________________\n";

      }
      else
      {
        snprintf(sline, sizeof (sline) - 1, "recycle-bin=%s usedbytes=%s maxbytes=%s volumeusage=%.02f%% inodeusage=%.02f%% lifetime=%s",
                 Recycle::gRecyclingPrefix.c_str(),
                 eos::common::StringConversion::GetSizeString(sizestring1, usedbytes),
                 eos::common::StringConversion::GetSizeString(sizestring2, maxbytes),
                 usedbytes * 100.0 / maxbytes,
                 usedfiles * 100.0 / maxfiles,
                 attrmap.count(Recycle::gRecyclingTimeAttribute) ? attrmap[Recycle::gRecyclingTimeAttribute].c_str() : "-1");
        stdOut += sline;
        stdOut += "\n";
      }
    }
  }
}

/*----------------------------------------------------------------------------*/
int
Recycle::Restore (XrdOucString &stdOut, XrdOucString &stdErr, eos::common::Mapping::VirtualIdentity_t &vid, const char* key, XrdOucString &option)
{
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);

  if (!key)
  {
    stdErr += "error: invalid argument as recycle key\n";
    return EINVAL;
  }

  unsigned long long fid = strtoull(key, 0, 16);

  //...........................................................................
  // convert the hex inode number into decimal and retrieve path name
  //...........................................................................
  eos::FileMD* fmd = 0;
  eos::ContainerMD* cmd = 0;
  std::string recyclepath;

  //-------------------------------------------
  {
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    try
    {
      fmd = gOFS->eosFileService->getFileMD(fid);
      recyclepath = gOFS->eosView->getUri(fmd);
    }
    catch (eos::MDException &e)
    {
    }
    try
    {
      cmd = gOFS->eosDirectoryService->getContainerMD(fid);
      recyclepath = gOFS->eosView->getUri(cmd);
    }
    catch (eos::MDException &e)
    {
    }
    if (!recyclepath.length())
    {
      stdErr = "error: cannot find object referenced by recycle-key=";
      stdErr += key;
      return ENOENT;
    }
  }

  // reconstruct original file name
  eos::common::Path cPath(recyclepath.c_str());

  XrdOucString originalpath = cPath.GetName();

  // demangle path
  while (originalpath.replace("#:#", "/"))
  {
  }

  if (originalpath.endswith(Recycle::gRecyclingPostFix.c_str()))
  {
    originalpath.erase(originalpath.length() - Recycle::gRecyclingPostFix.length() - 16 - 1);
  }
  else
  {
    originalpath.erase(originalpath.length() - 16 - 1);
  }

  XrdOucString repath = recyclepath.c_str();

  // check that this is a path to recycle
  if (!repath.beginswith(Recycle::gRecyclingPrefix.c_str()))
  {
    stdErr = "error: referenced object cannot be recycled\n";
    return EINVAL;
  }

  eos::common::Path oPath(originalpath.c_str());

  // check if the client is the owner of the object to recycle
  struct stat buf;
  XrdOucErrInfo lError;

  if (gOFS->_stat(cPath.GetPath(), &buf, lError, rootvid, ""))
  {
    stdErr += "error: unable to stat path to be recycled\n";
    return EIO;
  }

  // check that the client is the owner of that object
  if (vid.uid != buf.st_uid)
  {
    stdErr += "error: to recycle this file you have to have the role of the file owner: uid=";
    stdErr += (int) buf.st_uid;
    stdErr += "\n";
    return EPERM;
  }

  // check if original parent path exists
  if (gOFS->_stat(oPath.GetParentPath(), &buf, lError, rootvid, ""))
  {
    stdErr = "error: you have to recreate the restore directory path=";
    stdErr += oPath.GetParentPath();
    stdErr += " to be able to restore this file/tree\n";
    stdErr += "hint: retry after creating the mentioned directory\n";
    return ENOENT;
  }

  // check if original path is existing
  if (!gOFS->_stat(oPath.GetPath(), &buf, lError, rootvid, ""))
  {
    if ((option != "--force-original-name") && (option != "-f"))
    {
      stdErr += "error: the original path is already existing - use '--force-original-name' or '-f' to put the deleted file/tree back and rename the file/tree in place to <name>.<inode>\n";
      return EEXIST;
    }
    else
    {
      std::string newold = oPath.GetPath();
      char sp[256];
      snprintf(sp, sizeof (sp) - 1, "%016llx", (unsigned long long) (S_ISDIR(buf.st_mode) ? buf.st_ino : buf.st_ino >> 28));
      newold += ".";
      newold += sp;

      if (gOFS->_rename(oPath.GetPath(), newold.c_str(), lError, rootvid, "", "", true, true))
      {
        stdErr += "error: failed to rename the existing file/tree where we need to restore path=";
        stdErr += oPath.GetPath();
        stdErr += "\n";
        stdErr += lError.getErrText();
        return EIO;
      }
      else
      {
        stdOut += "warning: renamed restore path=";
        stdOut += oPath.GetPath();
        stdOut += " to backup-path=";
        stdOut += newold.c_str();
        stdOut += "\n";
      }
    }
  }

  // do the 'undelete' aka rename
  if (gOFS->_rename(cPath.GetPath(), oPath.GetPath(), lError, rootvid, "", "", true))
  {
    stdErr += "error: failed to undelete path=";
    oPath.GetPath();
    stdErr += "\n";
    return EIO;
  }
  else
  {
    stdOut += "success: restored path=";
    stdOut += oPath.GetPath();
    return 0;
  }
}

/*----------------------------------------------------------------------------*/
int
Recycle::Purge (XrdOucString &stdOut, XrdOucString &stdErr, eos::common::Mapping::VirtualIdentity_t &vid)
{
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);

  XrdMgmOfsDirectory dirl;
  char sdir[4096];
  snprintf(sdir, sizeof (sdir) - 1, "%s/%u/%u/", Recycle::gRecyclingPrefix.c_str(), (unsigned int) vid.gid, (unsigned int) vid.uid);
  int retc = dirl.open(sdir, vid, "");
  if (retc)
  {
    stdOut = "success: nothing has been purged!\n";
    return 0;
  }
  const char* dname;

  int nfiles_deleted = 0;
  int nbulk_deleted = 0;

  while ((dname = dirl.nextEntry()))
  {
    std::string sdname = dname;
    if ((sdname == ".") || (sdname == ".."))
    {
      continue;
    }

    std::string pathname = sdir;
    pathname += dname;
    struct stat buf;
    XrdOucErrInfo lError;

    if (!gOFS->_stat(pathname.c_str(), &buf, lError, vid, ""))
    {
      // execute a proc command
      ProcCommand Cmd;
      XrdOucString info;
      if (S_ISDIR(buf.st_mode))
      {
        // we need recursive deletion
        info = "mgm.cmd=rm&mgm.option=r&mgm.path=";
      }
      else
      {
        info = "mgm.cmd=rm&mgm.path=";
      }
      info += pathname.c_str();
      int result = Cmd.open("/proc/user", info.c_str(), rootvid, &lError);
      Cmd.AddOutput(stdOut, stdErr);
      if (!stdOut.endswith("\n"))
      {
        stdOut += "\n";
      }
      if (!stdErr.endswith("\n"))
      {
        stdErr += "\n";
      }
      Cmd.close();
      if (!result)
      {
        if (S_ISDIR(buf.st_mode))
        {
          nbulk_deleted++;
        }
        else
        {
          nfiles_deleted++;
        }
      }
    }
  }
  dirl.close();
  stdOut += "success: purged ";
  stdOut += (int) nbulk_deleted;
  stdOut += " bulk deletions and ";
  stdOut += (int) nfiles_deleted;
  stdOut += " individual files from the recycle bin!\n";
  return 0;
}

/*----------------------------------------------------------------------------*/
int
Recycle::Config (XrdOucString &stdOut, XrdOucString &stdErr, eos::common::Mapping::VirtualIdentity_t &vid, const char* arg, XrdOucString &option)
{
  XrdOucErrInfo lError;

  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);

  if (vid.uid != 0)
  {
    stdErr = "error: you need to be root to configure the recycle bin and/or recycle polcies";
    stdErr += "\n";
    return EPERM;
  }

  if (option == "--add-bin")
  {
    if (!arg)
    {
      stdErr = "error: missing subtree argument\n";
      return EINVAL;
    }

    // execute a proc command
    ProcCommand Cmd;
    XrdOucString info;

    info = "eos.rgid=0&eos.ruid=0&mgm.cmd=attr&mgm.subcmd=set&mgm.option=r&mgm.path=";
    info += arg;
    info += "&mgm.attr.key=";
    info += Recycle::gRecyclingAttribute.c_str();
    info += "&mgm.attr.value=";
    info += Recycle::gRecyclingPrefix.c_str();

    int result = Cmd.open("/proc/user", info.c_str(), rootvid, &lError);
    Cmd.AddOutput(stdOut, stdErr);
    if (!stdOut.endswith("\n"))
    {
      stdOut += "\n";
    }
    if (!stdErr.endswith("\n"))
    {
      stdErr += "\n";
    }
    Cmd.close();
    return result;
  }

  if (option == "--remove-bin")
  {
    if (!arg)
    {
      stdErr = "error: missing subtree argument\n";
      return EINVAL;
    }
    // execute a proc command
    ProcCommand Cmd;
    XrdOucString info;

    info = "eos.rgid=0&eos.ruid=0&mgm.cmd=attr&mgm.subcmd=rm&mgm.option=r&mgm.path=";
    info += arg;
    info += "&mgm.attr.key=";
    info += Recycle::gRecyclingAttribute.c_str();

    int result = Cmd.open("/proc/user", info.c_str(), rootvid, &lError);
    Cmd.AddOutput(stdOut, stdErr);
    if (!stdOut.endswith("\n"))
    {
      stdOut += "\n";
    }
    if (!stdErr.endswith("\n"))
    {
      stdErr += "\n";
    }
    Cmd.close();
    return result;
  }

  if (option == "--size")
  {
    if (!arg)
    {
      stdErr = "error: missing size argument\n";
      return EINVAL;
    }

    XrdOucString ssize = arg;
    unsigned long long size = eos::common::StringConversion::GetSizeFromString(ssize);
    if (!size)
    {
      stdErr = "error: size has been converted to 0 bytes - probably you made a type!\n";
      return EINVAL;
    }
    if (size < 1000ll * 1000ll * 100000ll)
    {
      stdErr = "error: a garbage bin smaller than 100 GB is not accepted!\n";
      return EINVAL;
    }

    // execute a proc command
    ProcCommand Cmd;
    XrdOucString info;

    info = "eos.rgid=0&eos.ruid=0&mgm.cmd=quota&mgm.subcmd=set&mgm.quota.space=";
    info += Recycle::gRecyclingPrefix.c_str();
    info += "&mgm.quota.gid=";
    info += (int) Quota::gProjectId;
    info += "&mgm.quota.maxbytes=";
    XrdOucString sizestring;
    info += eos::common::StringConversion::GetSizeString(sizestring, size);
    info += "&mgm.quota.maxinodes=10M&";

    int result = Cmd.open("/proc/user", info.c_str(), rootvid, &lError);
    Cmd.AddOutput(stdOut, stdErr);
    if (!stdOut.endswith("\n"))
    {
      stdOut += "\n";
    }
    if (!stdErr.endswith("\n"))
    {
      stdErr += "\n";
    }
    Cmd.close();
    if (!result)
    {
      stdOut += "success: recycle bin size configured!\n";
    }

    return result;
  }

  if (option == "--lifetime")
  {
    if (!arg)
    {
      stdErr = "error: missing lifetime argument\n";
      return EINVAL;
    }

    XrdOucString ssize = arg;
    unsigned long long size = eos::common::StringConversion::GetSizeFromString(ssize);
    if (!size)
    {
      stdErr = "error: lifetime has been converted to 0 seconds - probably you made a type!\n";
      return EINVAL;
    }
    if (size < 60)
    {
      stdErr = "error: a recycle bin lifetime less than 60s is not accepted!\n";
      return EINVAL;
    }

    char csize[256];
    snprintf(csize, sizeof (csize) - 1, "%llu", size);

    if (gOFS->_attr_set(Recycle::gRecyclingPrefix.c_str(),
                        lError,
                        rootvid,
                        "",
                        Recycle::gRecyclingTimeAttribute.c_str(),
                        csize))
    {
      stdErr = "error: failed to set extended attribute '";
      stdErr += Recycle::gRecyclingTimeAttribute.c_str();
      stdErr += "'";
      stdErr += " at '";
      stdErr += Recycle::gRecyclingPrefix.c_str();
      stdErr += "'";
      return EIO;
    }
    else
    {
      stdOut += "success: recycle bin lifetime configured!\n";
    }
  }
  return 0;
}
EOSMGMNAMESPACE_END
