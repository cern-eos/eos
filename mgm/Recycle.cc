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

#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/Mapping.hh"
#include "common/RWMutex.hh"
#include "common/Path.hh"
#include "mgm/Recycle.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Quota.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "namespace/interface/IView.hh"
#include "namespace/Prefetcher.hh"
#include "XrdOuc/XrdOucErrInfo.hh"

// MgmOfsConfigure prepends the proc directory path e.g. the bin is
// /eos/<instance/proc/recycle/
std::string Recycle::gRecyclingPrefix = "/recycle/";
std::string Recycle::gRecyclingAttribute = "sys.recycle";
std::string Recycle::gRecyclingTimeAttribute = "sys.recycle.keeptime";
std::string Recycle::gRecyclingKeepRatio = "sys.recycle.keepratio";
std::string Recycle::gRecyclingVersionKey = "sys.recycle.version.key";
std::string Recycle::gRecyclingPostFix = ".d";
int Recycle::gRecyclingPollTime = 30;

EOSMGMNAMESPACE_BEGIN


//------------------------------------------------------------------------------
// Run asynchronous recyling thread
//------------------------------------------------------------------------------
bool
Recycle::Start()
{
  eos_static_info("constructor");
  mThread.reset(&Recycle::Recycler, this);
  return true;
}

//------------------------------------------------------------------------------
// Cancel the asynchronous recycle thread
//------------------------------------------------------------------------------
void
Recycle::Stop()
{
  mThread.join();
}

//------------------------------------------------------------------------------
// Eternal thread doing garbage clean-up in the garbege bin
// - default garbage directory is '<instance-proc>/recycle/'
// - one should define an attribute like 'sys.recycle.keeptime' on this dir
//   to define the time in seconds how long files stay in the recycle bin
//------------------------------------------------------------------------------
void
Recycle::Recycler(ThreadAssistant& assistant) noexcept
{
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  XrdOucErrInfo lError;
  time_t lKeepTime = 0;
  double lSpaceKeepRatio = 0;
  std::multimap<time_t, std::string> lDeletionMap;
  time_t snoozetime = 10;
  unsigned long long lLowInodesWatermark = 0;
  unsigned long long lLowSpaceWatermark = 0;
  bool show_attribute_missing = true;
  eos_static_info("msg=\"async recycling thread started\"");
  gOFS->WaitUntilNamespaceIsBooted(assistant);

  if (assistant.terminationRequested()) {
    return;
  }

  assistant.wait_for(std::chrono::seconds(10));

  while (!assistant.terminationRequested()) {
    // Every now and then we wake up
    eos_static_info("snooze-time=%llu", snoozetime);

    for (int i = 0; i < snoozetime / 10; i++) {
      if (assistant.terminationRequested()) {
        return;
      }

      assistant.wait_for(std::chrono::seconds(10));
      XrdSysMutexHelper lock(mWakeUpMutex);

      if (mWakeUp) {
        mWakeUp = false;
        break;
      }
    }

    // This will be reconfigured to an appropriate value later
    snoozetime = gRecyclingPollTime;
    // Read our current policy setting
    eos::IContainerMD::XAttrMap attrmap;

    // Check if this path has a recycle attribute
    if (gOFS->_attr_ls(Recycle::gRecyclingPrefix.c_str(), lError, rootvid, "",
                       attrmap)) {
      eos_static_err("msg=\"unable to get attribute on recycle path\" recycle-path=%s",
                     Recycle::gRecyclingPrefix.c_str());
    } else {
      if (attrmap.count(Recycle::gRecyclingKeepRatio)) {
        // One can define a space threshold which actually leaves even older
        // files in the garbage bin until the threshold is reached for
        // simplicity we apply this threshold to volume & inodes
        lSpaceKeepRatio = strtod(attrmap[Recycle::gRecyclingKeepRatio].c_str(), 0);
        // Get group statistics for space and project id
        auto map_quotas = Quota::GetGroupStatistics(Recycle::gRecyclingPrefix,
                          Quota::gProjectId);

        if (!map_quotas.empty()) {
          unsigned long long usedbytes = map_quotas[SpaceQuota::kGroupBytesIs];
          unsigned long long maxbytes = map_quotas[SpaceQuota::kGroupBytesTarget];
          unsigned long long usedfiles = map_quotas[SpaceQuota::kGroupFilesIs];
          unsigned long long maxfiles = map_quotas[SpaceQuota::kGroupFilesTarget];

          if ((lSpaceKeepRatio > (1.0 * usedbytes / (maxbytes ? maxbytes : 999999999))) &&
              (lSpaceKeepRatio > (1.0 * usedfiles / (maxfiles ? maxfiles : 999999999)))) {
            eos_static_debug("msg=\"skipping recycle clean-up - ratio still low\" "
                             "ratio=%.02f space-ratio=%.02f inode-ratio=%.02f",
                             lSpaceKeepRatio,
                             1.0 * usedbytes / (maxbytes ? maxbytes : 999999999),
                             1.0 * usedfiles / (maxfiles ? maxfiles : 999999999));
            continue;
          }

          if ((lSpaceKeepRatio - 0.1) > 0) {
            lSpaceKeepRatio -= 0.1;
          }

          lLowInodesWatermark = (maxfiles * lSpaceKeepRatio);
          lLowSpaceWatermark = (maxbytes * lSpaceKeepRatio);
          eos_static_info("msg=\"cleaning by ratio policy\" low-inodes-mark=%lld "
                          "low-space-mark=%lld mark=%.02f", lLowInodesWatermark,
                          lLowSpaceWatermark, lSpaceKeepRatio);
        }
      }

      if (attrmap.count(Recycle::gRecyclingTimeAttribute)) {
        lKeepTime = strtoull(attrmap[Recycle::gRecyclingTimeAttribute].c_str(), 0, 10);
        eos_static_info("keep-time=%llu deletion-map=%llu", lKeepTime,
                        lDeletionMap.size());

        if (lKeepTime > 0) {
          if (!lDeletionMap.size()) {
            //...................................................................
            //  the deletion map is filled if there is nothing inside with files/
            //  directories found previously in the garbage bin
            //...................................................................
            // the old reyccle bin gid/uid/<contracted>
            {
              std::string subdirs;
              XrdMgmOfsDirectory dirl1;
              XrdMgmOfsDirectory dirl2;
              XrdMgmOfsDirectory dirl3;
              int listrc = dirl1.open(Recycle::gRecyclingPrefix.c_str(), rootvid,
                                      (const char*) 0);

              if (listrc) {
                eos_static_err("msg=\"unable to list the garbage directory level-1\" recycle-path=%s",
                               Recycle::gRecyclingPrefix.c_str());
              } else {
                // loop over all directories = group directories
                const char* dname1;

                while ((dname1 = dirl1.nextEntry())) {
                  {
                    std::string sdname = dname1;

                    if ((sdname == ".") || (sdname == "..")) {
                      continue;
                    }
                  }
                  std::string l2 = Recycle::gRecyclingPrefix;
                  l2 += dname1;
                  // list level-2 user directories
                  listrc = dirl2.open(l2.c_str(), rootvid, (const char*) 0);

                  if (listrc) {
                    eos_static_err("msg=\"unable to list the garbage directory level-2\" recycle-path=%s l2-path=%s",
                                   Recycle::gRecyclingPrefix.c_str(), l2.c_str());
                  } else {
                    const char* dname2;

                    while ((dname2 = dirl2.nextEntry())) {
                      {
                        std::string sdname = dname2;

                        if ((sdname == ".") || (sdname == "..")) {
                          continue;
                        }
                      }
                      std::string l3 = l2;
                      l3 += "/";
                      l3 += dname2;
                      // list the level-3 entries
                      listrc = dirl3.open(l3.c_str(), rootvid, (const char*) 0);

                      if (listrc) {
                        eos_static_err("msg=\"unable to list the garbage directory level-2\" recycle-path=%s l2-path=%s l3-path=%s",
                                       Recycle::gRecyclingPrefix.c_str(), l2.c_str(), l3.c_str());
                      } else {
                        const char* dname3;

                        while ((dname3 = dirl3.nextEntry())) {
                          {
                            std::string sdname = dname3;

                            if ((sdname == ".") || (sdname == "..")) {
                              continue;
                            }
                          }
                          std::string l4 = l3;
                          l4 += "/";
                          l4 += dname3;
                          eos_static_debug("path=%s", l4.c_str());
                          // Stat the directory to get the mtime
                          struct stat buf;

                          if (gOFS->_stat(l4.c_str(), &buf, lError, rootvid, "", nullptr, false)) {
                            eos_static_err("msg=\"unable to stat a garbage directory entry\" "
                                           "recycle-path=%s l2-path=%s l3-path=%s",
                                           Recycle::gRecyclingPrefix.c_str(), l2.c_str(), l3.c_str());
                          } else {
			    // Add to the garbage fifo deletion multimap
			    if (!S_ISDIR(buf.st_mode)) {
			      eos_static_debug("adding %s to deletion map", l4.c_str());
			      lDeletionMap.insert(std::pair<time_t, std::string > (buf.st_ctime, l4));
			    } else {
			      eos_static_debug("not adding %s to deletion map", l4.c_str());
			    }
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

              // the new recycle bin
              {
                std::map<std::string, std::set < std::string>> findmap;
                char sdir[4096];
                snprintf(sdir, sizeof(sdir) - 1, "%s/", Recycle::gRecyclingPrefix.c_str());
                XrdOucErrInfo lError;
                int depth = 6;
                XrdOucString err_msg;
		time_t now = time(NULL);

		// a recycle bin directory has the ctime with the last entry added, to get
		time_t max_ctime_dir = now - lKeepTime + (31*86400);
		time_t max_ctime_file = now - lKeepTime;
		std::map<std::string, time_t> ctime_map;

		// send a restricted query
                (void) gOFS->_find(sdir, lError, err_msg, rootvid, findmap,
                                   0, 0, false, 0, true, depth,0, true, false, NULL,
				   max_ctime_dir, max_ctime_file,
				   &ctime_map);

		eos_static_notice("time-limited query for ctime=%u:%u nfiles=%lu", max_ctime_dir, max_ctime_file, ctime_map.size());

                for (auto dirit = findmap.begin(); dirit != findmap.end(); ++dirit) {
                  XrdOucString dirname = dirit->first.c_str();

                  if (dirname.endswith(".d/")) {
                    dirname.erase(dirname.length() - 1);
                    eos::common::Path cpath(dirname.c_str());
                    dirname = cpath.GetParentPath();
                    dirit->second.insert(cpath.GetName());
                  }

                  eos_static_debug("dir=%s", dirit->first.c_str());

                  for (auto fileit = dirit->second.begin(); fileit != dirit->second.end();
                       ++fileit) {
                    // Symlink files returned by the find command above contain
                    // a pointer to the original name which needs to be removed
                    // so that we can properly stat the file.
                    std::string fname = *fileit;
                    size_t pos = fname.find(" -> ");

                    if (pos != std::string::npos) {
                      fname.erase(pos);
                      eos_static_debug("orig_path=\"%s\" symlink_path=\"%s\"",
                                       fileit->c_str(), fname.c_str());
                    }

                    XrdOucString originode;
                    XrdOucString origpath = fname.c_str();
                    eos_static_debug("path=%s", origpath.c_str());

                    if ((origpath != "/") && !origpath.beginswith("#")) {
                      continue;
                    }

                    struct stat buf;

                    std::string fullpath = dirname.c_str();

                    fullpath += fname;

		    // Add to the garbage fifo deletion multimap
		    lDeletionMap.insert(std::pair<time_t, std::string > (ctime_map[*fileit],
									 fullpath.c_str()));
		    eos_static_debug("new-bin: adding to deletionmap : %s ctime: %u", fullpath.c_str(), ctime_map[*fileit]);
                  }
                }
              }
            }
          } else {
            snoozetime = 0; // this will be redefined by the oldest entry time
            auto it = lDeletionMap.begin();
            time_t now = time(NULL);

            while (it != lDeletionMap.end()) {
              // take the first element and see if it is exceeding the keep time
              if ((it->first + lKeepTime) < now) {
                // This entry can be removed
                // If there is a keep-ratio policy defined we abort deletion once
                // we are enough under the thresholds
                if (attrmap.count(Recycle::gRecyclingKeepRatio)) {
                  auto map_quotas = Quota::GetGroupStatistics(Recycle::gRecyclingPrefix,
                                    Quota::gProjectId);

                  if (!map_quotas.empty()) {
                    unsigned long long usedbytes = map_quotas[SpaceQuota::kGroupBytesIs];
                    unsigned long long usedfiles = map_quotas[SpaceQuota::kGroupFilesIs];
                    eos_static_debug("low-volume=%lld is-volume=%lld low-inodes=%lld is-inodes=%lld",
                                     usedfiles,
                                     lLowInodesWatermark,
                                     usedbytes,
                                     lLowSpaceWatermark);

                    if ((lLowInodesWatermark >= usedfiles) &&
                        (lLowSpaceWatermark >= usedbytes)) {
                      eos_static_debug("msg=\"skipping recycle clean-up - ratio went under low watermarks\"");
                      break; // leave the deletion loop
                    }
                  }
                }

                XrdOucString delpath = it->second.c_str();

                if ((it->second.length()) &&
                    (delpath.endswith(Recycle::gRecyclingPostFix.c_str()))) {
                  // Do a directory deletion - first find all subtree children
                  std::map<std::string, std::set<std::string> > found;
                  std::map<std::string, std::set<std::string> >::const_reverse_iterator rfoundit;
                  XrdOucString err_msg;

                  if (gOFS->_find(it->second.c_str(), lError, err_msg, rootvid, found)) {
                    eos_static_err("msg=\"unable to do a find in subtree\" path=%s stderr=\"%s\"",
                                   it->second.c_str(), err_msg.c_str());
                  } else {
                    // Delete files starting at the deepest level
                    for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++) {
                      for (auto fileit = rfoundit->second.begin();
                           fileit != rfoundit->second.end();
                           fileit++) {
                        // Symlink files returned by the find command above contain
                        // a pointer to the original name which needs to be removed
                        // so that we can properly stat the file.
                        std::string fname = *fileit;
                        size_t pos = fname.find(" -> ");

                        if (pos != std::string::npos) {
                          fname.erase(pos);
                          eos_static_debug("orig_path=\"%s\" symlink_path=\"%s\"",
                                           fileit->c_str(), fname.c_str());
                        }

                        std::string fullpath = rfoundit->first;
                        fullpath += fname;

			if (gOFS->_rem(fullpath.c_str(), lError, rootvid, (const char*) 0)) {
			  eos_static_err("msg=\"unable to remove file\" path=%s",
					 fullpath.c_str());
                        } else {
                          eos_static_info("msg=\"permanently deleted file from recycle bin\" "
                                          "path=%s keep-time=%llu", fullpath.c_str(), lKeepTime);
                        }
                      }
                    }

                    // Delete directories starting at the deepest level
                    for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++) {
                      // Don't even try to delete the root directory
                      std::string fspath = rfoundit->first.c_str();

                      if (fspath == "/") {
                        continue;
                      }

		      if (gOFS->_remdir(rfoundit->first.c_str(), lError, rootvid, (const char*) 0)) {
			eos_static_err("msg=\"unable to remove directory\" path=%s",
				       fspath.c_str());
                      } else {
                        eos_static_info("msg=\"permanently deleted directory from "
                                        "recycle bin\" path=%s keep-time=%llu",
                                        fspath.c_str(), lKeepTime);
                      }
                    }
                  }

                  lDeletionMap.erase(it);
                  it = lDeletionMap.begin();
                } else {
                  // Do a single file deletion
                  if (gOFS->_rem(it->second.c_str(), lError, rootvid, (const char*) 0)) {
                    eos_static_err("msg=\"unable to remove file\" path=\"%s\" "
                                   "err_msg=\"%s\" errc=%i", it->second.c_str(),
                                   lError.getErrText(), lError.getErrInfo());
                  }

                  lDeletionMap.erase(it);
                  it = lDeletionMap.begin();
                }
              } else {
                // This entry has still to be kept
                eos_static_info("oldest entry: %lld sec to deletion",
                                it->first + lKeepTime - now);

                if (!snoozetime) {
                  // define the sleep period from the oldest entry
                  snoozetime = it->first + lKeepTime - now;

                  if (snoozetime < gRecyclingPollTime) {
                    // avoid to activate this thread too many times, 5 minutes
                    // resolution is perfectly fine
                    snoozetime = gRecyclingPollTime;
                  }

                  if (snoozetime > lKeepTime) {
                    eos_static_warning("msg=\"snooze time exceeds keeptime\" snooze-time=%llu keep-time=%llu",
                                       snoozetime, lKeepTime);
                    // That is sort of strange but let's have a fix for that
                    snoozetime = lKeepTime;
                  }
                }

                // we can leave the loop because all other entries don't match anymore the time constraint
                break;
              }
            }

            if (!snoozetime) {
              snoozetime = gRecyclingPollTime;
            }
          }
        } else {
          eos_static_warning("msg=\"parsed '%s' attribute as keep-time of %llu seconds - ignoring!\" recycle-path=%s",
                             Recycle::gRecyclingTimeAttribute.c_str(), Recycle::gRecyclingPrefix.c_str());
        }
      } else {
        if (show_attribute_missing) {
          eos_static_warning("msg=\"unable to read '%s' attribute on recycle path - undefined!\" recycle-path=%s",
                             Recycle::gRecyclingTimeAttribute.c_str(), Recycle::gRecyclingPrefix.c_str());
          show_attribute_missing = false;
        }
      }
    }
  }
}

/*----------------------------------------------------------------------------*/
int
Recycle::ToGarbage(const char* epname, XrdOucErrInfo& error, bool fusexcast)
{
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  char srecyclepath[4096];
  // If path ends with '/' we recycle a full directory tree aka directory
  bool isdir = false;
  // rewrite the file name /a/b/c as #:#a#:#b#:#c
  XrdOucString contractedpath = mPath.c_str();

  if (contractedpath.endswith("/")) {
    isdir = true;
    mPath.erase(mPath.length() - 1);
    // remove the '/' indicating a recursive directory recycling
    contractedpath.erase(contractedpath.length() - 1);
  }

  if (mRecycleDir.length() > 1) {
    if (mRecycleDir[mRecycleDir.length() - 1] == '/') {
      mRecycleDir.erase(mRecycleDir.length() - 1);
    }
  }

  while (contractedpath.replace("/", "#:#")) {
  }

  // For dir's we add a '.d' in the end of the recycle path
  std::string lPostFix = "";

  if (isdir) {
    lPostFix = Recycle::gRecyclingPostFix;
  }

  std::string rpath;
  int rc = 0;

  // retrieve the current valid index directory
  if ((rc = GetRecyclePrefix(epname, error, rpath))) {
    return rc;
  }

  snprintf(srecyclepath, sizeof(srecyclepath) - 1, "%s/%s.%016llx%s",
           rpath.c_str(),
           contractedpath.c_str(),
           mId, lPostFix.c_str());
  mRecyclePath = srecyclepath;

  // Finally do the rename
  if (gOFS->_rename(mPath.c_str(), srecyclepath, error, rootvid, "", "", true,
                    true, false, fusexcast)) {
    return gOFS->Emsg(epname, error, EIO, "rename file/directory", srecyclepath);
  }

  // store the recycle path in the error object
  error.setErrInfo(0, srecyclepath);
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
void
Recycle::Print(std::string& std_out, std::string& std_err,
               eos::common::VirtualIdentity& vid, bool monitoring,
               bool translateids, bool details, std::string date, bool global,
               Recycle::RecycleListing* rvec)
{
  XrdOucString uids;
  XrdOucString gids;
  std::map<uid_t, bool> printmap;
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  std::ostringstream oss_out;

  if (global && ((!vid.uid) ||
                 (vid.hasUid(3)) ||
                 (vid.hasGid(4)))) {
    // add everything found in the recycle directory structure to the printmap
    std::string subdirs;
    XrdMgmOfsDirectory dirl;
    int listrc = dirl.open(Recycle::gRecyclingPrefix.c_str(), rootvid,
                           (const char*) 0);

    if (listrc) {
      eos_static_err("msg=\"unable to list the garbage directory level-1\" recycle-path=%s",
                     Recycle::gRecyclingPrefix.c_str());
    } else {
      // loop over all directories = group directories
      const char* dname1;

      while ((dname1 = dirl.nextEntry())) {
        std::string sdname = dname1;

        if ((sdname == ".") || (sdname == "..")) {
          continue;
        }

        if (sdname.substr(0, 4) == "uid:") {
          uid_t uid = std::stoull(sdname.substr(4));
          printmap[uid] = true;
        }
      }

      dirl.close();
    }
  } else {
    // add only the virtual user to the printmap
    printmap[vid.uid] = true;
  }

  eos::common::Path dPath(std::string("/") + date);

  if (details) {
    size_t count = 0;

    for (auto ituid = printmap.begin(); ituid != printmap.end(); ituid++) {
      std::map<std::string, std::set < std::string>> findmap;
      char sdir[4096];
      snprintf(sdir, sizeof(sdir) - 1, "%s/uid:%u/%s",
               Recycle::gRecyclingPrefix.c_str(),
               (unsigned int) ituid->first, date.c_str());
      XrdOucErrInfo lError;
      int depth = 5 ;

      if (dPath.GetSubPathSize()) {
        if (depth > (int) dPath.GetSubPathSize()) {
          depth -= dPath.GetSubPathSize();
        }
      }

      XrdOucString err_msg;
      int retc = gOFS->_find(sdir, lError, err_msg, rootvid, findmap,
                             0, 0, false, 0, true, depth);

      if (retc && errno != ENOENT) {
        std_err = err_msg.c_str();
        eos_static_err("find command failed in dir='%s'", sdir);
      }

      for (auto dirit = findmap.begin(); dirit != findmap.end(); ++dirit) {
        XrdOucString dirname = dirit->first.c_str();

        if (dirname.endswith(".d/")) {
          dirname.erase(dirname.length() - 1);
          eos::common::Path cpath(dirname.c_str());
          dirname = cpath.GetParentPath();
          dirit->second.insert(cpath.GetName());
        }

        eos_static_debug("dir=%s", dirit->first.c_str());

        for (auto fileit = dirit->second.begin(); fileit != dirit->second.end();
             ++fileit) {
          // Symlink files returned by the find command above contain
          // a pointer to the original name which needs to be removed
          // so that we can properly stat the file.
          std::string fname = *fileit;
          size_t pos = fname.find(" -> ");

          if (pos != std::string::npos) {
            fname.erase(pos);
            eos_static_debug("orig_path=\"%s\" symlink_path=\"%s\"",
                             fileit->c_str(), fname.c_str());
          }

          std::string fullpath = dirname.c_str();
          fullpath += fname;
          XrdOucString originode;
          XrdOucString origpath = fname.c_str();
          eos_static_debug("file=%s", origpath.c_str());

          if ((origpath != "/") && !origpath.beginswith("#")) {
            continue;
          }

          // demangle the original pathname
          while (origpath.replace("#:#", "/")) {
          }

          XrdOucErrInfo error;
          XrdOucString type = "file";
          struct stat buf;

          if (!gOFS->_stat(fullpath.c_str(), &buf, error, vid, "", nullptr, false)) {
            if (translateids) {
              int errc = 0;
              uids = eos::common::Mapping::UidToUserName(buf.st_uid, errc).c_str();

              if (errc) {
                uids = eos::common::Mapping::UidAsString(buf.st_uid).c_str();
              }

              gids = eos::common::Mapping::GidToGroupName(buf.st_gid, errc).c_str();

              if (errc) {
                gids = eos::common::Mapping::GidAsString(buf.st_gid).c_str();
              }
            } else {
              uids = eos::common::Mapping::UidAsString(buf.st_uid).c_str();
              gids = eos::common::Mapping::GidAsString(buf.st_gid).c_str();
            }

            if (origpath.endswith(Recycle::gRecyclingPostFix.c_str())) {
              type = "recursive-dir";
              origpath.erase(origpath.length() - Recycle::gRecyclingPostFix.length());
            }

            originode = origpath;
            originode.erase(0, origpath.length() - 16);
            origpath.erase(origpath.length() - 17);

            // put the key prefixes
            if (type == "file") {
              originode.insert("fxid:", 0);
            } else {
              originode.insert("pxid:", 0);
            }

            if (monitoring) {
              oss_out << "recycle=ls recycle-bin=" << Recycle::gRecyclingPrefix
                      << " uid=" << uids.c_str() << " gid=" << gids.c_str()
                      << " size=" << std::to_string(buf.st_size)
                      << " deletion-time=" << std::to_string(buf.st_ctime)
                      << " type=" << type.c_str()
                      << " keylength.restore-path=" << origpath.length()
                      << " restore-path=" << origpath.c_str()
                      << " restore-key=" << originode.c_str()
                      << std::endl;

              if (rvec) {
                std::map<std::string, std::string> rmap;
                rmap["uid"] = std::to_string(buf.st_uid);
                rmap["gid"] = std::to_string(buf.st_gid);
                rmap["username"] = uids.c_str();
                rmap["groupname"] = gids.c_str();
                rmap["size"] = std::to_string(buf.st_size);
                rmap["dtime"] = std::to_string(buf.st_ctime);
                rmap["type"] = type.c_str();
                rmap["path"] = origpath.c_str();
                rmap["key"] = originode.c_str();
                rvec->push_back(rmap);
              }
            } else {
              char sline[4096];
              XrdOucString sizestring;

              if (count == 0) {
                // print a header
                snprintf(sline, sizeof(sline) - 1,
                         "# %-24s %-8s %-8s %-12s %-13s %-20s %-64s\n", "Deletion Time", "UID", "GID",
                         "SIZE", "TYPE", "RESTORE-KEY", "RESTORE-PATH");
                oss_out << sline
                        << "# ================================================"
                        << "=================================================="
                        << "============================" << std::endl;
              }

              char tdeltime[4096];
              std::string deltime = ctime_r(&buf.st_ctime, tdeltime);
              deltime.erase(deltime.length() - 1);
              snprintf(sline, sizeof(sline) - 1, "%-26s %-8s %-8s %-12s %-13s %-16s %-64s",
                       deltime.c_str(), uids.c_str(), gids.c_str(),
                       eos::common::StringConversion::GetSizeString(sizestring,
                           (unsigned long long) buf.st_size), type.c_str(), originode.c_str(),
                       origpath.c_str());

              if (oss_out.tellp() > 1 * 1024 * 1024 * 1024) {
                retc = E2BIG;
                oss_out << "... (truncated after 1G of output)" << std::endl;
                std_out += oss_out.str();
                std_err += "warning: list too long - truncated after 1GB of output!\n";
                return;
              }

              oss_out << sline << std::endl;
            }

            count++;

            if ((vid.uid) && (!vid.sudoer) && (count > 100000)) {
              retc = E2BIG;
              oss_out << "... (truncated)" << std::endl;
              std_out += oss_out.str();
              std_err += "warning: list too long - truncated after 100000 entries!\n";
              return;
            }
          }
        }
      }
    }
  } else {
    auto map_quotas = Quota::GetGroupStatistics(Recycle::gRecyclingPrefix,
                      Quota::gProjectId);

    if (!map_quotas.empty()) {
      unsigned long long usedbytes = map_quotas[SpaceQuota::kGroupBytesIs];
      unsigned long long maxbytes = map_quotas[SpaceQuota::kGroupBytesTarget];
      unsigned long long usedfiles = map_quotas[SpaceQuota::kGroupFilesIs];
      unsigned long long maxfiles = map_quotas[SpaceQuota::kGroupFilesTarget];
      char sline[1024];
      XrdOucString sizestring1;
      XrdOucString sizestring2;
      eos::IContainerMD::XAttrMap attrmap;
      XrdOucErrInfo error;

      // Check if this path has a recycle attribute
      if (gOFS->_attr_ls(Recycle::gRecyclingPrefix.c_str(), error, rootvid, "",
                         attrmap)) {
        eos_static_err("msg=\"unable to get attribute on recycle path\" "
                       "recycle-path=%s", Recycle::gRecyclingPrefix.c_str());
      }

      if (!monitoring) {
        oss_out << "# _________________________________________________________"
                << "___________________________________________________________"
                << "_______" << std::endl;
        snprintf(sline, sizeof(sline) - 1, "# used %s out of %s (%.02f%% volume "
                 "/ %.02f%% inodes used) Object-Lifetime %s [s] Keep-Ratio %s",
                 eos::common::StringConversion::GetReadableSizeString(sizestring1, usedbytes,
                     "B"),
                 eos::common::StringConversion::GetReadableSizeString(sizestring2, maxbytes,
                     "B"),
                 usedbytes * 100.0 / maxbytes,
                 usedfiles * 100.0 / maxfiles,
                 attrmap.count(Recycle::gRecyclingTimeAttribute) ?
                 attrmap[Recycle::gRecyclingTimeAttribute].c_str() : "not configured",
                 attrmap.count(Recycle::gRecyclingKeepRatio) ?
                 attrmap[Recycle::gRecyclingKeepRatio].c_str() : "not configured");
        oss_out << sline << std::endl
                << "# _________________________________________________________"
                << "___________________________________________________________"
                << "_______" << std::endl;
      } else {
        snprintf(sline, sizeof(sline) - 1, "recycle-bin=%s usedbytes=%s "
                 "maxbytes=%s volumeusage=%.02f%% inodeusage=%.02f%% lifetime=%s ratio=%s",
                 Recycle::gRecyclingPrefix.c_str(),
                 eos::common::StringConversion::GetSizeString(sizestring1, usedbytes),
                 eos::common::StringConversion::GetSizeString(sizestring2, maxbytes),
                 usedbytes * 100.0 / maxbytes,
                 usedfiles * 100.0 / maxfiles,
                 attrmap.count(Recycle::gRecyclingTimeAttribute) ?
                 attrmap[Recycle::gRecyclingTimeAttribute].c_str() : "-1",
                 attrmap.count(Recycle::gRecyclingKeepRatio) ?
                 attrmap[Recycle::gRecyclingKeepRatio].c_str() : "-1");
        oss_out << sline << std::endl;
      }
    }
  }

  std_out += oss_out.str();
}

/*----------------------------------------------------------------------------*/
void
Recycle::PrintOld(std::string& std_out, std::string& std_err,
                  eos::common::VirtualIdentity& vid, bool monitoring,
                  bool translateids, bool details)
{
  XrdOucString uids;
  XrdOucString gids;
  std::map<gid_t, std::map<uid_t, bool> > printmap;
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();

  if ((!vid.uid) ||
      (vid.hasUid(3)) ||
      (vid.hasGid(4))) {
    // add everything found in the recycle directory structure to the printmap
    std::string subdirs;
    XrdMgmOfsDirectory dirl1;
    XrdMgmOfsDirectory dirl2;
    int listrc = dirl1.open(Recycle::gRecyclingPrefix.c_str(), rootvid,
                            (const char*) 0);

    if (listrc) {
      eos_static_err("msg=\"unable to list the garbage directory level-1\" recycle-path=%s",
                     Recycle::gRecyclingPrefix.c_str());
    } else {
      // loop over all directories = group directories
      const char* dname1;

      while ((dname1 = dirl1.nextEntry())) {
        std::string sdname = dname1;

        if ((sdname == ".") || (sdname == "..")) {
          continue;
        }

        if (sdname.substr(0, 4) == "uid:") {
          continue;
        }

        gid_t gid = strtoull(dname1, 0, 10);
        std::string l2 = Recycle::gRecyclingPrefix;
        l2 += dname1;
        // list level-2 user directories
        listrc = dirl2.open(l2.c_str(), rootvid, (const char*) 0);

        if (listrc) {
          eos_static_err("msg=\"unable to list the garbage directory level-2\" recycle-path=%s l2-path=%s",
                         Recycle::gRecyclingPrefix.c_str(), l2.c_str());
        } else {
          const char* dname2;

          while ((dname2 = dirl2.nextEntry())) {
            std::string sdname = dname2;

            if ((sdname == ".") || (sdname == "..")) {
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
  } else {
    // add only the virtual user to the printmap
    printmap[vid.gid][vid.uid] = true;
  }

  std::ostringstream oss_out;

  if (details) {
    size_t count = 0;

    for (auto itgid = printmap.begin(); itgid != printmap.end(); itgid++) {
      for (auto ituid = itgid->second.begin(); ituid != itgid->second.end();
           ituid++) {
        XrdMgmOfsDirectory dirl;
        char sdir[4096];
        snprintf(sdir, sizeof(sdir) - 1, "%s/%u/%u/", Recycle::gRecyclingPrefix.c_str(),
                 (unsigned int) itgid->first, (unsigned int) ituid->first);
        int retc = dirl.open(sdir, vid, "");

        if (!retc) {
          const char* dname;

          while ((dname = dirl.nextEntry())) {
            std::string sdname = dname;

            if ((sdname == ".") || (sdname == "..")) {
              continue;
            }

            std::string fullpath = sdir;
            fullpath += dname;
            XrdOucString originode;
            XrdOucString origpath = dname;

            // demangle the original pathname
            while (origpath.replace("#:#", "/")) {
            }

            XrdOucString type = "file";
            struct stat buf;
            XrdOucErrInfo error;

            if (!gOFS->_stat(fullpath.c_str(), &buf, error, vid, "", nullptr, false)) {
              if (translateids) {
                int errc = 0;
                uids = eos::common::Mapping::UidToUserName(buf.st_uid, errc).c_str();

                if (errc) {
                  uids = eos::common::Mapping::UidAsString(buf.st_uid).c_str();
                }

                gids = eos::common::Mapping::GidToGroupName(buf.st_gid, errc).c_str();

                if (errc) {
                  gids = eos::common::Mapping::GidAsString(buf.st_gid).c_str();
                }
              } else {
                uids = eos::common::Mapping::UidAsString(buf.st_uid).c_str();
                gids = eos::common::Mapping::GidAsString(buf.st_gid).c_str();
              }

              if (origpath.endswith(Recycle::gRecyclingPostFix.c_str())) {
                type = "recursive-dir";
                origpath.erase(origpath.length() - Recycle::gRecyclingPostFix.length());
              }

              originode = origpath;
              originode.erase(0, origpath.length() - 16);
              origpath.erase(origpath.length() - 17);

              // put the key prefixes
              if (type == "file") {
                originode.insert("fxid:", 0);
              } else {
                originode.insert("pxid:", 0);
              }

              if (monitoring) {
                XrdOucString sizestring;
                oss_out << "recycle=ls recycle-bin=" <<  Recycle::gRecyclingPrefix.c_str()
                        << " uid=" << uids.c_str() << " gid=" << gids.c_str()
                        << " size=" << std::to_string(buf.st_size)
                        << " deletion-time=" << std::to_string(buf.st_ctime)
                        << " type=" << type.c_str()
                        << " keylength.restore-path=" << origpath.length()
                        << " restore-path=" << origpath.c_str()
                        << " restore-key=" << originode.c_str() << std::endl;
              } else {
                char sline[4096];
                XrdOucString sizestring;

                if (count == 0) {
                  // print a header
                  snprintf(sline, sizeof(sline) - 1,
                           "# %-24s %-8s %-8s %-12s %-13s %-20s %-64s\n",
                           "Deletion Time", "UID", "GID", "SIZE", "TYPE",
                           "RESTORE-KEY", "RESTORE-PATH");
                  oss_out << sline
                          << "# ================================================"
                          "=========================================================="
                          "====================\n";
                }

                char tdeltime[4096];
                std::string deltime = ctime_r(&buf.st_ctime, tdeltime);
                deltime.erase(deltime.length() - 1);
                snprintf(sline, sizeof(sline) - 1, "%-26s %-8s %-8s %-12s %-13s %-20s %-64s",
                         deltime.c_str(), uids.c_str(), gids.c_str(),
                         eos::common::StringConversion::GetSizeString(sizestring,
                             (unsigned long long) buf.st_size), type.c_str(), originode.c_str(),
                         origpath.c_str());

                if (oss_out.tellp() > 1 * 1024 * 1024 * 1024) {
                  retc = E2BIG;
                  oss_out << "... (truncated after 1G of output)" << std::endl;
                  std_out += oss_out.str();
                  std_err += "warning: list too long - truncated after 1GB of output!\n";
                  return;
                }

                oss_out << sline << std::endl;
              }

              count++;

              if ((vid.uid) && (!vid.sudoer) && (count > 100000)) {
                retc = E2BIG;
                oss_out << "... (truncated)" << std::endl;
                std_out += oss_out.str();
                std_err += "warning: list too long - truncated after 100000 entries!\n";
                return;
              }
            }
          }
        }
      }
    }
  }

  std_out += oss_out.str();
}

/*----------------------------------------------------------------------------*/
int
Recycle::Restore(std::string& std_out, std::string& std_err,
                 eos::common::VirtualIdentity& vid, const char* key,
                 bool force_orig_name, bool restore_versions, bool make_path)
{
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();

  if (!key) {
    std_err += "error: invalid argument as recycle key\n";
    return EINVAL;
  }

  XrdOucString skey = key;
  bool force_file = false;
  bool force_directory = false;

  if (skey.beginswith("fxid:")) {
    skey.erase(0, 5);
    force_file = true;
  }

  if (skey.beginswith("pxid:")) {
    skey.erase(0, 5);
    force_directory = true;
  }

  unsigned long long fid = strtoull(skey.c_str(), 0, 16);
  // convert the hex inode number into decimal and retrieve path name
  std::shared_ptr<eos::IFileMD> fmd;
  std::shared_ptr<eos::IContainerMD> cmd;
  std::string recyclepath;
  XrdOucString repath;
  XrdOucString rprefix = Recycle::gRecyclingPrefix.c_str();
  rprefix += "/";
  rprefix += (int) vid.gid;
  rprefix += "/";
  rprefix += (int) vid.uid;
  XrdOucString newrprefix = Recycle::gRecyclingPrefix.c_str();
  newrprefix += "/uid:";
  newrprefix += (int) vid.uid;

  while (rprefix.replace("//", "/")) {
  }

  while (newrprefix.replace("//", "/")) {
  }

  {
    // TODO(gbitzes): This could be more precise...
    eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, fid);
    eos::Prefetcher::prefetchContainerMDWithParentsAndWait(gOFS->eosView, fid);
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);

    if (!force_directory) {
      try {
        fmd = gOFS->eosFileService->getFileMD(fid);
        recyclepath = gOFS->eosView->getUri(fmd.get());
        repath = recyclepath.c_str();

        if (!repath.beginswith(rprefix.c_str()) &&
            !repath.beginswith(newrprefix.c_str())) {
          std_err = "error: this is not a file in your recycle bin - try to "
                    "prefix the key with pxid:<key>\n";
          return EPERM;
        }
      } catch (eos::MDException& e) {
      }
    }

    if (!force_file && !fmd) {
      try {
        cmd = gOFS->eosDirectoryService->getContainerMD(fid);
        recyclepath = gOFS->eosView->getUri(cmd.get());
        repath = recyclepath.c_str();

        if (!repath.beginswith(rprefix.c_str()) &&
            !repath.beginswith(newrprefix.c_str())) {
          std_err = "error: this is not a directory in your recycle bin\n";
          return EPERM;
        }
      } catch (eos::MDException& e) {
      }
    }

    if (!recyclepath.length()) {
      std_err = "error: cannot find object referenced by recycle-key=";
      std_err += key;
      return ENOENT;
    }
  }

  // reconstruct original file name
  eos::common::Path cPath(recyclepath.c_str());
  XrdOucString originalpath = cPath.GetName();

  // Demangle path
  while (originalpath.replace("#:#", "/")) {
  }

  if (originalpath.endswith(Recycle::gRecyclingPostFix.c_str())) {
    originalpath.erase(originalpath.length() - Recycle::gRecyclingPostFix.length() -
                       16 - 1);
  } else {
    originalpath.erase(originalpath.length() - 16 - 1);
  }

  // Check that this is a path to recycle
  if (!repath.beginswith(Recycle::gRecyclingPrefix.c_str())) {
    std_err = "error: referenced object cannot be recycled\n";
    return EINVAL;
  }

  eos::common::Path oPath(originalpath.c_str());
  // Check if the client is the owner of the object to recycle
  struct stat buf;
  XrdOucErrInfo lError;
  eos_static_info("msg=\"trying to restore file\" path=\"%s\"",
                  cPath.GetPath());

  if (gOFS->_stat(cPath.GetPath(), &buf, lError, rootvid, "", nullptr, false)) {
    std_err += "error: unable to stat path to be recycled\n";
    return EIO;
  }

  // check that the client is the owner of that object
  if (vid.uid != buf.st_uid) {
    std_err +=
      "error: to recycle this file you have to have the role of the file owner: uid=";
    std_err += (int) buf.st_uid;
    std_err += "\n";
    return EPERM;
  }

  // check if original parent path exists
  if (gOFS->_stat(oPath.GetParentPath(), &buf, lError, rootvid, "")) {
    if (make_path) {
      XrdOucErrInfo lError;
      // create path
      ProcCommand cmd;
      XrdOucString info = "mgm.cmd=mkdir&mgm.option=p&mgm.path=";
      info += oPath.GetParentPath();
      cmd.open("/proc/user", info.c_str(), vid, &lError);
      cmd.close();
      int rc = cmd.GetRetc();

      if (rc) {
        std_err += "error: creation failed: ";
        std_err += cmd.GetStdErr();
        return rc;
      }
    } else {
      std_err = "error: you have to recreate the restore directory path=";
      std_err += oPath.GetParentPath();
      std_err += " to be able to restore this file/tree\n";
      std_err += "hint: retry after creating the mentioned directory\n";
      return ENOENT;
    }
  }

  // check if original path is existing
  if (!gOFS->_stat(oPath.GetPath(), &buf, lError, rootvid, "", nullptr, false)) {
    if (force_orig_name == false) {
      std_err +=
        "error: the original path already exists, use '-f|--force-original-name' \n"
        "to put the deleted file/tree back and rename the file/tree in place to <name>.<inode>\n";
      return EEXIST;
    } else {
      std::string newold = oPath.GetPath();
      char sp[256];
      snprintf(sp, sizeof(sp) - 1, "%016llx",
               (unsigned long long)(S_ISDIR(buf.st_mode) ? buf.st_ino :
                                    eos::common::FileId::InodeToFid(buf.st_ino)));
      newold += ".";
      newold += sp;

      if (gOFS->_rename(oPath.GetPath(), newold.c_str(), lError, rootvid, "", "",
                        true, true)) {
        std_err +=
          "error: failed to rename the existing file/tree where we need to restore path=";
        std_err += oPath.GetPath();
        std_err += "\n";
        std_err += lError.getErrText();
        return EIO;
      } else {
        std_out += "warning: renamed restore path=";
        std_out += oPath.GetPath();
        std_out += " to backup-path=";
        std_out += newold.c_str();
        std_out += "\n";
      }
    }
  }

  // do the 'undelete' aka rename
  if (gOFS->_rename(cPath.GetPath(), oPath.GetPath(), lError, rootvid, "", "",
                    true)) {
    std_err += "error: failed to undelete path=";
    std_err += oPath.GetPath();
    std_err += "\n";
    return EIO;
  } else {
    std_out += "success: restored path=";
    std_out += oPath.GetPath();
    std_out += "\n";
  }

  if (restore_versions == false) {
    // don't restore old versions
    return 0;
  }

  XrdOucString vkey;

  if (gOFS->_attr_get(oPath.GetPath(), lError, rootvid, "",
                      Recycle::gRecyclingVersionKey.c_str(), vkey)) {
    // no version directory to restore
    return 0;
  }

  int retc = Restore(std_out, std_err, vid, vkey.c_str(), force_orig_name,
                     restore_versions);

  // mask an non existant version reference
  if (retc == ENOENT) {
    return 0;
  }

  return retc;
}

/*----------------------------------------------------------------------------*/
int
Recycle::PurgeOld(std::string& std_out, std::string& std_err,
                  eos::common::VirtualIdentity& vid)
{
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  XrdMgmOfsDirectory dirl;
  char sdir[4096];
  snprintf(sdir, sizeof(sdir) - 1, "%s/%u/%u/", Recycle::gRecyclingPrefix.c_str(),
           (unsigned int) vid.gid, (unsigned int) vid.uid);
  int retc = dirl.open(sdir, vid, "");

  if (retc) {
    std_out = "success: nothing has been purged in the old recycle bin!\n";
    return 0;
  }

  const char* dname;
  int nfiles_deleted = 0;
  int nbulk_deleted = 0;

  while ((dname = dirl.nextEntry())) {
    std::string sdname = dname;

    if ((sdname == ".") || (sdname == "..")) {
      continue;
    }

    std::string pathname = sdir;
    pathname += dname;
    struct stat buf;
    XrdOucErrInfo lError;

    if (!gOFS->_stat(pathname.c_str(), &buf, lError, vid, "", nullptr, false)) {
      // execute a proc command
      ProcCommand Cmd;
      XrdOucString info;

      if (S_ISDIR(buf.st_mode)) {
        // we need recursive deletion
        info = "mgm.cmd=rm&mgm.option=r&mgm.path=";
      } else {
        info = "mgm.cmd=rm&mgm.path=";
      }

      info += pathname.c_str();
      int result = Cmd.open("/proc/user", info.c_str(), rootvid, &lError);
      Cmd.AddOutput(std_out, std_err);

      if (*std_out.rbegin() != '\n') {
        std_out += "\n";
      }

      if (*std_err.rbegin() != '\n') {
        std_err += "\n";
      }

      Cmd.close();

      if (!result) {
        if (S_ISDIR(buf.st_mode)) {
          nbulk_deleted++;
        } else {
          nfiles_deleted++;
        }
      }
    }
  }

  dirl.close();
  std_out += "success: purged ";
  std_out += std::to_string(nbulk_deleted);
  std_out += " bulk deletions and ";
  std_out += std::to_string(nfiles_deleted);
  std_out += " individual files from the old recycle bin!\n";
  return 0;
}

/*----------------------------------------------------------------------------*/
int
Recycle::Purge(std::string& std_out, std::string& std_err,
               eos::common::VirtualIdentity& vid,
               std::string date,
               bool global,
               std::string key)
{
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  XrdMgmOfsDirectory dirl;
  char sdir[4096];
  XrdOucErrInfo lError;
  int nfiles_deleted = 0;
  int nbulk_deleted = 0;
  std::string rpath;

  // translate key into search pattern
  if (key.length()) {
    if (key.substr(0, 5) == "fxid:") {
      // purge file
      key.erase(0, 5);
    } else {
      if (key.substr(0, 5) == "pxid:") {
        // purge directory
        key.erase(0, 5);
        key += ".d";
      } else {
        std_err = "error: the given key to purge is invalid - must start with fxid: or pxid: (see output of recycle ls)";
        return EINVAL;
      }
    }
  }

  if (vid.uid && !vid.sudoer &&
      !(vid.hasUid(3)) &&
      !(vid.hasGid(4))) {
    std_err = "error: you cannot purge your recycle bin without being a sudor "
              "or having an admin role";
    return EPERM;
  }

  if (!global || (global && vid.uid)) {
    snprintf(sdir, sizeof(sdir) - 1, "%s/uid:%u/%s",
             Recycle::gRecyclingPrefix.c_str(),
             (unsigned int) vid.uid,
             date.c_str());
  } else {
    snprintf(sdir, sizeof(sdir) - 1, "%s/", Recycle::gRecyclingPrefix.c_str());
  }

  std::map<std::string, std::set < std::string>> findmap;
  int depth = 5 + (int) global;
  eos::common::Path dPath(std::string("/") + date);

  if (dPath.GetSubPathSize()) {
    if (depth > (int) dPath.GetSubPathSize()) {
      depth -= dPath.GetSubPathSize();
    }
  }

  XrdOucString err_msg;
  int retc = gOFS->_find(sdir, lError, err_msg, rootvid, findmap,
                         0, 0, false, 0, true, depth);

  if (retc && errno != ENOENT) {
    std_err = err_msg.c_str();
    eos_static_err("msg=\"find command failed\" dir=\"%s\"", sdir);
  }

  for (auto dirit = findmap.begin(); dirit != findmap.end(); ++dirit) {
    eos_static_debug("dir=%s", dirit->first.c_str());
    XrdOucString dirname = dirit->first.c_str();

    if (dirname.endswith(".d/")) {
      dirname.erase(dirname.length() - 1);
      eos::common::Path cpath(dirname.c_str());
      dirname = cpath.GetParentPath();
      dirit->second.insert(cpath.GetName());
    }

    for (auto fileit = dirit->second.begin(); fileit != dirit->second.end();
         ++fileit) {
      // Symlink files returned by the find command above contain
      // a pointer to the original name which needs to be removed
      // so that we can properly stat the file.
      std::string fname = *fileit;
      size_t pos = fname.find(" -> ");

      if (pos != std::string::npos) {
        fname.erase(pos);
        eos_static_debug("orig_path=\"%s\" symlink_path=\"%s\"",
                         fileit->c_str(), fname.c_str());
      }

      if ((fname != "/") && (fname.find('#') != 0)) {
        continue;
      }

      struct stat buf;

      XrdOucErrInfo lError;

      std::string fullpath = dirname.c_str();

      fullpath += fname;

      if (!gOFS->_stat(fullpath.c_str(), &buf, lError, rootvid, "", nullptr, false)) {
        if (key.length()) {
          // check for a particular string pattern
          if (fullpath.find(key) == std::string::npos) {
            continue;
          }
        }

        // execute a proc command
        ProcCommand Cmd;
        XrdOucString info;

        if (S_ISDIR(buf.st_mode)) {
          // we need recursive deletion
          info = "mgm.cmd=rm&mgm.option=r&mgm.path=";
        } else {
          info = "mgm.cmd=rm&mgm.path=";
        }

        info += fullpath.c_str();
        int result = Cmd.open("/proc/user", info.c_str(), rootvid, &lError);
        Cmd.AddOutput(std_out, std_err);

        if (*std_out.rbegin() != '\n') {
          std_out += "\n";
        }

        if (*std_err.rbegin() != '\n') {
          std_err += "\n";
        }

        Cmd.close();

        if (!result) {
          if (S_ISDIR(buf.st_mode)) {
            nbulk_deleted++;
          } else {
            nfiles_deleted++;
          }
        }
      }
    }
  }

  std_out += "success: purged ";
  std_out += std::to_string(nbulk_deleted);
  std_out += " bulk deletions and ";
  std_out += std::to_string(nfiles_deleted);
  std_out += " individual files from the recycle bin!";

  if (key.length() &&
      (!nbulk_deleted) &&
      (!nfiles_deleted)) {
    std_err += "error: no entry for key='";
    std_err += key;
    std_err += "'";
    return ENODATA;
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
int
Recycle::Config(std::string& std_out, std::string& std_err,
                eos::common::VirtualIdentity& vid,
                const std::string& key, const std::string& value)
{
  XrdOucErrInfo lError;
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();

  if (vid.uid != 0) {
    std_err = "error: you need to be root to configure the recycle bin"
              " and/or recycle polcies\n";
    return EPERM;
  }

  if (key == "--add-bin") {
    if (value.empty()) {
      std_err = "error: missing subtree argument\n";
      return EINVAL;
    }

    // execute a proc command
    ProcCommand Cmd;
    XrdOucString info;
    info = "eos.rgid=0&eos.ruid=0&mgm.cmd=attr&mgm.subcmd=set&mgm.option=r&mgm.path=";
    info += value.c_str();
    info += "&mgm.attr.key=";
    info += Recycle::gRecyclingAttribute.c_str();
    info += "&mgm.attr.value=";
    info += Recycle::gRecyclingPrefix.c_str();
    int result = Cmd.open("/proc/user", info.c_str(), rootvid, &lError);
    Cmd.AddOutput(std_out, std_err);
    Cmd.close();
    return result;
  }

  if (key == "--remove-bin") {
    if (value.empty()) {
      std_err = "error: missing subtree argument\n";
      return EINVAL;
    }

    // execute a proc command
    ProcCommand Cmd;
    XrdOucString info;
    info = "eos.rgid=0&eos.ruid=0&mgm.cmd=attr&mgm.subcmd=rm&mgm.option=r&mgm.path=";
    info += value.c_str();
    info += "&mgm.attr.key=";
    info += Recycle::gRecyclingAttribute.c_str();
    int result = Cmd.open("/proc/user", info.c_str(), rootvid, &lError);
    Cmd.AddOutput(std_out, std_err);
    Cmd.close();
    return result;
  }

  if (key == "--lifetime") {
    if (value.empty()) {
      std_err = "error: missing lifetime argument";
      return EINVAL;
    }

    uint64_t size = 0ull;

    try {
      size = std::stoull(value);
    } catch (...) {
      size = 0ull;
    }

    if (!size) {
      std_err = "error: lifetime has been converted to 0 seconds - probably you made a typo!";
      return EINVAL;
    }

    if (size < 60) {
      std_err = "error: a recycle bin lifetime less than 60s is not accepted!";
      return EINVAL;
    }

    if (gOFS->_attr_set(Recycle::gRecyclingPrefix.c_str(),
                        lError, rootvid, "",
                        Recycle::gRecyclingTimeAttribute.c_str(),
                        value.c_str())) {
      std_err = "error: failed to set extended attribute '";
      std_err += Recycle::gRecyclingTimeAttribute.c_str();
      std_err += "'";
      std_err += " at '";
      std_err += Recycle::gRecyclingPrefix.c_str();
      std_err += "'";
      return EIO;
    } else {
      std_out += "success: recycle bin lifetime configured!\n";
    }

    gOFS->Recycler->WakeUp();
  }

  if (key == "--ratio") {
    if (value.empty()) {
      std_err = "error: missing ratio argument\n";
      return EINVAL;
    }

    double ratio = 0.0;

    try {
      ratio = std::stod(value);
    } catch (...) {
      ratio = 0.0;
    }

    if (!ratio) {
      std_err = "error: ratio must be != 0";
      return EINVAL;
    }

    if ((ratio <= 0) || (ratio > 0.99)) {
      std_err = "error: a recycle bin ratio has to be 0 < ratio < 1.0!";
      return EINVAL;
    }

    if (gOFS->_attr_set(Recycle::gRecyclingPrefix.c_str(),
                        lError, rootvid, "",
                        Recycle::gRecyclingKeepRatio.c_str(),
                        value.c_str())) {
      std_err = "error: failed to set extended attribute '";
      std_err += Recycle::gRecyclingKeepRatio.c_str();
      std_err += "'";
      std_err += " at '";
      std_err += Recycle::gRecyclingPrefix.c_str();
      std_err += "'";
      return EIO;
    } else {
      std_out += "success: recycle bin ratio configured!";
    }

    gOFS->Recycler->WakeUp();
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
int
Recycle::GetRecyclePrefix(const char* epname, XrdOucErrInfo& error,
                          std::string& recyclepath, int i_index)
/*----------------------------------------------------------------------------*/
{
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  char srecycleuser[4096];
  time_t now = time(NULL);
  struct tm nowtm;
  localtime_r(&now, &nowtm);
  size_t index = (i_index == -1) ? 0 : i_index;

  do {
    snprintf(srecycleuser, sizeof(srecycleuser) - 1, "%s/uid:%u/%04u/%02u/%02u/%lu",
             mRecycleDir.c_str(),
             mOwnerUid,
             1900 + nowtm.tm_year,
             nowtm.tm_mon + 1,
             nowtm.tm_mday,
             index);
    struct stat buf;

    // if i_index is not -1, we just compute the path for the given index and return if it exists already
    if (i_index >= 0) {
      if (!gOFS->_stat(srecycleuser, &buf, error, rootvid, "")) {
        recyclepath = srecycleuser;
      } else {
        return gOFS->Emsg(epname, error, ENOENT, "stat index directory - "
                          "the computed index recycle directory does not exist");
      }

      return SFS_OK;
    }

    // check in case the index directory exists, that it has not more than 1M files,
    // otherwise increment the index by one
    if (!gOFS->_stat(srecycleuser, &buf, error, rootvid, "")) {
      if (buf.st_blksize > 100000) {
        index++;
        continue;
      }
    }

    // Verify/create group/user directory
    if (gOFS->_mkdir(srecycleuser, S_IRUSR | S_IXUSR | SFS_O_MKPTH, error, rootvid,
                     "")) {
      return gOFS->Emsg(epname, error, EIO, "remove existing file - the "
                        "recycle space user directory couldn't be created");
    }

    // Check the user recycle directory

    if (gOFS->_stat(srecycleuser, &buf, error, rootvid, "")) {
      return gOFS->Emsg(epname, error, EIO, "remove existing file - could not "
                        "determine ownership of the recycle space user directory",
                        srecycleuser);
    }

    // Check the ownership of the user directory
    if ((buf.st_uid != mOwnerUid) || (buf.st_gid != mOwnerGid)) {
      // Set the correct ownership
      if (gOFS->_chown(srecycleuser, mOwnerUid, mOwnerGid, error, rootvid, "")) {
        return gOFS->Emsg(epname, error, EIO, "remove existing file - could not "
                          "change ownership of the recycle space user directory",
                          srecycleuser);
      }
    }

    recyclepath = srecycleuser;
    return SFS_OK;
  } while (1);
}

EOSMGMNAMESPACE_END
