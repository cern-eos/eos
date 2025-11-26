//------------------------------------------------------------------------------
// File: Recycle.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

#include "mgm/recycle/Recycle.hh"
#include "mgm/recycle/RecyclePolicy.hh"
#include "common/Constants.hh"
#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/utils/BackOffInvoker.hh"
#include "common/RWMutex.hh"
#include "common/Path.hh"
#include "common/StringUtils.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Acl.hh"
#include "mgm/Quota.hh"
#include "mgm/QdbMaster.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "mgm/proc/user/AclCmd.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/interface/ContainerIterators.hh"
#include <XrdOuc/XrdOucErrInfo.hh>

namespace
{
//----------------------------------------------------------------------------
//! Check that all directories in the given path hierarchy contain the given
//! xattr key.
//!
//! @param path path hierarchy pointing to a directory
//! @param xattr_key extended attribute key
//! @param xattr_val extended attribute value
//!
//! @return true if successful, otherwise false
//----------------------------------------------------------------------------
bool AllHierarchyHasXattr(std::string_view path, std::string_view xattr_key,
                          std::string_view xattr_val)
{
  // Find all the directories that contain the given xattr
  XrdOucString lout;
  XrdOucErrInfo lerror;
  std::map<std::string, std::set<std::string>> found;

  // Get the total number of sub-dirs in the hierarchy
  if (gOFS->_find(path.data(), lerror, lout, Recycle::mRootVid,
                  found, nullptr, nullptr, true)) {
    eos_static_err("msg=\"failed computing number of sub-dirs in hierarchy\" "
                   "path=\"%s\"", path.data());
    return false;
  }

  uint64_t tree_num_dirs = found.size();
  found.clear();

  // Get the sub-dirs that contain the requested xattr key-value combination
  if (gOFS->_find(path.data(), lerror, lout, Recycle::mRootVid,
                  found, xattr_key.data(), xattr_val.data(), true)) {
    eos_static_err("msg=\"failed running find in hierarchy\" path=\"%s\"",
                   path.data());
    return false;
  }

  if (found.size() == tree_num_dirs) {
    return true;
  }

  return false;
}
}

EOSMGMNAMESPACE_BEGIN

// MgmOfsConfigure prepends the proc directory path e.g. the bin is
// /eos/<instance>/proc/recycle/
std::string Recycle::gRecyclingPrefix = "/recycle/";
std::string Recycle::gRecyclingAttribute = "sys.recycle";
std::string Recycle::gRecyclingTimeAttribute = "sys.recycle.keeptime";
std::string Recycle::gRecyclingKeepRatio = "sys.recycle.keepratio";
std::string Recycle::gRecyclingCollectInterval = "sys.recycle.collectinterval";
std::string Recycle::gRecyclingRemoveInterval = "sys.recycle.removeinterval";
std::string Recycle::gRecyclingDryRunAttribute = "sys.recycle.dryrun";
std::string Recycle::gRecyclingVersionKey = "sys.recycle.version.key";
std::string Recycle::gRecycleIdXattrKey = "sys.forced.recycleid";
std::string Recycle::gRecyclingPostFix = ".d";
eos::common::VirtualIdentity Recycle::mRootVid =
  eos::common::VirtualIdentity::Root();
std::chrono::seconds Recycle::mLastRemoveTs = std::chrono::seconds(0);

//------------------------------------------------------------------------------
// Default constructor
//------------------------------------------------------------------------------
Recycle::Recycle(bool fake_clock) :
  mPath(""), mRecycleDir(""), mRecyclePath(""), mOwnerUid(DAEMONUID),
  mOwnerGid(DAEMONGID), mId(0), mClock(fake_clock)
{}

//------------------------------------------------------------------------------
// Collect entries to recycle based on the current policy
//------------------------------------------------------------------------------
void
Recycle::CollectEntries(ThreadAssistant& assistant)
{
  auto now_ts = eos::common::SystemClock::SecondsSinceEpoch(mClock.GetTime());
  static std::chrono::seconds s_last_ts = now_ts;
  eos_static_debug("msg=\"recycle start collection\" ts=%llu", now_ts.count());

  // Run collection once every mCollectInterval
  if (now_ts - s_last_ts < mPolicy.mCollectInterval.load()) {
    eos_static_debug("msg=\"recycle skip collection\" last_ts=%llu "
                     "collect_interval_sec=%llu", s_last_ts.count(),
                     mPolicy.mCollectInterval.load().count());
    return;
  }

  // Clear the old list of deletions as we'll repopulate it
  mPendingDeletions.clear();
  s_last_ts = now_ts;
  int depth = 4;
  XrdOucErrInfo err_obj;
  XrdOucString err_msg;
  std::map<std::string, std::set<std::string>> find_map;
  // /eos/<instance>/proc/recycle/uid:<val>/year/month/day
  (void) gOFS->_find(Recycle::gRecyclingPrefix.c_str(),
                     err_obj, err_msg, mRootVid, find_map,
                     0, 0, true, 0, true, depth, nullptr, false,
                     false, nullptr, 0, 0, nullptr, &assistant);
  std::string cutoff_date = GetCutOffDate();
  eos_static_notice("msg=\"recycle find query\" cutoff_date=\"%s\"",
                    cutoff_date.c_str())

  for (auto it_dir = find_map.begin(); it_dir != find_map.end(); ++it_dir) {
    // Select all the directories with depth 8
    const std::string dir_path = it_dir->first;
    eos::common::Path cpath(dir_path);
    unsigned int path_levels = cpath.GetSubPathSize();

    if (path_levels == std::clamp(path_levels, 5u, 8u)) {
      bool exceeds_cutoff = false; // old directory to be removed
      bool top_dir = false; // top directory to be removed if empty

      if (path_levels == 8) {
        std::string dir_date = cpath.GetFullPath().c_str();
        dir_date.erase(0, strlen(cpath.GetSubPath(5)));
        eos_static_debug("dir_date=\"%s\" cutoff_date=\"%s\"",
                         dir_date.c_str(), cutoff_date.c_str());
        exceeds_cutoff = (cutoff_date.compare(dir_date) > 0);
      } else {
        top_dir = true;
      }

      // Select directories which are older than the cut off date
      if (exceeds_cutoff || top_dir) {
        try {
          eos::IContainerMDPtr cmd = gOFS->eosView->getContainer(dir_path);
          auto cmd_rd_lock = eos::MDLocking::readLock(cmd.get());

          // If no more children then add it to the list for deletion
          if (cmd->getNumContainers() == 0) {
            mPendingDeletions.emplace(cmd->getId(), dir_path);
          } else if (exceeds_cutoff) {
            // Otherwise add all the subcontainers used for sharding
            // .../year/month/day/[0,1,2, ... max_shard]/
            for (auto it = eos::ContainerMapIterator(cmd); it.valid(); it.next()) {
              std::string full_path = dir_path + it.key();
              mPendingDeletions.emplace(it.value(), full_path);
            }
          }
        } catch (const eos::MDException& e) {
          // skip missing directory
        }
      }
    }
  }

  if (EOS_LOGS_DEBUG) {
    for (const auto& pair : mPendingDeletions) {
      eos_static_debug("msg=\"recycle entry\" cxid=%08llx path=\"%s\"",
                       pair.first, pair.second.c_str());
    }
  }

  auto duration = std::chrono::duration_cast<std::chrono::seconds>
                  (eos::common::SystemClock::SecondsSinceEpoch(mClock.GetTime()) - now_ts);
  eos_static_notice("msg=\"recycle done collection\" num_entries=%llu "
                    "duration_sec=%llu", mPendingDeletions.size(),
                    duration.count());
}

//------------------------------------------------------------------------------
// Remove the pending deletions
//------------------------------------------------------------------------------
void
Recycle::RemoveEntries()
{
  auto now_ts = eos::common::SystemClock::SecondsSinceEpoch(mClock.GetTime());

  // Run removal every mRemoveInterval
  if (now_ts - mLastRemoveTs < mPolicy.mRemoveInterval.load()) {
    eos_static_debug("msg=\"recycle skip removal\" last_ts=%llu "
                     "removal_interval_sec=%llu", mLastRemoveTs.count(),
                     mPolicy.mRemoveInterval.load().count());
    return;
  }

  mLastRemoveTs = now_ts;

  if (mPendingDeletions.empty()) {
    return;
  }

  // Compute the index of the containers to be removed in the current slot
  int total_slots = mPolicy.mCollectInterval.load().count() /
                    mPolicy.mRemoveInterval.load().count();
  int current_slot = (now_ts.count() % mPolicy.mCollectInterval.load().count()) /
                     mPolicy.mRemoveInterval.load().count();

  // Catch all config in case the remove interval >= collect interval
  if (total_slots == 0) {
    total_slots = 1;
    current_slot = 0;
  }

  auto it = mPendingDeletions.begin();
  uint64_t count = 0;

  while (it != mPendingDeletions.end()) {
    // Keep ratio and watermarks already respected
    if ((count % 10 == 0) && mPolicy.IsWithinLimits()) {
      break;
    }

    ++count;
    // Decide if the current directory should be handled at this moment -
    // try to spread out the deletions throughout the day!
    eos::IContainerMD::id_t cid = it->first;

    if (cid % total_slots != current_slot) {
      eos_static_debug("msg=\"recycle skip directory removal\" cxid=%08llx"
                       " current_slot=%i slots=%i", cid, current_slot,
                       total_slots);
      ++it;
      continue;
    }

    if (mPolicy.mDryRun) {
      eos_static_info("msg=\"recycle skip removing entries in dry-run\" "
                      "cxid=%08llx", cid);
      ++it;
    } else {
      // Handle deletion
      RemoveSubtree(it->second);
      it = mPendingDeletions.erase(it);
    }
  }
}

//------------------------------------------------------------------------------
// Remove all the entries in the given subtree
//------------------------------------------------------------------------------
void
Recycle::RemoveSubtree(std::string_view dpath)
{
  std::map<std::string, std::set<std::string> > found;
  XrdOucString err_msg;
  XrdOucErrInfo lerror;

  if (gOFS->_find(dpath.data(), lerror, err_msg, mRootVid, found)) {
    eos_static_err("msg=\"failed doing find in subtree\" path=%s stderr=\"%s\"",
                   dpath.data(), err_msg.c_str());
  } else {
    // Delete files starting at the deepest level
    for (auto dit = found.rbegin(); dit != found.rend(); ++dit) {
      for (auto fit = dit->second.begin(); fit != dit->second.end(); ++fit) {
        const std::string fname = HandlePotentialSymlink(dit->first, *fit);
        eos_static_debug("orig_fname=\"%s\" new_fname=\"%s\"",
                         fit->c_str(), fname.c_str());
        std::string fpath = dit->first;
        fpath += fname;

        if (gOFS->_rem(fpath.c_str(), lerror, mRootVid, nullptr)) {
          eos_static_err("msg=\"unable to remove file\" path=%s", fpath.c_str());
        } else {
          eos_static_info("msg=\"permanently deleted file from recycle bin\" "
                          "path=%s", fpath.c_str());
        }
      }
    }

    // Delete directories starting at the deepest level
    for (auto dit = found.rbegin(); dit != found.rend(); ++dit) {
      eos_static_info("msg=\"handling directory\" path=%s", dit->first.c_str());
      std::string ldpath = dit->first.c_str();

      // Don't even try to delete the root directory or
      // something outside the recycle bin
      if ((ldpath == "/") || (ldpath.find(Recycle::gRecyclingPrefix) != 0)) {
        continue;
      }

      if (!gOFS->_remdir(ldpath.c_str(), lerror, mRootVid, (const char*) 0)) {
        eos_static_info("msg=\"permanently deleted directory from "
                        "recycle bin\" path=%s", ldpath.c_str());
      } else {
        eos_static_err("msg=\"unable to remove directory\" path=%s",
                       ldpath.c_str());
      }
    }

    // Delete parent directories if empty and still within the recycle bin.
    if (dpath.find(Recycle::gRecyclingPrefix) == 0) {
      eos_static_info("msg=\"delete parent directory\" path=%s", dpath.data());
      eos::common::Path cpath(std::string(dpath.data()));

      for (auto level = cpath.GetSubPathSize() - 1; level > 4; --level) {
        std::string sub_path = cpath.GetSubPath(level);

        if (!gOFS->_remdir(sub_path.c_str(), lerror, mRootVid, (const char*) 0)) {
          eos_static_info("msg=\"permanently deleted directory from "
                          "recycle bin\" path=%s", sub_path.c_str());
        } else {
          // Failed removal means directory is not empty so there is
          // no point in continuing.
          break;
        }
      }
    }
  }
}

//------------------------------------------------------------------------------
// Get cut-off date based on the configured retention policy with respect
// to the current timestamp.
//------------------------------------------------------------------------------
std::string
Recycle::GetCutOffDate()
{
  uint64_t now = eos::common::SystemClock::SecondsSinceEpoch(
                   mClock.GetTime()).count();
  // Add one extra day
  std::time_t cut_off_ts = now - mPolicy.mKeepTimeSec - 86400;
  auto tm = *std::localtime(&cut_off_ts);
  std::ostringstream oss;
  oss << std::put_time(&tm, "%Y/%m/%d");
  return oss.str();
}

//------------------------------------------------------------------------------
// Recycle method doing the clean-up
//------------------------------------------------------------------------------
void
Recycle::Recycler(ThreadAssistant& assistant) noexcept
{
  ThreadAssistant::setSelfThreadName("Recycler");
  eos_static_info("%s", "\"msg = \"recycle thread started\"");
  gOFS->WaitUntilNamespaceIsBooted(assistant);
  mLastRemoveTs = eos::common::SystemClock::SecondsSinceEpoch(mClock.GetTime());
  // Lambda computing the wait time for the configuration update condition
  // variable. We need to wait at most the remove interval time.
  auto getCvWaitFor = [&]() -> std::chrono::seconds {
    auto now_ts = eos::common::SystemClock::SecondsSinceEpoch(mClock.GetTime());
    std::chrono::seconds wait_for = now_ts - mLastRemoveTs;

    if (wait_for.count() > 5)
    {
      wait_for -= std::chrono::seconds(5);
    }

    return wait_for;
  };

  if (assistant.terminationRequested()) {
    return;
  }

  assistant.wait_for(std::chrono::seconds(10));
  eos::common::BackOffInvoker backoff_logger;
  mPolicy.Refresh(Recycle::gRecyclingPrefix);

  while (!assistant.terminationRequested()) {
    // Every now and then we wake up
    backoff_logger.invoke([this]() {
      eos_static_info("msg=\"recycle thread\" snooze-time=%llusec",
                      mPolicy.mRemoveInterval.load().count());
    });
    {
      // Wait for configuration update request or timeout, we don't care
      // about spurious wakeups.
      std::unique_lock<std::mutex> lock(mCvMutex);
      bool do_refresh = mCvCfgUpdate.wait_for(lock, getCvWaitFor(),
                                              [&] {return mTriggerRefresh;});

      if (do_refresh) {
        mTriggerRefresh = false;
        mPolicy.Refresh(Recycle::gRecyclingPrefix);
      }
    }

    if (!gOFS->mMaster->IsMaster() || (mPolicy.mEnforced == false)) {
      continue;
    }

    if (mPolicy.mSpaceKeepRatio) {
      mPolicy.RefreshWatermarks();
    }

    if (mPolicy.mKeepTimeSec && !mPolicy.IsWithinLimits()) {
      CollectEntries(assistant);
      RemoveEntries();
    }
  }

  eos_static_info("%s", "msg=\"recycler thread exiting\"");
}

//------------------------------------------------------------------------------
// Print the recycle bin contents
//------------------------------------------------------------------------------
int
Recycle::Print(std::string& std_out, std::string& std_err,
               eos::common::VirtualIdentity& vid, bool monitoring,
               bool translateids, bool details,
               std::string_view display_type,
               std::string_view display_val,
               std::string_view date, Recycle::RecycleListing* rvec,
               bool whodeleted, int32_t maxentries)
{
  using namespace eos::common;
  XrdOucString uids;
  XrdOucString gids;
  std::map<std::string, bool> printmap;
  std::ostringstream oss_out;

  // Sanitize user input
  if (!date.empty()) {
    for (const auto& ch : date) {
      if (!isdigit(ch) && (ch != '/')) {
        std_err = "error: invalid date format";
        return EINVAL;
      }
    }
  }

  if ((display_type == "all") &&
      ((!vid.uid) ||
       (vid.hasUid(eos::common::ADM_UID)) ||
       (vid.hasGid(eos::common::ADM_GID)))) {
    // Add everything found in the recycle directory structure to the printmap
    std::string subdirs;
    XrdMgmOfsDirectory dirl;
    int listrc = dirl.open(Recycle::gRecyclingPrefix.c_str(), mRootVid,
                           (const char*) 0);

    if (listrc) {
      eos_static_err("msg=\"unable to list the garbage directory level-1\" "
                     "recycle-path=%s", Recycle::gRecyclingPrefix.c_str());
    } else {
      // loop over all directories = group directories
      const char* dname1;

      while ((dname1 = dirl.nextEntry())) {
        std::string sdname = dname1;

        if ((sdname == ".") || (sdname == "..")) {
          continue;
        }

        if ((sdname.substr(0, 4) == "uid:") ||
            (sdname.substr(0, 4) == "rid:")) {
          printmap[sdname] = true;
        }
      }

      dirl.close();
    }
  } else if (display_type == "rid") {
    // Check that a recycle id value was given
    if (display_val.empty()) {
      std_err = "error: no recycle id value given";
      return EINVAL;
    }

    // Basic checks about the recyle id
    try {
      (void) std::stoull(display_val.data());
    } catch (...) {
      std_err = "error: recycle id must be numeric";
      return EINVAL;
    }

    // Add only the requested recycle id
    printmap[SSTR("rid:" << display_val.data())] = true;
  } else {
    // Add only the virtual user to the printmap
    printmap[SSTR("uid:" << vid.uid)] = true;
  }

  eos::common::Path dPath(std::string("/") + date.data());

  if (details) {
    size_t count = 0;

    for (auto it = printmap.begin(); it != printmap.end(); it++) {
      char sdir[4096];
      snprintf(sdir, sizeof(sdir) - 1, "%s/%s/%s",
               Recycle::gRecyclingPrefix.c_str(),
               it->first.c_str(), date.data());
      int depth = 5 ;

      if (dPath.GetSubPathSize()) {
        if (depth > (int) dPath.GetSubPathSize()) {
          depth -= dPath.GetSubPathSize();
        }
      }

      XrdOucString err_msg;
      XrdOucErrInfo lerror;
      std::map<std::string, std::set < std::string>> find_map;
      int retc = gOFS->_find(sdir, lerror, err_msg, mRootVid, find_map,
                             0, 0, false, 0, true, depth);

      if (retc) {
        if (errno != ENOENT) {
          std_err = err_msg.c_str();
          eos_static_err("msg=\"failed find command\" dir=\"%s\"", sdir);
        } else {
          continue;
        }
      }

      for (auto it_dir = find_map.begin(); it_dir != find_map.end(); ++it_dir) {
        XrdOucString dirname = it_dir->first.c_str();

        if (dirname.endswith(".d/")) {
          dirname.erase(dirname.length() - 1);
          eos::common::Path cpath(dirname.c_str());
          dirname = cpath.GetParentPath();
          it_dir->second.insert(cpath.GetName());
        }

        eos_static_debug("dir=%s", it_dir->first.c_str());

        for (auto it_file = it_dir->second.begin();
             it_file != it_dir->second.end(); ++it_file) {
          if (maxentries && (count >= (size_t)maxentries)) {
            retc = E2BIG;
            std_out += oss_out.str();
            return E2BIG;;
          }

          const std::string fname = HandlePotentialSymlink(dirname.c_str(), *it_file);
          eos_static_debug("orig_fname=\"%s\" new_fname=\"%s\"",
                           it_file->c_str(), fname.c_str());

          if ((fname != "/") && (fname.find('#') != 0)) {
            eos_static_debug("msg=\"skip unexpected entry\" fname=\"%s\"",
                             fname.c_str());
            continue;
          }

          std::string fullpath = dirname.c_str();
          fullpath += fname;
          XrdOucString originode;
          XrdOucString origpath = fname.c_str();

          // Demangle the original pathname
          while (origpath.replace("#:#", "/")) {
          }

          XrdOucErrInfo error;
          XrdOucString type = "file";
          std::string deleter;
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

            if (whodeleted) {
              if (!gOFS->_attr_get(fullpath.c_str(), error, vid, "",
                                   eos::common::EOS_DTRACE_ATTR,
                                   deleter)) {
              } else {
                deleter = "{}";
              }
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
                      << " dtrace=\"" << deleter.c_str() << "\""
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
                rmap["dtrace"] = deleter.c_str();
                rvec->push_back(rmap);
              }
            } else {
              char sline[4096];

              if (count == 0) {
                // print a header
                snprintf(sline, sizeof(sline) - 1,
                         "# %-24s %-8s %-8s %-12s %-13s %-21s %-64s %-32s\n", "Deletion Time", "UID",
                         "GID",
                         "SIZE", "TYPE", "RESTORE-KEY", "RESTORE-PATH", "DTRACE");
                oss_out << sline
                        << "# ================================================"
                        << "=================================================="
                        << "========================================================="
                        << "============================="
                        << std::endl;
              }

              char tdeltime[4096];
              std::string deltime = ctime_r(&buf.st_ctime, tdeltime);
              deltime.erase(deltime.length() - 1);
              snprintf(sline, sizeof(sline) - 1,
                       "%-26s %-8s %-8s %-12s %-13s %-16s %-64s %-32s",
                       deltime.c_str(), uids.c_str(), gids.c_str(),
                       StringConversion::GetSizeString((unsigned long long) buf.st_size).c_str(),
                       type.c_str(), originode.c_str(), origpath.c_str(), deleter.c_str());

              if (oss_out.tellp() > 1 * 1024 * 1024 * 1024) {
                retc = E2BIG;
                oss_out << "... (truncated after 1G of output)" << std::endl;
                std_out += oss_out.str();
                std_err += "warning: list too long - truncated after 1GB of output!\n";
                return E2BIG;
              }

              oss_out << sline << std::endl;
            }

            count++;

            if ((vid.uid) && (!vid.sudoer) && (count > 100000)) {
              retc = E2BIG;
              oss_out << "... (truncated)" << std::endl;
              std_out += oss_out.str();
              std_err += "warning: list too long - truncated after 100000 entries!\n";
              return E2BIG;
            }
          }
        }
      }
    }
  } else {
    auto map_quotas = Quota::GetGroupStatistics(Recycle::gRecyclingPrefix,
                      Quota::gProjectId);

    if (!map_quotas.empty()) {
      unsigned long long used_bytes = map_quotas[SpaceQuota::kGroupLogicalBytesIs];
      unsigned long long max_bytes = map_quotas[SpaceQuota::kGroupLogicalBytesTarget];
      unsigned long long used_inodes = map_quotas[SpaceQuota::kGroupFilesIs];
      unsigned long long max_inodes = map_quotas[SpaceQuota::kGroupFilesTarget];
      char sline[4096];
      eos::IContainerMD::XAttrMap attrmap;
      XrdOucErrInfo error;

      // Check if this path has a recycle attribute
      if (gOFS->_attr_ls(Recycle::gRecyclingPrefix.c_str(), error, mRootVid, "",
                         attrmap)) {
        eos_static_err("msg=\"unable to get attribute on recycle path\" "
                       "recycle-path=%s", Recycle::gRecyclingPrefix.c_str());
      }

      if (!monitoring) {
        oss_out << "# _________________________________________________________"
                << "___________________________________________________________"
                << "___________________________" << std::endl;
        snprintf(sline, sizeof(sline) - 1, "# used %s out of %s (%.02f%% volume) "
                 "used %llu out of %llu (%.02f%% inodes used) Object-Lifetime %s [s] Keep-Ratio %s",
                 StringConversion::GetReadableSizeString(used_bytes, "B").c_str(),
                 StringConversion::GetReadableSizeString(max_bytes, "B").c_str(),
                 used_bytes * 100.0 / max_bytes,
                 used_inodes, max_inodes, used_inodes * 100.0 / max_inodes,
                 attrmap.count(Recycle::gRecyclingTimeAttribute) ?
                 attrmap[Recycle::gRecyclingTimeAttribute].c_str() : "not configured",
                 attrmap.count(Recycle::gRecyclingKeepRatio) ?
                 attrmap[Recycle::gRecyclingKeepRatio].c_str() : "not configured");
        oss_out << sline << std::endl
                << "# _________________________________________________________"
                << "___________________________________________________________"
                << "___________________________" << std::endl;
      } else {
        snprintf(sline, sizeof(sline) - 1, "recycle-bin=%s usedbytes=%llu "
                 "maxbytes=%llu volumeusage=%.02f%% usedinodes=%llu "
                 "maxinodes=%llu inodeusage=%.02f%% lifetime=%s ratio=%s",
                 Recycle::gRecyclingPrefix.c_str(),
                 used_bytes, max_bytes, used_bytes * 100.0 / max_bytes,
                 used_inodes, max_inodes, used_inodes * 100.0 / max_inodes,
                 attrmap.count(Recycle::gRecyclingTimeAttribute) ?
                 attrmap[Recycle::gRecyclingTimeAttribute].c_str() : "-1",
                 attrmap.count(Recycle::gRecyclingKeepRatio) ?
                 attrmap[Recycle::gRecyclingKeepRatio].c_str() : "-1");
        oss_out << sline << std::endl;
      }
    }
  }

  std_out += oss_out.str();
  return 0;
}

//------------------------------------------------------------------------------
// Check if client is allowed to restore the given recyle path
//------------------------------------------------------------------------------
int
Recycle::IsAllowedToRestore(std::string_view recycle_path,
                            const eos::common::VirtualIdentity& vid)
{
  static const std::string sUserRecyclePrefix =
    SSTR(Recycle::gRecyclingPrefix << "uid:");
  eos_static_debug("msg=\"attempt file restore\" path=\"%s\"", recycle_path);

  // Root is allowed to restore anything
  if (vid.uid == 0) {
    return 0;
  }

  // If this is a user area then restore is allowed only for the owner
  if (recycle_path.find(sUserRecyclePrefix) == 0) {
    std::string usr_recycle = SSTR(sUserRecyclePrefix << vid.uid);

    if (recycle_path.find(usr_recycle) != 0) {
      return EPERM;
    }
  }

  struct stat buf;

  XrdOucErrInfo lerror;

  if (gOFS->_stat(recycle_path.data(), &buf, lerror,
                  mRootVid, "", nullptr, false)) {
    return EIO;
  }

  // If not owner of the recycle entry
  if (vid.uid != buf.st_uid) {
    // Evalue the parent ACLs for right to read
    eos::common::Path cpath(recycle_path.data());
    std::string parent_dir = cpath.GetParentPath();

    try {
      auto cmd = gOFS->eosView->getContainer(parent_dir);
      auto cmd_rlock = eos::MDLocking::readLock(cmd.get());
      auto xattrs = cmd->getAttributes();
      Acl acl(xattrs, vid);

      if (acl.CanRead()) {
        return 0;
      }

      return EPERM;
    } catch (const eos::MDException& e) {
      eos_static_err("msg=\"missing parent directory for restore check\" "
                     "path=\"%s\"", parent_dir.c_str());
      return ENOENT;
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// Get recycle bin path from the given restore key information
//------------------------------------------------------------------------------
int
Recycle::GetPathFromRestoreKey(std::string_view key,
                               const eos::common::VirtualIdentity& vid,
                               std::string& std_err, std::string& recycle_path)
{
  if (key.empty()) {
    std_err += "error: invalid argument as recycle key";
    return EINVAL;
  }

  bool force_file = false;
  bool force_dir = false;
  std::string skey(key);

  if (skey.find("fxid:") == 0) {
    skey.erase(0, 5);
    force_file = true;
  } else if (skey.find("pxid:") == 0) {
    skey.erase(0, 5);
    force_dir = true;
  } else {
    std_err = "error: unknow recycle key format";
    return EINVAL;
  }

  // Make sure the value is a hex
  unsigned long long id = 0ull;

  try {
    id = std::stoull(skey, 0, 16);
  } catch (...) {
    std_err = "error: recycle key must containe a hex value";
    return EINVAL;
  }

  // Full path of the target inside the recycle bin
  {
    std::shared_ptr<eos::IFileMD> fmd;

    if (!force_dir) {
      try {
        eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, id);
        auto fmd = gOFS->eosFileService->getFileMD(id);
        recycle_path = gOFS->eosView->getUri(fmd.get());
      } catch (const eos::MDException& e) {
        // empty
      }
    }

    if (!force_file && !fmd) {
      try {
        eos::Prefetcher::prefetchContainerMDWithParentsAndWait(gOFS->eosView, id);
        auto cmd = gOFS->eosDirectoryService->getContainerMD(id);
        recycle_path = gOFS->eosView->getUri(cmd.get());
      } catch (const eos::MDException& e) {
        // empty
      }
    }

    if (recycle_path.empty()) {
      std_err = SSTR("error: cannot find object referenced by recycle-key="
                     << key);
      return ENOENT;
    }
  }

  // Check that this is a path to recycle
  if (recycle_path.find(Recycle::gRecyclingPrefix.c_str()) != 0) {
    std_err = "error: referenced object is not in the recycle bin";
    return EINVAL;
  }

  return 0;
}

//----------------------------------------------------------------------------
// Demangle path from recycle bin to obtain the original path
//----------------------------------------------------------------------------
std::string
Recycle::DemanglePath(std::string_view recycle_path)
{
  std::string orig_path(recycle_path);

  // This should not contain any '/'
  if (orig_path.find('/') != std::string::npos) {
    return std::string();
  }

  eos::common::replace_all(orig_path, "#.#", "/");

  if (eos::common::endsWith(orig_path, Recycle::gRecyclingPostFix)) {
    orig_path.erase(orig_path.length() - Recycle::gRecyclingPostFix.length() -
                    16 - 1);
  } else {
    orig_path.erase(orig_path.length() - 16 - 1);
  }

  return orig_path;
}

//------------------------------------------------------------------------------
// Restore an entry from the recycle bin to the original location
//------------------------------------------------------------------------------
int
Recycle::Restore(std::string& std_out, std::string& std_err,
                 eos::common::VirtualIdentity& vid, std::string_view key,
                 bool force_orig_name, bool restore_versions, bool make_path)
{
  std::string recycle_path;
  int retc = GetPathFromRestoreKey(key, vid, std_err, recycle_path);

  if (retc) {
    return retc;
  }

  // Check if client is allowed to restore the given entry
  retc = IsAllowedToRestore(recycle_path, vid);

  if (retc) {
    std_err = "error: client not allowed to restore given path";
    return retc;
  }

  // Reconstruct original file name
  eos::common::Path cPath(recycle_path.c_str());
  std::string orig_path = DemanglePath(cPath.GetName());

  if (orig_path.empty()) {
    std_err = "error: failed to demangle recycle path";
    return EINVAL;
  }

  eos::common::Path oPath(orig_path.c_str());
  XrdOucErrInfo lerror;
  struct stat buf;

  // Check if original parent path exists
  if (gOFS->_stat(oPath.GetParentPath(), &buf, lerror, mRootVid, "")) {
    if (make_path) {
      ProcCommand cmd;
      XrdOucString info = "mgm.cmd=mkdir&mgm.option=p&mgm.path=";
      info += oPath.GetParentPath();
      cmd.open("/proc/user", info.c_str(), vid, &lerror);
      cmd.close();
      int rc = cmd.GetRetc();

      if (rc) {
        std_err = SSTR("error: creation failed: " << cmd.GetStdErr());
        return rc;
      }
    } else {
      std_err = SSTR("error: you have to recreate the restore directory path="
                     << oPath.GetParentPath()
                     << " to be able to restore this file/tree\n"
                     << "hint: retry after creating the mentioned directory");
      return ENOENT;
    }
  }

  // Check if original path exists
  if (!gOFS->_stat(oPath.GetPath(), &buf, lerror, mRootVid, "", nullptr, false)) {
    if (force_orig_name == false) {
      std_err = "error: the original path already exists, use "
                "'-f|--force-original-name' to put the deleted file/tree\n"
                " back and rename the file/tree in place to <name>.<inode>";
      return EEXIST;
    } else {
      char sp[256];
      snprintf(sp, sizeof(sp) - 1, "%016llx",
               (unsigned long long)(S_ISDIR(buf.st_mode) ? buf.st_ino :
                                    eos::common::FileId::InodeToFid(buf.st_ino)));
      std::string newold = SSTR(oPath.GetPath() << "." << sp);

      if (gOFS->_rename(oPath.GetPath(), newold.c_str(), lerror, mRootVid, "", "",
                        true, true)) {
        std_err = SSTR("error: failed to rename the existing file/tree where we "
                       "need to restore path=" << oPath.GetPath() << std::endl
                       << lerror.getErrText());
        return EIO;
      } else {
        std_out = SSTR("warning: renamed restore path=" << oPath.GetPath()
                       << " to backup-path=" << newold.c_str());
      }
    }
  }

  // Do the 'undelete' aka rename
  if (gOFS->_rename(cPath.GetPath(), oPath.GetPath(), lerror, mRootVid,
                    "", "", true)) {
    std_err = SSTR("error: failed to undelete path=" << oPath.GetPath());
    return EIO;
  } else {
    std_out = SSTR("success: restored path=" << oPath.GetPath());
  }

  // Skip version restore if not requested
  if (restore_versions == false) {
    return 0;
  }

  // Attempt to restore versions
  std::string vkey;

  if (gOFS->_attr_get(oPath.GetPath(), lerror, mRootVid, "",
                      Recycle::gRecyclingVersionKey.c_str(), vkey)) {
    // No version directory to restore
    return 0;
  }

  retc = Restore(std_out, std_err, vid, vkey.c_str(),
                 force_orig_name, restore_versions);

  // Mask an non existent version reference
  if (retc == ENOENT) {
    return 0;
  }

  return retc;
}

//------------------------------------------------------------------------------
// Purge files in the recycle bin
//------------------------------------------------------------------------------
int
Recycle::Purge(std::string& std_out, std::string& std_err,
               eos::common::VirtualIdentity& vid,
               std::string key, std::string_view date,
               std::string_view type, std::string_view recycle_id)
{
  // Basic permission checks
  if (vid.uid && !vid.sudoer &&
      !(vid.hasUid(eos::common::ADM_UID)) &&
      !(vid.hasGid(eos::common::ADM_GID))) {
    std_err = "error: you cannot purge your recycle bin without being a "
              "sudo or having an admin role";
    return EPERM;
  }

  if (!key.empty() && !date.empty()) {
    std_err = "error: recycle key and date can not be used together";
    return EINVAL;
  }

  // Sanitize user input
  if (!date.empty()) {
    for (const auto& ch : date) {
      if (!isdigit(ch) && (ch != '/')) {
        std_err = "error: invalid date format";
        return EINVAL;
      }
    }
  }

  // Path that needs to be purged
  std::string recycle_path;

  if (!key.empty()) {
    int retc = GetPathFromRestoreKey(key, vid, std_err, recycle_path);

    if (retc) {
      return retc;
    }
  } else if (!date.empty()) {
    char sdir[4096];

    if ((type == "all") && (vid.uid == 0)) {
      snprintf(sdir, sizeof(sdir) - 1, "%s/", Recycle::gRecyclingPrefix.c_str());
    } else if ((type == "rid") && !recycle_id.empty()) {
      snprintf(sdir, sizeof(sdir) - 1, "%s/rid:%s/%s",
               Recycle::gRecyclingPrefix.c_str(),
               recycle_id.data(), date.data());
    } else {
      snprintf(sdir, sizeof(sdir) - 1, "%s/uid:%u/%s",
               Recycle::gRecyclingPrefix.c_str(),
               (unsigned int) vid.uid, date.data());
    }

    recycle_path = sdir;
  }

  // Make sure the path to purge is inside the recycle bine
  if (recycle_path.find(Recycle::gRecyclingPrefix) != 0) {
    std_err = SSTR("error: purge path is " << recycle_path
                   << " not in the recyle bin ");
    return EINVAL;
  }

  // Determine if we need to remove a subtree or a file
  struct stat buf;
  XrdOucErrInfo lerror;

  if (gOFS->_stat(recycle_path.c_str(), &buf, lerror, mRootVid, "",
                  nullptr, false)) {
    std_err = SSTR("error: recycle path " << recycle_path
                   << " does not exist");
    return ENOENT;
  }

  if (S_ISDIR(buf.st_mode)) {
    RemoveSubtree(recycle_path.data());
  } else {
    (void) gOFS->_rem(recycle_path.c_str(), lerror, mRootVid, nullptr);
  }

  std_out = SSTR("success: purged path " << recycle_path
                 << " from recycle bin!");
  return 0;
}

//------------------------------------------------------------------------------
// Configure the recycle bin parameters
//------------------------------------------------------------------------------
int
Recycle::Config(std::string& std_out, std::string& std_err,
                eos::common::VirtualIdentity& vid,
                eos::console::RecycleProto_ConfigProto_OpType op,
                const std::string& value)
{
  XrdOucErrInfo lerror;

  if (vid.uid != 0) {
    std_err = "error: you need to be root to configure the recycle bin"
              " and/or recycle polcies\n";
    return EPERM;
  }

  if (op == eos::console::RecycleProto_ConfigProto::ADD_BIN) {
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
    int result = Cmd.open("/proc/user", info.c_str(), mRootVid, &lerror);
    Cmd.AddOutput(std_out, std_err);
    Cmd.close();
    return result;
  }

  if (op == eos::console::RecycleProto_ConfigProto::RM_BIN) {
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
    int result = Cmd.open("/proc/user", info.c_str(), mRootVid, &lerror);
    Cmd.AddOutput(std_out, std_err);
    Cmd.close();
    return result;
  }

  if (op == eos::console::RecycleProto_ConfigProto::LIFETIME) {
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
                        lerror, mRootVid, "",
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
  } else if (op == eos::console::RecycleProto_ConfigProto::RATIO) {
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
                        lerror, mRootVid, "",
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
  } else if (op == eos::console::RecycleProto_ConfigProto::COLLECT_INTERVAL) {
    if (value.empty()) {
      std_err = "error: missing collect interval value\n";
      return EINVAL;
    }

    try {
      uint64_t collect_interval = std::stoull(value);

      // Make sure the collect interval is never less than 10 sec
      if (collect_interval < 10) {
        std_err = "error: recycle collect interval has to be > 10";
        return EINVAL;
      }
    } catch (...) {
      std_err = "error: recycle collect interval not numeric";
      return EINVAL;
    }

    if (gOFS->_attr_set(Recycle::gRecyclingPrefix.c_str(),
                        lerror, mRootVid, "",
                        Recycle::gRecyclingCollectInterval.c_str(),
                        value.c_str())) {
      std_err = "error: failed to set extended attribute '";
      std_err += Recycle::gRecyclingCollectInterval.c_str();
      std_err += "'";
      std_err += " at '";
      std_err += Recycle::gRecyclingPrefix.c_str();
      std_err += "'";
      return EIO;
    } else {
      std_out += "success: recycle bin update collect interval";
    }
  } else if (op == eos::console::RecycleProto_ConfigProto::REMOVE_INTERVAL) {
    if (value.empty()) {
      std_err = "error: missing remove interval value\n";
      return EINVAL;
    }

    try {
      uint64_t remove_interval = std::stoull(value);

      // Make sure the collect interval is never less than 10 sec
      if (remove_interval < 10) {
        std_err = "error: recycle remove interval has to be > 10";
        return EINVAL;
      }

      if (remove_interval >= gOFS->mRecycler->GetCollectInterval()) {
        std_err = "erorr: remove interval needs to be smaller than "
                  "the collect interval";
        return EINVAL;
      }
    } catch (...) {
      std_err = "error: recycle remove interval not numeric";
      return EINVAL;
    }

    if (gOFS->_attr_set(Recycle::gRecyclingPrefix.c_str(),
                        lerror, mRootVid, "",
                        Recycle::gRecyclingRemoveInterval.c_str(),
                        value.c_str())) {
      std_err = "error: failed to set extended attribute '";
      std_err += Recycle::gRecyclingRemoveInterval.c_str();
      std_err += "'";
      std_err += " at '";
      std_err += Recycle::gRecyclingPrefix.c_str();
      std_err += "'";
      return EIO;
    } else {
      std_out += "success: recycle bin update remove interval";
    }
  } else if (op == eos::console::RecycleProto_ConfigProto::DRY_RUN) {
    if (value.empty() || ((value != "yes") && (value != "no"))) {
      std_err = "error: missing/wrong dry-run value\n";
      return EINVAL;
    }

    if (gOFS->_attr_set(Recycle::gRecyclingPrefix.c_str(),
                        lerror, mRootVid, "",
                        Recycle::gRecyclingDryRunAttribute.c_str(),
                        value.c_str())) {
      std_err = "error: failed to set extended attribute '";
      std_err += Recycle::gRecyclingDryRunAttribute.c_str();
      std_err += "'";
      std_err += " at '";
      std_err += Recycle::gRecyclingPrefix.c_str();
      std_err += "'";
      return EIO;
    } else {
      std_out += "success: recycle bin update dry-run option";
    }
  } else {
    std_err = "error: unknown configuration operation";
    return EINVAL;
  }

  gOFS->mRecycler->NotifyConfigUpdate();
  return 0;
}

//------------------------------------------------------------------------------
// Configure a recycle id for the given path
//------------------------------------------------------------------------------
int
Recycle::RecycleIdSetup(std::string_view path, std::string_view acl,
                        std::string& std_err)
{
  // Check that the given path exist and save the container id
  eos::ContainerIdentifier cid {0ull};
  std::string recycle_id_val;

  try {
    auto cmd = gOFS->eosView->getContainer(path.data());
    auto cmd_lock = eos::MDLocking::readLock(cmd.get());
    cid = cmd->getIdentifier();

    if (cmd->hasAttribute(Recycle::gRecycleIdXattrKey)) {
      recycle_id_val = cmd->getAttribute(Recycle::gRecycleIdXattrKey);
    }
  } catch (eos::MDException& e) {
    std_err = SSTR("error: path does not exist " << path.data()
                   << " msg=" << e.what()).c_str();
    return ENOENT;
  }

  // Don't allow if path is inside the "proc" hierarchy
  if (path.find(gOFS->MgmProcPath.c_str()) == 0) {
    std_err = "error: path can not be inside the proc hierarchy";
    return EPERM;
  }

  // If there is already a recycle id attribute then skip applying it
  if (recycle_id_val.empty()) {
    recycle_id_val = std::to_string(cid.getUnderlyingUInt64());
  }

  // Create the recycle directory for the given recycle id
  XrdOucErrInfo lerror;
  std::string proj_recycle_path = Recycle::gRecyclingPrefix + "rid:" +
                                  recycle_id_val;

  // @todo(esindril) consider either to avoid inheriting sys.recycle xattrs or
  // to move the Recycler configuration inside the EOS config and drop the
  // configuration via xattrs!
  if (gOFS->_mkdir(proj_recycle_path.c_str(), S_IRUSR | S_IXUSR | SFS_O_MKPTH,
                   lerror, mRootVid, "")) {
    std_err = "error: failed to create recycle project directory";
    return EINVAL;
  }

  // Add requested ACLs to the recycle bin top project directory
  if (!acl.empty()) {
    eos::console::RequestProto req;
    eos::console::AclProto* acl_req = req.mutable_acl();
    acl_req->set_recursive(true);
    acl_req->set_sys_acl(true);
    acl_req->set_op(eos::console::AclProto::MODIFY);
    acl_req->set_rule(acl.data());
    acl_req->set_path(proj_recycle_path);
    eos::mgm::AclCmd acl_cmd(std::move(req), mRootVid);
    eos::console::ReplyProto reply = acl_cmd.ProcessRequest();

    if (reply.retc()) {
      std_err = reply.std_err();
      return reply.retc();
    }
  }

  // Apply recursively the sys.forced.recycleid=<cid_val> to the entire
  // original subtree and make sure to sub-dir was skipped
  unsigned int attempts = 5;

  do {
    XrdOucString lerr;
    bool exclusive = false;
    std::map<std::string, std::set<std::string>> found;

    if (gOFS->_find(path.data(), lerror, lerr, mRootVid, found, nullptr,
                    nullptr, true)) {
      std_err = "error: failed to search in given path";
      return errno;
    }

    for (auto find_it = found.cbegin(); find_it != found.cend(); ++find_it) {
      if (gOFS->_attr_set(find_it->first.c_str(), lerror, mRootVid,
                          (const char*) 0, Recycle::gRecycleIdXattrKey.c_str(),
                          recycle_id_val.c_str(), exclusive)) {
        std_err = "error: failed to set xattr on path ";
        std_err += find_it->first;
        return errno;
      }
    }
  } while ((--attempts > 0) &&
           !AllHierarchyHasXattr(path, Recycle::gRecycleIdXattrKey,
                                 recycle_id_val));

  if (attempts == 0) {
    std_err = "error: failed to propagate sys.forced.recycleid in the hiearchy ";
    std_err += path;
    return EINVAL;
  }

  return 0;
}

//----------------------------------------------------------------------------
// Handle symlink or symlink like file names during recycle operations
//----------------------------------------------------------------------------
std::string
Recycle::HandlePotentialSymlink(const std::string& ppath,
                                const std::string& fn)
{
  size_t pos = fn.find(" -> ");

  if (pos == std::string::npos) {
    return fn;
  }

  // Check if this file name actually exists
  std::string fpath = ppath + fn;
  struct stat buf;
  XrdOucErrInfo lerror;

  if (gOFS->_stat(fpath.c_str(), &buf, lerror, mRootVid,
                  "", nullptr, false) == SFS_OK) {
    return fn;
  }

  // This means we are dealing with a symlink file so we need to remove the
  // target from the filename so that we actually work with it
  return fn.substr(0, pos);
}

EOSMGMNAMESPACE_END
