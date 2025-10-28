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
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Quota.hh"
#include "mgm/QdbMaster.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/interface/ContainerIterators.hh"
#include <XrdOuc/XrdOucErrInfo.hh>

EOSMGMNAMESPACE_BEGIN

// MgmOfsConfigure prepends the proc directory path e.g. the bin is
// /eos/<instance/proc/recycle/
std::string Recycle::gRecyclingPrefix = "/recycle/";
std::string Recycle::gRecyclingAttribute = "sys.recycle";
std::string Recycle::gRecyclingTimeAttribute = "sys.recycle.keeptime";
std::string Recycle::gRecyclingKeepRatio = "sys.recycle.keepratio";
std::string Recycle::gRecyclingPollAttribute = "sys.recycle.pollinterval";
std::string Recycle::gRecyclingCollectInterval = "sys.recycle.collectinterval";
std::string Recycle::gRecyclingRemoveInterval = "sys.recycle.removeinterval";
std::string Recycle::gRecyclingDryRunAttribute = "sys.recycle.dryrun";
std::string Recycle::gRecyclingVersionKey = "sys.recycle.version.key";
std::string Recycle::gRecyclingPostFix = ".d";
eos::common::VirtualIdentity Recycle::mRootVid =
  eos::common::VirtualIdentity::Root();

//------------------------------------------------------------------------------
// Default constructor
//------------------------------------------------------------------------------
Recycle::Recycle(bool fake_clock) :
  mPath(""), mRecycleDir(""), mRecyclePath(""),
  mOwnerUid(DAEMONUID), mOwnerGid(DAEMONGID), mId(0), mWakeUp(false),
  mClock(fake_clock)
{}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Recycle::Recycle(const char* path, const char* recycledir,
                 eos::common::VirtualIdentity* vid, uid_t ownerUid,
                 gid_t ownerGid, unsigned long long id, bool fake_clock) :
  mPath(path), mRecycleDir(recycledir), mRecyclePath(""),
  mOwnerUid(ownerUid), mOwnerGid(ownerGid), mId(id), mWakeUp(false),
  mClock(fake_clock)
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
  if (now_ts - s_last_ts < mPolicy.mCollectInterval) {
    eos_static_debug("msg=\"recycle skip collection\" last_ts=%llu "
                     "collect_interval_sec=%llu", s_last_ts.count(),
                     mPolicy.mCollectInterval.count());
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
  static std::chrono::seconds s_last_ts = now_ts;

  // Run removal every mRemoveInterval
  if (now_ts - s_last_ts < mPolicy.mRemoveInterval) {
    eos_static_debug("msg=\"recycle skip removal\" last_ts=%llu "
                     "removal_interval_sec=%llu", s_last_ts.count(),
                     mPolicy.mRemoveInterval.count());
    return;
  }

  s_last_ts = now_ts;

  if (mPendingDeletions.empty()) {
    return;
  }

  // Compute the index of the containers to be removed in the current slot
  int total_slots = mPolicy.mCollectInterval.count() /
                    mPolicy.mRemoveInterval.count();
  int current_slot = (now_ts.count() % mPolicy.mCollectInterval.count()) /
                     mPolicy.mRemoveInterval.count();

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

        if (gOFS->_rem(fpath.c_str(), lerror, mRootVid, (const char*) 0)) {
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
                      mPolicy.mPollInterval.count());
    });

    for (int i = 0; i <= mPolicy.mPollInterval.count() / 10; i++) {
      if (assistant.terminationRequested()) {
        return;
      }

      assistant.wait_for(std::chrono::seconds(10));

      if (mWakeUp) {
        mPolicy.Refresh(Recycle::gRecyclingPrefix);
        mWakeUp = false;
        break;
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
// Recycle the given object (file or subtree)
//------------------------------------------------------------------------------
int
Recycle::ToGarbage(const char* epname, XrdOucErrInfo& error, bool fusexcast)
{
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
  if (gOFS->_rename(mPath.c_str(), srecyclepath, error, mRootVid, "", "", true,
                    true, false, fusexcast)) {
    return gOFS->Emsg(epname, error, EIO, "rename file/directory", srecyclepath);
  }

  // store the recycle path in the error object
  error.setErrInfo(0, srecyclepath);
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Print the recycle bin contents
//------------------------------------------------------------------------------
int
Recycle::Print(std::string& std_out, std::string& std_err,
               eos::common::VirtualIdentity& vid, bool monitoring,
               bool translateids, bool details, std::string date, bool global,
               Recycle::RecycleListing* rvec, bool whodeleted, int32_t maxentries)
{
  using namespace eos::common;
  XrdOucString uids;
  XrdOucString gids;
  std::map<uid_t, bool> printmap;
  std::ostringstream oss_out;

  // fix security hole
  if (date.find("..") != std::string::npos) {
    return EINVAL;
  }

  if (global && ((!vid.uid) ||
                 (vid.hasUid(eos::common::ADM_UID)) ||
                 (vid.hasGid(eos::common::ADM_GID)))) {
    // add everything found in the recycle directory structure to the printmap
    std::string subdirs;
    XrdMgmOfsDirectory dirl;
    int listrc = dirl.open(Recycle::gRecyclingPrefix.c_str(), mRootVid,
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
      XrdOucErrInfo lerror;
      int depth = 5 ;

      if (dPath.GetSubPathSize()) {
        if (depth > (int) dPath.GetSubPathSize()) {
          depth -= dPath.GetSubPathSize();
        }
      }

      XrdOucString err_msg;
      int retc = gOFS->_find(sdir, lerror, err_msg, mRootVid, findmap,
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

        for (auto fileit = dirit->second.begin();
             fileit != dirit->second.end(); ++fileit) {
          if (maxentries && (count >= (size_t)maxentries)) {
            retc = E2BIG;
            std_out += oss_out.str();
            return E2BIG;;
          }

          const std::string fname = HandlePotentialSymlink(dirname.c_str(), *fileit);
          eos_static_debug("orig_fname=\"%s\" new_fname=\"%s\"",
                           fileit->c_str(), fname.c_str());

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


/*----------------------------------------------------------------------------*/
int
Recycle::Restore(std::string& std_out, std::string& std_err,
                 eos::common::VirtualIdentity& vid, const char* key,
                 bool force_orig_name, bool restore_versions, bool make_path)
{
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
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

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
  XrdOucErrInfo lerror;
  eos_static_info("msg=\"trying to restore file\" path=\"%s\"",
                  cPath.GetPath());

  if (gOFS->_stat(cPath.GetPath(), &buf, lerror, mRootVid, "", nullptr, false)) {
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
  if (gOFS->_stat(oPath.GetParentPath(), &buf, lerror, mRootVid, "")) {
    if (make_path) {
      XrdOucErrInfo lerror;
      // create path
      ProcCommand cmd;
      XrdOucString info = "mgm.cmd=mkdir&mgm.option=p&mgm.path=";
      info += oPath.GetParentPath();
      cmd.open("/proc/user", info.c_str(), vid, &lerror);
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
  if (!gOFS->_stat(oPath.GetPath(), &buf, lerror, mRootVid, "", nullptr, false)) {
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

      if (gOFS->_rename(oPath.GetPath(), newold.c_str(), lerror, mRootVid, "", "",
                        true, true)) {
        std_err +=
          "error: failed to rename the existing file/tree where we need to restore path=";
        std_err += oPath.GetPath();
        std_err += "\n";
        std_err += lerror.getErrText();
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
  if (gOFS->_rename(cPath.GetPath(), oPath.GetPath(), lerror, mRootVid, "", "",
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

  std::string vkey;

  if (gOFS->_attr_get(oPath.GetPath(), lerror, mRootVid, "",
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
Recycle::Purge(std::string& std_out, std::string& std_err,
               eos::common::VirtualIdentity& vid,
               std::string date,
               bool global,
               std::string key)
{
  XrdMgmOfsDirectory dirl;
  char sdir[4096];
  XrdOucErrInfo lerror;
  int nfiles_deleted = 0;
  int nbulk_deleted = 0;
  std::string rpath;

  // fix security hole
  if (date.find("..") != std::string::npos) {
    std_err = "error: the date contains an illegal character sequence";
    return EINVAL;
  }

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
        std_err = "error: the given key to purge is invalid - must start "
                  "with fxid: or pxid: (see output of recycle ls)";
        return EINVAL;
      }
    }
  }

  if (vid.uid && !vid.sudoer &&
      !(vid.hasUid(eos::common::ADM_UID)) &&
      !(vid.hasGid(eos::common::ADM_GID))) {
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
  int retc = gOFS->_find(sdir, lerror, err_msg, mRootVid, findmap,
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

    for (auto fileit = dirit->second.begin();
         fileit != dirit->second.end(); ++fileit) {
      const std::string fname = HandlePotentialSymlink(dirname.c_str(), *fileit);
      eos_static_debug("orig_fname=\"%s\" new_fname=\"%s\"",
                       fileit->c_str(), fname.c_str());

      if ((fname != "/") && (fname.find('#') != 0)) {
        eos_static_debug("msg=\"skip unexpected entry\" fname=\"%s\"",
                         fname.c_str());
        continue;
      }

      struct stat buf;

      XrdOucErrInfo lerror;

      std::string fullpath = dirname.c_str();

      fullpath += fname;

      if (!gOFS->_stat(fullpath.c_str(), &buf, lerror, mRootVid, "", nullptr,
                       false)) {
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
        int result = Cmd.open("/proc/user", info.c_str(), mRootVid, &lerror);
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
  XrdOucErrInfo lerror;

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
    int result = Cmd.open("/proc/user", info.c_str(), mRootVid, &lerror);
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
    int result = Cmd.open("/proc/user", info.c_str(), mRootVid, &lerror);
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
  } else if (key == "--ratio") {
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
  } else if (key == "--poll-interval") {
    if (value.empty()) {
      std_err = "error: missing poll interval value\n";
      return EINVAL;
    }

    try {
      uint64_t poll_interval = std::stoull(value);

      // Make sure the poll interval is never less than 10 seconds
      if (poll_interval < 10) {
        std_err = "error: recycle poll interval has to be > 10";
        return EINVAL;
      }
    } catch (...) {
      std_err = "error: recycle poll interval not numeric";
      return EINVAL;
    }

    if (gOFS->_attr_set(Recycle::gRecyclingPrefix.c_str(),
                        lerror, mRootVid, "",
                        Recycle::gRecyclingPollAttribute.c_str(),
                        value.c_str())) {
      std_err = "error: failed to set extended attribute '";
      std_err += Recycle::gRecyclingPollAttribute.c_str();
      std_err += "'";
      std_err += " at '";
      std_err += Recycle::gRecyclingPrefix.c_str();
      std_err += "'";
      return EIO;
    } else {
      std_out += "success: recycle bin update poll interval";
    }
  } else if (key == "--collect-interval") {
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
  } else if (key == "--remove-interval") {
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
  } else if (key == "--dry-run") {
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
    std_err = "error: unknown configuration key";
    return EINVAL;
  }

  gOFS->Recycler->WakeUp();
  return 0;
}


//------------------------------------------------------------------------------
// Compute recycle path directory for given user and timestamp
//------------------------------------------------------------------------------
int
Recycle::GetRecyclePrefix(const char* epname, XrdOucErrInfo& error,
                          std::string& recyclepath)
{
  char srecycleuser[4096];
  time_t now = time(NULL);
  struct tm nowtm;
  localtime_r(&now, &nowtm);
  size_t index = 0;

  do {
    snprintf(srecycleuser, sizeof(srecycleuser) - 1, "%s/uid:%u/%04u/%02u/%02u/%lu",
             mRecycleDir.c_str(), mOwnerUid, 1900 + nowtm.tm_year,
             nowtm.tm_mon + 1, nowtm.tm_mday, index);
    struct stat buf;

    // check in case the index directory exists, that it has not more than 1M files,
    // otherwise increment the index by one
    if (!gOFS->_stat(srecycleuser, &buf, error, mRootVid, "")) {
      if (buf.st_blksize > 100000) {
        index++;
        continue;
      }
    }

    // Verify/create group/user directory
    if (gOFS->_mkdir(srecycleuser, S_IRUSR | S_IXUSR | SFS_O_MKPTH, error, mRootVid,
                     "")) {
      return gOFS->Emsg(epname, error, EIO, "remove existing file - the "
                        "recycle space user directory couldn't be created");
    }

    // Check the user recycle directory
    if (gOFS->_stat(srecycleuser, &buf, error, mRootVid, "")) {
      return gOFS->Emsg(epname, error, EIO, "remove existing file - could not "
                        "determine ownership of the recycle space user directory",
                        srecycleuser);
    }

    // Check the ownership of the user directory
    if ((buf.st_uid != mOwnerUid) || (buf.st_gid != mOwnerGid)) {
      // Set the correct ownership
      if (gOFS->_chown(srecycleuser, mOwnerUid, mOwnerGid, error, mRootVid, "")) {
        return gOFS->Emsg(epname, error, EIO, "remove existing file - could not "
                          "change ownership of the recycle space user directory",
                          srecycleuser);
      }
    }

    recyclepath = srecycleuser;
    return SFS_OK;
  } while (1);
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
