// ----------------------------------------------------------------------
// File: Quota.cc
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

#include "mgm/quota/Quota.hh"
#include "mgm/policy/Policy.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "common/StringUtils.hh"
#include "common/table_formatter/TableFormatterBase.hh"
#include "namespace/interface/IView.hh"
#include "namespace/ns_quarkdb/NamespaceGroup.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "mgm/config/IConfigEngine.hh"
#include <errno.h>

EOSMGMNAMESPACE_BEGIN

std::map<std::string, SpaceQuota*> Quota::pMapQuota;
std::map<eos::IContainerMD::id_t, SpaceQuota*> Quota::pMapInodeQuota;
eos::common::RWMutex Quota::pMapMutex;
gid_t Quota::gProjectId = 99;

#ifdef __APPLE__
#define ENONET 64
#endif

//------------------------------------------------------------------------------
// *** Class SpaceQuota implementaion ***
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor - binds to an existing quota node container given by its id.
// Never creates a namespace container. Requires the eosViewRWMutex write-lock.
//------------------------------------------------------------------------------
SpaceQuota::SpaceQuota(const char* path, eos::IContainerMD::id_t cont_id)
    : eos::common::LogId()
    , pPath(path)
    , mContId(cont_id)
    , mQuotaNode(nullptr)
    , mLastEnableCheck(0)
    , mLastRefresh(0)
    , mLastLayoutFactorRefresh(0)
    , mLastProjectRefresh(0)
    , mLayoutSizeFactor(1.0)
    , mDirtyTarget(true)
{
  std::shared_ptr<eos::IContainerMD> quotadir =
      gOFS->eosDirectoryService->getContainerMD(mContId);

  try {
    mQuotaNode = gOFS->eosView->getQuotaNode(quotadir.get(), false);
    eos_info("msg=\"%s ns quota node\" path=\"%s\"", (mQuotaNode ? "found" : "no"), path);
  } catch (const eos::MDException& e) {
    mQuotaNode = nullptr;
  }

  if (!mQuotaNode) {
    // Registering a quota node only marks an already existing container.
    mQuotaNode = gOFS->eosView->registerQuotaNode(quotadir.get());
  }

  UpdateLogicalSizeFactor();
}

//------------------------------------------------------------------------------
// Get quota status
//------------------------------------------------------------------------------
const char*
SpaceQuota::GetQuotaStatus(unsigned long long is, unsigned long long avail)
{
  if (!avail) {
    return "ignored";
  }

  double p = (100.0 * is / avail);

  if (p < 90) {
    return "ok";
  } else if (p < 99) {
    return "warning";
  } else {
    return "exceeded";
  }
}

//------------------------------------------------------------------------------
// Get current quota value as percentage of the available one
//------------------------------------------------------------------------------
float
SpaceQuota::GetQuotaPercentage(unsigned long long is, unsigned long long avail)
{
  float fp = avail ? (100.0 * is / avail) : 100.0;

  if (fp > 100.0) {
    fp = 100.0;
  }

  if (fp < 0) {
    fp = 0;
  }

  return fp;
}

//------------------------------------------------------------------------------
// Update ns quota node address referred to by current space quota
//------------------------------------------------------------------------------
bool
SpaceQuota::UpdateQuotaNodeAddress()
{
  try {
    // Bind by container id rather than resolving pPath by name. The id is
    // stable across renames and this is a single lookup instead of one per
    // path component, which matters because this runs for every quota node
    // on every refresh.
    std::shared_ptr<eos::IContainerMD> quotadir =
        gOFS->eosDirectoryService->getContainerMD(mContId);
    mQuotaNode = gOFS->eosView->getQuotaNode(quotadir.get(), false);

    if (!mQuotaNode) {
      return false;
    }
  } catch (eos::MDException& e) {
    mQuotaNode = nullptr;
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Calculate the size factor used to estimate the logical available bytes
//------------------------------------------------------------------------------
void
SpaceQuota::UpdateLogicalSizeFactor(time_t age)
{
  time_t now = time(NULL);

  // Resolving the path and evaluating the space policies below is expensive,
  // so only redo it once the cached factor is older than the given age. An
  // age of 0 forces the recomputation.
  if (age) {
    if ((now - age) < mLastLayoutFactorRefresh) {
      return;
    }
  }

  mLastLayoutFactorRefresh = now;
  XrdOucErrInfo error;
  eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
  vid.sudoer = 1;
  eos::IContainerMD::XAttrMap map;
  int retc = gOFS->_attr_ls(pPath.c_str(), error, vid, 0, map);

  if (!retc) {
    unsigned long layoutId;
    XrdOucEnv env;
    unsigned long forcedfsid;
    long forcedgroup;
    std::string bandwidth;
    std::string spn = pPath; // Is this necessary?
    bool schedule = false;
    std::string iopriority;
    std::string iotype;
    // get the layout in this quota node
    Policy::GetLayoutAndSpace(pPath.c_str(), map, vid, layoutId, spn, env,
                              forcedfsid, forcedgroup, bandwidth, schedule, iopriority, iotype, false);
    mLayoutSizeFactor = eos::common::LayoutId::GetSizeFactor(layoutId);
  } else {
    mLayoutSizeFactor = 1.0;
  }

  // Protect for division by 0
  if (mLayoutSizeFactor < 1.0) {
    mLayoutSizeFactor = 1.0;
  }
}

//------------------------------------------------------------------------------
// Remove quota
//------------------------------------------------------------------------------
bool
SpaceQuota::RmQuota(unsigned long tag, unsigned long id)
{
  eos_debug("rm quota tag=%lu id=%lu", tag, id);
  XrdSysMutexHelper scope_lock(mMutex);
  bool erased = mMapIdQuota.erase(Index(tag, id));

  if (erased) {
    mDirtyTarget = true;
  }

  return erased;
}

//------------------------------------------------------------------------------
// Get quota value
//------------------------------------------------------------------------------
long long
SpaceQuota::GetQuota(unsigned long tag, unsigned long id)
{
  XrdSysMutexHelper scope_lock(mMutex);
  auto it = mMapIdQuota.find(Index(tag, id));

  if (it != mMapIdQuota.end()) {
    return static_cast<long long>(it->second);
  }

  return 0;
}

//------------------------------------------------------------------------------
// Set quota
//------------------------------------------------------------------------------
void
SpaceQuota::SetQuota(unsigned long tag, unsigned long id,
                     unsigned long long value)
{
  eos_debug("set quota tag=%lu id=%lu value=%llu", tag, id, value);
  XrdSysMutexHelper scope_lock(mMutex);
  mMapIdQuota[Index(tag, id)] = value;

  switch(tag) {
    case kUserBytesTarget:
    mMapIdQuota.try_emplace(Index(kUserLogicalBytesTarget, id), value / mLayoutSizeFactor);
    break;

    case kGroupBytesTarget:
    mMapIdQuota.try_emplace(Index(kGroupLogicalBytesTarget, id), value / mLayoutSizeFactor);
    break;
  }

  if ((tag == kUserBytesTarget) ||
      (tag == kGroupBytesTarget) ||
      (tag == kUserFilesTarget) ||
      (tag == kGroupFilesTarget) ||
      (tag == kUserLogicalBytesTarget) ||
      (tag == kGroupLogicalBytesTarget)) {
    mDirtyTarget = true;
  }
}

//------------------------------------------------------------------------------
// Reset quota
//------------------------------------------------------------------------------
void
SpaceQuota::ResetQuota(unsigned long tag, unsigned long id)
{
  mMapIdQuota[Index(tag, id)] = 0;

  if ((tag == kUserBytesTarget) ||
      (tag == kGroupBytesTarget) ||
      (tag == kUserFilesTarget) ||
      (tag == kGroupFilesTarget) ||
      (tag == kUserLogicalBytesTarget) ||
      (tag == kGroupLogicalBytesTarget)) {
    mDirtyTarget = true;
  }
}

//------------------------------------------------------------------------------
// Add quota
//------------------------------------------------------------------------------
void
SpaceQuota::AddQuota(unsigned long tag, unsigned long id, long long value)
{
  eos_debug("add quota tag=%lu id=%lu value=%llu", tag, id, value);

  // Avoid negative numbers
  if (((long long) mMapIdQuota[Index(tag, id)] + value) >= 0) {
    mMapIdQuota[Index(tag, id)] += value;
  }

  eos_debug("sum quota tag=%lu id=%lu value=%llu", tag, id,
            mMapIdQuota[Index(tag, id)]);
}

//------------------------------------------------------------------------------
// Update
//------------------------------------------------------------------------------
void
SpaceQuota::UpdateTargetSums()
{
  if (!mDirtyTarget) {
    return;
  }

  eos_debug("%s", "updating targets");
  XrdSysMutexHelper scope_lock(mMutex);
  mDirtyTarget = false;
  mMapIdQuota[Index(kAllUserBytesTarget, 0)] = 0;
  mMapIdQuota[Index(kAllUserFilesTarget, 0)] = 0;
  mMapIdQuota[Index(kAllGroupBytesTarget, 0)] = 0;
  mMapIdQuota[Index(kAllGroupFilesTarget, 0)] = 0;
  mMapIdQuota[Index(kAllUserLogicalBytesTarget, 0)] = 0;
  mMapIdQuota[Index(kAllGroupLogicalBytesTarget, 0)] = 0;

  for (const auto& [tag, value] : mMapIdQuota) {
    switch(UnIndex(tag)) {
      case kUserBytesTarget:
        AddQuota(kAllUserBytesTarget, 0, value);
        break;
      case kUserLogicalBytesTarget:
        AddQuota(kAllUserLogicalBytesTarget, 0, value);
        break;
      case kUserFilesTarget:
        AddQuota(kAllUserFilesTarget, 0, value);
        break;
      case kGroupBytesTarget:
        AddQuota(kAllGroupBytesTarget, 0, value);
        break;
      case kGroupLogicalBytesTarget:
        AddQuota(kAllGroupLogicalBytesTarget, 0, value);
        break;
      case kGroupFilesTarget:
        AddQuota(kAllGroupFilesTarget, 0, value);
        break;
    }
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
SpaceQuota::UpdateIsSums()
{
  eos_debug("%s", "updating IS values");
  XrdSysMutexHelper scope_lock(mMutex);
  mMapIdQuota[Index(kAllUserBytesIs, 0)] = 0;
  mMapIdQuota[Index(kAllUserLogicalBytesIs, 0)] = 0;
  mMapIdQuota[Index(kAllUserFilesIs, 0)] = 0;
  mMapIdQuota[Index(kAllGroupBytesIs, 0)] = 0;
  mMapIdQuota[Index(kAllGroupFilesIs, 0)] = 0;
  mMapIdQuota[Index(kAllGroupLogicalBytesIs, 0)] = 0;
  bool has_project_quota = false;
  auto it = mMapIdQuota.find(Index(kGroupLogicalBytesTarget, Quota::gProjectId));

  if ((it != mMapIdQuota.end()) && it->second) {
    has_project_quota = true;
  }

  // If project quota is defined for the current quota node then use that
  // value to avoid possible double counting
  if (has_project_quota) {
    AddQuota(kAllGroupFilesIs, 0,
             mMapIdQuota[Index(kGroupFilesIs, Quota::gProjectId)]);
    AddQuota(kAllGroupBytesIs, 0,
             mMapIdQuota[Index(kGroupBytesIs, Quota::gProjectId)]);
    AddQuota(kAllGroupLogicalBytesIs, 0,
             mMapIdQuota[Index(kGroupLogicalBytesIs, Quota::gProjectId)]);
  } else {
    for (auto it = mMapIdQuota.begin(); it != mMapIdQuota.end(); it++) {
      if ((UnIndex(it->first) == kUserBytesIs)) {
        AddQuota(kAllUserBytesIs, 0, it->second);
      }

      if ((UnIndex(it->first) == kUserLogicalBytesIs)) {
        AddQuota(kAllUserLogicalBytesIs, 0, it->second);
      }

      if ((UnIndex(it->first) == kUserFilesIs)) {
        AddQuota(kAllUserFilesIs, 0, it->second);
      }

      if ((UnIndex(it->first) == kGroupFilesIs)) {
        AddQuota(kAllGroupFilesIs, 0, it->second);
      }

      if ((UnIndex(it->first) == kGroupBytesIs)) {
        AddQuota(kAllGroupBytesIs, 0, it->second);
      }

      if ((UnIndex(it->first) == kGroupLogicalBytesIs)) {
        AddQuota(kAllGroupLogicalBytesIs, 0, it->second);
      }
    }
  }
}

//------------------------------------------------------------------------------
// Update uid/gid values from quota node
//------------------------------------------------------------------------------
void
SpaceQuota::UpdateFromQuotaNode(uid_t uid, gid_t gid, bool upd_proj_quota)
{
  eos_debug("%s", "updating uid/gid values from quota node");
  XrdSysMutexHelper scope_lock(mMutex);

  if (mQuotaNode) {
    mMapIdQuota[Index(kUserBytesIs, uid)] = 0;
    mMapIdQuota[Index(kUserLogicalBytesIs, uid)] = 0;
    mMapIdQuota[Index(kUserFilesIs, uid)] = 0;
    mMapIdQuota[Index(kGroupBytesIs, gid)] = 0;
    mMapIdQuota[Index(kGroupFilesIs, gid)] = 0;
    mMapIdQuota[Index(kGroupLogicalBytesIs, gid)] = 0;
    AddQuota(kUserBytesIs, uid, mQuotaNode->getPhysicalSpaceByUser(uid));
    AddQuota(kUserLogicalBytesIs, uid, mQuotaNode->getUsedSpaceByUser(uid));
    AddQuota(kUserFilesIs, uid, mQuotaNode->getNumFilesByUser(uid));
    AddQuota(kGroupBytesIs, gid, mQuotaNode->getPhysicalSpaceByGroup(gid));
    AddQuota(kGroupLogicalBytesIs, gid, mQuotaNode->getUsedSpaceByGroup(gid));
    AddQuota(kGroupFilesIs, gid, mQuotaNode->getNumFilesByGroup(gid));
    mMapIdQuota[Index(kUserBytesIs, Quota::gProjectId)] = 0;
    mMapIdQuota[Index(kUserLogicalBytesIs, Quota::gProjectId)] = 0;
    mMapIdQuota[Index(kUserFilesIs, Quota::gProjectId)] = 0;

    if (upd_proj_quota) {
      // Recalculate the project quota only every sProjectUsageAge seconds to
      // boost perf - the loop below walks every uid of the quota node, unlike
      // the per-identity lookups above.
      //
      // The throttle is per quota node on purpose. It used to be a function
      // local static, therefore shared by every SpaceQuota object, so whichever
      // node asked first won the window and all the others kept a project usage
      // which could be arbitrarily old - and CheckWriteQuota compares exactly
      // that value against the project target to allow or deny a write. The
      // cost of getting this right scales with the number of quota nodes being
      // written to, not with the number of quota nodes defined.
      //
      // Unlike the other refresh stamps, mLastProjectRefresh gates mMapIdQuota
      // entries, so it belongs under mMutex - held for the whole of this
      // method. It is touched nowhere else but the constructor.
      time_t now = time(NULL);

      if (((now - sProjectUsageAge) >= mLastProjectRefresh) ||
          (gid == Quota::gProjectId)) {
        mLastProjectRefresh = now;
        mMapIdQuota[Index(kGroupBytesIs, Quota::gProjectId)] = 0;
        mMapIdQuota[Index(kGroupFilesIs, Quota::gProjectId)] = 0;
        mMapIdQuota[Index(kGroupLogicalBytesIs, Quota::gProjectId)] = 0;
        // Loop over users and fill project quota
        auto uids = mQuotaNode->getUids();

        for (auto itu = uids.begin(); itu != uids.end(); ++itu) {
          AddQuota(kGroupBytesIs, Quota::gProjectId,
                   mQuotaNode->getPhysicalSpaceByUser(*itu));
          AddQuota(kGroupLogicalBytesIs, Quota::gProjectId,
                   mQuotaNode->getUsedSpaceByUser(*itu));
          AddQuota(kGroupFilesIs, Quota::gProjectId, mQuotaNode->getNumFilesByUser(*itu));
        }
      }
    }
  }
}

//------------------------------------------------------------------------------
// Refresh counters
//------------------------------------------------------------------------------
void
SpaceQuota::Refresh(time_t age)
{
  time_t now = time(NULL);

  // since this loads all quota node info all the time, we don't do this for GetIndividualQuota in realtime all the time
  if (age) {
    if ((now - age) < mLastRefresh) {
      return;
    }
  }

  mLastRefresh = now;
  AccountNsToSpace();
  UpdateLogicalSizeFactor();
  UpdateIsSums();
  UpdateTargetSums();
}


//------------------------------------------------------------------------------
// Print quota information
//------------------------------------------------------------------------------
void
SpaceQuota::PrintOut(XrdOucString& output, long long int uid_sel,
                     long long int gid_sel, bool monitoring, bool translate_ids)
{
  using eos::common::StringConversion;
  // 'quota set' writes the per-id target values straight into mMapIdQuota so
  // that they show up at once, but the aggregate target rows are only
  // recomputed by Refresh. Do it here as well, so that the "All users"/"All
  // groups" row can never disagree with the per-id rows printed above it while
  // the usage refresh age has not elapsed yet. Free if nothing changed, since
  // mDirtyTarget guards it.
  UpdateTargetSums();
  // Make a map containing once all the defined uid's and gid's
  std::vector<std::pair<std::string, unsigned>> uids, gids;
  {
    XrdSysMutexHelper scope_lock(mMutex);

    // For project space we just print the user/group entry gProjectId
    if (mMapIdQuota[Index(kGroupBytesTarget, Quota::gProjectId)] > 0) {
      gid_sel = Quota::gProjectId;
    }

    for (auto it = mMapIdQuota.begin(); it != mMapIdQuota.end(); ++it) {
      if ((UnIndex(it->first) >= kUserBytesIs) &&
          (UnIndex(it->first) <= kUserFilesTarget)) {
        long long int uid = (long long int)((it->first) & 0xffffffff);

        // uid selection filter
        if ((uid_sel >= 0LL) && (uid != uid_sel)) {
          continue;
        }

        // we don't print the users if a gid is selected
        if (gid_sel >= 0LL) {
          continue;
        }

        // Translate IDs
        std::string name = std::to_string(uid).c_str();
        uids.push_back(std::make_pair(name, uid));
      }

      if ((UnIndex(it->first) >= kGroupBytesIs) &&
          (UnIndex(it->first) <= kGroupFilesTarget)) {
        long long int gid = (it->first) & 0xfffffff;

        // uid selection filter
        if ((gid_sel >= 0LL) && (gid != gid_sel)) {
          continue;
        }

        // We don't print the group if a uid is selected
        if (uid_sel >= 0LL) {
          continue;
        }

        // Translate IDs
        std::string name = std::to_string(gid).c_str();
        gids.push_back(std::make_pair(name, gid));
      }
    }
  }

  // translate ids without mutex held
  if (translate_ids) {
    std::string name;
    std::vector<std::pair<std::string, unsigned>> tuids, tgids;

    for (auto u : uids) {
      if (gid_sel == Quota::gProjectId) {
        name = "project";
      } else {
        int errc = 0;
        name = eos::common::Mapping::UidToUserName(u.second, errc);
      }

      tuids.push_back(std::make_pair(name, u.second));
    }

    for (auto g : gids) {
      if (gid_sel == Quota::gProjectId) {
        name = "project";
      } else {
        int errc = 0;
        name = eos::common::Mapping::GidToGroupName(g.second, errc);
      }

      tgids.push_back(std::make_pair(name, g.second));
    }

    uids = tuids;
    gids = tgids;
  }

  // Sort and erase duplicated uids and gids
  eos_info("uids_size=%i, gids_size=%i", uids.size(), gids.size());
  std::sort(uids.begin(), uids.end());
  uids.erase(std::unique(uids.begin(), uids.end()), uids.end());
  std::sort(gids.begin(), gids.end());
  gids.erase(std::unique(gids.begin(), gids.end()), gids.end());

  if (EOS_LOGS_DEBUG) {
    eos_debug("%s","sorted");

    for (unsigned int i = 0; i < uids.size(); ++i) {
      eos_debug("sort %d %d", i, uids[i].second);
    }

    for (unsigned int i = 0; i < gids.size(); ++i) {
      eos_debug("sort %d %d", i, gids[i].second);
    }
  }

  // Print the header for selected uid/gid's only if there is something to print
  // If we have a full listing we print even empty quota nodes (=header only)
  if (!monitoring) {
    if (((uid_sel < 0) && (gid_sel < 0)) || !uids.empty() || !gids.empty()) {
      output += "\n┏━> Quota Node: ";
      output += pPath.c_str();
      output += "\n";
    }
  }

  //! Quota node - Users
  TableFormatterBase table_user;

  if (!uids.empty()) {
    // Table header
    if (!monitoring) {
      table_user.SetHeader({
        std::make_tuple(GetTagCategory(kUserBytesIs), 10, "-s"),
        std::make_tuple(GetTagName(kUserBytesIs), 10, "+l"),
        std::make_tuple(GetTagName(kUserLogicalBytesIs), 10, "+l"),
        std::make_tuple(GetTagName(kUserFilesIs), 10, "+l"),
        std::make_tuple(GetTagName(kUserBytesTarget), 10, "+l"),
        std::make_tuple(GetTagName(kUserLogicalBytesTarget), 10, "+l"),
        std::make_tuple(GetTagName(kUserFilesTarget), 10, "+l"),
        std::make_tuple("filled[%]", 10, "f"),
        std::make_tuple("vol-status", 10, "s"),
        std::make_tuple("ino-status", 10, "s")
      });
    } else {
      table_user.SetHeader({
        std::make_tuple("quota", 0, "os"),
        std::make_tuple("uid", 0, "os"),
        std::make_tuple("space", 0, "os"),
        std::make_tuple("usedbytes", 0, "ol"),
        std::make_tuple("usedlogicalbytes", 0, "ol"),
        std::make_tuple("usedfiles", 0, "ol"),
        std::make_tuple("maxbytes", 0, "ol"),
        std::make_tuple("maxlogicalbytes", 0, "ol"),
        std::make_tuple("maxfiles", 0, "ol"),
        std::make_tuple("percentageusedbytes", 0, "of"),
        std::make_tuple("statusbytes", 0, "os"),
        std::make_tuple("statusfiles", 0, "os")
      });
    }

    for (const auto& [name, id] : uids) {
      eos_debug("loop with id=%d", id);
      TableData table_data;
      table_data.emplace_back();

      if (!monitoring) {
        table_data.back().push_back(TableCell(name.c_str(), "-s"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kUserBytesIs, id), "+l", "B"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kUserLogicalBytesIs, id), "+l", "B"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kUserFilesIs, id), "+l"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kUserBytesTarget, id), "+l", "B"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kUserLogicalBytesTarget, id), "+l", "B"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kUserFilesTarget, id), "+l"));
        table_data.back().push_back(TableCell(GetQuotaPercentage(
                                                GetQuota(kUserLogicalBytesIs, id), GetQuota(kUserLogicalBytesTarget, id)), "f", "%"));
        table_data.back().push_back(TableCell(GetQuotaStatus(
                                                GetQuota(kUserLogicalBytesIs, id), GetQuota(kUserLogicalBytesTarget, id)), "s"));
        table_data.back().push_back(TableCell(GetQuotaStatus(
                                                GetQuota(kUserFilesIs, id), GetQuota(kUserFilesTarget, id)), "s"));
      } else {
        table_data.back().push_back(TableCell("node", "os"));
        table_data.back().push_back(TableCell(name.c_str(), "os"));
        table_data.back().push_back(TableCell(pPath.c_str(), "os"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kUserBytesIs, id), "ol"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kUserLogicalBytesIs, id), "ol"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kUserFilesIs, id), "ol"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kUserBytesTarget, id), "ol"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kUserLogicalBytesTarget, id), "ol"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kUserFilesTarget, id), "ol"));
        table_data.back().push_back(TableCell(GetQuotaPercentage(
                                                GetQuota(kUserLogicalBytesIs, id), GetQuota(kUserLogicalBytesTarget, id)), "of"));
        table_data.back().push_back(TableCell(GetQuotaStatus(
                                                GetQuota(kUserLogicalBytesIs, id), GetQuota(kUserLogicalBytesTarget, id)), "os"));
        table_data.back().push_back(TableCell(GetQuotaStatus(
                                                GetQuota(kUserFilesIs, id), GetQuota(kUserFilesTarget, id)), "os"));
      }

      table_user.AddRows(table_data);
    }
  }

  // Nothing was added to the table if there is no uid to print, and an empty
  // table renders to the empty string, so skip the style setup and the empty
  // body pass altogether. A node carrying only group quota hits this on every
  // listing, and vice versa for the group table below.
  if (!uids.empty()) {
    if ((uid_sel < 0) && (gid_sel < 0)) {
      output += table_user.GenerateTable(HEADER).c_str();
    } else {
      output += table_user.GenerateTable(HEADER2).c_str();
    }
  }

  //! Quota node - Group
  TableFormatterBase table_group;

  if (!gids.empty()) {
    // group loop
    if (!monitoring) {
      table_group.SetHeader({
        std::make_tuple(GetTagCategory(kGroupBytesIs), 10, "-s"),
        std::make_tuple(GetTagName(kGroupBytesIs), 10, "+l"),
        std::make_tuple(GetTagName(kGroupLogicalBytesIs), 10, "+l"),
        std::make_tuple(GetTagName(kGroupFilesIs), 10, "+l"),
        std::make_tuple(GetTagName(kGroupBytesTarget), 10, "+l"),
        std::make_tuple(GetTagName(kGroupLogicalBytesTarget), 10, "+l"),
        std::make_tuple(GetTagName(kGroupFilesTarget), 10, "+l"),
        std::make_tuple("filled[%]", 10, "f"),
        std::make_tuple("vol-status", 10, "s"),
        std::make_tuple("ino-status", 10, "s")
      });
    } else {
      table_group.SetHeader({
        std::make_tuple("quota", 0, "os"),
        std::make_tuple("gid", 0, "os"),
        std::make_tuple("space", 0, "os"),
        std::make_tuple("usedbytes", 0, "ol"),
        std::make_tuple("usedlogicalbytes", 0, "ol"),
        std::make_tuple("usedfiles", 0, "ol"),
        std::make_tuple("maxbytes", 0, "ol"),
        std::make_tuple("maxlogicalbytes", 0, "ol"),
        std::make_tuple("maxfiles", 0, "ol"),
        std::make_tuple("percentageusedbytes", 0, "of"),
        std::make_tuple("statusbytes", 0, "os"),
        std::make_tuple("statusfiles", 0, "os")
      });
    }

    for (const auto& [name, id] : gids) {
      eos_debug("loop with id=%d", id);
      TableData table_data;
      table_data.emplace_back();

      if (!monitoring) {
        table_data.back().push_back(TableCell(name.c_str(), "-s"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kGroupBytesIs, id), "+l", "B"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kGroupLogicalBytesIs, id), "+l", "B"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kGroupFilesIs, id), "+l"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kGroupBytesTarget, id), "+l", "B"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kGroupLogicalBytesTarget, id), "+l", "B"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kGroupFilesTarget, id), "+l"));
        table_data.back().push_back(TableCell(GetQuotaPercentage(
                                                GetQuota(kGroupLogicalBytesIs, id), GetQuota(kGroupLogicalBytesTarget, id)), "f", "%"));
        table_data.back().push_back(TableCell(GetQuotaStatus(
                                                GetQuota(kGroupLogicalBytesIs, id), GetQuota(kGroupLogicalBytesTarget, id)), "s"));
        table_data.back().push_back(TableCell(GetQuotaStatus(
                                                GetQuota(kGroupFilesIs, id), GetQuota(kGroupFilesTarget, id)), "s"));
      } else {
        table_data.back().push_back(TableCell("node", "os"));
        table_data.back().push_back(TableCell(name.c_str(), "os"));
        table_data.back().push_back(TableCell(pPath.c_str(), "os"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kGroupBytesIs, id), "ol"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kGroupLogicalBytesIs, id), "ol"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kGroupFilesIs, id), "ol"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kGroupBytesTarget, id), "ol"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kGroupLogicalBytesTarget, id), "ol"));
        table_data.back().push_back(TableCell(
                                      GetQuota(kGroupFilesTarget, id), "ol"));
        table_data.back().push_back(TableCell(GetQuotaPercentage(
                                                GetQuota(kGroupLogicalBytesIs, id), GetQuota(kGroupLogicalBytesTarget, id)), "of"));
        table_data.back().push_back(TableCell(GetQuotaStatus(
                                                GetQuota(kGroupLogicalBytesIs, id), GetQuota(kGroupLogicalBytesTarget, id)), "os"));
        table_data.back().push_back(TableCell(GetQuotaStatus(
                                                GetQuota(kGroupFilesIs, id), GetQuota(kGroupFilesTarget, id)), "os"));
      }

      table_group.AddRows(table_data);
    }
  }

  if (!gids.empty()) {
    if ((uid_sel < 0) && (gid_sel < 0)) {
      output += table_group.GenerateTable(HEADER).c_str();
    } else {
      output += table_group.GenerateTable(HEADER2).c_str();
    }
  }

  //! Quota node - Summary
  if ((uid_sel < 0) && (gid_sel < 0)) {
    if (!monitoring) {
      TableFormatterBase table_summary;
      table_summary.SetHeader({
        std::make_tuple("summary", 10, "-s"),
        std::make_tuple(GetTagName(kAllUserBytesIs), 10, "+l"),
        std::make_tuple(GetTagName(kAllUserLogicalBytesIs), 10, "+l"),
        std::make_tuple(GetTagName(kAllUserFilesIs), 10, "+l"),
        std::make_tuple(GetTagName(kAllUserBytesTarget), 10, "+l"),
        std::make_tuple(GetTagName(kAllUserLogicalBytesTarget), 10, "+l"),
        std::make_tuple(GetTagName(kAllUserFilesTarget), 10, "+l"),
        std::make_tuple("filled[%]", 10, "f"),
        std::make_tuple("vol-status", 10, "s"),
        std::make_tuple("ino-status", 10, "s")
      });
      TableData table_data;
      table_data.emplace_back();
      table_data.back().push_back(TableCell("All users", "-s"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllUserBytesIs, 0), "+l", "B"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllUserLogicalBytesIs, 0), "+l", "B"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllUserFilesIs, 0), "+l"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllUserBytesTarget, 0), "+l", "B"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllUserLogicalBytesTarget, 0), "+l", "B"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllUserFilesTarget, 0), "+l"));
      table_data.back().push_back(TableCell(GetQuotaPercentage(
                                              GetQuota(kAllUserLogicalBytesIs, 0), GetQuota(kAllUserLogicalBytesTarget, 0)), "f", "%"));
      table_data.back().push_back(TableCell(GetQuotaStatus(
                                              GetQuota(kAllUserLogicalBytesIs, 0), GetQuota(kAllUserLogicalBytesTarget, 0)), "s"));
      table_data.back().push_back(TableCell(GetQuotaStatus(
                                              GetQuota(kAllUserFilesIs, 0), GetQuota(kAllUserFilesTarget, 0)), "s"));
      table_data.emplace_back();
      table_data.back().push_back(TableCell("All groups", "-ls"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllGroupBytesIs, 0), "+l", "B"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllGroupLogicalBytesIs, 0), "+l", "B"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllGroupFilesIs, 0), "+l"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllGroupBytesTarget, 0), "+l", "B"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllGroupLogicalBytesTarget, 0), "+l", "B"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllGroupFilesTarget, 0), "+l"));
      table_data.back().push_back(TableCell(GetQuotaPercentage(
                                              GetQuota(kAllGroupLogicalBytesIs, 0), GetQuota(kAllGroupLogicalBytesTarget, 0)), "f", "%"));
      table_data.back().push_back(TableCell(GetQuotaStatus(
                                              GetQuota(kAllGroupLogicalBytesIs, 0), GetQuota(kAllGroupLogicalBytesTarget, 0)), "s"));
      table_data.back().push_back(TableCell(GetQuotaStatus(
                                              GetQuota(kAllGroupFilesIs, 0), GetQuota(kAllGroupFilesTarget, 0)), "s"));
      table_summary.AddRows(table_data);
      output += table_summary.GenerateTable(HEADER2).c_str();
    } else {
      TableFormatterBase table_summary_user;
      table_summary_user.SetHeader({
        std::make_tuple("quota", 0, "os"),
        std::make_tuple("uid", 0, "os"),
        std::make_tuple("space", 0, "os"),
        std::make_tuple("usedbytes", 0, "ol"),
        std::make_tuple("usedlogicalbytes", 0, "ol"),
        std::make_tuple("usedfiles", 0, "ol"),
        std::make_tuple("maxbytes", 0, "ol"),
        std::make_tuple("maxlogicalbytes", 0, "ol"),
        std::make_tuple("maxfiles", 0, "ol"),
        std::make_tuple("percentageusedbytes", 0, "of"),
        std::make_tuple("statusbytes", 0, "os"),
        std::make_tuple("statusfiles", 0, "os")
      });
      TableData table_data;
      table_data.emplace_back();
      table_data.back().push_back(TableCell("node", "os"));
      table_data.back().push_back(TableCell("ALL", "os"));
      table_data.back().push_back(TableCell(pPath.c_str(), "os"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllUserBytesIs, 0), "ol"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllUserLogicalBytesIs, 0), "ol"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllUserFilesIs, 0), "ol"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllUserBytesTarget, 0), "ol"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllUserLogicalBytesTarget, 0), "ol"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllUserFilesTarget, 0), "ol"));
      table_data.back().push_back(TableCell(GetQuotaPercentage(
                                              GetQuota(kAllUserLogicalBytesIs, 0), GetQuota(kAllUserLogicalBytesTarget, 0)), "of"));
      table_data.back().push_back(TableCell(GetQuotaStatus(
                                              GetQuota(kAllUserLogicalBytesIs, 0), GetQuota(kAllUserLogicalBytesTarget, 0)), "os"));
      table_data.back().push_back(TableCell(GetQuotaStatus(
                                              GetQuota(kAllUserFilesIs, 0), GetQuota(kAllUserFilesTarget, 0)), "os"));
      table_summary_user.AddRows(table_data);
      output += table_summary_user.GenerateTable().c_str();
      TableFormatterBase table_summary_group;
      table_summary_group.SetHeader({
        std::make_tuple("quota", 0, "os"),
        std::make_tuple("gid", 0, "os"),
        std::make_tuple("space", 0, "os"),
        std::make_tuple("usedbytes", 0, "ol"),
        std::make_tuple("usedlogicalbytes", 0, "ol"),
        std::make_tuple("usedfiles", 0, "ol"),
        std::make_tuple("maxbytes", 0, "ol"),
        std::make_tuple("maxlogicalbytes", 0, "ol"),
        std::make_tuple("maxfiles", 0, "ol"),
        std::make_tuple("percentageusedbytes", 0, "of"),
        std::make_tuple("statusbytes", 0, "os"),
        std::make_tuple("statusfiles", 0, "os")
      });
      table_data.clear();
      table_data.emplace_back();
      table_data.back().push_back(TableCell("node", "os"));
      table_data.back().push_back(TableCell("ALL", "os"));
      table_data.back().push_back(TableCell(pPath.c_str(), "os"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllGroupBytesIs, 0), "ol"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllGroupLogicalBytesIs, 0), "ol"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllGroupFilesIs, 0), "ol"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllGroupBytesTarget, 0), "ol"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllGroupLogicalBytesTarget, 0), "ol"));
      table_data.back().push_back(TableCell(
                                    GetQuota(kAllGroupFilesTarget, 0), "ol"));
      table_data.back().push_back(TableCell(GetQuotaPercentage(
                                              GetQuota(kAllGroupLogicalBytesIs, 0), GetQuota(kAllGroupLogicalBytesTarget, 0)), "of"));
      table_data.back().push_back(TableCell(GetQuotaStatus(
                                              GetQuota(kAllGroupLogicalBytesIs, 0), GetQuota(kAllGroupLogicalBytesTarget, 0)), "os"));
      table_data.back().push_back(TableCell(GetQuotaStatus(
                                              GetQuota(kAllGroupFilesIs, 0), GetQuota(kAllGroupFilesTarget, 0)), "os"));
      table_summary_group.AddRows(table_data);
      output += table_summary_group.GenerateTable().c_str();
    }
  }
}

//------------------------------------------------------------------------------
// User/group/project quota checks. If both user and group quotas are defined,
// then both need to be satisfied.
//------------------------------------------------------------------------------
bool
SpaceQuota::CheckWriteQuota(uid_t uid, gid_t gid, long long desired_vol,
                            unsigned int inodes)
{
  bool hasquota = false;
  // Update info from the ns quota node - user, group and project quotas
  UpdateFromQuotaNode(uid, gid, GetQuota(kGroupLogicalBytesTarget, Quota::gProjectId)
                      ? true : false);
  eos_info("uid=%d gid=%d size=%llu quota=%llu", uid, gid, desired_vol,
           GetQuota(kUserLogicalBytesTarget, uid));
  bool userquota = false;
  bool groupquota = false;
  bool projectquota = false;
  bool hasuserquota = false;
  bool hasgroupquota = false;
  bool hasprojectquota = false;
  bool uservolumequota = false;
  bool userinodequota = false;
  bool groupvolumequota = false;
  bool groupinodequota = false;

  if (GetQuota(kUserLogicalBytesTarget, uid) > 0) {
    userquota = true;
    uservolumequota = true;
  }

  if (GetQuota(kGroupLogicalBytesTarget, gid) > 0) {
    groupquota = true;
    groupvolumequota = true;
  }

  if (GetQuota(kUserFilesTarget, uid) > 0) {
    userquota = true;
    userinodequota = true;
  }

  if (GetQuota(kGroupFilesTarget, gid) > 0) {
    groupquota = true;
    groupinodequota = true;
  }

  if (uservolumequota) {
    if ((GetQuota(kUserLogicalBytesTarget, uid) - GetQuota(kUserLogicalBytesIs,
         uid)) > (long long)desired_vol) {
      hasuserquota = true;
    } else {
      hasuserquota = false;
    }
  }

  if (userinodequota) {
    // The +1 comes from the fact the the current file is already accounted to
    // the ns quota by doing ns_quota->addFile previously in the open function.
    if ((GetQuota(kUserFilesTarget, uid) - GetQuota(kUserFilesIs,
         uid) + 1) >= inodes) {
      if (!uservolumequota) {
        hasuserquota = true;
      }
    } else {
      hasuserquota = false;
    }
  }

  if (groupvolumequota) {
    if ((GetQuota(kGroupLogicalBytesTarget, gid) - GetQuota(kGroupLogicalBytesIs,
         gid)) > desired_vol) {
      hasgroupquota = true;
    } else {
      hasgroupquota = false;
    }
  }

  if (groupinodequota) {
    if ((GetQuota(kGroupFilesTarget, gid) - GetQuota(kGroupFilesIs,
         gid)) > inodes) {
      if (!groupvolumequota) {
        hasgroupquota = true;
      }
    } else {
      hasgroupquota = false;
    }
  }

  if (((GetQuota(kGroupLogicalBytesTarget, Quota::gProjectId) -
        GetQuota(kGroupLogicalBytesIs, Quota::gProjectId)) > desired_vol)) {
    hasprojectquota = true;

    if ((GetQuota(kGroupFilesTarget, Quota::gProjectId)) &&
        ((GetQuota(kGroupFilesTarget, Quota::gProjectId) <
          (GetQuota(kGroupFilesIs, Quota::gProjectId) + inodes)))) {
      hasprojectquota = false;
    }
  }

  if (!userquota && !groupquota) {
    projectquota = true;
  }

  eos_info("userquota=%d groupquota=%d hasuserquota=%d hasgroupquota=%d "
           "userinodequota=%d uservolumequota=%d projectquota=%d "
           "hasprojectquota=%d", userquota, groupquota, hasuserquota,
           hasgroupquota, userinodequota, uservolumequota, projectquota,
           hasprojectquota);

  // If both quotas are defined we need to have both
  if (userquota && groupquota) {
    hasquota = hasuserquota & hasgroupquota;
  } else {
    hasquota = hasuserquota || hasgroupquota;
  }

  if (projectquota && hasprojectquota) {
    hasquota = true;
  }

  // Root does not need any quota
  if (uid == 0) {
    hasquota = true;
  }

  return hasquota;
}

//------------------------------------------------------------------------------
// Import ns quota values into current space quota
//------------------------------------------------------------------------------
void
SpaceQuota::AccountNsToSpace()
{
  if (UpdateQuotaNodeAddress()) {
    XrdSysMutexHelper scope_lock(mMutex);
    // Insert current state of a single quota node into a SpaceQuota
    ResetQuota(kGroupBytesIs, Quota::gProjectId);
    ResetQuota(kGroupFilesIs, Quota::gProjectId);
    ResetQuota(kGroupLogicalBytesIs, Quota::gProjectId);
    // Loop over users
    auto uids = mQuotaNode->getUids();

    for (auto itu = uids.begin(); itu != uids.end(); ++itu) {
      ResetQuota(kUserBytesIs, *itu);
      AddQuota(kUserBytesIs, *itu, mQuotaNode->getPhysicalSpaceByUser(*itu));
      ResetQuota(kUserFilesIs, *itu);
      AddQuota(kUserFilesIs, *itu, mQuotaNode->getNumFilesByUser(*itu));
      ResetQuota(kUserLogicalBytesIs, *itu);
      AddQuota(kUserLogicalBytesIs, *itu, mQuotaNode->getUsedSpaceByUser(*itu));

      if (mMapIdQuota[Index(kGroupBytesTarget, Quota::gProjectId)] > 0) {
        // Only account in project quota nodes
        AddQuota(kGroupBytesIs, Quota::gProjectId,
                 mQuotaNode->getPhysicalSpaceByUser(*itu));
        AddQuota(kGroupLogicalBytesIs, Quota::gProjectId,
                 mQuotaNode->getUsedSpaceByUser(*itu));
        AddQuota(kGroupFilesIs, Quota::gProjectId, mQuotaNode->getNumFilesByUser(*itu));
      }
    }

    auto gids = mQuotaNode->getGids();

    for (auto itg = gids.begin(); itg != gids.end(); ++itg) {
      // Don't update the project quota directory from the quota
      if (*itg == Quota::gProjectId) {
        continue;
      }

      ResetQuota(kGroupBytesIs, *itg);
      AddQuota(kGroupBytesIs, *itg, mQuotaNode->getPhysicalSpaceByGroup(*itg));
      ResetQuota(kGroupFilesIs, *itg);
      AddQuota(kGroupFilesIs, *itg, mQuotaNode->getNumFilesByGroup(*itg));
      ResetQuota(kGroupLogicalBytesIs, *itg);
      AddQuota(kGroupLogicalBytesIs, *itg, mQuotaNode->getUsedSpaceByGroup(*itg));
    }
  }
}

//------------------------------------------------------------------------------
// Convert int tag to string representation
//------------------------------------------------------------------------------
const char* SpaceQuota::GetTagAsString(int tag)
{
  if (tag == kUserBytesTarget) {
    return "userbytes";
  }

  if (tag == kUserLogicalBytesTarget) {
    return "userlogicalbytes";
  }

  if (tag == kUserFilesTarget) {
    return "userfiles";
  }

  if (tag == kGroupBytesTarget) {
    return "groupbytes";
  }

  if (tag == kGroupLogicalBytesTarget) {
    return "grouplogicalbytes";
  }

  if (tag == kGroupFilesTarget) {
    return "groupfiles";
  }

  if (tag == kAllUserBytesTarget) {
    return "alluserbytes";
  }

  if (tag == kAllUserLogicalBytesTarget) {
    return "alluserlogicalbytes";
  }

  if (tag == kAllUserFilesTarget) {
    return "alluserfiles";
  }

  if (tag == kAllGroupBytesTarget) {
    return "allgroupbytes";
  }

  if (tag == kAllGroupLogicalBytesTarget) {
    return "allgrouplogicalbytes";
  }

  if (tag == kAllGroupFilesTarget) {
    return "allgroupfiles";
  }

  return 0;
}

//------------------------------------------------------------------------------
// Convert string tag to int representation
//------------------------------------------------------------------------------
unsigned long SpaceQuota::GetTagFromString(const std::string& tag)
{
  if (tag == "userbytes") {
    return kUserBytesTarget;
  }

  if (tag == "userlogicalbytes") {
    return kUserLogicalBytesTarget;
  }

  if (tag == "userfiles") {
    return kUserFilesTarget;
  }

  if (tag == "groupbytes") {
    return kGroupBytesTarget;
  }

  if (tag == "grouplogicalbytes") {
    return kGroupLogicalBytesTarget;
  }

  if (tag == "groupfiles") {
    return kGroupFilesTarget;
  }

  if (tag == "alluserbytes") {
    return kAllUserBytesTarget;
  }

  if (tag == "alluserlogicalbytes") {
    return kAllUserLogicalBytesTarget;
  }

  if (tag == "alluserfiles") {
    return kAllUserFilesTarget;
  }

  if (tag == "allgroupbytes") {
    return kAllGroupBytesTarget;
  }

  if (tag == "allgrouplogicalbytes") {
    return kAllGroupLogicalBytesTarget;
  }

  if (tag == "allgroupfiles") {
    return kAllGroupFilesTarget;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Convert int tag to user or group category
//------------------------------------------------------------------------------
const char* SpaceQuota::GetTagCategory(int tag)
{
  if ((tag == kUserBytesIs) || (tag == kUserBytesTarget) ||
      (tag == kUserLogicalBytesIs) || (tag == kUserLogicalBytesTarget) ||
      (tag == kUserFilesIs) || (tag == kUserFilesTarget) ||
      (tag == kAllUserBytesIs) || (tag == kAllUserBytesTarget) ||
      (tag == kAllUserFilesIs) || (tag == kAllUserFilesTarget)) {
    return "user";
  }

  if ((tag == kGroupBytesIs) || (tag == kGroupBytesTarget) ||
      (tag == kGroupLogicalBytesIs) || (tag == kGroupLogicalBytesTarget) ||
      (tag == kGroupFilesIs) || (tag == kGroupFilesTarget) ||
      (tag == kAllGroupBytesIs) || (tag == kAllGroupBytesTarget) ||
      (tag == kAllGroupFilesIs) || (tag == kAllGroupFilesTarget)) {
    return "group";
  }

  return "-----";
}

//------------------------------------------------------------------------------
// Convert int tag to string description
//------------------------------------------------------------------------------
const char* SpaceQuota::GetTagName(int tag)
{
  if (tag == kUserBytesIs) {
    return "used bytes";
  }

  if (tag == kUserLogicalBytesIs) {
    return "logi bytes";
  }

  if (tag == kUserBytesTarget) {
    return "aval bytes";
  }

  if (tag == kUserFilesIs) {
    return "used files";
  }

  if (tag == kUserFilesTarget) {
    return "aval files";
  }

  if (tag == kUserLogicalBytesTarget) {
    return "aval logib";
  }

  if (tag == kGroupBytesIs) {
    return "used bytes";
  }

  if (tag == kGroupLogicalBytesIs) {
    return "logi bytes";
  }

  if (tag == kGroupBytesTarget) {
    return "aval bytes";
  }

  if (tag == kGroupFilesIs) {
    return "used files";
  }

  if (tag == kGroupFilesTarget) {
    return "aval files";
  }

  if (tag == kGroupLogicalBytesTarget) {
    return "aval logib";
  }

  if (tag == kAllUserBytesIs) {
    return "used bytes";
  }

  if (tag == kAllUserLogicalBytesIs) {
    return "logi bytes";
  }

  if (tag == kAllUserBytesTarget) {
    return "aval bytes";
  }

  if (tag == kAllUserFilesIs) {
    return "used files";
  }

  if (tag == kAllUserFilesTarget) {
    return "aval files";
  }

  if (tag == kAllUserLogicalBytesTarget) {
    return "aval logib";
  }

  if (tag == kAllGroupBytesIs) {
    return "used bytes";
  }

  if (tag == kAllGroupLogicalBytesIs) {
    return "logi bytes";
  }

  if (tag == kAllGroupBytesTarget) {
    return "aval bytes";
  }

  if (tag == kAllGroupFilesIs) {
    return "used files";
  }

  if (tag == kAllGroupFilesTarget) {
    return "aval files";
  }

  if (tag == kAllGroupLogicalBytesTarget) {
    return "aval logib";
  }

  return "---- -----";
}

//------------------------------------------------------------------------------
// *** Class Quota implementaion ***
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Get space quota object for exact path - caller has to have a read lock on
// pMapMutex.
//------------------------------------------------------------------------------
SpaceQuota*
Quota::GetSpaceQuota(const std::string& qpath)
{
  std::string path = NormalizePath(qpath);

  if (pMapQuota.count(path)) {
    return pMapQuota[path];
  } else {
    return nullptr;
  }
}

//------------------------------------------------------------------------------
// Get space quota object responsible for path (find best match) - caller has
// to have a read lock on pMapMutex.
//------------------------------------------------------------------------------
SpaceQuota*
Quota::GetResponsibleSpaceQuota(const std::string& path)
{
  SpaceQuota* squota = nullptr;

  for (auto it = pMapQuota.begin(); it != pMapQuota.end(); ++it) {
    if (common::startsWith(path, it->second->GetSpaceName())) {
      if (squota == nullptr) {
        squota = it->second;
      }

      // Save if it's a better match
      if (it->second->GetSpaceNameStr().size() > squota->GetSpaceNameStr().size()) {
        squota = it->second;
      }
    }
  }

  return squota;
}

//----------------------------------------------------------------------------
//  Get space quota node path
//----------------------------------------------------------------------------
std::string
Quota::GetResponsibleSpaceQuotaPath(const std::string& path)
{
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetResponsibleSpaceQuota(path);

  if (squota) {
    return squota->GetSpaceNameStr();
  } else {
    return "";
  }
}



//------------------------------------------------------------------------------
// Check if space quota exists
//------------------------------------------------------------------------------
bool
Quota::Exists(const std::string& qpath)
{
  std::string path = NormalizePath(qpath);
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  return (pMapQuota.count(path) != 0);
}

//------------------------------------------------------------------------------
// Check if there is a SpaceQuota responsible for the given path
//------------------------------------------------------------------------------
bool
Quota::ExistsResponsible(const std::string& path)
{
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  return (GetResponsibleSpaceQuota(path) != 0);
}

//------------------------------------------------------------------------------
// Get individual quota - called only from mgm/http/webdav/PropFindResponse
//------------------------------------------------------------------------------
void
Quota::GetIndividualQuota(eos::common::VirtualIdentity& vid,
                          const std::string& path,
                          long long& max_bytes,
                          long long& free_bytes,
                          long long& max_files,
                          long long& free_files,
                          bool logical)
{
  // Check for sys.auth='*'
  eos::common::VirtualIdentity m_vid = vid;
  std::string ownerauth;
  XrdOucErrInfo error;
  struct stat buf;

  if (!gOFS->_stat(path.c_str(), &buf, error, vid, "")) {
    gOFS->_attr_get(path.c_str(), error, vid, "", "sys.owner.auth", ownerauth);

    if (ownerauth.length()) {
      if (ownerauth == "*") {
        eos_static_info("msg=\"client authenticated as directory owner\" "
                        "path=\"%s\"uid=\"%u=>%u\" gid=\"%u=>%u\"", path.c_str(),
                        vid.uid, vid.gid, buf.st_uid, buf.st_gid);
        // The client can operate as the owner, we rewrite the virtual id
        m_vid.uid = buf.st_uid;
        m_vid.gid = buf.st_gid;
      } else {
        ownerauth += ",";
        std::string ownerkey = vid.prot.c_str();
        ownerkey += ":";

        if (vid.prot == "gsi") {
          ownerkey += vid.dn.c_str();
        } else {
          ownerkey += vid.uid_string.c_str();
        }

        if ((ownerauth.find(ownerkey)) != std::string::npos) {
          eos_static_info("msg=\"client authenticated as directory owner\" "
                          "path=\"%s\"uid=\"%u=>%u\" gid=\"%u=>%u\"", path.c_str(),
                          vid.uid, vid.gid, buf.st_uid, buf.st_gid);
          // The client can operate as the owner, we rewrite the virtual id
          m_vid.uid = buf.st_uid;
          m_vid.gid = buf.st_gid;
        }
      }
    }
  }

  eos::common::RWMutexReadLock rd_ns_lock(gOFS->eosViewRWMutex);
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* space = GetResponsibleSpaceQuota(path);

  if (space) {
    space->Refresh(60);
    long long max_bytes_usr, max_bytes_grp, max_bytes_prj;
    long long free_bytes_usr, free_bytes_grp, free_bytes_prj;
    long long max_files_usr, max_files_grp, max_files_prj;
    long long free_files_usr, free_files_grp, free_files_prj;
    free_bytes_usr = free_bytes_grp = free_bytes_prj = 0;
    max_bytes_usr = max_bytes_grp = max_bytes_prj = 0;
    free_files_usr = free_files_grp = free_files_prj = 0;
    (void) free_files_usr; // not used - avoid compile warning
    max_files_usr = max_files_grp = max_files_prj = 0;
    (void) max_files_usr; // not used -avoid compile warning

    if(logical) {
      max_bytes_usr  = space->GetQuota(SpaceQuota::kUserLogicalBytesTarget, m_vid.uid);
      max_bytes_grp = space->GetQuota(SpaceQuota::kGroupLogicalBytesTarget, m_vid.gid);
      max_bytes_prj = space->GetQuota(SpaceQuota::kGroupLogicalBytesTarget, Quota::gProjectId);

      free_bytes_usr = max_bytes_usr -
        space->GetQuota(SpaceQuota::kUserLogicalBytesIs, m_vid.uid);
      free_bytes_grp = max_bytes_grp -
        space->GetQuota(SpaceQuota::kGroupLogicalBytesIs, m_vid.gid);
      free_bytes_prj = max_bytes_prj -
        space->GetQuota(SpaceQuota::kGroupLogicalBytesIs, Quota::gProjectId);
    } else {
      max_bytes_usr = space->GetQuota(SpaceQuota::kUserBytesTarget, m_vid.uid);
      max_bytes_grp = space->GetQuota(SpaceQuota::kGroupBytesTarget, m_vid.gid);
      max_bytes_prj = space->GetQuota(SpaceQuota::kGroupBytesTarget, Quota::gProjectId);

      free_bytes_usr = max_bytes_usr -
        space->GetQuota(SpaceQuota::kUserBytesIs, m_vid.uid);
      free_bytes_grp = max_bytes_grp -
        space->GetQuota(SpaceQuota::kGroupBytesIs, m_vid.gid);
      free_bytes_prj = max_bytes_prj -
        space->GetQuota(SpaceQuota::kGroupBytesIs, Quota::gProjectId);
    }

    if (free_bytes_usr > free_bytes) {
      free_bytes = free_bytes_usr;
    }

    if (free_bytes_grp > free_bytes) {
      free_bytes = free_bytes_grp;
    }

    if (free_bytes_prj > free_bytes) {
      free_bytes = free_bytes_prj;
    }

    if (max_bytes_usr > max_bytes) {
      max_bytes = max_bytes_usr;
    }

    if (max_bytes_grp > max_bytes) {
      max_bytes = max_bytes_grp;
    }

    if (max_bytes_prj > max_bytes) {
      max_bytes = max_bytes_prj;
    }
  }
}

//------------------------------------------------------------------------------
// Set quota type for id
//------------------------------------------------------------------------------
bool
Quota::SetQuotaTypeForId(const std::string& qpath, long id, Quota::IdT id_type,
                         Quota::Type quota_type, unsigned long long value,
                         std::string& msg, int& retc)
{
  std::ostringstream oss_msg;
  std::string path = NormalizePath(qpath);
  retc = EINVAL;

  // If no path use "/eos/"
  if (path.empty()) {
    path = "/eos/";
  }

  // Make sure the quota directory exists - this is the only place allowed to
  // create the namespace container backing a quota node.
  std::string cerr;
  eos::IContainerMD::id_t cont_id = 0;

  if (!CreateQuotaDir(path, cont_id, cerr)) {
    oss_msg << "error: failed to create quota directory: " << path << " (" << cerr << ")";
    msg = oss_msg.str();
    return false;
  }

  // Make sure the quota node exists
  if (!CreateQuotaObj(path, cont_id)) {
    oss_msg << "error: failed to create quota node: " << path;
    msg = oss_msg.str();
    return false;
  }

  // Get type of quota to set and construct config entry
  std::ostringstream oss_config;
  SpaceQuota::eQuotaTag quota_tag;
  oss_config << path << ":";

  if (id_type == IdT::kUid) {
    oss_config << "uid=";

    if (quota_type == Type::kVolume) {
      quota_tag = SpaceQuota::kUserLogicalBytesTarget;
    } else {
      quota_tag = SpaceQuota::kUserFilesTarget;
    }
  } else {
    oss_config << "gid=";

    if (quota_type == Type::kVolume) {
      quota_tag = SpaceQuota::kGroupLogicalBytesTarget;
    } else {
      quota_tag = SpaceQuota::kGroupFilesTarget;
    }
  }

  oss_config << id << ":" << SpaceQuota::GetTagAsString(quota_tag);

  // The layout size factor is only refreshed periodically, but the raw <->
  // logical byte conversion below has to use an up to date value. Force a
  // recomputation in its own scope: it needs the namespace lock, which must be
  // taken before pMapMutex and must not be held while the configuration is
  // saved and broadcasted further down.
  {
    eos::common::RWMutexReadLock rd_ns_lock(gOFS->eosViewRWMutex);
    eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
    SpaceQuota* squota = GetSpaceQuota(path);

    if (squota) {
      squota->UpdateLogicalSizeFactor(0);
    }
  }

  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetSpaceQuota(path);

  if (!squota) {
    oss_msg << "error: no quota space defined for node " << path;
    msg = oss_msg.str();
    return false;
  }

  // When setting logical bytes quota, set also raw bytes for backward compatibility
  if (quota_type == Type::kVolume) {
    long long raw_bytes, log_bytes;

    std::string raw_config = oss_config.str();
    raw_config.erase(raw_config.find("logical"), 7);

    SpaceQuota::eQuotaTag quota_raw = (id_type == IdT::kUid) ? SpaceQuota::kUserBytesTarget
                                                             : SpaceQuota::kGroupBytesTarget;

    if (getenv("EOS_MGM_QUOTA_SET_BY_LOGICAL")) {
      log_bytes = value;
      raw_bytes = value * squota->GetLayoutSizeFactor();
    } else {
      log_bytes = value / squota->GetLayoutSizeFactor();
      raw_bytes = value;
    }

    std::string log_value = std::to_string(log_bytes);
    std::string raw_value = std::to_string(raw_bytes);

    oss_msg << "updating quota using " << log_bytes << " bytes (" << raw_bytes << " raw bytes)\n";

    squota->SetQuota(quota_tag, id, log_bytes);
    squota->SetQuota(quota_raw, id, raw_bytes);
    gOFS->mConfigEngine->SetConfigValue("quota", oss_config.str().c_str(), log_value.c_str());
    gOFS->mConfigEngine->SetConfigValue("quota", raw_config.c_str(), raw_value.c_str());
  } else {
    std::string svalue = std::to_string(value);
    squota->SetQuota(quota_tag, id, value);
    gOFS->mConfigEngine->SetConfigValue("quota", oss_config.str().c_str(), svalue.c_str());
  }

  oss_msg << "success: updated "
          << ((quota_type == Type::kVolume) ? "volume" : "inode")
          << " quota for "
          << ((id_type == IdT::kUid) ? "uid=" : "gid=") << id
          << " for node " << path << std::endl;
  msg = oss_msg.str();
  retc = 0;
  return true;
}

//------------------------------------------------------------------------------
// Set quota depending on the quota tag.
//------------------------------------------------------------------------------
bool
Quota::SetQuotaForTag(const std::string& qpath,
                      const std::string& quota_stag,
                      long id, unsigned long long value)
{
  unsigned long spaceq_type = SpaceQuota::GetTagFromString(quota_stag);
  // Make sure the quota node exists
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetSpaceQuota(qpath);

  if (squota) {
    squota->SetQuota(spaceq_type, id, value);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Remove quota type for id
//------------------------------------------------------------------------------
bool
Quota::RmQuotaTypeForId(const std::string& qpath, long id, Quota::IdT id_type,
                        Quota::Type quota_type, std::string& msg, int& retc)
{
  std::ostringstream oss_msg;
  std::string path = NormalizePath(qpath);
  retc = EINVAL;

  // If no path use "/eos/"
  if (path.empty()) {
    path = "/eos/";
  }

  // Get type of quota to remove and construct config entry
  std::ostringstream oss_config;
  SpaceQuota::eQuotaTag quota_tag;
  oss_config << path << ":";

  if (id_type == IdT::kUid) {
    oss_config << "uid=";

    if (quota_type == Type::kVolume) {
      quota_tag = SpaceQuota::kUserBytesTarget;
    } else {
      quota_tag = SpaceQuota::kUserFilesTarget;
    }
  } else {
    oss_config << "gid=";

    if (quota_type == Type::kVolume) {
      quota_tag = SpaceQuota::kGroupBytesTarget;
    } else {
      quota_tag = SpaceQuota::kGroupFilesTarget;
    }
  }

  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetSpaceQuota(path);

  if (!squota) {
    oss_msg << "error: no quota space defined for node " << path;
    msg = oss_msg.str();
    return false;
  }

  if (squota->RmQuota(quota_tag, id)) {
    oss_config << id << ":" << SpaceQuota::GetTagAsString(quota_tag);
    gOFS->mConfigEngine->DeleteConfigValue("quota", oss_config.str().c_str());

    // If this is a volume quota, remove also the equivalent logical quota if set
    if (quota_type == Type::kVolume) {
      std::string log_config = oss_config.str();
      log_config.replace(log_config.find("bytes"), 5, "logicalbytes");
      SpaceQuota::eQuotaTag tag = (id_type == IdT::kUid) ? SpaceQuota::kUserLogicalBytesTarget
                                                         : SpaceQuota::kGroupLogicalBytesTarget;
      if (squota->RmQuota(tag, id))
        gOFS->mConfigEngine->DeleteConfigValue("quota", log_config.c_str());
    }

    oss_msg << "success: removed "
            << ((quota_type == Type::kVolume) ? "volume" : "inode")
            << " quota for "
            << ((id_type == IdT::kUid) ? "uid=" : "gid=") << id
            << " from node " << path;
    msg = oss_msg.str();
    retc = 0;
    return true;
  } else {
    oss_msg << "error: no "
            << ((quota_type == Type::kVolume) ? "volume" : "inode")
            << " quota defined on node " << path << " for "
            << ((id_type == IdT::kUid) ? "user id" : "group id");
    msg = oss_msg.str();
    return false;
  }
}

//------------------------------------------------------------------------------
// Remove all quota types for an id
//------------------------------------------------------------------------------
bool
Quota::RmQuotaForId(const std::string& path, long id, Quota::IdT id_type,
                    std::string& msg, int& retc)
{
  eos_static_debug("path=%s", path.c_str());
  std::string msg_vol, msg_inode;
  bool rm_vol = RmQuotaTypeForId(path, id, id_type, Type::kVolume, msg_vol, retc);
  bool rm_inode = RmQuotaTypeForId(path, id, id_type, Type::kInode,
                                   msg_inode, retc);

  if (rm_vol || rm_inode) {
    if (rm_vol) {
      msg += msg_vol;
    }

    if (rm_inode) {
      msg += msg_inode;
    }

    return true;
  } else {
    msg = "error: no quota defined for node ";
    msg += path;
    return false;
  }
}

//------------------------------------------------------------------------------
// Remove space quota
//------------------------------------------------------------------------------
bool
Quota::RmSpaceQuota(const std::string& qpath, std::string& msg, int& retc)
{
  std::string path = NormalizePath(qpath);
  eos_static_debug("qpath=%s, path=%s", qpath.c_str(), path.c_str());
  bool removed = false;
  {
    eos::common::RWMutexWriteLock wr_ns_lock(gOFS->eosViewRWMutex);
    eos::common::RWMutexWriteLock wr_quota_lock(pMapMutex);
    std::unique_ptr<SpaceQuota> squota(GetSpaceQuota(path));

    if (squota) {
      removed = true;
      // Remove space quota from map
      pMapQuota.erase(path);
      // Delete also from the pMapInodeQuota
      (void)pMapInodeQuota.erase(squota->GetQuotaNode()->getId());

      // Remove ns quota node
      try {
        std::shared_ptr<eos::IContainerMD> qcont = gOFS->eosView->getContainer(path);
        gOFS->eosView->removeQuotaNode(qcont.get());
        retc = 0;
      } catch (eos::MDException& e) {
        retc = e.getErrno();
        msg = e.getMessage().str().c_str();
      }
    }
  }

  // Remove all configuration entries - outside the namespace lock since this
  // saves the configuration and broadcasts it
  std::string match = path;
  match += ":";
  size_t num_keys = gOFS->mConfigEngine->DropConfigValueByMatch("quota", match.c_str());

  if (removed) {
    msg = "success: removed space quota for ";
    msg += path;
    return true;
  }

  // No quota node, but an interrupted rename can leave a configuration behind.
  // It is skipped on load, so this is the only way of getting rid of it.
  if (num_keys) {
    retc = 0;
    msg = "success: removed left over quota configuration for ";
    msg += path;
    return true;
  }

  retc = EINVAL;
  msg = "error: there is no quota node under path ";
  msg += path;
  return false;
}

//------------------------------------------------------------------------------
// Check whether the given directory is a quota node or contains one
//------------------------------------------------------------------------------
bool
Quota::HasNodesInSubtree(const std::string& path)
{
  const std::string prefix = NormalizePath(path);
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);

  for (const auto& elem : pMapQuota) {
    if (eos::common::startsWith(elem.first, prefix)) {
      return true;
    }
  }

  return false;
}

//------------------------------------------------------------------------------
// Prepare the quota book-keeping for a directory about to be renamed/moved
//------------------------------------------------------------------------------
std::vector<std::string>
Quota::PrepareRenameNodes(const std::string& old_path, const std::string& new_path)
{
  const std::string old_prefix = NormalizePath(old_path);
  const std::string new_prefix = NormalizePath(new_path);

  if (old_prefix == new_prefix) {
    return {};
  }

  // Renaming a directory is common, renaming one holding quota nodes is not -
  // check the small quota map before scanning the big configuration map
  if (!HasNodesInSubtree(old_prefix) || (gOFS->mConfigEngine == nullptr)) {
    return {};
  }

  // A quota key is "<node_path>:uid=<id>:<tag>", so one prefix match covers the
  // directory itself and every node below it
  return gOFS->mConfigEngine->CopyConfigValueByMatch("quota", old_prefix.c_str(),
                                                     new_prefix.c_str());
}

//------------------------------------------------------------------------------
// Complete the quota book-keeping after a successful namespace rename
//------------------------------------------------------------------------------
size_t
Quota::CommitRenameNodes(const std::string& old_path, const std::string& new_path)
{
  const std::string old_prefix = NormalizePath(old_path);
  const std::string new_prefix = NormalizePath(new_path);

  if (old_prefix == new_prefix) {
    return 0;
  }

  // Re-key the renamed directory, if it is a quota node, and any node below it
  std::vector<std::pair<std::string, std::string>> renamed;
  {
    eos::common::RWMutexWriteLock wr_quota_lock(pMapMutex);

    for (const auto& elem : pMapQuota) {
      if (eos::common::startsWith(elem.first, old_prefix)) {
        renamed.emplace_back(elem.first,
                             new_prefix + elem.first.substr(old_prefix.length()));
      }
    }

    for (const auto& elem : renamed) {
      auto it = pMapQuota.find(elem.first);

      if (it == pMapQuota.end()) {
        continue;
      }

      SpaceQuota* squota = it->second;
      pMapQuota.erase(it);
      // The target should never hold a quota node, but a leftover would leak
      // and shadow the renamed one
      auto stale = pMapQuota.find(elem.second);

      if (stale != pMapQuota.end()) {
        eos_static_crit("msg=\"dropping stale quota node at rename target\" "
                        "path=\"%s\"",
                        elem.second.c_str());

        if (stale->second->GetQuotaNode()) {
          auto inode_it = pMapInodeQuota.find(stale->second->GetQuotaNode()->getId());

          if ((inode_it != pMapInodeQuota.end()) && (inode_it->second == stale->second)) {
            pMapInodeQuota.erase(inode_it);
          }
        }

        delete stale->second;
        pMapQuota.erase(stale);
      }

      // Writing these in place is safe under the pMapMutex write lock - see
      // "Thread safety" in the SpaceQuota declaration.
      squota->pPath = elem.second;
      // Let the next refresh pick up the layout size factor of the new
      // location - recomputing it here would need the namespace lock. Both
      // stamps have to be cleared: the layout factor has an age of its own, so
      // clearing mLastRefresh alone keeps the factor of the old location until
      // sLayoutFactorAge elapses.
      squota->mLastRefresh = 0;
      squota->mLastLayoutFactorRefresh = 0;
      pMapQuota[elem.second] = squota;
      // pMapInodeQuota is keyed by container id and needs no update
    }
  }

  // Drop the old location, PrepareRenameNodes already wrote the new one. Done
  // outside the quota map lock since it saves and broadcasts, and skipped
  // entirely without quota nodes. The new prefix covers a concurrent LoadNodes
  // which re-keyed them already.
  size_t num_keys = 0;

  if ((!renamed.empty() || HasNodesInSubtree(new_prefix)) &&
      (gOFS->mConfigEngine != nullptr)) {
    num_keys = gOFS->mConfigEngine->DropConfigValueByMatch("quota", old_prefix.c_str());
  }

  if (!renamed.empty() || num_keys) {
    eos_static_notice("msg=\"moved quota nodes\" old_path=\"%s\" "
                      "new_path=\"%s\" num_nodes=%llu num_config_keys=%llu",
                      old_prefix.c_str(), new_prefix.c_str(),
                      (unsigned long long)renamed.size(), (unsigned long long)num_keys);

    // Without autosave the new location only lives in memory
    if (num_keys && !gOFS->mConfigEngine->GetAutoSave()) {
      eos_static_warning("msg=\"quota nodes moved but autosave is off, run "
                         "'eos config save' to make this persistent\" "
                         "old_path=\"%s\" new_path=\"%s\"",
                         old_prefix.c_str(), new_prefix.c_str());
    }
  }

  return renamed.size();
}

//------------------------------------------------------------------------------
// Undo PrepareRenameNodes after a failed rename
//------------------------------------------------------------------------------
void
Quota::AbortRenameNodes(const std::vector<std::string>& keys)
{
  if (keys.empty()) {
    return;
  }

  if (gOFS->mConfigEngine == nullptr) {
    return;
  }

  eos_static_warning("msg=\"rename failed, dropping the quota configuration "
                     "copied for the new location\" num_config_keys=%llu",
                     (unsigned long long)keys.size());
  gOFS->mConfigEngine->DropConfigValues("quota", keys);
}

//------------------------------------------------------------------------------
// Remove quota depending on the quota tag. Convenience wrapper around the
// default RmQuotaTypeForId.
//------------------------------------------------------------------------------
bool
Quota::RmQuotaForTag(const std::string& path, const std::string& quota_stag,
                     long id)
{
  unsigned long spaceq_type = SpaceQuota::GetTagFromString(quota_stag);
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetSpaceQuota(path);

  if (squota) {
    squota->RmQuota(spaceq_type, id);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Callback function to calculate how much pyhisical space a file occupies
//------------------------------------------------------------------------------
uint64_t
Quota::MapSizeCB(const eos::IFileMD* file)
{
  if (!file) {
    return 0;
  }

  eos::IFileMD::layoutId_t lid = file->getLayoutId();
  return (uint64_t) file->getSize() * eos::common::LayoutId::GetSizeFactor(lid);
}

//------------------------------------------------------------------------------
// Discover the ns quota nodes and create the missing SpaceQuota objects
//------------------------------------------------------------------------------
void
Quota::DiscoverNodes(bool detect_renames)
{
  std::vector<std::pair<std::string, eos::IContainerMD::id_t>> create_quota;
  std::map<eos::IContainerMD::id_t, std::string> map_id_path;
  std::set<eos::IContainerMD::id_t> known_ids;

  if (!detect_renames) {
    // Without rename detection only the ids which are not known yet need their
    // URI resolved below. Collect the known ones in their own scope since
    // Quota::Exists takes pMapMutex itself, so it must not be held while the
    // namespace is queried.
    eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);

    for (const auto& elem : pMapInodeQuota) {
      known_ids.insert(elem.first);
    }
  }

  // Load all known nodes
  {
    std::string quota_path;
    std::shared_ptr<eos::IContainerMD> container;
    eos::common::RWMutexReadLock rd_ns_lock(gOFS->eosViewRWMutex);
    auto set_ids = gOFS->eosView->getQuotaStats()->getAllIds();

    for (const auto elem : set_ids) {
      if (!detect_renames && (known_ids.count(elem) != 0)) {
        // Already known and renames are not our business here - skip the URI
        // resolution, which is the expensive part of this loop
        continue;
      }

      try {
        container = gOFS->eosDirectoryService->getContainerMD(elem);
        quota_path = gOFS->eosView->getUri(container.get());

        // Make sure directories are '/' terminated
        if (quota_path.back() != '/') {
          quota_path += '/';
        }

        map_id_path.emplace(elem, quota_path);

        if (!Exists(quota_path)) {
          create_quota.emplace_back(quota_path, elem);
        }
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_static_err("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                       e.getErrno(), e.getMessage().str().c_str());
      }
    }
  }

  // Re-key the quota nodes whose directory was renamed in the meantime - this
  // syncs a slave with a rename done by the master. The configuration is
  // broadcasted by the MGM which did the rename, so it is not touched here.
  // A rename going through this MGM is already applied synchronously by
  // Quota::CommitRenameNodes, hence this is skipped when the caller asked not
  // to detect renames - it also spares an exclusive pMapMutex lock.
  if (detect_renames) {
    eos::common::RWMutexWriteLock wr_quota_lock(pMapMutex);
    std::vector<std::pair<std::string, std::string>> renamed;

    // The id keyed map avoids dereferencing the ns quota node, which may
    // already be gone if it was removed on another MGM
    for (const auto& elem : pMapInodeQuota) {
      auto it = map_id_path.find(elem.first);

      if ((it != map_id_path.end()) && (it->second != elem.second->GetSpaceNameStr())) {
        renamed.emplace_back(elem.second->GetSpaceNameStr(), it->second);
      }
    }

    for (const auto& elem : renamed) {
      auto it = pMapQuota.find(elem.first);

      if ((it == pMapQuota.end()) || (pMapQuota.count(elem.second) != 0)) {
        continue;
      }

      SpaceQuota* squota = it->second;
      pMapQuota.erase(it);
      squota->pPath = elem.second;
      // See Quota::CommitRenameNodes on why both timestamps are cleared
      squota->mLastRefresh = 0;
      squota->mLastLayoutFactorRefresh = 0;
      pMapQuota[elem.second] = squota;
      eos_static_notice("msg=\"re-keyed renamed quota node\" old_path=\"%s\" "
                        "new_path=\"%s\"",
                        elem.first.c_str(), elem.second.c_str());
    }
  }

  // Load all the necessary space quota nodes. Bind directly by container id so
  // that we never re-resolve the path by name (which could rebind to a wrong or
  // shadow container) and never create a namespace container.
  for (auto it = create_quota.begin(); it != create_quota.end(); ++it) {
    if (Exists(it->first)) {
      // Already covered by the re-keying above
      continue;
    }

    eos_static_notice("msg=\"load quota node\" path=\"%s\" cont_id=%llu",
                      it->first.c_str(), (unsigned long long)it->second);
    (void)CreateQuotaObj(it->first, it->second);
  }
}

//------------------------------------------------------------------------------
// Import the ns usage counters into every known quota node
//------------------------------------------------------------------------------
void
Quota::RefreshAllNodes()
{
  std::vector<eos::IContainerMD::id_t> cont_ids;
  {
    eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
    cont_ids.reserve(pMapInodeQuota.size());

    for (const auto& elem : pMapInodeQuota) {
      cont_ids.push_back(elem.first);
    }
  }

  // Refresh one node at a time, taking and releasing both locks per node so
  // that a long list of quota nodes does not block the namespace. Working on a
  // snapshot is what makes releasing the locks in between safe - no iterator
  // survives it. The snapshot is keyed by container id because a concurrent
  // rename can re-key the path while the id stays stable.
  //
  // Note that the set refreshed here comes from pMapInodeQuota while the
  // callers list from pMapQuota. Keeping the two in step is an invariant of
  // this file: CreateQuotaObj inserts into both, RmSpaceQuota and CleanUp erase
  // from both, and CommitRenameNodes re-keys pMapQuota alone only because
  // pMapInodeQuota is keyed by the container id. An entry which reached
  // pMapQuota only would still be refreshed by the single node paths, which all
  // resolve through pMapQuota, but the listings which go over every node would
  // show it with whatever counters it last had.
  for (const auto cont_id : cont_ids) {
    eos::common::RWMutexReadLock rd_ns_lock(gOFS->eosViewRWMutex);
    eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
    auto it = pMapInodeQuota.find(cont_id);

    if (it != pMapInodeQuota.end()) {
      it->second->Refresh(sUsageRefreshAge);
    }
  }
}

//------------------------------------------------------------------------------
// Import the ns usage counters into the quota node responsible for a path
//------------------------------------------------------------------------------
void
Quota::RefreshResponsibleNode(const std::string& path)
{
  eos::common::RWMutexReadLock rd_ns_lock(gOFS->eosViewRWMutex);
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetResponsibleSpaceQuota(path);

  if (squota) {
    squota->Refresh(sUsageRefreshAge);
  }
}

//------------------------------------------------------------------------------
// Load nodes
//------------------------------------------------------------------------------
void
Quota::LoadNodes()
{
  DiscoverNodes(true);
  RefreshAllNodes();
}

//------------------------------------------------------------------------------
// Print out quota information
//------------------------------------------------------------------------------
bool
Quota::PrintOut(const std::string& path, XrdOucString& output,
                long long int uid_sel, long long int gid_sel, bool monitoring,
                bool translate_ids)
{
  output = "";
  // Add this to have all quota nodes visible even if they are not in the
  // configuration file. Rename detection is deliberately left out: a rename is
  // applied synchronously by CommitRenameNodes, and LoadNodes still re-syncs
  // the whole view at configuration application and master transition.
  DiscoverNodes(false);

  // Import the usage counters of what is about to be printed only
  if (path.empty()) {
    RefreshAllNodes();
  } else {
    RefreshResponsibleNode(path);
  }

  eos::common::RWMutexReadLock rd_fs_lock(FsView::gFsView.ViewMutex);
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);

  if (path.empty()) {
    for (auto it = pMapQuota.begin(); it != pMapQuota.end(); ++it) {
      it->second->PrintOut(output, uid_sel, gid_sel, monitoring, translate_ids);
    }
  } else {
    SpaceQuota* squota = GetResponsibleSpaceQuota(path);

    if (squota) {
      squota->PrintOut(output, uid_sel, gid_sel, monitoring, translate_ids);
    } else {
      output = "error: no quota for path ";
      output += path.c_str();
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Get group quota values for a particular path and id
//------------------------------------------------------------------------------
std::map<int, unsigned long long>
Quota::GetGroupStatistics(const std::string& qpath, long id)
{
  std::string path = NormalizePath(qpath);
  std::map<int, unsigned long long> map;
  eos::common::RWMutexReadLock rd_ns_lock(gOFS->eosViewRWMutex);
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetResponsibleSpaceQuota(path);

  if (!squota) {
    return map;
  }

  squota->Refresh(60);
  unsigned long long value;
  // Set of all group related quota keys
  std::set<int> set_keys = {SpaceQuota::kGroupLogicalBytesIs, SpaceQuota::kGroupLogicalBytesTarget,
                            SpaceQuota::kGroupFilesIs, SpaceQuota::kGroupFilesTarget,
                            SpaceQuota::kAllGroupLogicalBytesTarget,
                            SpaceQuota::kAllGroupLogicalBytesIs
                           };

  for (auto it = set_keys.begin(); it != set_keys.end(); ++it) {
    value = squota->GetQuota(*it, id);
    map.insert(std::make_pair(*it, value));
  }

  return map;
}

//------------------------------------------------------------------------------
// Update quota from the namespace quota only if the requested path is actually
// a ns quota node.
//------------------------------------------------------------------------------
bool
Quota::UpdateFromNsQuota(const std::string& path, uid_t uid, gid_t gid)
{
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetResponsibleSpaceQuota(path);

  // No quota or this is not the space quota itself - do nothing
  if (!squota || squota->GetSpaceNameStr().compare(path)) {
    return false;
  }

  squota->UpdateFromQuotaNode(uid, gid, true);
  return true;
}

//----------------------------------------------------------------------------
// Check if the requested volume and inode values respect the quota
//----------------------------------------------------------------------------
bool
Quota::Check(const std::string& path, uid_t uid, gid_t gid,
             long long desired_vol, unsigned int desired_inodes)
{
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetResponsibleSpaceQuota(path);

  if (!squota) {
    return true;
  }

  return squota->CheckWriteQuota(uid, gid, desired_vol, desired_inodes);
}

//------------------------------------------------------------------------------
// Clean-up all space quotas by deleting them and clearing the maps
//------------------------------------------------------------------------------
void
Quota::CleanUp()
{
  eos::common::RWMutexWriteLock wr_lock(pMapMutex);

  for (auto it = pMapQuota.begin(); it != pMapQuota.end(); ++it) {
    delete it->second;
  }

  pMapQuota.clear();
  pMapInodeQuota.clear();
}

//------------------------------------------------------------------------------
// Take the decision where to place a new file in the system. The core of the
// implementation is in the Scheduler and GeoTreeEngine.
//------------------------------------------------------------------------------
int
Quota::FilePlacement(Scheduler::PlacementArguments* args)
{
  if (FsView::gFsView.mSpaceGroupView.count(*args->spacename) == 0) {
    eos_static_err("msg=\"no filesystem in space\" space=\"%s\"",
                   args->spacename->c_str());
    args->selected_filesystems->clear();
    return ENOSPC;
  }

  // Check if quota enabled for current space
  if (FsView::gFsView.IsQuotaEnabled(*args->spacename)) {
    eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
    if (SpaceQuota* squota = GetResponsibleSpaceQuota(args->path)) {
      if (!squota->CheckWriteQuota(args->vid->uid, args->vid->gid, args->bookingsize, 1)) {
        eos_static_debug("not enough quota for uid=%u gid=%u grouptag=%s bookingsize=%lu at path=%s",
                         args->vid->uid, args->vid->gid, args->grouptag, args->bookingsize, args->path);
        return EDQUOT;
      }
    }
  } else {
    eos_static_debug("quota is disabled for space=%s", args->spacename->c_str());
  }

  if (!FsView::gFsView.UnderNominalQuota(*args->spacename, args->vid->sudoer)) {
    eos_static_err("msg=\"over physical quota limit (nominal space setting)\" space=\"%s\"",
                   args->spacename->c_str());
    return ENOSPC;
  }

  eos_static_debug("%s", "nominal quota ok");

  // Call the scheduler implementation
  return Scheduler::Placement(args);
}

//------------------------------------------------------------------------------
// Create the namespace container backing a quota node, if missing, and return
// its id. Only to be called from the explicit admin "quota set" command path.
//------------------------------------------------------------------------------
bool
Quota::CreateQuotaDir(const std::string& path, eos::IContainerMD::id_t& cont_id,
                      std::string& err)
{
  cont_id = 0ull;

  // Check if path is correct
  if (path.empty() || path[0] != '/' || (*path.rbegin()) != '/') {
    err = "invalid quota path";
    return false;
  }

  {
    eos::common::RWMutexWriteLock wr_ns_lock(gOFS->eosViewRWMutex);

    // Return the existing container id if it already exists
    try {
      cont_id = gOFS->eosView->getContainer(path)->getId();
      return true;
    } catch (const eos::MDException& e) {
      if (e.getErrno() != ENOENT) {
        // Transient or any other error - refuse to create so that a struggling
        // backend can never cause a shadow container to be fabricated.
        err = e.what();
        eos_static_err("msg=\"refusing to create quota dir on non-ENOENT error\" "
                       "path=\"%s\" ec=%d err_msg=\"%s\"",
                       path.c_str(), e.getErrno(), e.what());
        return false;
      }
    }

    // Genuinely absent - create it deliberately. The in-memory addContainer
    // conflict check (EEXIST) remains the backstop against duplicating a name.
    try {
      std::shared_ptr<eos::IContainerMD> qdir =
          gOFS->eosView->createContainer(path, true);
      qdir->setMode(S_IRWXU | S_IRGRP | S_IXGRP | S_IFDIR);
      gOFS->eosView->updateContainerStore(qdir.get());
      cont_id = qdir->getId();
    } catch (const eos::MDException& e) {
      err = e.what();
      eos_static_crit("msg=\"failed to create quota dir\" path=\"%s\" "
                      "err_msg=\"%s\"",
                      path.c_str(), e.what());
      return false;
    }
  }

  // Synchronize the flusher so the new directory is durable before we register
  // the quota node and persist the quota configuration.
  auto* qdb_ns_grp = dynamic_cast<eos::QuarkNamespaceGroup*>
                     (gOFS->namespaceGroup.get());

  if (qdb_ns_grp) {
    qdb_ns_grp->getMetadataFlusher()->synchronize();
  }

  return true;
}

//------------------------------------------------------------------------------
// Create the in-memory quota object (SpaceQuota) for an existing quota node
//------------------------------------------------------------------------------
bool
Quota::CreateQuotaObj(const std::string& path, eos::IContainerMD::id_t cont_id)
{
  // Check if path is correct
  if (path.empty() || path[0] != '/' || (*path.rbegin()) != '/') {
    return false;
  }

  eos::common::RWMutexWriteLock wr_ns_lock(gOFS->eosViewRWMutex);
  eos::common::RWMutexWriteLock wr_quota_lock(pMapMutex);

  if (pMapQuota.count(path) == 0) {
    try {
      SpaceQuota* squota = new SpaceQuota(path.c_str(), cont_id);
      pMapQuota[path] = squota;
      pMapInodeQuota[squota->GetQuotaNode()->getId()] = squota;
    } catch (const eos::MDException& e) {
      eos_static_crit("msg=\"failed to create quota node\" path=\"%s\" "
                      "err_msg=\"%s\"",
                      path.c_str(), e.what());
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Retrieve the kAllGroupLogicalBytesIs and kAllGroupLogicalBytesTarget
// values for the quota nodes.
//------------------------------------------------------------------------------
std::map<std::string, std::tuple<unsigned long long,
    unsigned long long,
    unsigned long long>>
    Quota::GetAllGroupsLogicalQuotaValues()
{
  std::map<std::string, std::tuple<unsigned long long,
      unsigned long long,
      unsigned long long>> allGroupLogicalByteValues;
  // Add this to have all quota nodes visible even if they are not in the
  // configuration file - see Quota::PrintOut on why renames are not detected
  // here.
  DiscoverNodes(false);
  RefreshAllNodes();
  eos::common::RWMutexReadLock rd_fs_lock(FsView::gFsView.ViewMutex);
  eos::common::RWMutexReadLock rd_ns_lock(gOFS->eosViewRWMutex);
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);

  for (const auto& quotaNode : pMapQuota) {
    // quotaNode.second->Refresh();
    allGroupLogicalByteValues[quotaNode.first] = std::make_tuple
        (quotaNode.second->GetQuota(SpaceQuota::eQuotaTag::kAllGroupLogicalBytesIs, 0),
         quotaNode.second->GetQuota(SpaceQuota::eQuotaTag::kAllGroupLogicalBytesTarget,
                                    0),
         quotaNode.second->GetQuota(SpaceQuota::eQuotaTag::kAllGroupFilesIs, 0));
  }

  return allGroupLogicalByteValues;
}

//------------------------------------------------------------------------------
// Get quota for requested user and group by path
//------------------------------------------------------------------------------
int
Quota::QuotaByPath(const char* path, uid_t uid, gid_t gid,
                   long long& avail_files, long long& avail_bytes,
                   eos::IContainerMD::id_t& quota_inode)
{
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetResponsibleSpaceQuota(path);

  if (squota) {
    quota_inode = squota->GetQuotaNode()->getId();
    return GetQuotaInfo(squota, uid, gid, avail_files, avail_bytes);
  }

  return -1;
}

//------------------------------------------------------------------------------
// Get quota for requested user and group by quota inode
//------------------------------------------------------------------------------
int
Quota::QuotaBySpace(const eos::IContainerMD::id_t qino, uid_t uid, gid_t gid,
                    long long& avail_files, long long& avail_bytes)
{
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  auto it = pMapInodeQuota.find(qino);

  if (it != pMapInodeQuota.end()) {
    return GetQuotaInfo(it->second, uid, gid, avail_files, avail_bytes);
  }

  return -1;
}

//------------------------------------------------------------------------------
// Private method to collect desired info from a quota node
// returns std::numeric_limits<long>::max() / 2; in avail_files or avail_bytes if not quota is set
//------------------------------------------------------------------------------
int
Quota::GetQuotaInfo(SpaceQuota* squota, uid_t uid, gid_t gid,
                    long long& avail_files, long long& avail_bytes)
{
  long long maxbytes_user, maxbytes_group, maxbytes_project;
  long long freebytes_user, freebytes_group, freebytes_project;
  long long freebytes = 0 ;
  long long maxbytes = 0;
  freebytes_user = freebytes_group = freebytes_project = 0;
  maxbytes_user = maxbytes_group = maxbytes_project = 0;
  squota->UpdateFromQuotaNode(uid, gid,
                              squota->GetQuota(SpaceQuota::kGroupBytesTarget, Quota::gProjectId)
                              ? true : false);
  maxbytes_user  = squota->GetQuota(SpaceQuota::kUserLogicalBytesTarget, uid);
  maxbytes_group = squota->GetQuota(SpaceQuota::kGroupLogicalBytesTarget, gid);
  maxbytes_project = squota->GetQuota(SpaceQuota::kGroupLogicalBytesTarget,
                                      Quota::gProjectId);
  freebytes_user = maxbytes_user - squota->GetQuota(
                     SpaceQuota::kUserLogicalBytesIs, uid);
  freebytes_group = maxbytes_group - squota->GetQuota(
                      SpaceQuota::kGroupLogicalBytesIs, gid);
  freebytes_project = maxbytes_project - squota->GetQuota(
                        SpaceQuota::kGroupLogicalBytesIs, Quota::gProjectId);

  if (freebytes_user > freebytes) {
    freebytes = freebytes_user;
  }

  if (freebytes_group > freebytes) {
    freebytes = freebytes_group;
  }

  if (freebytes_project > freebytes) {
    freebytes = freebytes_project;
  }

  if (maxbytes_user > maxbytes) {
    maxbytes = maxbytes_user;
  }

  if (maxbytes_group > maxbytes) {
    maxbytes = maxbytes_group;
  }

  if (maxbytes_project > maxbytes) {
    maxbytes = maxbytes_project;
  }

  if (!freebytes && (maxbytes == 0)) {
    // this is no quota set
    freebytes = std::numeric_limits<long>::max() / 2;
  }

  long long maxfiles_user, maxfiles_group, maxfiles_project;
  long long freefiles_user, freefiles_group, freefiles_project;
  long long freefiles = 0;
  long long maxfiles = 0;
  freefiles_user = freefiles_group = freefiles_project = 0;
  maxfiles_user = maxfiles_group = maxfiles_project = 0;
  maxfiles_user  = squota->GetQuota(SpaceQuota::kUserFilesTarget, uid);
  maxfiles_group = squota->GetQuota(SpaceQuota::kGroupFilesTarget, gid);
  maxfiles_project = squota->GetQuota(SpaceQuota::kGroupFilesTarget,
                                      Quota::gProjectId);
  freefiles_user = maxfiles_user - squota->GetQuota(SpaceQuota::kUserFilesIs,
                   uid);
  freefiles_group = maxfiles_group - squota->GetQuota(SpaceQuota::kGroupFilesIs,
                    gid);
  freefiles_project = maxfiles_project - squota->GetQuota(
                        SpaceQuota::kGroupFilesIs, Quota::gProjectId);

  if (freefiles_user > freefiles) {
    freefiles = freefiles_user;
  }

  if (freefiles_group > freefiles) {
    freefiles = freefiles_group;
  }

  if (freefiles_project > freefiles) {
    freefiles = freefiles_project;
  }

  if (maxfiles_user > maxfiles) {
    maxfiles = maxfiles_user;
  }

  if (maxfiles_group > maxfiles) {
    maxfiles = maxfiles_group;
  }

  if (maxfiles_project > maxfiles) {
    maxfiles = maxfiles_project;
  }

  if (!freefiles && (maxfiles == 0)) {
    // this is no quota set
    freefiles = std::numeric_limits<long>::max() / 2;
  }

  avail_files = freefiles;
  avail_bytes = freebytes;
  return 0;
}


//------------------------------------------------------------------------------
// Get logical max and free bytes for the given space
//------------------------------------------------------------------------------
void
Quota::GetStatfs(const std::string& path, unsigned long long& maxbytes,
                 unsigned long long& freebytes)
{
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* space = GetResponsibleSpaceQuota(path);

  if (space) {
    space->Refresh(60);
    maxbytes = space->GetQuota(SpaceQuota::kAllGroupLogicalBytesTarget, 0);
    freebytes = maxbytes - space->GetQuota(SpaceQuota::kAllGroupLogicalBytesIs, 0);
  } else {
    maxbytes = freebytes = 0;
  }
}


//------------------------------------------------------------------------------
// Remove file from corresponding quota node
//------------------------------------------------------------------------------
bool
Quota::RemoveFile(eos::IFileMD::id_t fid)
{
  std::shared_ptr<eos::IFileMD> fmd {nullptr};
  std::shared_ptr<eos::IContainerMD> cmd {nullptr};
  eos::IQuotaNode* ns_quota {nullptr};
  eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex);

  try {
    fmd = gOFS->eosFileService->getFileMD(fid);
    cmd = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
    ns_quota = gOFS->eosView->getQuotaNode(cmd.get());
  } catch (const eos::MDException& e) {
    return false;
  }

  if (ns_quota && fmd) {
    ns_quota->removeFile(fmd.get());
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Remove file from corresponding quota node
//------------------------------------------------------------------------------
bool
Quota::AddFile(eos::IFileMD::id_t fid)
{
  std::shared_ptr<eos::IFileMD> fmd {nullptr};
  std::shared_ptr<eos::IContainerMD> cmd {nullptr};
  eos::IQuotaNode* ns_quota {nullptr};
  eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex);

  try {
    fmd = gOFS->eosFileService->getFileMD(fid);
    cmd = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
    ns_quota = gOFS->eosView->getQuotaNode(cmd.get());
  } catch (const eos::MDException& e) {
    return false;
  }

  if (ns_quota && fmd) {
    ns_quota->addFile(fmd.get());
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Force refresh of space quota from namespace quota node
//------------------------------------------------------------------------------
bool
Quota::RefreshFromNsQuota(const std::string& path)
{
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetResponsibleSpaceQuota(path);

  if (!squota || squota->GetSpaceNameStr().compare(path)) {
    return false;
  }

  {
    XrdSysMutexHelper scope_lock(squota->mMutex);
    auto it = squota->mMapIdQuota.begin();
    while (it != squota->mMapIdQuota.end()) {
      unsigned long tag = squota->UnIndex(it->first);

      // Only remove "Is" tags
      if (tag == SpaceQuota::kUserBytesIs || tag == SpaceQuota::kUserLogicalBytesIs ||
          tag == SpaceQuota::kUserFilesIs || tag == SpaceQuota::kGroupBytesIs ||
          tag == SpaceQuota::kGroupLogicalBytesIs || tag == SpaceQuota::kGroupFilesIs ||
          tag == SpaceQuota::kAllUserBytesIs ||
          tag == SpaceQuota::kAllUserLogicalBytesIs ||
          tag == SpaceQuota::kAllUserFilesIs || tag == SpaceQuota::kAllGroupBytesIs ||
          tag == SpaceQuota::kAllGroupLogicalBytesIs ||
          tag == SpaceQuota::kAllGroupFilesIs) {
        it = squota->mMapIdQuota.erase(it);
      } else {
        ++it;
      }
    }
  }
  squota->Refresh(0);

  return true;
}

EOSMGMNAMESPACE_END
