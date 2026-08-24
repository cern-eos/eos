// ----------------------------------------------------------------------
// File: Scheduler.cc
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

#include "mgm/scheduler/Scheduler.hh"
#include "mgm/geotreeengine/GeoTreeEngine.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/placement/FsScheduler.hh"
#include "mgm/quota/Quota.hh"
#include "mgm/stat/Stat.hh"

EOSMGMNAMESPACE_BEGIN

std::mutex Scheduler::sMapMutex;
std::map<std::string, FsGroup*> Scheduler::schedulingGroup;

//------------------------------------------------------------------------------
// Name of a scheduling engine, for logs
//------------------------------------------------------------------------------
const char*
Scheduler::SchedEngineName(SchedEngine engine)
{
  switch (engine) {
  case SchedEngine::kFlat:
    return "flat";

  case SchedEngine::kGeoTree:
    return "geotree";

  case SchedEngine::kFlatFallback:
    return "geotree-fb";

  case SchedEngine::kNone:
  default:
    return "none";
  }
}

//------------------------------------------------------------------------------
// Get the number of replicas that should stay in the client's geolocation
//------------------------------------------------------------------------------
unsigned int
Scheduler::GetCollocatedReplicas(tPlctPolicy plctpolicy, unsigned long lid,
                                 bool has_geolocation)
{
  const unsigned int nfilesystems = eos::common::LayoutId::GetStripeNumber(lid) + 1;

  switch (plctpolicy) {
  case kScattered:
    // one replica next to the client, the rest as far away as possible
    return has_geolocation ? 1 : 0;

  case kHybrid:
    switch (eos::common::LayoutId::GetLayoutType(lid)) {
    case eos::common::LayoutId::kPlain:
      return 1;

    case eos::common::LayoutId::kReplica:
      return nfilesystems - 1;

    default:
      return nfilesystems - eos::common::LayoutId::GetRedundancyStripeNumber(lid);
    }

  // we only do geolocations for replica layouts
  case kGathered:
    return nfilesystems;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Map the MGM placement policy onto the flat scheduler one
//------------------------------------------------------------------------------
static placement::PlacementPolicyT
toPlacementPolicy(Scheduler::tPlctPolicy policy)
{
  switch (policy) {
  case Scheduler::kGathered:
    return placement::PlacementPolicyT::kGathered;

  case Scheduler::kHybrid:
    return placement::PlacementPolicyT::kHybrid;

  case Scheduler::kScattered:
    [[fallthrough]];
  default:
    return placement::PlacementPolicyT::kScattered;
  }
}

//------------------------------------------------------------------------------
// Schedule file placement using the flat scheduler of the MGM
//------------------------------------------------------------------------------
int
Scheduler::FlatSchedulerPlacement(PlacementArguments* args)
{
  return FlatSchedulerPlacement(args, *gOFS->mFsScheduler);
}

//------------------------------------------------------------------------------
// Schedule file placement using the given flat scheduler
//------------------------------------------------------------------------------
int
Scheduler::FlatSchedulerPlacement(PlacementArguments* args,
                                  placement::FsScheduler& fs_sched)
{
  auto config = fs_sched.GetSchedConfig(*args->spacename);

  if (args->sched_strategy_cstr != nullptr) {
    config = placement::SchedConfigFromStr(args->sched_strategy_cstr);
  }

  // Routing, and the one way left to ask for the legacy engine per request -
  // an escape hatch kept while the flat scheduler soaks
  if (!config.IsFlat()) {
    return EINVAL;
  }

  // From here on the flat scheduler owns this request. The caller needs to tell
  // a routing rejection above from an engine failure below, and a return code
  // cannot carry that - FsScheduler reports both as EINVAL on the access path.
  args->flat_engine_ran = true;
  const placement::PlacementStrategyT strategy = config.strategy;

  uint8_t n_replicas = eos::common::LayoutId::GetStripeNumber(args->lid) + 1;
  placement::PlacementArgs plct_args{
      n_replicas, placement::SchedOp{args->activity, placement::SchedDirection::kCreate},
      strategy};

  plct_args.excludefs.assign(args->exclude_filesystems->begin(),
                             args->exclude_filesystems->end());

  // The file systems already holding a replica are just as ineligible as the
  // explicitly excluded ones - placing a second replica of a file on a disk
  // that already has one silently costs the redundancy the layout asked for
  if (args->alreadyused_filesystems) {
    plct_args.excludefs.insert(plct_args.excludefs.end(),
                               args->alreadyused_filesystems->begin(),
                               args->alreadyused_filesystems->end());
  }

  plct_args.forced_group_index = args->forced_scheduling_group_index;
  plct_args.fid = args->inode;
  plct_args.bookingsize = args->bookingsize;
  plct_args.plctpolicy = toPlacementPolicy(args->plctpolicy);
  // A forced target geotag replaces the client's geolocation as the anchor the
  // collocated replicas gather around, which is what the geotree engine does
  // with its startFromGeoTag argument
  const bool has_target_geotag =
      (args->plctTrgGeotag != nullptr) && !args->plctTrgGeotag->empty();
  plct_args.geolocation =
      has_target_geotag ? *args->plctTrgGeotag : args->vid->geolocation;
  plct_args.ncollocatedfs = static_cast<uint8_t>(
      GetCollocatedReplicas(args->plctpolicy, args->lid, !plct_args.geolocation.empty()));

  auto ret = fs_sched.Schedule(*args->spacename, plct_args);
  if (!ret) {
    eos_static_err("unable to place files with FlatScheduler err=%s",
                   ret.ErrorString().c_str());
    return ENOSPC;
  }

  args->selected_filesystems->assign(ret.ids.begin(), ret.ids.begin() + n_replicas);
  return 0;
}

int
Scheduler::Placement(PlacementArguments* args)
{
  // The one place that decides which engine schedules a placement: the flat
  // scheduler first, the legacy geotree engine as the fallback. Each engine
  // invocation is timed on its own so that the two can be compared. Samples go
  // in under uid/gid 0 on purpose - a StatAvg is ~31kB and Stat::Add allocates
  // one per uid *and* one per gid, so accounting these to the client would cost
  // tens of MB per tag on a busy instance for a breakdown nobody reads. The
  // aggregated row in 'eos ns stat' is identical either way.
  ScopedExecTiming timer;
  int rc = FlatSchedulerPlacement(args);

  if (args->flat_engine_ran) {
    args->sched_exec_ms += timer.Record("Sched::Placement::Flat");

    if (rc == 0) {
      args->sched_engine = SchedEngine::kFlat;
      return 0;
    }
  }

  timer.Restart();
  rc = GeoTreePlacement(args);
  args->sched_exec_ms += timer.Record("Sched::Placement::GeoTree");
  args->sched_engine =
      args->flat_engine_ran ? SchedEngine::kFlatFallback : SchedEngine::kGeoTree;
  return rc;
}

//------------------------------------------------------------------------------
// Schedule file placement using the legacy geotree engine
//------------------------------------------------------------------------------
int
Scheduler::GeoTreePlacement(PlacementArguments* args)
{
  eos_static_debug("requesting file placement from geolocation %s",
                   args->vid->geolocation.c_str());
  // The caller routine has to lock via =>
  //  eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex)
  std::map<eos::common::FileSystem::fsid_t, float> availablefs;
  std::map<eos::common::FileSystem::fsid_t, std::string> availablefsgeolocation;
  std::list<eos::common::FileSystem::fsid_t> availablevector;
  // fill the avoid list from the selected_filesystems input vector
  unsigned int nfilesystems = eos::common::LayoutId::GetStripeNumber(
                                args->lid) + 1;
  unsigned int ncollocatedfs =
      GetCollocatedReplicas(args->plctpolicy, args->lid, !args->vid->geolocation.empty());
  eos_static_debug("checking placement policy : policy is %d, nfilesystems is"
                   " %d and ncollocated is %d", (int)args->plctpolicy, (int)nfilesystems,
                   (int)ncollocatedfs);
  uid_t uid = args->vid->uid;
  gid_t gid = args->vid->gid;
  XrdOucString lindextag = "";

  if (args->grouptag) {
    lindextag = args->grouptag;
  } else {
    lindextag += (int) uid;
    lindextag += ":";
    lindextag += (int) gid;
  }

  std::string indextag = lindextag.c_str();
  std::set<FsGroup*>::const_iterator git;
  std::vector<std::string> fsidsgeotags;
  std::vector<FsGroup*> groupsToTry;

  // place the group iterator
  if (!args->alreadyused_filesystems->empty()) {
    if (!gOFS->mGeoTreeEngine->getInfosFromFsIds(*args->alreadyused_filesystems,
        &fsidsgeotags,
        0, &groupsToTry)) {
      eos_static_debug("could not retrieve scheduling group for all avoid fsids");
    } else {
      eos_static_debug("succesfully retrieved scheduling groups for all avoid fsids");
    }
  }

  if (args->forced_scheduling_group_index >= 0) {
    eos_static_debug("searching for forced scheduling group=%i",
                     args->forced_scheduling_group_index);

    for (git = FsView::gFsView.mSpaceGroupView[*args->spacename].begin();
         git != FsView::gFsView.mSpaceGroupView[*args->spacename].end(); ++git) {
      if ((*git)->GetIndex() == (unsigned int) args->forced_scheduling_group_index) {
        break;
      }
    }

    if ((git != FsView::gFsView.mSpaceGroupView[*args->spacename].end()) &&
        ((*git)->GetIndex() != (unsigned int) args->forced_scheduling_group_index)) {
      args->selected_filesystems->clear();
      return ENOSPC;
    }

    if (git == FsView::gFsView.mSpaceGroupView[*args->spacename].end()) {
      args->selected_filesystems->clear();
      return ENOSPC;
    }

    eos_static_debug("forced scheduling group index %d",
                     args->forced_scheduling_group_index);
  } else {
    std::lock_guard lock(sMapMutex);

    if (schedulingGroup.count(indextag)) {
      git = FsView::gFsView.mSpaceGroupView[*args->spacename].find(
              schedulingGroup[indextag]);
      schedulingGroup[indextag] = *git;
    } else {
      git = FsView::gFsView.mSpaceGroupView[*args->spacename].begin();
      schedulingGroup[indextag] = *git;
    }

    if (git ==  FsView::gFsView.mSpaceGroupView[*args->spacename].end()) {
      git = FsView::gFsView.mSpaceGroupView[*args->spacename].begin();
    }
  }

  // Rotate scheduling view ptr,updating schedulingGroup map
  // if groupsToTry is not empty we try to first use the same scheduling groups of the already used filesystems
  for (unsigned int groupindex = 0;
       groupindex < FsView::gFsView.mSpaceGroupView[*args->spacename].size() +
       groupsToTry.size(); groupindex++) {
    FsGroup* group = nullptr;

    // Try first the forced scheduling group and fail if we cannot schedule there
    if (args->forced_scheduling_group_index >= 0) {
      group = *git;
    } else {
      // Rotate scheduling view ptr -  we select a random one
      group = (groupindex < groupsToTry.size() ? groupsToTry[groupindex] :
               *git);
    }

    eos_static_debug("Trying GeoTree Placement on group: %s, total groups: %d, groupsToTry: %d ",
                     group->mName.c_str(), FsView::gFsView.mSpaceGroupView[*args->spacename].size(),
                     groupsToTry.size());
    bool placeRes = gOFS->mGeoTreeEngine->placeNewReplicasOneGroup(
        group, nfilesystems, args->selected_filesystems, args->inode, nullptr, nullptr,
        GeoTreeEngine::regularRW,
        // file systems to avoid are assumed to already host a replica
        args->alreadyused_filesystems, &fsidsgeotags, args->bookingsize,
        args->plctTrgGeotag ? *args->plctTrgGeotag : "", args->vid->geolocation,
        ncollocatedfs, args->exclude_filesystems, NULL);
    eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

    if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
      char buffer[1024];
      buffer[0] = 0;
      char* buf = buffer;

      for (auto it = args->selected_filesystems->begin();
           it != args->selected_filesystems->end(); ++it) {
        buf += sprintf(buf, "%lu  ", (unsigned long)(*it));
      }

      eos_static_debug("GeoTree Placement returned %d with fs id's -> %s",
                       (int)placeRes, buffer);
    }

    if (placeRes) {
      eos_static_debug("placing replicas for %s in subgroup %s", args->path,
                       group->mName.c_str());
    } else {
      if (args->forced_scheduling_group_index >= 0) {
        eos_static_debug("msg=\"could not place all replica(s) for %s in the "
                         "forced subgroup %s\"", args->path, group->mName.c_str());
        args->selected_filesystems->clear();
        return ENOSPC;
      } else {
        eos_static_debug("msg=\"could not place all replica(s) for %s in subgroup %s, "
                         "checking next group\"", args->path, group->mName.c_str());
      }
    }

    if (groupindex >= groupsToTry.size()) {
      if ((git == FsView::gFsView.mSpaceGroupView[*args->spacename].end()) ||
          (++git == FsView::gFsView.mSpaceGroupView[*args->spacename].end())) {
        git = FsView::gFsView.mSpaceGroupView[*args->spacename].begin();
      }

      // remember the last group for that indextag
      std::lock_guard lock(sMapMutex);
      schedulingGroup[indextag] = *git;
    }

    if (placeRes) {
      return 0;
    } else {
      continue;
    }
  }

  // Check if we are in any kind of no-update mode
  args->selected_filesystems->clear();
  return ENOSPC;
}

//------------------------------------------------------------------------------
// Resolve the traffic class of a request from its eos.schedclass opaque value
//------------------------------------------------------------------------------
eos::common::SchedActivity
Scheduler::SchedActivityFromRequest(const char* schedclass,
                                    const eos::common::VirtualIdentity& vid)
{
  using eos::common::SchedActivity;

  if ((schedclass == nullptr) || (schedclass != eos::common::kSchedClassInternal)) {
    return SchedActivity::kClient;
  }

  // Only an internal engine may label itself internal, so a client that simply
  // types the key gets scheduled as the client it is
  if (!vid.IsInternalEngine()) {
    eos_static_info("msg=\"ignoring %s on an untrusted identity\" prot=\"%s\" "
                    "uid=%u",
                    eos::common::kSchedClassKey.data(), vid.prot.c_str(), vid.uid);
    return SchedActivity::kClient;
  }

  return SchedActivity::kInternal;
}

//------------------------------------------------------------------------------
// Add the locations the client already tried to the unavailable list, so that
// a retry is not handed straight back to the host that just failed it. This
// matters for RAID layouts in particular, where the driver has to be told which
// stripes are online.
//
// @note the caller has to hold a read lock on FsView::gFsView.ViewMutex
//------------------------------------------------------------------------------
static void
MarkTriedLocationsUnavailable(Scheduler::AccessArguments* args)
{
  if (!args->tried_cgi || args->tried_cgi->empty() || !args->unavailfs) {
    return;
  }

  for (const auto fsid : *args->locationsfs) {
    auto fs = FsView::gFsView.mIdView.lookupByID(fsid);

    if (!fs) {
      continue;
    }

    // tried_cgi is a comma terminated list of host names, so match on the
    // separator rather than on a prefix
    const std::string host = fs->GetHost();

    if (!host.empty() && (args->tried_cgi->find(host + ",") != std::string::npos)) {
      args->unavailfs->push_back(fsid);
    }
  }
}

//------------------------------------------------------------------------------
// File access method
//------------------------------------------------------------------------------
int
Scheduler::Access(AccessArguments* args)
{
  size_t req_stripes =
      (args->isRW ? eos::common::LayoutId::GetOnlineStripeNumber(args->lid)
                  : eos::common::LayoutId::GetMinOnlineReplica(args->lid));
  // pre-checks before deciding the scheduler

  if (req_stripes > args->locationsfs->size()) {
    eos_static_debug("not enough filesystems available for access: "
                     "required=%zu, available=%zu",
                     req_stripes, args->locationsfs->size());
    return EROFS;
  }

  if (args->forcedfsid > 0 &&
      std::find(args->locationsfs->begin(),
                args->locationsfs->end(),
                args->forcedfsid) == args->locationsfs->end()) {
    eos_static_debug("forced filesystem %lu not in available locations",
                     (unsigned long)args->forcedfsid);
    return ENODATA;
  }

  // Both engines need this, so it happens before either of them is asked - and
  // outside the timed region, since it belongs to neither of them
  MarkTriedLocationsUnavailable(args);
  // See Scheduler::Placement for why the samples are accounted to uid/gid 0
  ScopedExecTiming timer;
  int rc = FlatSchedulerAccess(args);

  if (args->flat_engine_ran) {
    args->sched_exec_ms += timer.Record("Sched::Access::Flat");

    if (rc == 0) {
      args->sched_engine = SchedEngine::kFlat;
      eos_static_debug("msg=\"successfully accessed file via FlatScheduler\" index=%zu",
                       *args->fsindex);
    }
  }

  if (rc) {
    if (args->flat_engine_ran) {
      eos_static_info("%s",
                      "msg=\"Failed access via FlatScheduler, falling back to geotree\"");
    }

    timer.Restart();
    rc = GeoTreeAccess(args);
    args->sched_exec_ms += timer.Record("Sched::Access::GeoTree");
    args->sched_engine =
        args->flat_engine_ran ? SchedEngine::kFlatFallback : SchedEngine::kGeoTree;
  }

  if (rc == 0) {
    rc = ApplyAccessExclusionFilter(args);
  }

  return rc;
}

//------------------------------------------------------------------------------
// Schedule file access using the legacy geotree engine
//------------------------------------------------------------------------------
int
Scheduler::GeoTreeAccess(AccessArguments* args)
{
  eos_static_debug("requesting file access from geolocation %s",
                   args->vid->geolocation.c_str());
  const size_t req_stripes =
      (args->isRW ? eos::common::LayoutId::GetOnlineStripeNumber(args->lid)
                  : eos::common::LayoutId::GetMinOnlineReplica(args->lid));
  // The legacy engine keeps its own vocabulary; the only two types the access
  // path has ever produced are the regular ones
  GeoTreeEngine::SchedType st =
      args->isRW ? GeoTreeEngine::regularRW : GeoTreeEngine::regularRO;
  return gOFS->mGeoTreeEngine->accessHeadReplicaMultipleGroup(
      req_stripes, *args->fsindex, *args->locationsfs, args->inode, nullptr, nullptr, st,
      args->vid->geolocation, args->forcedfsid, args->unavailfs);
}

//------------------------------------------------------------------------------
// Honour the eos.excludefsid exclusion list on an already selected location
//------------------------------------------------------------------------------
int
Scheduler::ApplyAccessExclusionFilter(AccessArguments* args)
{
  // If the chosen file system is excluded, try to find another suitable
  // location that is neither excluded nor unavailable. If no such location
  // exists then the access request can not be satisfied.
  if (!args->exclude_filesystems || args->exclude_filesystems->empty() ||
      (*args->fsindex >= args->locationsfs->size())) {
    return 0;
  }

  auto chosen = (*args->locationsfs)[*args->fsindex];
  bool is_excluded =
      std::find(args->exclude_filesystems->begin(), args->exclude_filesystems->end(),
                chosen) != args->exclude_filesystems->end();

  if (!is_excluded) {
    return 0;
  }

  for (size_t i = 0; i < args->locationsfs->size(); ++i) {
    auto fsid = (*args->locationsfs)[i];
    bool excluded =
        std::find(args->exclude_filesystems->begin(), args->exclude_filesystems->end(),
                  fsid) != args->exclude_filesystems->end();
    bool unavail = args->unavailfs &&
                   std::find(args->unavailfs->begin(), args->unavailfs->end(), fsid) !=
                       args->unavailfs->end();

    if (!excluded && !unavail) {
      *args->fsindex = i;
      return 0;
    }
  }

  eos_static_err("%s", "msg=\"no accessible file system location left after "
                       "applying the eos.excludefsid exclusion list\"");
  return ENODATA;
}

//------------------------------------------------------------------------------
// Schedule file access using the flat scheduler of the MGM
//------------------------------------------------------------------------------
int
Scheduler::FlatSchedulerAccess(AccessArguments* args)
{
  return FlatSchedulerAccess(args, *gOFS->mFsScheduler);
}

//------------------------------------------------------------------------------
// Schedule file access using the given flat scheduler
//------------------------------------------------------------------------------
int
Scheduler::FlatSchedulerAccess(AccessArguments* args, placement::FsScheduler& fs_sched)
{
  // A request without a forced space falls back to the global default strategy
  const std::string spaceName(args->forcedspace ? args->forcedspace : "");
  const auto config = fs_sched.GetSchedConfig(spaceName);

  if (!config.IsFlat()) {
    return EINVAL;
  }

  // See FlatSchedulerPlacement - from here on a non-zero return is a real
  // failure of this engine, not a routing rejection
  args->flat_engine_ran = true;
  const placement::PlacementStrategyT strategy = config.strategy;

  placement::AccessArgs access_args{
      *args->fsindex,           args->inode,     strategy,
      args->vid->geolocation,   args->unavailfs, *args->locationsfs,
      args->exclude_filesystems};
  access_args.forcedfsid = args->forcedfsid;
  // A read is content with a replica that only serves reads, an update is not
  access_args.op =
      placement::SchedOp{args->activity, args->isRW ? placement::SchedDirection::kUpdate
                                                    : placement::SchedDirection::kRead};
  // The layout decides how many stripes have to be reachable: one for a plain
  // or replica read, the reconstruction minimum for a RAIN layout
  size_t req_stripes =
      (args->isRW ? eos::common::LayoutId::GetOnlineStripeNumber(args->lid)
                  : eos::common::LayoutId::GetMinOnlineReplica(args->lid));
  access_args.n_replicas = static_cast<uint8_t>(req_stripes);
  return fs_sched.Access(spaceName, access_args);
}

//------------------------------------------------------------------------------
// Get the writable placement capacity of a space in bytes, from whichever
// engine actually schedules it
//------------------------------------------------------------------------------
uint64_t
Scheduler::GetPlacementCapacity(const std::string& spacename)
{
  if (auto capacity = gOFS->mFsScheduler->GetPlacementCapacity(spacename)) {
    return *capacity;
  }

  const std::string nogroup;
  return gOFS->mGeoTreeEngine->placementSpace(spacename, nogroup);
}

//------------------------------------------------------------------------------
// Select a drain destination file system for a single replica
//------------------------------------------------------------------------------
eos::common::FileSystem::fsid_t
Scheduler::PlaceDrainReplica(
    const std::string& spacename, FsGroup* group, unsigned long long fid,
    unsigned long long bookingsize,
    const std::vector<eos::common::FileSystem::fsid_t>& existing_repl,
    const std::vector<eos::common::FileSystem::fsid_t>& exclude_dsts)
{
  if (group == nullptr) {
    return 0;
  }

  // Flat scheduler first - a drain replica is a placement of a single stripe
  // pinned to its source group. It is internal traffic, so a destination that
  // takes no client writes is still a valid target.
  const auto config = gOFS->mFsScheduler->GetSchedConfig(spacename);

  if (config.IsFlat()) {
    placement::PlacementArgs args(1, placement::kInternalCreate, config.strategy);
    args.fid = fid;
    args.bookingsize = bookingsize;
    args.forced_group_index = group->GetIndex();
    // Avoid both the file systems already holding the file and the ones this
    // job has already tried
    args.excludefs.assign(existing_repl.begin(), existing_repl.end());
    args.excludefs.insert(args.excludefs.end(), exclude_dsts.begin(), exclude_dsts.end());
    // Drain gets its own tags: it is internal, single stripe and group pinned
    // traffic running at a rate nothing like client opens, so folding it into
    // Sched::Placement::* would poison the client facing comparison
    ScopedExecTiming timer;
    auto result = gOFS->mFsScheduler->Schedule(spacename, args);
    timer.Record("Sched::DrainPlacement::Flat");

    if (result && (result.n_filled >= 1)) {
      eos_static_debug("msg=\"flat scheduler selected drain destination\" "
                       "fxid=%08llx fsid=%u",
                       fid, result.ids[0]);
      return result.ids[0];
    }

    eos_static_warning("msg=\"flat scheduler could not place a drain replica, "
                       "falling back to the geotree engine\" fxid=%08llx "
                       "err=\"%s\"",
                       fid, result.ErrorString().c_str());
  }

  // Geotree engine (draining policy) fallback - kept behaviour-identical to the
  // legacy path so it remains a viable backup
  std::vector<std::string> fsid_geotags;
  std::vector<eos::common::FileSystem::fsid_t> new_repl;
  std::vector<eos::common::FileSystem::fsid_t> avoid(existing_repl.begin(),
                                                     existing_repl.end());
  std::vector<eos::common::FileSystem::fsid_t> excludes(exclude_dsts.begin(),
                                                        exclude_dsts.end());

  if (!gOFS->mGeoTreeEngine->getInfosFromFsIds(avoid, &fsid_geotags, 0, 0)) {
    eos_static_err("msg=\"failed to retrieve info for existing replicas\" "
                   "fxid=%08llx",
                   fid);
    return 0;
  }

  ScopedExecTiming geo_timer;
  bool res = gOFS->mGeoTreeEngine->placeNewReplicasOneGroup(
      group, 1, &new_repl, (ino64_t)fid,
      NULL, // entrypoints
      NULL, // firewall
      GeoTreeEngine::draining, &avoid, &fsid_geotags, bookingsize,
      "", // start from geotag
      "", // client geo tag
      0,  // ncollocatedfs
      &excludes,
      &fsid_geotags); // excludeGeoTags
  geo_timer.Record("Sched::DrainPlacement::GeoTree");

  if (!res || new_repl.empty()) {
    eos_static_err("msg=\"could not place new drain replica\" fxid=%08llx", fid);
    return 0;
  }

  return new_repl[0];
}

//------------------------------------------------------------------------------
// Reshuffle the selected file system ids
//------------------------------------------------------------------------------
void Scheduler::ReshuffleFs(std::vector<unsigned int> &selectedfs)
{
  if (selectedfs.size() > 0) {
    std::vector<unsigned int> newselectedfs;
    auto result = std::minmax_element(selectedfs.begin(), selectedfs.end());
    int sum = std::accumulate(selectedfs.begin(), selectedfs.end(), 0);

    if ((sum % 2) == 0) {
      newselectedfs.push_back(*result.second);
    } else {
      newselectedfs.push_back(*result.first);
    }

    for (const auto& i : selectedfs) {
      if (i != newselectedfs.front()) {
        newselectedfs.push_back(i);
      }
    }

    selectedfs.swap(newselectedfs);
  }
}

EOSMGMNAMESPACE_END
