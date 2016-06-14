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

/*----------------------------------------------------------------------------*/
#include "mgm/Scheduler.hh"
#include "mgm/Quota.hh"
#include "GeoTreeEngine.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

// Initialize static variables
XrdSysMutex Scheduler::pMapMutex;
std::map<std::string, FsGroup*> Scheduler::schedulingGroup;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Scheduler::Scheduler() { }

//------------------------------------------------------------------------------
// Desstructor
//------------------------------------------------------------------------------
Scheduler::~Scheduler() { }

//------------------------------------------------------------------------------
// Take the decision where to place a new file in the system
//------------------------------------------------------------------------------
int
Scheduler::FilePlacement(PlacementArguments *args)
{
  eos_static_debug("requesting file placement from geolocation %s", args->vid->geolocation.c_str());
  // the caller routine has to lock via => eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex)
  std::map<eos::common::FileSystem::fsid_t, float> availablefs;
  std::map<eos::common::FileSystem::fsid_t, std::string> availablefsgeolocation;
  std::list<eos::common::FileSystem::fsid_t> availablevector;
  // Compute the number of locations of stripes according to the placement policy
  // 0 = 1 replica !
  unsigned int nfilesystems = eos::common::LayoutId::GetStripeNumber(args->lid) + 1;
  unsigned int ncollocatedfs = 0;

  switch (args->plctpolicy)
  {
  case kScattered:
    ncollocatedfs = 0;
    break;

  case kHybrid:
    switch (eos::common::LayoutId::GetLayoutType(args->lid))
    {
    case eos::common::LayoutId::kPlain:
      ncollocatedfs = 1;
      break;

    case eos::common::LayoutId::kReplica:
      ncollocatedfs = nfilesystems - 1;
      break;

    default:
      ncollocatedfs = nfilesystems - eos::common::LayoutId::GetRedundancyStripeNumber(args->lid);
      break;
    }

    break;

  case kGathered:
    ncollocatedfs = nfilesystems;
  }

  eos_static_debug("checking placement policy : policy is %d, nfilesystems is"
                   " %d and ncollocated is %d", (int)args->plctpolicy, (int)nfilesystems,
                   (int)ncollocatedfs);
  uid_t uid = args->vid->uid;
  gid_t gid = args->vid->gid;
  XrdOucString lindextag = "";

  if (args->grouptag)
  {
    lindextag = args->grouptag;
  }
  else
  {
    lindextag += (int) uid;
    lindextag += ":";
    lindextag += (int) gid;
  }

  std::string indextag = lindextag.c_str();
  std::set<FsGroup*>::const_iterator git;
  std::vector<std::string> fsidsgeotags;
  std::vector<FsGroup*> groupsToTry;

  // If there are pre-existing replicas, check in which group they are located
  // and chose the group where they are located the most
  if (!args->alreadyused_filesystems->empty())
  {
    if (!gGeoTreeEngine.getGroupsFromFsIds(*args->alreadyused_filesystems, &fsidsgeotags,
                                           &groupsToTry))
    {
      eos_static_debug("could not retrieve scheduling group for all avoid fsids");
    }
  }

  // Place the group iterator
  if (args->forced_scheduling_group_index >= 0)
  {
    for (git = FsView::gFsView.mSpaceGroupView[*args->spacename].begin();
         git != FsView::gFsView.mSpaceGroupView[*args->spacename].end(); git++)
    {
      if ((*git)->GetIndex() == (unsigned int) args->forced_scheduling_group_index)
        break;
    }

    if ((git != FsView::gFsView.mSpaceGroupView[*args->spacename].end()) &&
        ((*git)->GetIndex() != (unsigned int) args->forced_scheduling_group_index))
    {
      args->selected_filesystems->clear();
      return ENOSPC;
    }
  }
  else
  {
    XrdSysMutexHelper scope_lock(pMapMutex);

    if (schedulingGroup.count(indextag))
    {
      git = FsView::gFsView.mSpaceGroupView[*args->spacename].find(
              schedulingGroup[indextag]);
      schedulingGroup[indextag] = *git;
    }
    else
    {
      git = FsView::gFsView.mSpaceGroupView[*args->spacename].begin();
      schedulingGroup[indextag] = *git;
    }

    git++;

    if (git ==  FsView::gFsView.mSpaceGroupView[*args->spacename].end())
      git = FsView::gFsView.mSpaceGroupView[*args->spacename].begin();
  }

  // We can loop over all existing scheduling views
  for (unsigned int groupindex = 0;
       groupindex < FsView::gFsView.mSpaceGroupView[*args->spacename].size() +
       groupsToTry.size();
       groupindex++)
  {
    // In case there are pre existing replicas, search for space in the groups
    // they lay in first if it's unsuccessful, go to the other groups
    FsGroup* group = groupindex < groupsToTry.size() ? groupsToTry[groupindex] : (*git);

    // We search for available slots for replicas but all in the same group.
    // If we fail on a group, we look in the next one  placement is spread
    // out in all the tree to strengthen reliability ( -> "" )
    bool placeRes = gGeoTreeEngine.placeNewReplicasOneGroup(
        group, nfilesystems,
        args->selected_filesystems,
        args->inode,
        args->dataproxys,
        args->firewallentpts,
        GeoTreeEngine::regularRW,
        args->alreadyused_filesystems,// file systems to avoid are assumed to already host a replica
        &fsidsgeotags,
        args->bookingsize,
        *args->plctTrgGeotag,
        args->vid->geolocation,
        ncollocatedfs,
        NULL,
        NULL,
        NULL);

    if (eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
    {
      char buffer[1024];
      buffer[0] = 0;
      char* buf = buffer;

      for (auto it = args->selected_filesystems->begin(); it != args->selected_filesystems->end(); ++it)
        buf += sprintf(buf, "%lu  ", (unsigned long)(*it));

      eos_static_debug("GeoTree Placement returned %d with fs id's -> %s",
                       (int)placeRes, buffer);
    }

    if (placeRes)
    {
      eos_static_debug("placing replicas for %s in subgroup %s", args->path,
                       group->mName.c_str());
    }
    else
    {
      eos_static_debug("could not place all replica(s) for %s in subgroup %s, "
                       "checking next group", args->path, group->mName.c_str());
    }

    if (groupindex >= groupsToTry.size())
    {
      git++;

      if (git == FsView::gFsView.mSpaceGroupView[*args->spacename].end())
        git = FsView::gFsView.mSpaceGroupView[*args->spacename].begin();

      // remember the last group for that indextag
      pMapMutex.Lock();
      schedulingGroup[indextag] = *git;
      pMapMutex.UnLock();
    }

    if (placeRes)
      return 0;
    else
      continue;
  }

  args->selected_filesystems->clear();
  return ENOSPC;
}

//------------------------------------------------------------------------------
// Take the decision from where to access a file
//------------------------------------------------------------------------------

int Scheduler::FileAccess(AccessArguments *args)
{
  size_t nReqStripes = args->isRW ? eos::common::LayoutId::GetOnlineStripeNumber(args->lid) :
  eos::common::LayoutId::GetMinOnlineReplica(args->lid);
  eos_static_debug("requesting file access from geolocation %s",
      args->vid->geolocation.c_str());

  GeoTreeEngine::SchedType st=GeoTreeEngine::regularRO;
  if(args->schedtype==regular)
  {
    if(args->isRW) st = GeoTreeEngine::regularRW;
    else st = GeoTreeEngine::regularRO;
  }
  if(args->schedtype==draining) st = GeoTreeEngine::draining;
  if(args->schedtype==balancing) st = GeoTreeEngine::balancing;

  return gGeoTreeEngine.accessHeadReplicaMultipleGroup(nReqStripes, *args->fsindex,
      args->locationsfs,
      args->inode,
      args->dataproxys,
      args->firewallentpts,
      st,
      args->overridegeoloc->empty() ? args->vid->geolocation : *args->overridegeoloc,
      args->forcedfsid, args->unavailfs, args->noIO);
}

EOSMGMNAMESPACE_END
