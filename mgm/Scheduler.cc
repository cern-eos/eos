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
Scheduler::FilePlacement(const std::string& spacename,
                         const char* path,
                         eos::common::Mapping::VirtualIdentity_t& vid,
                         const char* grouptag,
                         unsigned long lid,
                         std::vector<unsigned int>& alreadyused_filesystems,
                         std::vector<unsigned int>& selected_filesystems,
                         tPlctPolicy plctpolicy,
                         const std::string& plctTrgGeotag,
                         bool truncate,
                         int forced_scheduling_group_index,
                         unsigned long long bookingsize)
{
  eos_static_debug("requesting file placement from geolocation %s", vid.geolocation.c_str());
  // the caller routine has to lock via => eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex)
  std::map<eos::common::FileSystem::fsid_t, float> availablefs;
  std::map<eos::common::FileSystem::fsid_t, std::string> availablefsgeolocation;
  std::list<eos::common::FileSystem::fsid_t> availablevector;
  // Compute the number of locations of stripes according to the placement policy
  // 0 = 1 replica !
  unsigned int nfilesystems = eos::common::LayoutId::GetStripeNumber(lid) + 1; 
  unsigned int ncollocatedfs = 0;

  switch (plctpolicy)
  {
  case kScattered:
    ncollocatedfs = 0;
    break;

  case kHybrid:
    switch (eos::common::LayoutId::GetLayoutType(lid))
    {
    case eos::common::LayoutId::kPlain:
      ncollocatedfs = 1;
      break;

    case eos::common::LayoutId::kReplica:
      ncollocatedfs = nfilesystems - 1;
      break;

    default:
      ncollocatedfs = nfilesystems - eos::common::LayoutId::GetRedundancyStripeNumber(lid);
      break;
    }

    break;

  case kGathered:
    ncollocatedfs = nfilesystems;
  }

  eos_static_debug("checking placement policy : policy is %d, nfilesystems is"
                   " %d and ncollocated is %d", (int)plctpolicy, (int)nfilesystems,
                   (int)ncollocatedfs);
  uid_t uid = vid.uid;
  gid_t gid = vid.gid;
  XrdOucString lindextag = "";

  if (grouptag)
  {
    lindextag = grouptag;
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
  if (!alreadyused_filesystems.empty())
  {
    if (!gGeoTreeEngine.getGroupsFromFsIds(alreadyused_filesystems, &fsidsgeotags,
                                           &groupsToTry))
    {
      eos_static_debug("could not retrieve scheduling group for all avoid fsids");
    }
  }

  // Place the group iterator
  if (forced_scheduling_group_index >= 0)
  {
    for (git = FsView::gFsView.mSpaceGroupView[spacename].begin();
         git != FsView::gFsView.mSpaceGroupView[spacename].end(); git++)
    {
      if ((*git)->GetIndex() == (unsigned int) forced_scheduling_group_index)
        break;
    }

    if ((git != FsView::gFsView.mSpaceGroupView[spacename].end()) &&
        ((*git)->GetIndex() != (unsigned int) forced_scheduling_group_index))
    {
      selected_filesystems.clear();
      return ENOSPC;
    }
  }
  else
  {
    XrdSysMutexHelper scope_lock(schedulingMutex);

    if (schedulingGroup.count(indextag))
    {
      git = FsView::gFsView.mSpaceGroupView[spacename].find(
              schedulingGroup[indextag]);
      schedulingGroup[indextag] = *git;
    }
    else
    {
      git = FsView::gFsView.mSpaceGroupView[spacename].begin();
      schedulingGroup[indextag] = *git;
    }

    git++;

    if (git ==  FsView::gFsView.mSpaceGroupView[spacename].end())
      git = FsView::gFsView.mSpaceGroupView[spacename].begin();
  }

  // We can loop over all existing scheduling views
  for (unsigned int groupindex = 0;
       groupindex < FsView::gFsView.mSpaceGroupView[spacename].size() +
       groupsToTry.size();
       groupindex++)
  {
    // In case there are pre existing replicas, search for space in the groups
    // they lay in first if it's unsuccessful, go to the other groups
    FsGroup* group = groupindex < groupsToTry.size() ? groupsToTry[groupindex] : (*git);

    // We search for available slots for replicas but all in the same group.
    // If we fail on a group, we look in the next one  placement is spread
    // out in all the tree to strengthen reliability ( -> "" )
    bool placeRes = gGeoTreeEngine.placeNewReplicasOneGroup(group, nfilesystems,
                    &selected_filesystems, GeoTreeEngine::regularRW,
                    &alreadyused_filesystems, &fsidsgeotags, bookingsize,
                    plctTrgGeotag, ncollocatedfs, NULL, NULL, NULL);

    if (eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
    {
      char buffer[1024];
      buffer[0] = 0;
      char* buf = buffer;

      for (auto it = selected_filesystems.begin(); it != selected_filesystems.end(); ++it)
        buf += sprintf(buf, "%lu  ", (unsigned long)(*it));

      eos_static_debug("GeoTree Placement returned %d with fs id's -> %s",
                       (int)placeRes, buffer);
    }

    if (placeRes)
    {
      eos_static_debug("placing replicas for %s in subgroup %s", path,
                       group->mName.c_str());
    }
    else
    {
      eos_static_debug("could not place all replica(s) for %s in subgroup %s, "
                       "checking next group", path, group->mName.c_str());
    }

    if (groupindex >= groupsToTry.size())
    {
      git++;

      if (git == FsView::gFsView.mSpaceGroupView[spacename].end())
        git = FsView::gFsView.mSpaceGroupView[spacename].begin();

      // remember the last group for that indextag
      schedulingMutex.Lock();
      schedulingGroup[indextag] = *git;
      schedulingMutex.UnLock();
    }

    if (placeRes)
      return 0;
    else
      continue;
  }

  selected_filesystems.clear();
  return ENOSPC;
}

//------------------------------------------------------------------------------
// Take the decision from where to access a file
//------------------------------------------------------------------------------
int
Scheduler::FileAccess(eos::common::Mapping::VirtualIdentity_t& vid,
                      unsigned long forcedfsid,
                      const char* forcedspace,
                      std::string tried_cgi,
                      unsigned long lid,
                      std::vector<unsigned int>& locationsfs,
                      unsigned long& fsindex,
                      bool isRW,
                      unsigned long long bookingsize,
                      std::vector<unsigned int>& unavailfs,
                      eos::common::FileSystem::fsstatus_t min_fsstatus,
                      std::string overridegeoloc,
                      bool noIO)
{
  size_t nReqStripes = isRW ? eos::common::LayoutId::GetOnlineStripeNumber(lid) :
                       eos::common::LayoutId::GetMinOnlineReplica(lid);
  eos_static_debug("requesting file access from geolocation %s",
                   vid.geolocation.c_str());
  return gGeoTreeEngine.accessHeadReplicaMultipleGroup(nReqStripes, fsindex,
         &locationsfs,
         isRW ? GeoTreeEngine::regularRW : GeoTreeEngine::regularRO,
         overridegeoloc.empty() ? vid.geolocation : overridegeoloc,
         forcedfsid, &unavailfs, noIO);
}

EOSMGMNAMESPACE_END
