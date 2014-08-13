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
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/* ------------------------------------------------------------------------- */
Scheduler::Scheduler () { }

/* ------------------------------------------------------------------------- */
Scheduler::~Scheduler () { }

/* ------------------------------------------------------------------------- */
int
Scheduler::FilePlacement (const char* path, //< path to place
                          eos::common::Mapping::VirtualIdentity_t &vid, //< virtual id of client
                          const char* grouptag, //< group tag for placement
                          unsigned long lid, //< layout to be placed
                          std::vector<unsigned int> &avoid_filesystems, //< filesystems to avoid
                          std::vector<unsigned int> &selected_filesystems, //< return filesystems selected by scheduler
                          bool truncate, //< indicates placement with truncation
                          int forced_scheduling_group_index, //< forced index for the scheduling subgroup to be used 
                          unsigned long long bookingsize //< size to book for the placement
                          )
{
  //! -------------------------------------------------------------
  //! the write placement routine
  //! ------------------------------------------------------------- 

	eos_static_debug("requesting file placement from geolocation %s",vid.geolocation.c_str());

  // the caller routine has to lock via => eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex) 
  std::set<eos::common::FileSystem::fsid_t> fsidavoidlist;
  std::map<eos::common::FileSystem::fsid_t, float> availablefs;
  std::map<eos::common::FileSystem::fsid_t, std::string> availablefsgeolocation;
  std::list<eos::common::FileSystem::fsid_t> availablevector;

  // fill the avoid list from the selected_filesystems input vector
  for (unsigned int i = 0; i < avoid_filesystems.size(); i++)
  {
    fsidavoidlist.insert(avoid_filesystems[i]);
  }

  unsigned int nfilesystems = eos::common::LayoutId::GetStripeNumber(lid) + 1; // 0 = 1 replica !

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

  std::string spacename = SpaceName.c_str();

  std::set<FsGroup*>::const_iterator git;

  // place the group iterator
  if (forced_scheduling_group_index >= 0)
  {
    for (git = FsView::gFsView.mSpaceGroupView[spacename].begin(); git != FsView::gFsView.mSpaceGroupView[spacename].end(); git++)
    {
      if ((*git)->GetIndex() == (unsigned int) forced_scheduling_group_index)
        break;
    }
    if ((git != FsView::gFsView.mSpaceGroupView[spacename].end()) && ((*git)->GetIndex() != (unsigned int) forced_scheduling_group_index))
    {
      selected_filesystems.clear();
      return ENOSPC;
    }
  }
  else
  {
    schedulingMutex.Lock();
    if (schedulingGroup.count(indextag))
    {
      git = FsView::gFsView.mSpaceGroupView[spacename].find(schedulingGroup[indextag]);
    }
    else
    {
      git = FsView::gFsView.mSpaceGroupView[spacename].begin();
      schedulingGroup[indextag] = *git;
    }
    schedulingMutex.UnLock();
  }

  // we can loop over all existing scheduling views
  for (unsigned int groupindex = 0; groupindex < FsView::gFsView.mSpaceGroupView[spacename].size(); groupindex++)
  {
    // we search for available slots for replicas but all in the same group. If we fail on a group, we look in the next one
  	// placement is spread out in all the tree to strengthen reliability ( -> "" )
			bool placeRes = gGeoTreeEngine.placeNewReplicasOneGroup(
					(*git), nfilesystems,
					&selected_filesystems,
					GeoTreeEngine::regularRW,
					NULL,
					bookingsize,
					"",
					&avoid_filesystems,
					NULL,
					NULL);

			if(eos::common::Logging::gLogMask & LOG_INFO)
    {
				char buffer[1024];
				buffer[0]=0;
				char *buf = buffer;
				for(auto it = selected_filesystems.begin(); it!= selected_filesystems.end(); ++it)
				buf += sprintf(buf,"%lu  ",(unsigned long)(*it));

				eos_static_debug("GeoTree Placement returned %d with fs id's -> %s", (int)placeRes, buffer);
    }

    git++;
    if (git == FsView::gFsView.mSpaceGroupView[spacename].end())
    {
      git = FsView::gFsView.mSpaceGroupView[spacename].begin();
    }

    // remember the last group for that indextag
    schedulingMutex.Lock();
    schedulingGroup[indextag] = *git;
    schedulingMutex.UnLock();

			if (placeRes)
    {
    return 0;
  }
  else
				continue;
  }
    selected_filesystems.clear();
    return ENOSPC;
}

/* ------------------------------------------------------------------------- */
int
Scheduler::FileAccess (
                       eos::common::Mapping::VirtualIdentity_t &vid, //< virtual id of client
                       unsigned long forcedfsid, //< forced file system for access
                       const char* forcedspace, //< forced space for access
                       unsigned long lid, //< layout of the file
                       std::vector<unsigned int> &locationsfs, //< filesystem id's where layout is stored
                       unsigned long &fsindex, //< return index pointing to layout entry filesystem
                       bool isRW, //< indicating if pure read or read/write access
                       unsigned long long bookingsize, //< size to book additionally for read/write access
                       std::vector<unsigned int> &unavailfs, //< return filesystems currently unavailable
                       eos::common::FileSystem::fsstatus_t min_fsstatus //< defines minimum filesystem state to allow filesystem selection
                       )
{
  //! -------------------------------------------------------------
  //! the read(/write) access routine
  //! -------------------------------------------------------------
	size_t nReqStripes = isRW?eos::common::LayoutId::GetMinOnlineReplica(lid):eos::common::LayoutId::GetOnlineStripeNumber(lid);
	eos_static_debug("requesting file access from geolocation %s",vid.geolocation.c_str());
  return gGeoTreeEngine.accessHeadReplicaMultipleGroup(nReqStripes,fsindex,&locationsfs,isRW?GeoTreeEngine::regularRW:GeoTreeEngine::regularRO,vid.geolocation,forcedfsid,&unavailfs);
      }


EOSMGMNAMESPACE_END
