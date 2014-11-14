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
  unsigned int nassigned = 0;

  uid_t uid = vid.uid;
  gid_t gid = vid.gid;

  bool hasgeolocation = false;

  if (vid.geolocation.length())
  {
    if ((eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kReplica))
    {
      // we only do geolocations for replica layouts
      hasgeolocation = true;
    }
  }

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
    eos_static_debug("scheduling group loop %d", forced_scheduling_group_index);
    selected_filesystems.clear();
    availablefs.clear();
    availablefsgeolocation.clear();

    std::set<eos::common::FileSystem::fsid_t>::const_iterator fsit;
    eos::common::FileSystem::fsid_t fsid = 0;

    // create the string map key for this group/index pair
    XrdOucString fsindextag = "";
    fsindextag += (int) (*git)->GetIndex();
    // place the filesystem iterator
    fsindextag += "|";
    fsindextag += indextag.c_str();
    std::string sfsindextag = fsindextag.c_str();

    schedulingMutex.Lock();
    if (schedulingFileSystem.count(sfsindextag))
    {
      //
      fsid = schedulingFileSystem[sfsindextag];
      fsit = (*git)->find(fsid);
      if (fsit == (*git)->end())
      {
        // this filesystem is not anymore there, we start with the first one
        fsit = (*git)->begin();
        fsid = *fsit;
      }
    }
    else
    {
      fsit = (*git)->begin();
      fsid = *fsit;
    }
    schedulingMutex.UnLock();

    eos_static_debug("Enter %s points to %d", sfsindextag.c_str(), *fsit);

    // remember the one we started with ...

    // we loop over some filesystems in that group
    for (unsigned int fsindex = 0; fsindex < (*git)->size(); fsindex++)
    {
      eos_static_debug("checking scheduling group %d filesystem %d", (*git)->GetIndex(), *fsit);

      // take filesystem snapshot
      eos::common::FileSystem::fs_snapshot_t snapshot;
      // we are already in a locked section
      FileSystem* fs = 0;
      if (FsView::gFsView.mIdView.count(fsid))
        fs = FsView::gFsView.mIdView[fsid];
      else
      {
	fsit++;
	
	// create cycling
	if (fsit == (*git)->end())
	{
	  fsit = (*git)->begin();
	}
        continue;
      }

      fs->SnapShotFileSystem(snapshot, false);

      // the weight is given mainly by the disk performance and the network load has a weaker impact (sqrt)
      double weight = (1.0 - snapshot.mDiskUtilization);
      double netweight = (1.0 - ((snapshot.mNetEthRateMiB) ? (snapshot.mNetInRateMiB / snapshot.mNetEthRateMiB) : 0.0));
      double netoutweight = (1.0 - ((snapshot.mNetEthRateMiB) ? (snapshot.mNetOutRateMiB / snapshot.mNetEthRateMiB) : 0.0));
      weight *= ((netweight > 0) ? sqrt(netweight) : 0);
      if (weight < 0.1)
      {
        weight = 0.1;
      }

      if (netoutweight < 0.05)
      {
        eos_static_info("msg=\"skipping node with overloaded eth-out\"");
	fsit++;
	
	// create cycling
	if (fsit == (*git)->end())
	{
	  fsit = (*git)->begin();
	}
        continue;
      }

      // check if this filesystem can be used (online, enough space etc...)
      if ((snapshot.mStatus == eos::common::FileSystem::kBooted) &&
          (snapshot.mConfigStatus == eos::common::FileSystem::kRW) &&
          (snapshot.mErrCode == 0) && // this we probably don't need 
          (fs->GetActiveStatus(snapshot)) && // this checks the heartbeat and the group & node are enabled
          (fs->ReserveSpace(snapshot, bookingsize)))
      {

        if (!fsidavoidlist.count(fsid))
        {
          availablefs[fsid] = weight;

          if (hasgeolocation)
          {
            // only track the geo location if the client has one, otherwise we don't care about the target locations
            availablefsgeolocation[fsid] = snapshot.mGeoTag;
          }

          availablevector.push_back(fsid);
        }
      }
      else
      {
        //      eos_static_err("%d %d %d\n", (snapshot.mStatus), (snapshot.mConfigStatus), (snapshot.mErrCode      == 0 ));
      }
      fsit++;

      // create cycling
      if (fsit == (*git)->end())
      {
        fsit = (*git)->begin();
      }

      if (fsindex == 0)
      {
        // we move the iterator only by one position
        schedulingMutex.Lock();
        schedulingFileSystem[sfsindextag] = *fsit;
        eos_static_debug("Exit %s points to %d", sfsindextag.c_str(), *fsit);
        schedulingMutex.UnLock();
      }


      fsid = *fsit;

      if (!hasgeolocation)
      {
        // -------------------------------------------------------------------------------------------------------------------------------------------------------
        // if we have geolocations we (unfortunately) look through the complete scheduling group, otherwise we just take half of it if we found enough filesystems
        // -------------------------------------------------------------------------------------------------------------------------------------------------------

        // -----------------------------
        // evt. this has to be commented
        // -----------------------------
        if ((availablefs.size() >= nfilesystems) && (availablefs.size() > ((*git)->size() / 2)))
        {
          // we stop if we have found enough ... atleast half of the scheduling group
          break;
        }
      }
    }

    // -------------------------------------------------------------------------------
    // Currently this code can deal only with two GEO locations !!!
    // -------------------------------------------------------------------------------
    std::string selected_geo_location;
    int n_geolocations = 0;
    // check if there are atlast <nfilesystems> in the available map
    if (availablefs.size() >= nfilesystems)
    {
      std::list<eos::common::FileSystem::fsid_t>::iterator ait;
      ait = availablevector.begin();

      for (unsigned int loop = 0; loop < 1000; loop++)
      {
        // we cycle over the available filesystems
        float randomacceptor = (0.999999 * random() / RAND_MAX);
        eos_static_debug("fs %u acceptor %f/%f for %d. replica [loop=%d] [avail=%d]", *ait, randomacceptor, availablefs[*ait], nassigned + 1, loop, availablevector.size());

        if (nassigned == 0)
        {
          if (availablefs[*ait] < randomacceptor)
          {
            ait++;
            if (ait == availablevector.end())
              ait = availablevector.begin();
            continue;
          }
          else
          {
            // push it on the selection list
            selected_filesystems.push_back(*ait);
            if (hasgeolocation)
            {
              selected_geo_location = availablefsgeolocation[*ait];
            }

            eos_static_debug("fs %u selected for %d. replica", *ait, nassigned + 1);

            // remove it from the selection map
            availablefs.erase(*ait);
            ait = availablevector.erase(ait);
            if (ait == availablevector.end())
              ait = availablevector.begin();

            // rotate scheduling view ptr
            nassigned++;
          }
        }
        else
        {
          // we select a random one
          unsigned int randomindex;
          randomindex = (unsigned int) ((0.999999 * random() * availablefs.size()) / RAND_MAX);
          eos_static_debug("trying random index %d", randomindex);

          for (unsigned int i = 0; i < randomindex; i++)
          {
            ait++;
            if (ait == availablevector.end())
              ait = availablevector.begin();
          }


          float fsweight = availablefs[*ait];

          // only when we need one more geo location, we lower the selection probability
          if ((hasgeolocation) && (n_geolocations != 1) && (selected_geo_location == availablefsgeolocation[*ait]))
          {
            // we reduce the probability to select a filesystem in an already existing location to 1/20th
            fsweight *= 0.05;
          }

          if (fsweight > randomacceptor)
          {
            // push it on the selection list
            selected_filesystems.push_back(*ait);
            if (hasgeolocation)
            {
              if (selected_geo_location != availablefsgeolocation[*ait])
              {
                n_geolocations++;
              }
            }

            eos_static_debug("fs %u selected for %d. replica", *ait, nassigned + 1);

            // remove it from the selection map
            availablefs.erase(*ait);
            ait = availablevector.erase(ait);
            nassigned++;
            if (ait == availablevector.end())
              ait = availablevector.begin();
          }
        }
        if (nassigned >= nfilesystems)
          break;
      } // leave the <loop> where filesystems get selected by weight
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

    if (nassigned >= nfilesystems)
    {
      // leave the group loop - we got enough
      break;
    }

    selected_filesystems.clear();
    nassigned = 0;

    if (forced_scheduling_group_index >= 0)
    {
      // in this case we leave, the requested one was tried and we finish here
      break;
    }
  }

  if (nassigned == nfilesystems)
  {
    // now we reshuffle the order using a random number
    unsigned int randomindex;
    randomindex = (unsigned int) ((0.999999 * random() * selected_filesystems.size()) / RAND_MAX);

    std::vector<unsigned int> randomselectedfs;
    randomselectedfs = selected_filesystems;

    selected_filesystems.clear();

    int rrsize = randomselectedfs.size();
    for (int i = 0; i < rrsize; i++)
    {
      selected_filesystems.push_back(randomselectedfs[(randomindex + i) % rrsize]);
    }
    return 0;
  }
  else
  {
    selected_filesystems.clear();
    return ENOSPC;
  }
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


  // the caller routing has to lock via => eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex) !!!

  int returnCode = 0;

  // --------------------------------------------------------------------------------
  // ! PLAIN Layout Scheduler
  // --------------------------------------------------------------------------------

  if (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kPlain)
  {
    // we have one or more replica's ... find the best place to schedule this IO
    if (locationsfs.size() && locationsfs[0])
    {
      eos::common::FileSystem* filesystem = 0;
      if (FsView::gFsView.mIdView.count(locationsfs[0]))
      {
        filesystem = FsView::gFsView.mIdView[locationsfs[0]];
      }

      std::set<eos::common::FileSystem::fsid_t> availablefs;

      if (!filesystem)
        return ENODATA;

      // take filesystem snapshot
      eos::common::FileSystem::fs_snapshot_t snapshot;
      // we are already in a locked section

      FileSystem* fs = FsView::gFsView.mIdView[locationsfs[0]];

      fs->SnapShotFileSystem(snapshot, false);

      if (isRW)
      {
        if ((snapshot.mStatus == eos::common::FileSystem::kBooted) &&
            (snapshot.mConfigStatus >= eos::common::FileSystem::kWO) &&
            (snapshot.mErrCode == 0) && // this we probably don't need 
            (fs->GetActiveStatus(snapshot)) && // this checks the heartbeat and the group & node are enabled
            (fs->ReserveSpace(snapshot, bookingsize)))
        {
          // perfect!
          fsindex = 0;
          eos_static_debug("selected plain file access via filesystem %u", locationsfs[0]);
          return returnCode;
        }
        else
        {
          // check if we are in any kind of no-update mode
          if ((snapshot.mConfigStatus == eos::common::FileSystem::kRO) ||
              (snapshot.mConfigStatus == eos::common::FileSystem::kWO))
          {
            return EROFS;
          }

          // we are off the wire
          return ENONET;
        }
      }
      else
      {
        if ((snapshot.mStatus == eos::common::FileSystem::kBooted) &&
            (snapshot.mConfigStatus >= min_fsstatus) &&
            (snapshot.mErrCode == 0) && // this we probably don't need 
            (fs->GetActiveStatus(snapshot)))
        {

          // perfect!
          fsindex = 0;
          return returnCode;
        }
        else
        {
          return ENONET;
        }
      }
    }
    else
    {
      return ENODATA;
    }
  }

  // --------------------------------------------------------------------------------
  // ! REPLICA/RAID Layout Scheduler
  // --------------------------------------------------------------------------------

  if ((eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kReplica) ||
      (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kRaidDP) ||
      (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kArchive) ||
      (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kRaid6))
  {
    std::set<eos::common::FileSystem::fsid_t> availablefs;
    std::multimap<double, eos::common::FileSystem::fsid_t> availablefsweightsort;

    double renorm = 0; // this is the sum of all weights, we renormalize each weight in the selection with this sum

    bool hasgeolocation = false;

    if (vid.geolocation.length())
    {
      hasgeolocation = true;
    }

    // -----------------------------------------------------------------------
    // check all the locations - for write we need all - for read atleast one
    // -----------------------------------------------------------------------
    for (size_t i = 0; i < locationsfs.size(); i++)
    {
      FileSystem* filesystem = 0;

      if (FsView::gFsView.mIdView.count(locationsfs[i]))
      {
        filesystem = FsView::gFsView.mIdView[locationsfs[i]];
      }
      if (!filesystem)
      {
        if (isRW)
          return ENONET;
        else
          continue;
      }

      // take filesystem snapshot
      eos::common::FileSystem::fs_snapshot_t snapshot;
      // we are already in a locked section

      FileSystem* fs = FsView::gFsView.mIdView[locationsfs[i]];
      fs->SnapShotFileSystem(snapshot, false);

      if (isRW)
      {
        if ((snapshot.mStatus == eos::common::FileSystem::kBooted) &&
            (snapshot.mConfigStatus >= eos::common::FileSystem::kWO) &&
            (snapshot.mErrCode == 0) && // this we probably don't need 
            (fs->GetActiveStatus(snapshot)) && // this checks the heartbeat and the group & node are enabled
            (fs->ReserveSpace(snapshot, bookingsize)))
        {
          // perfect!
          availablefs.insert(snapshot.mId);

          // the weight is given mainly by the disk performance and the network load has a weaker impact (sqrt)
          double weight = (1.0 - snapshot.mDiskUtilization);
          double netweight = (1.0 - ((snapshot.mNetEthRateMiB) ? (snapshot.mNetInRateMiB / snapshot.mNetEthRateMiB) : 0.0));
          weight *= ((netweight > 0) ? sqrt(netweight) : 0);

          availablefsweightsort.insert(std::pair<double, eos::common::FileSystem::fsid_t > (weight, snapshot.mId));
          renorm += weight;
        }
        else
        {
          // check if we are in any kind of no-update mode
          if ((snapshot.mConfigStatus == eos::common::FileSystem::kRO) ||
              (snapshot.mConfigStatus == eos::common::FileSystem::kWO))
          {
            return EROFS;
          }

          // we are off the wire
          return ENONET;
        }
      }
      else
      {
        if ((snapshot.mStatus == eos::common::FileSystem::kBooted) &&
            (snapshot.mConfigStatus >= min_fsstatus) &&
            (snapshot.mErrCode == 0) && // this we probably don't need 
            (snapshot.mActiveStatus))
        {
          availablefs.insert(snapshot.mId);

          // the weight is given mainly by the disk performance and the network load has a weaker impact (sqrt)
          double weight = (1.0 - snapshot.mDiskUtilization);
          double netweight = (1.0 - ((snapshot.mNetEthRateMiB) ? (snapshot.mNetOutRateMiB / snapshot.mNetEthRateMiB) : 0.0));
          weight *= ((netweight > 0) ? sqrt(netweight) : 0);
          // drain patch
          if (snapshot.mConfigStatus == eos::common::FileSystem::kDrain)
          {
            // we set a low weight for drain filesystems if there is more than one replica
            if (locationsfs.size() == 1)
            {
              weight = 1.0;
            }
            else
            {
              // this is a protection to get atleast something selected even if the weights are small
              if (weight > 0.1)
                weight = 0.1;
            }
          }

          // geo patch
          if (hasgeolocation)
          {
            if (snapshot.mGeoTag != vid.geolocation)
            {
              // we reduce the probability to 1/10th
              weight *= 0.1;
            }
          }

          availablefsweightsort.insert(std::pair<double, eos::common::FileSystem::fsid_t > (weight, snapshot.mId));
          renorm += weight;

	  if ((!(forcedfsid > 0)) && (snapshot.mHost == vid.host.substr(0, snapshot.mHost.length())))
          {
            // if the client sit's on an FST we force this file system
            forcedfsid = snapshot.mId;
            eos_static_info("msg=\"enforcing local replica access\" client=\"%s\"", vid.host.c_str());
          }

          eos_static_debug("weight=%f netweight=%f renorm=%f disk-geotag=%s client-geotag=%s id=%d utilization=%f\n", weight, netweight, renorm, snapshot.mGeoTag.c_str(), vid.geolocation.c_str(), snapshot.mId, snapshot.mDiskUtilization);
        }
        else
        {
          // -----------------------------------------------------------------------
          // we store not available filesystems in the unavail vector 
          // - this matters for RAID layouts because we have to remove there URLs to let the RAID driver use only online stripes
          // -----------------------------------------------------------------------
          unavailfs.push_back(snapshot.mId);
        }
      }
    }

    eos_static_debug("Requesting %d/%d replicas to be online\n", availablefs.size(), eos::common::LayoutId::GetMinOnlineReplica(lid));
    // -----------------------------------------------------------------------
    // check if there are enough stripes available for a read operation of the given layout
    // -----------------------------------------------------------------------
    if (availablefs.size() < eos::common::LayoutId::GetMinOnlineReplica(lid))
    {
      return ENONET;
    }

    if ((eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kRaidDP) ||
        (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kRaid6) ||
        (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kArchive))
    {
      if (availablefs.size() != eos::common::LayoutId::GetOnlineStripeNumber(lid))
      {
        // -----------------------------------------------------------------------
        // check if there are enough stripes available for the layout type chosen
        // if not we set EXDEV as return code indicating that the caller needs
        // to place new stripes for each unavailfs entry
        // -----------------------------------------------------------------------
        returnCode = EXDEV;
      }
    }

    // -----------------------------------------------------------------------
    // for write we can just return if they are all available, otherwise we would never go here ....
    // -----------------------------------------------------------------------
    if (isRW)
    {
      fsindex = 0;
      return returnCode;
    }

    // -----------------------------------------------------------------------
    // if there was a forced one, see if it is there
    // -----------------------------------------------------------------------
    if (forcedfsid > 0)
    {
      if (availablefs.count(forcedfsid) == 1)
      {
        for (size_t i = 0; i < locationsfs.size(); i++)
        {
          if (locationsfs[i] == forcedfsid)
          {
            fsindex = i;
            return returnCode;
          }
        }
        // uuh! - this should NEVER happen!
        eos_static_crit("fatal inconsistency in scheduling - file system missing after selection of forced fsid");
        return EIO;
      }
      return ENONET;
    }

    if (!renorm)
    {
      renorm = 1.0;
    }

    // -----------------------------------------------------------------------
    // if there was none available, return
    // -----------------------------------------------------------------------
    if (!availablefs.size())
    {
      return ENONET;
    }

    // -----------------------------------------------------------------------
    // if there was only one available, use that one
    // -----------------------------------------------------------------------
    if (availablefs.size() == 1)
    {
      for (size_t i = 0; i < locationsfs.size(); i++)
      {
        if (locationsfs[i] == *(availablefs.begin()))
        {
          fsindex = i;
          return returnCode;
        }
      }
      // uuh! - this should NEVER happen!
      eos_static_crit("fatal inconsistency in scheduling - file system missing after selection of single replica");
      return EIO;
    }

    // ------------------------------------------------------------------------------------------
    // if we have geo location tags, we reweight the possible fs according to their geo location
    // ------------------------------------------------------------------------------------------


    // -----------------------------------------------------------------------
    // now start with the one with the highest weight, but still use probabilty to select it
    // -----------------------------------------------------------------------
    std::multimap<double, eos::common::FileSystem::fsid_t>::reverse_iterator wit;
    for (wit = availablefsweightsort.rbegin(); wit != availablefsweightsort.rend(); wit++)
    {
      float randomacceptor = (0.999999 * random() / RAND_MAX);
      eos_static_debug("random acceptor=%.02f norm=%.02f weight=%.02f normweight=%.02f fsid=%u", randomacceptor, renorm, wit->first, wit->first / renorm, wit->second);

      if ((wit->first / renorm) > randomacceptor)
      {
        // take this
        for (size_t i = 0; i < locationsfs.size(); i++)
        {
          if (locationsfs[i] == wit->second)
          {
            fsindex = i;
            return returnCode;
          }
        }
        // uuh! - this should NEVER happen!
        eos_static_crit("fatal inconsistency in scheduling - file system missing after selection in randomacceptor");
        return EIO;
      }
    }
    // -----------------------------------------------------------------------
    // if we don't succeed by the randomized weight, we return the one with the highest weight
    // -----------------------------------------------------------------------

    for (size_t i = 0; i < locationsfs.size(); i++)
    {
      if (locationsfs[i] == availablefsweightsort.begin()->second)
      {
        fsindex = i;
        return returnCode;
      }
    }
    // uuh! - this should NEVER happen!
    eos_static_crit("fatal inconsistency in scheduling - file system missing after selection");
    return EIO;
  }

  return EINVAL;
}

EOSMGMNAMESPACE_END
