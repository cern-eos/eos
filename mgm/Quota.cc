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

/*----------------------------------------------------------------------------*/
#include "mgm/Quota.hh"
#include "mgm/Policy.hh"
#include "mgm/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/
#include <errno.h>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

std::map<std::string, SpaceQuota*> Quota::pMapQuota;
eos::common::RWMutex Quota::pMapMutex;
gid_t Quota::gProjectId = 99;

#ifdef __APPLE__
#define ENONET 64
#endif

//------------------------------------------------------------------------------
// *** Class SpaceQuota implementaion ***
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor - requires the eosViewRWMutex write-lock
//------------------------------------------------------------------------------
SpaceQuota::SpaceQuota(const char* path):
  mEnabled(false),
  mQuotaNode(0),
  mLastEnableCheck(0),
  mLayoutSizeFactor(1.0),
  mDirtyTarget(true)
{
  SpaceName = path;
  eos::IContainerMD* quotadir = 0;

  try
  {
    quotadir = gOFS->eosView->getContainer(path);
  }
  catch (eos::MDException& e)
  {
    quotadir = 0;
  }

  if (!quotadir)
  {
    try
    {
      quotadir = gOFS->eosView->createContainer(path, true);
      quotadir->setMode(S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
      gOFS->eosView->updateContainerStore(quotadir);
    }
    catch (eos::MDException& e)
    {
      eos_static_crit("Cannot create quota directory %s", path);
    }
  }
  else
  {
    try
    {
      mQuotaNode = gOFS->eosView->getQuotaNode(quotadir, false);
      eos_static_info("Found ns quota node for path=%s", path);
    }
    catch (eos::MDException& e)
    {
      mQuotaNode = 0;
    }

    if (!mQuotaNode)
    {
      try
      {
	mQuotaNode = gOFS->eosView->registerQuotaNode(quotadir);
      }
      catch (eos::MDException &e)
      {
	mQuotaNode = 0;
	eos_static_crit("Cannot register quota node %s", path);
      }
    }
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
SpaceQuota::~SpaceQuota() { }

//------------------------------------------------------------------------------
// Check if quota is enabled
//------------------------------------------------------------------------------
bool SpaceQuota::IsEnabled()
{
  time_t now = time(NULL);

  if (now > (mLastEnableCheck + 5))
  {
    mLastEnableCheck = now;
    std::string spacename = SpaceName.c_str();
    std::string key = "quota";

    if (FsView::gFsView.mSpaceView.count(spacename))
    {
      std::string ison = FsView::gFsView.mSpaceView[spacename]->GetConfigMember(key);
      mEnabled = (ison == "on");
    }
    else
    {
      mEnabled = false;
    }
  }

  return mEnabled;
}

//------------------------------------------------------------------------------
// Get quota status
//------------------------------------------------------------------------------
const char*
SpaceQuota::GetQuotaStatus(unsigned long long is, unsigned long long avail)
{
  if (!avail)
    return "ignored";

  double p = (100.0 * is / avail);

  if (p < 90)
    return "ok";
  else if (p < 99)
    return "warning";
  else
    return "exceeded";
}

//------------------------------------------------------------------------------
// Get current quota value as percentage of the available one
//------------------------------------------------------------------------------
const char*
SpaceQuota::GetQuotaPercentage(unsigned long long is, unsigned long long avail,
			       XrdOucString& spercentage)
{
  char percentage[1024];
  float fp = avail ? (100.0 * is / avail) : 100.0;

  if (fp > 100.0)
    fp = 100.0;

  if (fp < 0)
    fp = 0;

  sprintf(percentage, "%.02f", fp);
  spercentage = percentage;
  return spercentage.c_str();
}

//------------------------------------------------------------------------------
// Update ns quota node address referred to by current space quota
//------------------------------------------------------------------------------
bool
SpaceQuota::UpdateQuotaNodeAddress()
{
  try
  {
    eos::IContainerMD* quotadir = gOFS->eosView->getContainer(SpaceName.c_str());
    mQuotaNode = gOFS->eosView->getQuotaNode(quotadir, false);

    if (!mQuotaNode)
      return false;
  }
  catch (eos::MDException& e)
  {
    mQuotaNode = 0;
    return false;
  }

  return true;
}


//------------------------------------------------------------------------------
// Calculate the size factor used to estimate the logical available bytes
//------------------------------------------------------------------------------
void
SpaceQuota::UpdateLogicalSizeFactor()
{
  XrdOucErrInfo error;
  eos::common::Mapping::VirtualIdentity vid;
  eos::common::Mapping::Root(vid);
  vid.sudoer = 1;
  eos::IContainerMD::XAttrMap map;
  int retc = gOFS->_attr_ls(SpaceName.c_str(), error, vid, 0, map, false);

  if (!retc)
  {
    unsigned long layoutId;
    XrdOucEnv env;
    unsigned long forcedfsid;
    long forcedgroup;
    XrdOucString spn = SpaceName;
    // get the layout in this quota node
    Policy::GetLayoutAndSpace(SpaceName.c_str(), map, vid, layoutId, spn, env,
			      forcedfsid, forcedgroup);
    mLayoutSizeFactor = eos::common::LayoutId::GetSizeFactor(layoutId);
  }
  else
  {
    mLayoutSizeFactor = 1.0;
  }

  // Protect for division by 0
  if (mLayoutSizeFactor < 1.0)
    mLayoutSizeFactor = 1.0;
}

//------------------------------------------------------------------------------
// Remove quota
//------------------------------------------------------------------------------
bool
SpaceQuota::RmQuota(unsigned long tag, unsigned long id)
{
  eos_static_debug("rm quota tag=%lu id=%lu", tag, id);
  XrdSysMutexHelper scope_lock(mMutex);

  if (mMapIdQuota.count(Index(tag, id)))
  {
    mMapIdQuota.erase(Index(tag, id));
    mDirtyTarget = true;
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Get quota value
//------------------------------------------------------------------------------
long long
SpaceQuota::GetQuota(unsigned long tag, unsigned long id)
{
  XrdSysMutexHelper scope_lock(mMutex);
  return static_cast<long long>(mMapIdQuota[Index(tag, id)]);
}

//------------------------------------------------------------------------------
// Set quota
//------------------------------------------------------------------------------
void
SpaceQuota::SetQuota(unsigned long tag, unsigned long id, unsigned long long value)
{
  eos_static_debug("set quota tag=%lu id=%lu value=%llu", tag, id, value);
  XrdSysMutexHelper scope_lock(mMutex);
  mMapIdQuota[Index(tag, id)] = value;

  if ((tag == kUserBytesTarget) ||
      (tag == kGroupBytesTarget) ||
      (tag == kUserFilesTarget) ||
      (tag == kGroupFilesTarget) ||
      (tag == kUserLogicalBytesTarget) ||
      (tag == kGroupLogicalBytesTarget))
  {
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
      (tag == kGroupLogicalBytesTarget))
  {
    mDirtyTarget = true;
  }
}

//------------------------------------------------------------------------------
// Add quota
//------------------------------------------------------------------------------
void
SpaceQuota::AddQuota(unsigned long tag, unsigned long id, long long value)
{
  eos_static_debug("add quota tag=%lu id=%lu value=%llu", tag, id, value);

  // Avoid negative numbers
  if (((long long) mMapIdQuota[Index(tag, id)] + value) >= 0)
    mMapIdQuota[Index(tag, id)] += value;

  eos_static_debug("sum quota tag=%lu id=%lu value=%llu", tag, id,
		   mMapIdQuota[Index(tag, id)]);

}

//------------------------------------------------------------------------------
// Update
//------------------------------------------------------------------------------
void
SpaceQuota::UpdateTargetSums()
{
  if (!mDirtyTarget)
    return;

  eos_static_debug("updating targets");
  XrdSysMutexHelper scope_lock(mMutex);
  mDirtyTarget = false;
  mMapIdQuota[Index(kAllUserBytesTarget, 0)] = 0;
  mMapIdQuota[Index(kAllUserFilesTarget, 0)] = 0;
  mMapIdQuota[Index(kAllGroupBytesTarget, 0)] = 0;
  mMapIdQuota[Index(kAllGroupFilesTarget, 0)] = 0;
  mMapIdQuota[Index(kAllUserLogicalBytesTarget, 0)] = 0;
  mMapIdQuota[Index(kAllGroupLogicalBytesTarget, 0)] = 0;

  for (auto it = mMapIdQuota.begin(); it != mMapIdQuota.end(); it++)
  {
    if ((UnIndex(it->first) == kUserBytesTarget))
    {
      AddQuota(kAllUserBytesTarget, 0, it->second);
      AddQuota(kAllUserLogicalBytesTarget, 0, it->second / mLayoutSizeFactor);
    }

    if ((UnIndex(it->first) == kUserFilesTarget))
      AddQuota(kAllUserFilesTarget, 0, it->second);

    if ((UnIndex(it->first) == kGroupBytesTarget))
    {
      AddQuota(kAllGroupBytesTarget, 0, it->second);
      AddQuota(kAllGroupLogicalBytesTarget, 0, it->second / mLayoutSizeFactor);
    }

    if ((UnIndex(it->first) == kGroupFilesTarget))
      AddQuota(kAllGroupFilesTarget, 0, it->second);
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
SpaceQuota::UpdateIsSums()
{
  eos_static_debug("updating IS values");

  XrdSysMutexHelper scope_lock(mMutex);
  mMapIdQuota[Index(kAllUserBytesIs, 0)] = 0;
  mMapIdQuota[Index(kAllUserLogicalBytesIs, 0)] = 0;
  mMapIdQuota[Index(kAllUserFilesIs, 0)] = 0;
  mMapIdQuota[Index(kAllGroupBytesIs, 0)] = 0;
  mMapIdQuota[Index(kAllGroupFilesIs, 0)] = 0;
  mMapIdQuota[Index(kAllGroupLogicalBytesIs, 0)] = 0;
  mMapIdQuota[Index(kAllUserBytesIs, 0)] = 0;
  mMapIdQuota[Index(kAllUserLogicalBytesIs, 0)] = 0;
  mMapIdQuota[Index(kAllUserFilesIs, 0)] = 0;
  mMapIdQuota[Index(kAllGroupBytesIs, 0)] = 0;
  mMapIdQuota[Index(kAllGroupFilesIs, 0)] = 0;
  mMapIdQuota[Index(kAllGroupLogicalBytesIs, 0)] = 0;

  for (auto it = mMapIdQuota.begin(); it != mMapIdQuota.end(); it++)
  {
    if ((UnIndex(it->first) == kUserBytesIs))
      AddQuota(kAllUserBytesIs, 0, it->second);

    if ((UnIndex(it->first) == kUserLogicalBytesIs))
      AddQuota(kAllUserLogicalBytesIs, 0, it->second);

    if ((UnIndex(it->first) == kUserFilesIs))
      AddQuota(kAllUserFilesIs, 0, it->second);

    if ((UnIndex(it->first) == kGroupBytesIs))
      AddQuota(kAllGroupBytesIs, 0, it->second);

    if ((UnIndex(it->first) == kGroupLogicalBytesIs))
      AddQuota(kAllGroupLogicalBytesIs, 0, it->second);

    if ((UnIndex(it->first) == kGroupFilesIs))
      AddQuota(kAllGroupFilesIs, 0, it->second);
  }
}

//------------------------------------------------------------------------------
// Update uid/gid values from quota node
//------------------------------------------------------------------------------
void
SpaceQuota::UpdateFromQuotaNode(uid_t uid, gid_t gid, bool upd_proj_quota)
{
  eos_static_debug("updating uid/gid values from quota node");
  XrdSysMutexHelper scope_lock(mMutex);

  if (mQuotaNode)
  {
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

    if (upd_proj_quota)
    {
      // Recalculate the project quota only every 5 seconds to boost perf.
      static XrdSysMutex lMutex;
      static time_t lUpdateTime = 0;
      bool docalc = false;
      {
	XrdSysMutexHelper lock(lMutex);
	time_t now = time(NULL);

	if (lUpdateTime < now)
	{
	  // Next recalculation in 5 second
	  docalc = true;
	  lUpdateTime = now + 5;
	}
      }

      if (docalc)
      {
	mMapIdQuota[Index(kGroupBytesIs, Quota::gProjectId)] = 0;
	mMapIdQuota[Index(kGroupFilesIs, Quota::gProjectId)] = 0;
	mMapIdQuota[Index(kGroupLogicalBytesIs, Quota::gProjectId)] = 0;

	// Loop over users and fill project quota
	for (auto itu = mQuotaNode->userUsageBegin();
	     itu != mQuotaNode->userUsageEnd(); ++itu)
	{
	  AddQuota(kGroupBytesIs, Quota::gProjectId, itu->second.physicalSpace);
	  AddQuota(kGroupLogicalBytesIs, Quota::gProjectId, itu->second.space);
	  AddQuota(kGroupFilesIs, Quota::gProjectId, itu->second.files);
	}
      }
    }
  }
}

//------------------------------------------------------------------------------
// Refresh counters
//------------------------------------------------------------------------------
void
SpaceQuota::Refresh()
{
  NsQuotaToSpaceQuota();
  UpdateLogicalSizeFactor();
  UpdateIsSums();
  UpdateTargetSums();
}

//------------------------------------------------------------------------------
// Print quota information
//------------------------------------------------------------------------------
void
SpaceQuota::PrintOut(XrdOucString& output, long uid_sel, long gid_sel,
		     bool monitoring, bool translate_ids)
{
  using eos::common::StringConversion;
  char headerline[4096];
  // Make a map containing once all the defined uid's and gid's
  std::set<unsigned long> set_uids, set_gids;
  XrdOucString header;

  {
    XrdSysMutexHelper scope_lock(mMutex);

    // For project space we just print the user/group entry gProjectId
    if (mMapIdQuota[Index(kGroupBytesTarget, Quota::gProjectId)] > 0)
      gid_sel = Quota::gProjectId;

    for (auto it = mMapIdQuota.begin(); it != mMapIdQuota.end(); it++)
    {
      if ((UnIndex(it->first) >= kUserBytesIs)
	  && (UnIndex(it->first) <= kUserFilesTarget))
      {
	eos_static_debug("adding %llx to print list ", UnIndex(it->first));
	unsigned long ugid = (it->first) & 0xffffffff;

	// uid selection filter
	if ((uid_sel >= 0) && (ugid != (unsigned long) uid_sel))
	  continue;

	// we don't print the users if a gid is selected
	if (gid_sel >= 0)
	  continue;

	set_uids.insert(ugid);
      }

      if ((UnIndex(it->first) >= kGroupBytesIs)
	  && (UnIndex(it->first) <= kGroupFilesTarget))
      {
	unsigned long ugid = (it->first) & 0xffffffff;

	// uid selection filter
	if ((gid_sel >= 0) && (ugid != (unsigned long) gid_sel))
	  continue;

	// We don't print the group if a uid is selected
	if (uid_sel >= 0)
	  continue;

	set_gids.insert(ugid);
      }
    }
  }

  std::vector<unsigned long> uids (set_uids.begin(), set_uids.end());
  std::vector<unsigned long> gids (set_gids.begin(), set_gids.end());

  // Sort the uids and gids
  std::sort(uids.begin(), uids.end());
  std::sort(gids.begin(), gids.end());
  eos_static_debug("sorted");

  for (unsigned int i = 0; i < uids.size(); ++i)
    eos_static_debug("sort %d %d", i, uids[i]);

  for (unsigned int i = 0; i < gids.size(); ++i)
    eos_static_debug("sort %d %d", i, gids[i]);

  // Uid and gid output lines
  std::vector <std::string> uidout;
  std::vector <std::string> gidout;

  if (!monitoring)
  {
    header += "# ___________________________________________________________"
      "____________________________________\n";
    sprintf(headerline, "# ==> Quota Node: %-16s\n", SpaceName.c_str());
    header += headerline;
    header += "# ___________________________________________________________"
      "____________________________________\n";
  }

  // Print the header for selected uid/gid's only if there is something to print
  // If we have a full listing we print even empty quota nodes (=header only)
  if (((uid_sel < 0) && (gid_sel < 0)) || !uids.empty() || !gids.empty())
    output += header;

  if (!uids.empty())
  {
    // Table header
    if (!monitoring)
    {
      sprintf(headerline, "%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s "
	      "%-10s %-10s\n", GetTagCategory(kUserBytesIs), GetTagName(kUserBytesIs),
	      GetTagName(kUserLogicalBytesIs), GetTagName(kUserFilesIs),
	      GetTagName(kUserBytesTarget), GetTagName(kUserLogicalBytesTarget),
	      GetTagName(kUserFilesTarget), "filled[%]", "vol-status", "ino-status");
      output += headerline;
    }
  }

  for (unsigned int lid = 0; lid < uids.size(); lid++)
  {
    eos_static_debug("loop with id=%d", lid);
    XrdOucString value1 = "";
    XrdOucString value2 = "";
    XrdOucString value3 = "";
    XrdOucString value4 = "";
    XrdOucString value5 = "";
    XrdOucString value6 = "";
    XrdOucString id = "";
    id += std::to_string((long long unsigned int)uids[lid]).c_str();

    if (translate_ids)
    {
      if (gid_sel == Quota::gProjectId)
      {
	id = "project";
      }
      else
      {
	int errc = 0;
	std::string username = eos::common::Mapping::UidToUserName(uids[lid], errc);
	char uidlimit[16];

	if (username.length())
	{
	  snprintf(uidlimit, 11, "%s", username.c_str());
	  id = uidlimit;
	}
      }
    }

    XrdOucString percentage = "";

    if (!monitoring)
    {
      sprintf(headerline,
	      "%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", id.c_str(),
	      StringConversion::GetReadableSizeString(value1,
		  GetQuota(kUserBytesIs, uids[lid]), "B"),
	      StringConversion::GetReadableSizeString(value2,
		  GetQuota(kUserLogicalBytesIs, uids[lid]), "B"),
	      StringConversion::GetReadableSizeString(value3,
		  GetQuota(kUserFilesIs, uids[lid]), "-"),
	      StringConversion::GetReadableSizeString(value4,
		  GetQuota(kUserBytesTarget, uids[lid]), "B"),
	      StringConversion::GetReadableSizeString(value5,
		  (long long)(GetQuota(kUserBytesTarget, uids[lid]) / mLayoutSizeFactor),
		  "B"),
	      StringConversion::GetReadableSizeString(value6,
		  GetQuota(kUserFilesTarget, uids[lid]), "-"),
	      GetQuotaPercentage(GetQuota(kUserBytesIs, uids[lid]),
				 GetQuota(kUserBytesTarget, uids[lid]), percentage),
	      GetQuotaStatus(GetQuota(kUserBytesIs, uids[lid]),
			     GetQuota(kUserBytesTarget, uids[lid])),
	      GetQuotaStatus(GetQuota(kUserFilesIs, uids[lid]),
			     GetQuota(kUserFilesTarget, uids[lid])));
    }
    else
    {
      sprintf(headerline, "quota=node uid=%s space=%s usedbytes=%llu "
	      "usedlogicalbytes=%llu usedfiles=%llu maxbytes=%llu "
	      "maxlogicalbytes=%llu maxfiles=%llu percentageusedbytes=%s "
	      "statusbytes=%s statusfiles=%s\n", id.c_str(), SpaceName.c_str(),
	      GetQuota(kUserBytesIs, uids[lid]),
	      GetQuota(kUserLogicalBytesIs, uids[lid]),
	      GetQuota(kUserFilesIs, uids[lid]),
	      GetQuota(kUserBytesTarget, uids[lid]),
	      (unsigned long long)(GetQuota(kUserBytesTarget,
					    uids[lid]) / mLayoutSizeFactor),
	      GetQuota(kUserFilesTarget, uids[lid]),
	      GetQuotaPercentage(GetQuota(kUserBytesIs, uids[lid]),
				 GetQuota(kUserBytesTarget, uids[lid]), percentage),
	      GetQuotaStatus(GetQuota(kUserBytesIs, uids[lid]),
			     GetQuota(kUserBytesTarget, uids[lid])),
	      GetQuotaStatus(GetQuota(kUserFilesIs, uids[lid]),
			     GetQuota(kUserFilesTarget, uids[lid])));
    }

    if (!translate_ids)
      output += headerline;
    else
      uidout.push_back(headerline);
  }

  if (translate_ids)
  {
    std::sort(uidout.begin(), uidout.end());

    for (size_t i = 0; i < uidout.size(); i++)
      output += uidout[i].c_str();
  }

  if (!gids.empty())
  {
    // group loop
    if (!monitoring)
    {
      output += "# ........................................................."
		"......................................\n";
      sprintf(headerline,
	      "%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n",
	      GetTagCategory(kGroupBytesIs), GetTagName(kGroupBytesIs),
	      GetTagName(kGroupLogicalBytesIs), GetTagName(kGroupFilesIs),
	      GetTagName(kGroupBytesTarget), GetTagName(kGroupLogicalBytesTarget),
	      GetTagName(kGroupFilesTarget), "filled[%]", "vol-status", "ino-status");
      output += headerline;
    }
  }

  for (unsigned int lid = 0; lid < gids.size(); lid++)
  {
    eos_static_debug("loop with id=%d", lid);
    XrdOucString value1 = "";
    XrdOucString value2 = "";
    XrdOucString value3 = "";
    XrdOucString value4 = "";
    XrdOucString value5 = "";
    XrdOucString value6 = "";
    XrdOucString id = "";
    id += std::to_string((long long unsigned int)gids[lid]).c_str();

    if (translate_ids)
    {
      if (gid_sel == Quota::gProjectId)
      {
	id = "project";
      }
      else
      {
	int errc = 0;
	std::string groupname = eos::common::Mapping::GidToGroupName(gids[lid],
				errc);
	char gidlimit[16];

	if (groupname.length())
	{
	  snprintf(gidlimit, 11, "%s", groupname.c_str());
	  id = gidlimit;
	}
      }
    }

    XrdOucString percentage = "";

    if (!monitoring)
    {
      sprintf(headerline,
	      "%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", id.c_str(),
	      StringConversion::GetReadableSizeString(value1,
		  GetQuota(kGroupBytesIs, gids[lid]), "B"),
	      StringConversion::GetReadableSizeString(value2,
		  GetQuota(kGroupLogicalBytesIs, gids[lid]), "B"),
	      StringConversion::GetReadableSizeString(value3,
		  GetQuota(kGroupFilesIs, gids[lid]), "-"),
	      StringConversion::GetReadableSizeString(value4,
		  GetQuota(kGroupBytesTarget, gids[lid]), "B"),
	      StringConversion::GetReadableSizeString(value5,
		  (long long)(GetQuota(kGroupBytesTarget, gids[lid]) / mLayoutSizeFactor),
		  "B"),
	      StringConversion::GetReadableSizeString(value6,
		  GetQuota(kGroupFilesTarget, gids[lid]), "-"),
	      GetQuotaPercentage(GetQuota(kGroupBytesIs, gids[lid]),
				 GetQuota(kGroupBytesTarget, gids[lid]), percentage),
	      GetQuotaStatus(GetQuota(kGroupBytesIs, gids[lid]),
			     GetQuota(kGroupBytesTarget, gids[lid])),
	      GetQuotaStatus(GetQuota(kGroupFilesIs, gids[lid]),
			     GetQuota(kGroupFilesTarget, gids[lid])));
    }
    else
    {
      sprintf(headerline,
	      "quota=node gid=%s space=%s usedbytes=%llu usedlogicalbytes=%llu "
	      "usedfiles=%llu maxbytes=%llu maxlogicalbytes=%llu maxfiles=%llu "
	      "percentageusedbytes=%s statusbytes=%s statusfiles=%s\n",
	      id.c_str(), SpaceName.c_str(),
	      GetQuota(kGroupBytesIs, gids[lid]),
	      GetQuota(kGroupLogicalBytesIs, gids[lid]),
	      GetQuota(kGroupFilesIs, gids[lid]),
	      GetQuota(kGroupBytesTarget, gids[lid]),
	      (unsigned long long)(GetQuota(kGroupBytesTarget,
					    gids[lid]) / mLayoutSizeFactor),
	      GetQuota(kGroupFilesTarget, gids[lid]),
	      GetQuotaPercentage(GetQuota(kGroupBytesIs, gids[lid]),
				 GetQuota(kGroupBytesTarget, gids[lid]), percentage),
	      GetQuotaStatus(GetQuota(kGroupBytesIs, gids[lid]),
			     GetQuota(kGroupBytesTarget, gids[lid])),
	      GetQuotaStatus(GetQuota(kGroupFilesIs, gids[lid]),
			     GetQuota(kGroupFilesTarget, gids[lid])));
    }

    if (!translate_ids)
      output += headerline;
    else
      gidout.push_back(headerline);
  }

  if (translate_ids)
  {
    std::sort(gidout.begin(), gidout.end());

    for (size_t i = 0; i < gidout.size(); i++)
      output += gidout[i].c_str();
  }

  if ((uid_sel < 0) && (gid_sel < 0))
  {
    if (!monitoring)
    {
      output += "# ---------------------------------------------------------"
		"-------------------------------------------------\n";
      output += "# ==> Summary\n";
    }

    XrdOucString value1 = "";
    XrdOucString value2 = "";
    XrdOucString value3 = "";
    XrdOucString value4 = "";
    XrdOucString value5 = "";
    XrdOucString value6 = "";
    XrdOucString id = "ALL";
    XrdOucString percentage = "";

    if (!monitoring)
    {
      sprintf(headerline,
	      "%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n",
	      GetTagCategory(kAllUserBytesIs), GetTagName(kAllUserBytesIs),
	      GetTagName(kAllUserLogicalBytesIs), GetTagName(kAllUserFilesIs),
	      GetTagName(kAllUserBytesTarget), GetTagName(kAllUserLogicalBytesTarget),
	      GetTagName(kAllUserFilesTarget), "filled[%]", "vol-status", "ino-status");
      output += headerline;
      sprintf(headerline,
	      "%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", id.c_str(),
	      StringConversion::GetReadableSizeString(value1,
		  GetQuota(kAllUserBytesIs, 0), "B"),
	      StringConversion::GetReadableSizeString(value2,
		  GetQuota(kAllUserLogicalBytesIs, 0), "B"),
	      StringConversion::GetReadableSizeString(value3,
		  GetQuota(kAllUserFilesIs, 0), "-"),
	      StringConversion::GetReadableSizeString(value4,
		  GetQuota(kAllUserBytesTarget, 0), "B"),
	      StringConversion::GetReadableSizeString(value5,
		  GetQuota(kAllUserLogicalBytesTarget, 0), "B"),
	      StringConversion::GetReadableSizeString(value6,
		  GetQuota(kAllUserFilesTarget, 0), "-"),
	      GetQuotaPercentage(GetQuota(kAllUserBytesIs, 0), GetQuota(kAllUserBytesTarget,
				 0), percentage),
	      GetQuotaStatus(GetQuota(kAllUserBytesIs, 0), GetQuota(kAllUserBytesTarget, 0)),
	      GetQuotaStatus(GetQuota(kAllUserFilesIs, 0), GetQuota(kAllUserFilesTarget, 0)));
    }
    else
    {
      sprintf(headerline,
	      "quota=node uid=%s space=%s usedbytes=%llu usedlogicalbytes=%llu "
	      "usedfiles=%llu maxbytes=%llu maxlogicalbytes=%llu maxfiles=%llu "
	      "percentageusedbytes=%s statusbytes=%s statusfiles=%s\n",
	      id.c_str(), SpaceName.c_str(),
	      GetQuota(kAllUserBytesIs, 0),
	      GetQuota(kAllUserLogicalBytesIs, 0),
	      GetQuota(kAllUserFilesIs, 0),
	      GetQuota(kAllUserBytesTarget, 0),
	      GetQuota(kAllUserLogicalBytesTarget, 0),
	      GetQuota(kAllUserFilesTarget, 0),
	      GetQuotaPercentage(GetQuota(kAllUserBytesIs, 0), GetQuota(kAllUserBytesTarget,
				 0), percentage),
	      GetQuotaStatus(GetQuota(kAllUserBytesIs, 0), GetQuota(kAllUserBytesTarget, 0)),
	      GetQuotaStatus(GetQuota(kAllUserFilesIs, 0), GetQuota(kAllUserFilesTarget, 0)));
    }

    output += headerline;

    if (!monitoring)
    {
      sprintf(headerline,
	      "%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n",
	      GetTagCategory(kAllGroupBytesIs), GetTagName(kAllGroupBytesIs),
	      GetTagName(kAllGroupLogicalBytesIs), GetTagName(kAllGroupFilesIs),
	      GetTagName(kAllGroupBytesTarget), GetTagName(kAllGroupLogicalBytesTarget),
	      GetTagName(kAllGroupFilesTarget), "filled[%]", "vol-status", "ino-status");
      output += headerline;
      sprintf(headerline,
	      "%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", id.c_str(),
	      StringConversion::GetReadableSizeString(value1,
		  GetQuota(kAllGroupBytesIs, 0), "B"),
	      StringConversion::GetReadableSizeString(value2,
		  GetQuota(kAllGroupLogicalBytesIs, 0), "B"),
	      StringConversion::GetReadableSizeString(value3,
		  GetQuota(kAllGroupFilesIs, 0), "-"),
	      StringConversion::GetReadableSizeString(value4,
		  GetQuota(kAllGroupBytesTarget, 0), "B"),
	      StringConversion::GetReadableSizeString(value5,
		  GetQuota(kAllGroupLogicalBytesTarget, 0), "B"),
	      StringConversion::GetReadableSizeString(value6,
		  GetQuota(kAllGroupFilesTarget, 0), "-"),
	      GetQuotaPercentage(GetQuota(kAllGroupBytesIs, 0), GetQuota(kAllGroupBytesTarget,
				 0), percentage),
	      GetQuotaStatus(GetQuota(kAllGroupBytesIs, 0), GetQuota(kAllGroupBytesTarget,
			     0)),
	      GetQuotaStatus(GetQuota(kAllGroupFilesIs, 0), GetQuota(kAllGroupFilesTarget,
			     0)));
    }
    else
    {
      sprintf(headerline,
	      "quota=node gid=%s space=%s usedbytes=%llu usedlogicalbytes=%llu "
	      "usedfiles=%llu maxbytes=%llu maxlogicalbytes=%llu maxfiles=%llu "
	      "percentageusedbytes=%s statusbytes=%s statusfiles=%s\n",
	      id.c_str(), SpaceName.c_str(),
	      GetQuota(kAllGroupBytesIs, 0),
	      GetQuota(kAllGroupLogicalBytesIs, 0),
	      GetQuota(kAllGroupFilesIs, 0),
	      GetQuota(kAllGroupBytesTarget, 0),
	      GetQuota(kAllGroupLogicalBytesTarget, 0),
	      GetQuota(kAllGroupFilesTarget, 0),
	      GetQuotaPercentage(GetQuota(kAllGroupBytesIs, 0), GetQuota(kAllGroupBytesTarget,
				 0), percentage),
	      GetQuotaStatus(GetQuota(kAllGroupBytesIs, 0), GetQuota(kAllGroupBytesTarget,
			     0)),
	      GetQuotaStatus(GetQuota(kAllGroupFilesIs, 0), GetQuota(kAllGroupFilesTarget,
			     0)));
    }

    output += headerline;
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
  UpdateFromQuotaNode(uid, gid, GetQuota(kGroupBytesTarget, Quota::gProjectId)
		      ? true : false);
  eos_static_info("uid=%d gid=%d size=%llu quota=%llu", uid, gid, desired_vol,
		  GetQuota(kUserBytesTarget, uid));
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

  if (GetQuota(kUserBytesTarget, uid) > 0)
  {
    userquota = true;
    uservolumequota = true;
  }

  if (GetQuota(kGroupBytesTarget, gid) > 0)
  {
    groupquota = true;
    groupvolumequota = true;
  }

  if (GetQuota(kUserFilesTarget, uid) > 0)
  {
    userquota = true;
    userinodequota = true;
  }

  if (GetQuota(kGroupFilesTarget, gid) > 0)
  {
    groupquota = true;
    groupinodequota = true;
  }

  if (uservolumequota)
  {
    if ((GetQuota(kUserBytesTarget, uid) - GetQuota(kUserBytesIs, uid)) > (long long)desired_vol)
    {
      hasuserquota = true;
    }
    else
    {
      hasuserquota = false;
    }
  }

  if (userinodequota)
  {
    if ((GetQuota(kUserFilesTarget, uid) - GetQuota(kUserFilesIs, uid)) > inodes)
    {
      if (!uservolumequota)
	hasuserquota = true;
    }
    else
    {
      hasuserquota = false;
    }
  }

  if (groupvolumequota)
  {
    if ((GetQuota(kGroupBytesTarget, gid) - GetQuota(kGroupBytesIs, gid)) > desired_vol)
    {
      hasgroupquota = true;
    }
    else
    {
      hasgroupquota = false;
    }
  }

  if (groupinodequota)
  {
    if ((GetQuota(kGroupFilesTarget, gid) - GetQuota(kGroupFilesIs, gid)) > inodes)
    {
      if (!groupvolumequota)
	hasgroupquota = true;
    }
    else
    {
      hasgroupquota = false;
    }
  }

  if (((GetQuota(kGroupBytesTarget, Quota::gProjectId) -
	GetQuota(kGroupBytesIs, Quota::gProjectId)) > desired_vol) &&
      ((GetQuota(kGroupFilesTarget, Quota::gProjectId) -
	GetQuota(kGroupFilesIs, Quota::gProjectId)) > inodes))
  {
    hasprojectquota = true;
  }

  if (!userquota && !groupquota)
    projectquota = true;

  eos_static_info("userquota=%d groupquota=%d userquota=%d groupquota=%d "
		  "userinodequota=%d uservolumequota=%d projectquota=%d "
		  "hasprojectquota=%d\n", userquota, groupquota, hasuserquota,
		  hasgroupquota, userinodequota, uservolumequota, projectquota,
		  hasprojectquota);

  // If both quotas are defined we need to have both
  if (userquota && groupquota)
    hasquota = hasuserquota & hasgroupquota;
  else
    hasquota = hasuserquota || hasgroupquota;

  if (projectquota && hasprojectquota)
    hasquota = true;

  // Root does not need any quota
  if (uid == 0)
    hasquota = true;

  return hasquota;
}

//------------------------------------------------------------------------------
// Import ns quota values into current space quota
//------------------------------------------------------------------------------
void
SpaceQuota::NsQuotaToSpaceQuota()
{
  if (UpdateQuotaNodeAddress())
  {
    XrdSysMutexHelper scope_lock(mMutex);

    // Insert current state of a single quota node into a SpaceQuota
    ResetQuota(kGroupBytesIs, Quota::gProjectId);
    ResetQuota(kGroupFilesIs, Quota::gProjectId);
    ResetQuota(kGroupLogicalBytesIs, Quota::gProjectId);

    // Loop over users
    for (auto itu = mQuotaNode->userUsageBegin();
	 itu != mQuotaNode->userUsageEnd(); itu++)
    {
      ResetQuota(kUserBytesIs, itu->first);
      AddQuota(kUserBytesIs, itu->first, itu->second.physicalSpace);
      ResetQuota(kUserFilesIs, itu->first);
      AddQuota(kUserFilesIs, itu->first, itu->second.files);
      ResetQuota(kUserLogicalBytesIs, itu->first);
      AddQuota(kUserLogicalBytesIs, itu->first, itu->second.space);

      if (mMapIdQuota[Index(kGroupBytesTarget, Quota::gProjectId)] > 0)
      {
	// Only account in project quota nodes
	AddQuota(kGroupBytesIs, Quota::gProjectId, itu->second.physicalSpace);
	AddQuota(kGroupLogicalBytesIs, Quota::gProjectId, itu->second.space);
	AddQuota(kGroupFilesIs, Quota::gProjectId, itu->second.files);
      }
    }

    for (auto itg = mQuotaNode->groupUsageBegin();
	 itg != mQuotaNode->groupUsageEnd(); itg++)
    {
      // Don't update the project quota directory from the quota
      if (itg->first == Quota::gProjectId)
	continue;

      ResetQuota(kGroupBytesIs, itg->first);
      AddQuota(kGroupBytesIs, itg->first, itg->second.physicalSpace);
      ResetQuota(kGroupFilesIs, itg->first);
      AddQuota(kGroupFilesIs, itg->first, itg->second.files);
      ResetQuota(kGroupLogicalBytesIs, itg->first);
      AddQuota(kGroupLogicalBytesIs, itg->first, itg->second.space);
    }
  }
}

//------------------------------------------------------------------------------
// Convert int tag to string representation
//------------------------------------------------------------------------------
const char* SpaceQuota::GetTagAsString(int tag)
{
  if (tag == kUserBytesTarget) return "userbytes";
  if (tag == kUserFilesTarget) return "userfiles";
  if (tag == kGroupBytesTarget) return "groupbytes";
  if (tag == kGroupFilesTarget) return "groupfiles";
  if (tag == kAllUserBytesTarget) return "alluserbytes";
  if (tag == kAllUserFilesTarget) return "alluserfiles";
  if (tag == kAllGroupBytesTarget) return "allgroupbytes";
  if (tag == kAllGroupFilesTarget) return "allgroupfiles";
  return 0;
}

//------------------------------------------------------------------------------
// Convert string tag to int representation
//------------------------------------------------------------------------------
unsigned long SpaceQuota::GetTagFromString(const std::string& tag)
{
  if (tag == "userbytes") return kUserBytesTarget;
  if (tag == "userfiles") return kUserFilesTarget;
  if (tag == "groupbytes") return kGroupBytesTarget;
  if (tag == "groupfiles") return kGroupFilesTarget;
  if (tag == "alluserbytes") return kAllUserBytesTarget;
  if (tag == "alluserfiles") return kAllUserFilesTarget;
  if (tag == "allgroupbytes") return kAllGroupBytesTarget;
  if (tag == "allgroupfiles") return kAllGroupFilesTarget;
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
      (tag == kAllUserFilesIs) || (tag == kAllUserFilesTarget))
  {
    return "user";
  }

  if ((tag == kGroupBytesIs) || (tag == kGroupBytesTarget) ||
      (tag == kGroupLogicalBytesIs) || (tag == kGroupLogicalBytesTarget) ||
      (tag == kGroupFilesIs) || (tag == kGroupFilesTarget) ||
      (tag == kAllGroupBytesIs) || (tag == kAllGroupBytesTarget) ||
      (tag == kAllGroupFilesIs) || (tag == kAllGroupFilesTarget))
  {
    return "group";
  }

  return "-----";
}

//------------------------------------------------------------------------------
// Convert int tag to string description
//------------------------------------------------------------------------------
const char* SpaceQuota::GetTagName(int tag)
{
  if (tag == kUserBytesIs) return "used bytes";
  if (tag == kUserLogicalBytesIs) return "logi bytes";
  if (tag == kUserBytesTarget) return "aval bytes";
  if (tag == kUserFilesIs) return "used files";
  if (tag == kUserFilesTarget) return "aval files";
  if (tag == kUserLogicalBytesTarget) return "aval logib";
  if (tag == kGroupBytesIs) return "used bytes";
  if (tag == kGroupLogicalBytesIs) return "logi bytes";
  if (tag == kGroupBytesTarget) return "aval bytes";
  if (tag == kGroupFilesIs)  return "used files";
  if (tag == kGroupFilesTarget) return "aval files";
  if (tag == kGroupLogicalBytesTarget) return "aval logib";
  if (tag == kAllUserBytesIs) return "used bytes";
  if (tag == kAllUserLogicalBytesIs) return "logi bytes";
  if (tag == kAllUserBytesTarget) return "aval bytes";
  if (tag == kAllUserFilesIs) return "used files";
  if (tag == kAllUserFilesTarget) return "aval files";
  if (tag == kAllUserLogicalBytesTarget) return "aval logib";
  if (tag == kAllGroupBytesIs) return "used bytes";
  if (tag == kAllGroupLogicalBytesIs) return "logi bytes";
  if (tag == kAllGroupBytesTarget) return "aval bytes";
  if (tag == kAllGroupFilesIs) return "used files";
  if (tag == kAllGroupFilesTarget) return "aval files";
  if (tag == kAllGroupLogicalBytesTarget) return "aval logib";
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
Quota::GetSpaceQuota(const std::string& path)
{
  std::string lpath = path;

  if (lpath[lpath.length() - 1]!= '/')
    lpath += "/";

  if (pMapQuota.count(lpath))
    return pMapQuota[lpath];
  else
    return static_cast<SpaceQuota*>(0);
}

//------------------------------------------------------------------------------
// Get space quota object responsible for path (find best match) - caller has
// to have a read lock on pMapMutex.
//------------------------------------------------------------------------------
SpaceQuota*
Quota::GetResponsibleSpaceQuota(const std::string& path)
{
  XrdOucString matchpath = path.c_str();
  SpaceQuota* spacequota = 0;

  for (auto it = pMapQuota.begin(); it != pMapQuota.end(); ++it)
  {
    if (matchpath.beginswith(it->second->GetSpaceName()))
    {
      if (!spacequota)
	spacequota = it->second;

      // Save if it's a better match
      if (strlen(it->second->GetSpaceName()) > strlen(spacequota->GetSpaceName()))
	spacequota = it->second;
    }
  }

  return spacequota;
}

//------------------------------------------------------------------------------
// Check if space quota exists
//------------------------------------------------------------------------------
bool
Quota::ExistsSpace(const std::string& space)
{
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  return (pMapQuota.count(space) != 0);
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
Quota::GetIndividualQuota(eos::common::Mapping::VirtualIdentity_t& vid,
			  const std::string& path, long long& max_bytes,
			  long long& free_bytes)
{
  eos::common::RWMutexReadLock rd_ns_lock(gOFS->eosViewRWMutex);
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* space = GetResponsibleSpaceQuota(path);

  if (space)
  {
    space->Refresh();

    long long max_bytes_usr, max_bytes_grp, max_bytes_prj;
    long long free_bytes_usr, free_bytes_grp, free_bytes_prj;
    free_bytes_usr = free_bytes_grp = free_bytes_prj = 0;
    max_bytes_usr = max_bytes_grp = max_bytes_prj = 0;
    max_bytes_usr  = space->GetQuota(SpaceQuota::kUserBytesTarget, vid.uid);
    max_bytes_grp = space->GetQuota(SpaceQuota::kGroupBytesTarget, vid.gid);
    max_bytes_prj = space->GetQuota(SpaceQuota::kGroupBytesTarget, Quota::gProjectId);
    free_bytes_usr = max_bytes_usr - space->GetQuota(SpaceQuota::kUserBytesIs,
						     vid.uid);
    free_bytes_grp = max_bytes_grp - space->GetQuota(SpaceQuota::kGroupBytesIs,
						     vid.gid);
    free_bytes_prj = max_bytes_prj - space->GetQuota(SpaceQuota::kGroupBytesIs,
						     Quota::gProjectId);

    if (free_bytes_usr > free_bytes) free_bytes = free_bytes_usr;
    if (free_bytes_grp > free_bytes) free_bytes = free_bytes_grp;
    if (free_bytes_prj > free_bytes) free_bytes = free_bytes_prj;
    if (max_bytes_usr > max_bytes) max_bytes = max_bytes_usr;
    if (max_bytes_grp > max_bytes) max_bytes = max_bytes_grp;
    if (max_bytes_prj > max_bytes) max_bytes = max_bytes_prj;
  }
}

//------------------------------------------------------------------------------
// Set quota type for id
//------------------------------------------------------------------------------
bool
Quota::SetQuotaTypeForId(const std::string& space, long id, Quota::IdT id_type,
			 Quota::Type quota_type, unsigned long long value,
			 std::string& msg, int& retc)
{
  std::ostringstream oss_msg;
  std::string path = space;
  retc = EINVAL;

  // If no path use "/eos/"
  if (path.empty())
    path = "/eos/";
  else if (path[path.length() - 1] != '/')
    path += '/';

  // Get type of quota to set and construct config entry
  std::ostringstream oss_config;
  SpaceQuota::eQuotaTag quota_tag;
  oss_config << path << ":";

  if (id_type == IdT::kUid)
  {
    oss_config << "uid=";

    if (quota_type == Type::kVolume)
      quota_tag = SpaceQuota::kUserBytesTarget;
    else
      quota_tag = SpaceQuota::kUserFilesTarget;
  }
  else
  {
    oss_config << "gid=";

    if (quota_type == Type::kVolume)
      quota_tag = SpaceQuota::kGroupBytesTarget;
    else
      quota_tag = SpaceQuota::kGroupFilesTarget;
  }

  // Quota values need to be positive
  if (value < 0)
  {
    oss_msg << "error: " << ((quota_type == Type::kVolume) ? "volume" : "inode")
	    << " quota value needs to be positive" << std::endl;
    msg = oss_msg.str();
    return false;
  }

  // Make sure the quota node exist
  (void) CreateSpaceQuota(path);

  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetSpaceQuota(path);

  if (!squota)
  {
    oss_msg << "error: no quota space defined for node " << path << std::endl;
    msg = oss_msg.str();
    return false;
  }

  squota->SetQuota(quota_tag, id, value);
  std::string svalue = std::to_string(value);
  oss_config << id << ":" << SpaceQuota::GetTagAsString(quota_tag);
  gOFS->ConfEngine->SetConfigValue("quota", oss_config.str().c_str(), svalue.c_str());
  oss_msg << "success: updated "
	  << ((quota_type == Type::kVolume) ? "volume" : "inode")
	  <<" quota for "
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
Quota::SetQuotaForTag(const std::string& space,
		      const std::string& quota_stag,
		      long id, unsigned long long value)
{
  unsigned long spaceq_type = SpaceQuota::GetTagFromString(quota_stag);
  // Make sure the quta node exists

  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetSpaceQuota(space);

  if (squota)
  {
    squota->SetQuota(spaceq_type, id, value);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Remove quota type for id
//------------------------------------------------------------------------------
bool
Quota::RmQuotaTypeForId(const std::string& space, long id, Quota::IdT id_type,
			Quota::Type quota_type, std::string& msg, int& retc)
{
  std::ostringstream oss_msg;
  std::string path = space;
  retc = EINVAL;

  // If no path use "/eos/"
  if (path.empty())
    path = "/eos/";
  else if (path[path.length() - 1] != '/')
    path += '/';

  // Get type of quota to remove and construct config entry
  std::ostringstream oss_config;
  SpaceQuota::eQuotaTag quota_tag;
  oss_config << path << ":";

  if (id_type == IdT::kUid)
  {
    oss_config << "uid=";

    if (quota_type == Type::kVolume)
      quota_tag = SpaceQuota::kUserBytesTarget;
    else
      quota_tag = SpaceQuota::kUserFilesTarget;
  }
  else
  {
    oss_config << "gid=";

    if (quota_type == Type::kVolume)
      quota_tag = SpaceQuota::kGroupBytesTarget;
    else
      quota_tag = SpaceQuota::kGroupFilesTarget;
  }

  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetSpaceQuota(path);

  if (!squota)
  {
    oss_msg << "error: no quota space defined for node " << path << std::endl;
    msg = oss_msg.str();
    return false;
  }

  if (squota->RmQuota(quota_tag, id))
  {
    oss_config << id << ":" << SpaceQuota::GetTagAsString(quota_tag);
    gOFS->ConfEngine->DeleteConfigValue("quota", oss_config.str().c_str());
    oss_msg << "success: removed "
	    << ((quota_type == Type::kVolume) ? "volume" : "inode")
	    <<" quota for "
	    << ((id_type == IdT::kUid) ? "uid=" : "gid=") << id
	    << " from node " << path << std::endl;
    msg = oss_msg.str();
    retc = 0;
    return true;
  }
  else
  {
    oss_msg << "error: no "
	    << ((quota_type == Type::kVolume) ? "volume" : "inode")
	    << " quota defined on node " << path << " for "
	    << ((id_type == IdT::kUid) ? "user id" : "group id")
	    << std::endl;
    msg = oss_msg.str();
    return false;
  }
}

//------------------------------------------------------------------------------
// Remove all quota types for an id
//------------------------------------------------------------------------------
bool
Quota::RmQuotaForId(const std::string& space, long id, Quota::IdT id_type,
		    std::string& msg, int& retc)
{
  eos_static_debug("space=%s", space.c_str());
  std::string msg_vol, msg_inode;
  bool rm_vol = RmQuotaTypeForId(space, id, id_type, Type::kVolume, msg_vol, retc);
  bool rm_inode = RmQuotaTypeForId(space, id, id_type, Type::kInode,
				   msg_inode, retc);

  if (rm_vol || rm_inode)
  {
    if (rm_vol)
      msg += msg_vol;

    if (rm_inode)
      msg += msg_inode;

    return true;
  }
  else
  {
    msg = "error: no quota defined for node ";
    msg += space;
    return false;
  }
}

//------------------------------------------------------------------------------
// Remove space quota
//------------------------------------------------------------------------------
bool
Quota::RmSpaceQuota(std::string& path, std::string& msg, int& retc)
{
  eos_static_debug("space=%s", path.c_str());
  eos::common::RWMutexWriteLock wr_ns_lock(gOFS->eosViewRWMutex);
  eos::common::RWMutexWriteLock wr_quota_lock(pMapMutex);
  SpaceQuota* squota = GetSpaceQuota(path);

  if (!squota)
  {
    retc = EINVAL;
    msg = "error: there is no quota node under path ";
    msg += path;
    return false;
  }
  else
  {
    // Remove space quota from map
    if (path[path.length() -1 ] != '/')
      path += '/';

    pMapQuota.erase(path);

    // Remove ns quota node
    try
    {
      eos::IContainerMD* qcont = gOFS->eosView->getContainer(path);
      gOFS->eosView->removeQuotaNode(qcont);
      retc = 0;
    }
    catch (eos::MDException& e)
    {
      retc = e.getErrno();
      msg = e.getMessage().str().c_str();
    }

    // Remove all configuration entries
    std::string match = path;
    match += ":";
    gOFS->ConfEngine->DeleteConfigValueByMatch("quota", match.c_str());

    msg = "success: removed space quota for ";
    msg += path;

    if (!gOFS->ConfEngine->AutoSave())
      return false;

    return true;
  }
}

//------------------------------------------------------------------------------
// Remove quota depending on the quota tag. Convenience wrapper around the
// default RmQuotaTypeForId.
//------------------------------------------------------------------------------
bool
Quota::RmQuotaForTag(const std::string& space, const std::string& quota_stag,
		     long id)
{
  unsigned long spaceq_type = SpaceQuota::GetTagFromString(quota_stag);
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetSpaceQuota(space);

  if (squota)
  {
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
  if (!file)
    return 0;

  eos::IFileMD::layoutId_t lid = file->getLayoutId();
  return (uint64_t) file->getSize() * eos::common::LayoutId::GetSizeFactor(lid);
}

//------------------------------------------------------------------------------
// Load nodes
//------------------------------------------------------------------------------
void
Quota::LoadNodes()
{
  eos_static_info("Calling LoadNodes");
  std::vector<std::string> create_quota;

  // Load all known nodes
  {
    std::string quota_path;
    eos::IContainerMD* container = 0;
    eos::common::RWMutexReadLock rd_ns_lock(gOFS->eosViewRWMutex);

    for (auto it = gOFS->eosView->getQuotaStats()->nodesBegin();
	 it != gOFS->eosView->getQuotaStats()->nodesEnd(); ++it)
    {
      try
      {
	container = gOFS->eosDirectoryService->getContainerMD(it->first);
	quota_path = gOFS->eosView->getUri(container);

	// Make sure directories are '/' terminated
	if (quota_path[quota_path.length() - 1] != '/')
	  quota_path += '/';

	create_quota.push_back(quota_path);
      }
      catch (eos::MDException& e)
      {
	errno = e.getErrno();
	eos_static_err("msg=\"exception\" ec=%d emsg=\"%s\"\n",
		       e.getErrno(), e.getMessage().str().c_str());
      }
    }
  }

  // Create all the necessary space quota nodes
  for (auto it = create_quota.begin(); it != create_quota.end(); ++it)
  {
    eos_static_notice("Created space for quota node: %s", it->c_str());
    (void) CreateSpaceQuota(it->c_str());
  }

  // Refresh the space quota objects
  {
    eos::common::RWMutexReadLock rd_ns_lock(gOFS->eosViewRWMutex);
    eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);

    for (auto it = pMapQuota.begin(); it != pMapQuota.end(); ++it)
      it->second->Refresh();
  }
}

//------------------------------------------------------------------------------
// Print out quota information
//------------------------------------------------------------------------------
void
Quota::PrintOut(const std::string& space, XrdOucString& output, long uid_sel,
		long gid_sel, bool monitoring, bool translate_ids)
{
  // Add this to have all quota nodes visible even if they are not in
  // the configuration file
  LoadNodes();
  eos::common::RWMutexReadLock rd_fs_lock(FsView::gFsView.ViewMutex);
  eos::common::RWMutexReadLock rd_ns_lcok(gOFS->eosViewRWMutex);
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  output = "";
  XrdOucString spacenames = "";

  if (space.empty())
  {
    // make sure all configured spaces exist In the quota views
    std::map<std::string, FsSpace*>::const_iterator sit;

    for (sit = FsView::gFsView.mSpaceView.begin();
	 sit != FsView::gFsView.mSpaceView.end(); sit++)
    {
      GetSpaceQuota(sit->second->GetMember("name").c_str());
    }

    std::map<std::string, SpaceQuota*>::const_iterator it;

    for (it = pMapQuota.begin(); it != pMapQuota.end(); it++)
    {
      it->second->Refresh();
      it->second->PrintOut(output, uid_sel, gid_sel, monitoring, translate_ids);
    }
  }
  else
  {
    SpaceQuota* squota = GetResponsibleSpaceQuota(space);

    if (squota)
    {
      squota->Refresh();
      squota->PrintOut(output, uid_sel, gid_sel, monitoring, translate_ids);
    }
  }
}

//------------------------------------------------------------------------------
// Get group quota values for a particular space and id
//------------------------------------------------------------------------------
std::map<int, unsigned long long>
Quota::GetGroupStatistics(const std::string& space, long id)
{
  std::map<int, unsigned long long> map;
  eos::common::RWMutexReadLock rd_ns_lock(gOFS->eosViewRWMutex);
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetResponsibleSpaceQuota(space);

  if (!squota)
    return map;

  squota->Refresh();
  unsigned long long value;
  // Set of all group related quota keys
  std::set<int> set_keys = {SpaceQuota::kGroupBytesIs, SpaceQuota::kGroupBytesTarget,
			    SpaceQuota::kGroupFilesIs, SpaceQuota::kGroupFilesTarget};

  for (auto it = set_keys.begin(); it != set_keys.end(); ++it)
  {
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
  if (!squota || (strcmp(squota->GetSpaceName(), path.c_str())))
    return false;

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

  if (!squota)
    return true;

  return squota->CheckWriteQuota(uid, gid, desired_vol, desired_inodes);
}

//------------------------------------------------------------------------------
// Clean-up all space quotas by deleting them and clearing the map
//------------------------------------------------------------------------------
void
Quota::CleanUp()
{
  eos::common::RWMutexWriteLock wr_lock(pMapMutex);

  for (auto it = pMapQuota.begin(); it != pMapQuota.end(); ++it)
    delete it->second;

  pMapQuota.clear();
}

//------------------------------------------------------------------------------
// Take the decision where to place a new file in the system. The core of the
// implementation is in the Scheduler and GeoTreeEngine.
//------------------------------------------------------------------------------
int
Quota::FilePlacement(const std::string& space,
		     const char* path,
		     eos::common::Mapping::VirtualIdentity_t& vid,
		     const char* grouptag,
		     unsigned long lid,
		     std::vector<unsigned int>& alreadyused_filesystems,
		     std::vector<unsigned int>& selected_filesystems,
		     Scheduler::tPlctPolicy plctpolicy,
		     const std::string& plctTrgGeotag,
		     bool truncate,
		     int forced_scheduling_group_index,
		     unsigned long long bookingsize)
{
  // 0 = 1 replica !
  unsigned int nfilesystems = eos::common::LayoutId::GetStripeNumber(lid) + 1;
  // First figure out how many filesystems we need
  eos_static_debug("uid=%u gid=%u grouptag=%s place filesystems=%u", vid.uid,
		   vid.gid, grouptag, nfilesystems);

  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetSpaceQuota(space);

  if (!squota)
  {
    eos_static_err("no space quota for space=%s", space.c_str());
    return ENOSPC;
  }

  // Check if the uid/gid has enough quota configured to place in this space
  if (squota->IsEnabled())
  {
    bool has_quota = false;
    SpaceQuota* nquota = GetResponsibleSpaceQuota(path);

    if (nquota)
    {
      long long desired_vol = 1ll * nfilesystems * bookingsize;
      has_quota = nquota->CheckWriteQuota(vid.uid, vid.gid, desired_vol,
					  nfilesystems);

      if (!has_quota)
      {
	eos_static_debug("uid=%u gid=%u grouptag=%s place filesystems=%u "
			 "has no quota left!", vid.uid, vid.gid, grouptag,
			 nfilesystems);
	return EDQUOT;
      }
    }
    else
    {
      eos_static_err("no namespace quota found for path=%s", path);
      return EDQUOT;
    }
  }
  else
  {
    eos_static_debug("quota is disabled for space=%s", space.c_str());
  }

  if (!FsView::gFsView.mSpaceGroupView.count(space))
  {
    eos_static_err("msg=\"no filesystem in space\" space=\"%s\"", space.c_str());
    selected_filesystems.clear();
    return ENOSPC;
  }

  // Call the scheduler implementation
  return squota->FilePlacement(path, vid, grouptag, lid,
			       alreadyused_filesystems,
			       selected_filesystems, plctpolicy, plctTrgGeotag,
			       truncate, forced_scheduling_group_index, bookingsize);
}

//------------------------------------------------------------------------------
// Take the decision from where to access a file. The core of the
// implementation is in the Scheduler and GeoTreeEngine.
//------------------------------------------------------------------------------
int
Quota::FileAccess(const std::string& space,
		  eos::common::Mapping::VirtualIdentity_t& vid,
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
  eos::common::RWMutexReadLock rd_quota_lock(pMapMutex);
  SpaceQuota* squota = GetSpaceQuota(space);

  if (!squota)
  {
    eos_static_err("no space quota for space=%s", space.c_str());
    return ENOSPC;
  }

  return squota->FileAccess(vid, forcedfsid, forcedspace, tried_cgi, lid,
			    locationsfs, fsindex, isRW, bookingsize, unavailfs,
			    min_fsstatus, overridegeoloc, noIO);
}

//------------------------------------------------------------------------------
// Create space quota
//------------------------------------------------------------------------------
void
Quota::CreateSpaceQuota(const std::string& path)
{
  eos::common::RWMutexWriteLock wr_ns_lock(gOFS->eosViewRWMutex);
  eos::common::RWMutexWriteLock wr_quota_lock(pMapMutex);

  if (pMapQuota.count(path) == 0)
  {
    SpaceQuota* squota = new SpaceQuota(path.c_str());
    pMapQuota[path] = squota;
  }
}

EOSMGMNAMESPACE_END
