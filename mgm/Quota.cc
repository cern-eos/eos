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
eos::common::RWMutex Quota::gQuotaMutex;
gid_t Quota::gProjectId = 99;

#ifdef __APPLE__
#define ENONET 64
#endif

//------------------------------------------------------------------------------
// *** Class SpaceQuota implementaion ***
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
SpaceQuota::SpaceQuota(const char* name):
  mEnabled(false),
  mQuotaNode(0),
  mLastEnableCheck(0),
  mLayoutSizeFactor(1.0)
{
  SpaceName = name;
  std::string path = name;
  eos::IContainerMD* quotadir = 0;
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);

  if (path[0] == '/')
  {
    if (path[path.length() - 1] != '/')
      path += "/";

    SpaceName = path.c_str();

    try
    {
      quotadir = gOFS->eosView->getContainer(path.c_str());
    }
    catch (eos::MDException& e)
    {
      quotadir = 0;
    }

    if (!quotadir)
    {
      try
      {
	quotadir = gOFS->eosView->createContainer(name, true);
	quotadir->setMode(S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
	gOFS->eosView->updateContainerStore(quotadir);
      }
      catch (eos::MDException& e)
      {
	eos_static_crit("Cannot create quota directory %s", name);
      }
    }
    else
    {
      try
      {
	mQuotaNode = gOFS->eosView->getQuotaNode(quotadir, false);
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
	catch (eos::MDException& e)
	{
	  mQuotaNode = 0;
	  eos_static_crit("Cannot register quota node %s", name);
	}
      }
    }
  }

  mDirtyTarget = true;
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
//
//------------------------------------------------------------------------------
bool
SpaceQuota::UpdateQuotaNodeAddress()
{
  // this routine has to be called with eosViewMutexRW locked
  eos::IContainerMD* quotadir = 0;

  try
  {
    quotadir = gOFS->eosView->getContainer(SpaceName.c_str());

    try
    {
      mQuotaNode = gOFS->eosView->getQuotaNode(quotadir, false);
    }
    catch (eos::MDException& e)
    {
      mQuotaNode = 0;
      return false;
    }
  }
  catch (eos::MDException& e)
  {
    quotadir = 0;
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Remove quota node
//------------------------------------------------------------------------------
void
SpaceQuota::RemoveQuotaNode(std::string& msg, int& retc)

{
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  eos::IContainerMD* quotadir = 0;

  try
  {
    quotadir = gOFS->eosView->getContainer(SpaceName.c_str());
    gOFS->eosView->removeQuotaNode(quotadir);
    retc = 0;
    msg = "success: removed quota node ";
    msg += SpaceName.c_str();
  }
  catch (eos::MDException& e)
  {
    quotadir = 0;
    retc = e.getErrno();
    msg = e.getMessage().str().c_str();
  }
}

//------------------------------------------------------------------------------
// Calculate the default factor for a quota node to calculate the logical
// available bytes
//------------------------------------------------------------------------------
void
SpaceQuota::UpdateLogicalSizeFactor()
{
  if (!SpaceName.beginswith("/"))
    return;

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

  // just a /0 protection
  if (mLayoutSizeFactor < 1.0)
    mLayoutSizeFactor = 1.0;
}

//------------------------------------------------------------------------------
// Remove quota
//------------------------------------------------------------------------------
bool
SpaceQuota::RmQuota(unsigned long tag, unsigned long id, bool lock)
{
  eos_static_debug("rm quota tag=%lu id=%lu", tag, id);
  bool removed = false;

  if (lock) mMutex.Lock();

  if (mMapIdQuota.count(Index(tag, id)))
  {
    removed = true;
    mMapIdQuota.erase(Index(tag, id));
    mDirtyTarget = true;
  }

  if (lock) mMutex.UnLock();
  return removed;
}

//------------------------------------------------------------------------------
// Get quota value
//------------------------------------------------------------------------------
long long
SpaceQuota::GetQuota(unsigned long tag, unsigned long id, bool lock)
{
  if (lock) mMutex.Lock();
  long long ret = mMapIdQuota[Index(tag, id)];
  if (lock) mMutex.UnLock();

  eos_static_debug("get quota tag=%lu id=%lu value=%lld", tag, id, ret);
  return ret;
}

//------------------------------------------------------------------------------
// Set quota
//------------------------------------------------------------------------------
void
SpaceQuota::SetQuota(unsigned long tag, unsigned long id,
		     unsigned long long value, bool lock)
{
  eos_static_debug("set quota tag=%lu id=%lu value=%llu", tag, id, value);

  if (lock) mMutex.Lock();

  mMapIdQuota[Index(tag, id)] = value;

  if (lock) mMutex.UnLock();

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
SpaceQuota::AddQuota(unsigned long tag, unsigned long id, long long value,
		     bool lock)
{
  eos_static_debug("add quota tag=%lu id=%lu value=%llu", tag, id, value);

  if (lock) mMutex.Lock();

  // user/group quota implementation
  // fix for avoiding negative numbers
  if ((((long long) mMapIdQuota[Index(tag, id)]) + (long long) value) >= 0)
    mMapIdQuota[Index(tag, id)] += value;

  eos_static_debug("sum quota tag=%lu id=%lu value=%llu", tag, id,
		   mMapIdQuota[Index(tag, id)]);

  if (lock) mMutex.UnLock();
}

//------------------------------------------------------------------------------
// Update
//------------------------------------------------------------------------------
void
SpaceQuota::UpdateTargetSums()
{
  if (!mDirtyTarget)
    return;

  XrdSysMutexHelper scope_lock(mMutex);
  mDirtyTarget = false;
  eos_static_debug("updating targets");
  ResetQuota(kAllUserBytesTarget, 0, false);
  ResetQuota(kAllUserFilesTarget, 0, false);
  ResetQuota(kAllGroupBytesTarget, 0, false);
  ResetQuota(kAllGroupFilesTarget, 0, false);
  ResetQuota(kAllUserLogicalBytesTarget, 0, false);
  ResetQuota(kAllGroupLogicalBytesTarget, 0, false);
  std::map<long long, unsigned long long>::const_iterator it;

  for (it = mMapIdQuota.begin(); it != mMapIdQuota.end(); it++)
  {
    if ((UnIndex(it->first) == kUserBytesTarget))
    {
      AddQuota(kAllUserBytesTarget, 0, it->second, false);
      AddQuota(kAllUserLogicalBytesTarget, 0, it->second / mLayoutSizeFactor, false);
    }

    if ((UnIndex(it->first) == kUserFilesTarget))
      AddQuota(kAllUserFilesTarget, 0, it->second, false);

    if ((UnIndex(it->first) == kGroupBytesTarget))
    {
      AddQuota(kAllGroupBytesTarget, 0, it->second, false);
      AddQuota(kAllGroupLogicalBytesTarget, 0, it->second / mLayoutSizeFactor, false);
    }

    if ((UnIndex(it->first) == kGroupFilesTarget))
      AddQuota(kAllGroupFilesTarget, 0, it->second, false);
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

  ResetQuota(kAllUserBytesIs, 0, false);
  ResetQuota(kAllUserLogicalBytesIs, 0, false);
  ResetQuota(kAllUserFilesIs, 0, false);
  ResetQuota(kAllGroupBytesIs, 0, false);
  ResetQuota(kAllGroupFilesIs, 0, false);
  ResetQuota(kAllGroupLogicalBytesIs, 0, false);
  ResetQuota(kAllUserBytesIs, 0, false);
  ResetQuota(kAllUserLogicalBytesIs, 0, false);
  ResetQuota(kAllUserFilesIs, 0, false);
  ResetQuota(kAllGroupBytesIs, 0, false);
  ResetQuota(kAllGroupFilesIs, 0, false);
  ResetQuota(kAllGroupLogicalBytesIs, 0, false);
  std::map<long long, unsigned long long>::const_iterator it;

  for (it = mMapIdQuota.begin(); it != mMapIdQuota.end(); it++)
  {
    if ((UnIndex(it->first) == kUserBytesIs))
      AddQuota(kAllUserBytesIs, 0, it->second, false);

    if ((UnIndex(it->first) == kUserLogicalBytesIs))
      AddQuota(kAllUserLogicalBytesIs, 0, it->second, false);

    if ((UnIndex(it->first) == kUserFilesIs))
    {
      AddQuota(kAllUserFilesIs, 0, it->second, false);
    }

    if ((UnIndex(it->first) == kGroupBytesIs))
      AddQuota(kAllGroupBytesIs, 0, it->second, false);

    if ((UnIndex(it->first) == kGroupLogicalBytesIs))
      AddQuota(kAllGroupLogicalBytesIs, 0, it->second, false);

    if ((UnIndex(it->first) == kGroupFilesIs))
      AddQuota(kAllGroupFilesIs, 0, it->second, false);
  }
}

//------------------------------------------------------------------------------
// Update uid/gid values from quota node
//------------------------------------------------------------------------------
void
SpaceQuota::UpdateFromQuotaNode(uid_t uid, gid_t gid, bool calc_project_quota)
{
  eos_static_debug("updating uid/gid values from quota node");
  XrdSysMutexHelper scope_lock(mMutex);

  if (mQuotaNode)
  {
    ResetQuota(kUserBytesIs, uid, false);
    ResetQuota(kUserLogicalBytesIs, uid, false);
    ResetQuota(kUserFilesIs, uid, false);
    ResetQuota(kGroupBytesIs, gid, false);
    ResetQuota(kGroupFilesIs, gid, false);
    ResetQuota(kGroupLogicalBytesIs, gid, false);
    AddQuota(kUserBytesIs, uid, mQuotaNode->getPhysicalSpaceByUser(uid), false);
    AddQuota(kUserLogicalBytesIs, uid, mQuotaNode->getUsedSpaceByUser(uid), false);
    AddQuota(kUserFilesIs, uid, mQuotaNode->getNumFilesByUser(uid), false);
    AddQuota(kGroupBytesIs, gid, mQuotaNode->getPhysicalSpaceByGroup(gid), false);
    AddQuota(kGroupLogicalBytesIs, gid, mQuotaNode->getUsedSpaceByGroup(gid), false);
    AddQuota(kGroupFilesIs, gid, mQuotaNode->getNumFilesByGroup(gid), false);
    ResetQuota(kUserBytesIs, Quota::gProjectId, false);
    ResetQuota(kUserLogicalBytesIs, Quota::gProjectId, false);
    ResetQuota(kUserFilesIs, Quota::gProjectId, false);

    if (calc_project_quota)
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
	ResetQuota(SpaceQuota::kGroupBytesIs, Quota::gProjectId, false);
	ResetQuota(SpaceQuota::kGroupFilesIs, Quota::gProjectId, false);
	ResetQuota(SpaceQuota::kGroupLogicalBytesIs, Quota::gProjectId, false);

	// loop over user and fill project quota
	for (auto itu = GetQuotaNode()->userUsageBegin();
	     itu != GetQuotaNode()->userUsageEnd();
	     itu++)
	{
	  AddQuota(SpaceQuota::kGroupBytesIs,
		   Quota::gProjectId,
		   itu->second.physicalSpace,
		   false);
	  AddQuota(SpaceQuota::kGroupLogicalBytesIs,
		   Quota::gProjectId,
		   itu->second.space,
		   false);
	  AddQuota(SpaceQuota::kGroupFilesIs,
		   Quota::gProjectId,
		   itu->second.files,
		   false);
	}
      }
    }
  }
}

//------------------------------------------------------------------------------
// Refresh
//------------------------------------------------------------------------------
void
SpaceQuota::Refresh()
{
  eos::common::RWMutexReadLock nlock(gOFS->eosViewRWMutex);
  Quota::NodeToSpaceQuota(SpaceName.c_str());
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
  char headerline[4096];
  std::map<long long, unsigned long long>::const_iterator it;
  int* sortuidarray = (int*) malloc(sizeof(int) * (mMapIdQuota.size() + 1));
  int* sortgidarray = (int*) malloc(sizeof(int) * (mMapIdQuota.size() + 1));
  int userentries = 0;
  int groupentries = 0;
  // Make a map containing once all the defined uid's+gid's
  std::map<unsigned long, unsigned long > sortuidhash;
  std::map<unsigned long, unsigned long > sortgidhash;
  std::map<unsigned long, unsigned long >::const_iterator sortit;

  // for project space we just print the user/group entry gProjectId
  if (mMapIdQuota[Index(kGroupBytesTarget, Quota::gProjectId)] > 0)
    gid_sel = Quota::gProjectId;

  if (!SpaceName.beginswith("/"))
  {
    free(sortuidarray);
    free(sortgidarray);
    // we don't show them right now ... maybe if we put quota on physical spaces we will
    return;
  }
  else
  {
    XrdOucString header;

    // This is a virtual quota node
    if (!monitoring)
    {
      header += "# ___________________________________________________________"
	"____________________________________\n";
      sprintf(headerline, "# ==> Quota Node: %-16s\n", SpaceName.c_str());
      header += headerline;
      header += "# ___________________________________________________________"
	"____________________________________\n";
    }

    for (it = mMapIdQuota.begin(); it != mMapIdQuota.end(); it++)
    {
      if ((UnIndex(it->first) >= kUserBytesIs)
	  && (UnIndex(it->first) <= kUserFilesTarget))
      {
	eos_static_debug("adding %llx to print list ", UnIndex(it->first));
	unsigned long ugid = (it->first) & 0xffffffff;

	// uid selection filter
	if (uid_sel >= 0)
	  if (ugid != (unsigned long) uid_sel)
	    continue;

	// we don't print the users if a gid is selected
	if (gid_sel >= 0)
	  continue;

	sortuidhash[ugid] = ugid;
      }

      if ((UnIndex(it->first) >= kGroupBytesIs)
	  && (UnIndex(it->first) <= kGroupFilesTarget))
      {
	unsigned long ugid = (it->first) & 0xffffffff;

	// uid selection filter
	if (gid_sel >= 0)
	  if (ugid != (unsigned long) gid_sel)
	    continue;

	// We don't print the group if a uid is selected
	if (uid_sel >= 0)
	  continue;

	sortgidhash[ugid] = ugid;
      }
    }

    for (sortit = sortuidhash.begin(); sortit != sortuidhash.end(); sortit++)
    {
      sortuidarray[userentries] = (sortit->first);
      eos_static_debug("loop %d %d", userentries, sortuidarray[userentries]);
      userentries++;
    }

    for (sortit = sortgidhash.begin(); sortit != sortgidhash.end(); sortit++)
    {
      // sort only based on the user bytes entries
      sortgidarray[groupentries] = (sortit->first);
      eos_static_debug("loop %d %d", groupentries, sortgidarray[groupentries]);
      groupentries++;
    }

    sort(sortuidarray, sortuidarray + userentries);
    sort(sortgidarray, sortgidarray + groupentries);
    eos_static_debug("sorted");

    for (int k = 0; k < userentries; k++)
      eos_static_debug("sort %d %d", k, sortuidarray[k]);

    for (int k = 0; k < groupentries; k++)
      eos_static_debug("sort %d %d", k, sortgidarray[k]);

    std::vector <std::string> uidout;
    std::vector <std::string> gidout;

    if (((uid_sel < 0) && (gid_sel < 0)) || userentries || groupentries)
    {
      // Print the header for selected uid/gid's only if there is something to print
      // If we have a full listing we print even empty quota nodes (=header only)
      output += header;
    }

    if (userentries)
    {
      // user loop
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

    for (int lid = 0; lid < userentries; lid++)
    {
      eos_static_debug("loop with id=%d", lid);
      XrdOucString value1 = "";
      XrdOucString value2 = "";
      XrdOucString value3 = "";
      XrdOucString value4 = "";
      XrdOucString value5 = "";
      XrdOucString value6 = "";
      XrdOucString id = "";
      id += sortuidarray[lid];

      if (translate_ids)
      {
	if (gid_sel == Quota::gProjectId)
	{
	  id = "project";
	}
	else
	{
	  int errc = 0;
	  std::string username = eos::common::Mapping::UidToUserName(sortuidarray[lid],
				 errc);
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
		eos::common::StringConversion::GetReadableSizeString(value1,
		    GetQuota(kUserBytesIs, sortuidarray[lid]), "B"),
		eos::common::StringConversion::GetReadableSizeString(value2,
		    GetQuota(kUserLogicalBytesIs, sortuidarray[lid]), "B"),
		eos::common::StringConversion::GetReadableSizeString(value3,
		    GetQuota(kUserFilesIs, sortuidarray[lid]), "-"),
		eos::common::StringConversion::GetReadableSizeString(value4,
		    GetQuota(kUserBytesTarget, sortuidarray[lid]), "B"),
		eos::common::StringConversion::GetReadableSizeString(value5,
		    (long long)(GetQuota(kUserBytesTarget, sortuidarray[lid]) / mLayoutSizeFactor),
		    "B"),
		eos::common::StringConversion::GetReadableSizeString(value6,
		    GetQuota(kUserFilesTarget, sortuidarray[lid]), "-"),
		GetQuotaPercentage(GetQuota(kUserBytesIs, sortuidarray[lid]),
				   GetQuota(kUserBytesTarget, sortuidarray[lid]), percentage),
		GetQuotaStatus(GetQuota(kUserBytesIs, sortuidarray[lid]),
			       GetQuota(kUserBytesTarget, sortuidarray[lid])),
		GetQuotaStatus(GetQuota(kUserFilesIs, sortuidarray[lid]),
			       GetQuota(kUserFilesTarget, sortuidarray[lid])));
      }
      else
      {
	sprintf(headerline, "quota=node uid=%s space=%s usedbytes=%llu "
		"usedlogicalbytes=%llu usedfiles=%llu maxbytes=%llu "
		"maxlogicalbytes=%llu maxfiles=%llu percentageusedbytes=%s "
		"statusbytes=%s statusfiles=%s\n", id.c_str(), SpaceName.c_str(),
		GetQuota(kUserBytesIs, sortuidarray[lid]),
		GetQuota(kUserLogicalBytesIs, sortuidarray[lid]),
		GetQuota(kUserFilesIs, sortuidarray[lid]),
		GetQuota(kUserBytesTarget, sortuidarray[lid]),
		(unsigned long long)(GetQuota(kUserBytesTarget,
					      sortuidarray[lid]) / mLayoutSizeFactor),
		GetQuota(kUserFilesTarget, sortuidarray[lid]),
		GetQuotaPercentage(GetQuota(kUserBytesIs, sortuidarray[lid]),
				   GetQuota(kUserBytesTarget, sortuidarray[lid]), percentage),
		GetQuotaStatus(GetQuota(kUserBytesIs, sortuidarray[lid]),
			       GetQuota(kUserBytesTarget, sortuidarray[lid])),
		GetQuotaStatus(GetQuota(kUserFilesIs, sortuidarray[lid]),
			       GetQuota(kUserFilesTarget, sortuidarray[lid])));
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

    if (groupentries)
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

    for (int lid = 0; lid < groupentries; lid++)
    {
      eos_static_debug("loop with id=%d", lid);
      XrdOucString value1 = "";
      XrdOucString value2 = "";
      XrdOucString value3 = "";
      XrdOucString value4 = "";
      XrdOucString value5 = "";
      XrdOucString value6 = "";
      XrdOucString id = "";
      id += sortgidarray[lid];

      if (translate_ids)
      {
	if (gid_sel == Quota::gProjectId)
	{
	  id = "project";
	}
	else
	{
	  int errc = 0;
	  std::string groupname = eos::common::Mapping::GidToGroupName(sortgidarray[lid],
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
		eos::common::StringConversion::GetReadableSizeString(value1,
		    GetQuota(kGroupBytesIs, sortgidarray[lid]), "B"),
		eos::common::StringConversion::GetReadableSizeString(value2,
		    GetQuota(kGroupLogicalBytesIs, sortgidarray[lid]), "B"),
		eos::common::StringConversion::GetReadableSizeString(value3,
		    GetQuota(kGroupFilesIs, sortgidarray[lid]), "-"),
		eos::common::StringConversion::GetReadableSizeString(value4,
		    GetQuota(kGroupBytesTarget, sortgidarray[lid]), "B"),
		eos::common::StringConversion::GetReadableSizeString(value5,
		    (long long)(GetQuota(kGroupBytesTarget, sortgidarray[lid]) / mLayoutSizeFactor),
		    "B"),
		eos::common::StringConversion::GetReadableSizeString(value6,
		    GetQuota(kGroupFilesTarget, sortgidarray[lid]), "-"),
		GetQuotaPercentage(GetQuota(kGroupBytesIs, sortgidarray[lid]),
				   GetQuota(kGroupBytesTarget, sortgidarray[lid]), percentage),
		GetQuotaStatus(GetQuota(kGroupBytesIs, sortgidarray[lid]),
			       GetQuota(kGroupBytesTarget, sortgidarray[lid])),
		GetQuotaStatus(GetQuota(kGroupFilesIs, sortgidarray[lid]),
			       GetQuota(kGroupFilesTarget, sortgidarray[lid])));
      }
      else
      {
	sprintf(headerline,
		"quota=node gid=%s space=%s usedbytes=%llu usedlogicalbytes=%llu "
		"usedfiles=%llu maxbytes=%llu maxlogicalbytes=%llu maxfiles=%llu "
		"percentageusedbytes=%s statusbytes=%s statusfiles=%s\n",
		id.c_str(), SpaceName.c_str(),
		GetQuota(kGroupBytesIs, sortgidarray[lid]),
		GetQuota(kGroupLogicalBytesIs, sortgidarray[lid]),
		GetQuota(kGroupFilesIs, sortgidarray[lid]),
		GetQuota(kGroupBytesTarget, sortgidarray[lid]),
		(unsigned long long)(GetQuota(kGroupBytesTarget,
					      sortgidarray[lid]) / mLayoutSizeFactor),
		GetQuota(kGroupFilesTarget, sortgidarray[lid]),
		GetQuotaPercentage(GetQuota(kGroupBytesIs, sortgidarray[lid]),
				   GetQuota(kGroupBytesTarget, sortgidarray[lid]), percentage),
		GetQuotaStatus(GetQuota(kGroupBytesIs, sortgidarray[lid]),
			       GetQuota(kGroupBytesTarget, sortgidarray[lid])),
		GetQuotaStatus(GetQuota(kGroupFilesIs, sortgidarray[lid]),
			       GetQuota(kGroupFilesTarget, sortgidarray[lid])));
      }

      if (!translate_ids)
      {
	output += headerline;
      }
      else
      {
	gidout.push_back(headerline);
      }
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
		eos::common::StringConversion::GetReadableSizeString(value1,
		    GetQuota(kAllUserBytesIs, 0), "B"),
		eos::common::StringConversion::GetReadableSizeString(value2,
		    GetQuota(kAllUserLogicalBytesIs, 0), "B"),
		eos::common::StringConversion::GetReadableSizeString(value3,
		    GetQuota(kAllUserFilesIs, 0), "-"),
		eos::common::StringConversion::GetReadableSizeString(value4,
		    GetQuota(kAllUserBytesTarget, 0), "B"),
		eos::common::StringConversion::GetReadableSizeString(value5,
		    GetQuota(kAllUserLogicalBytesTarget, 0), "B"),
		eos::common::StringConversion::GetReadableSizeString(value6,
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
		eos::common::StringConversion::GetReadableSizeString(value1,
		    GetQuota(kAllGroupBytesIs, 0), "B"),
		eos::common::StringConversion::GetReadableSizeString(value2,
		    GetQuota(kAllGroupLogicalBytesIs, 0), "B"),
		eos::common::StringConversion::GetReadableSizeString(value3,
		    GetQuota(kAllGroupFilesIs, 0), "-"),
		eos::common::StringConversion::GetReadableSizeString(value4,
		    GetQuota(kAllGroupBytesTarget, 0), "B"),
		eos::common::StringConversion::GetReadableSizeString(value5,
		    GetQuota(kAllGroupLogicalBytesTarget, 0), "B"),
		eos::common::StringConversion::GetReadableSizeString(value6,
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

  free(sortuidarray);
  free(sortgidarray);
}

//------------------------------------------------------------------------------
// Straigh-forward user/group quota checks
// If user & group quota is defined, both have to be fullfilled
//------------------------------------------------------------------------------
bool
SpaceQuota::CheckWriteQuota(uid_t uid, gid_t gid, long long desired_space,
			    unsigned int inodes)
{
  bool hasquota = false;
  // copy info from namespace Quota Node ...
  // get user/group and if defined project quota
  UpdateFromQuotaNode(uid, gid, GetQuota(kGroupBytesTarget, Quota::gProjectId,
					 false) ? true : false);
  eos_static_info("uid=%d gid=%d size=%llu quota=%llu", uid, gid, desired_space,
		  GetQuota(kUserBytesTarget, uid, false));
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

  if (GetQuota(kUserBytesTarget, uid, false) > 0)
  {
    userquota = true;
    uservolumequota = true;
  }

  if (GetQuota(kGroupBytesTarget, gid, false) > 0)
  {
    groupquota = true;
    groupvolumequota = true;
  }

  if (GetQuota(kUserFilesTarget, uid, false) > 0)
  {
    userquota = true;
    userinodequota = true;
  }

  if (GetQuota(kGroupFilesTarget, gid, false) > 0)
  {
    groupquota = true;
    groupinodequota = true;
  }

  if (uservolumequota)
  {
    if (((GetQuota(kUserBytesTarget, uid, false)) - (GetQuota(kUserBytesIs, uid,
	 false))) > (long long)(desired_space))
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
    if (((GetQuota(kUserFilesTarget, uid, false)) - (GetQuota(kUserFilesIs, uid,
	 false))) > (inodes))
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
    if (((GetQuota(kGroupBytesTarget, gid, false)) - (GetQuota(kGroupBytesIs, gid,
	 false))) > (long long)(desired_space))
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
    if ((((GetQuota(kGroupFilesTarget, gid, false)) - (GetQuota(kGroupFilesIs, gid,
	  false))) > (inodes)))
    {
      if (!groupvolumequota)
	hasgroupquota = true;
    }
    else
    {
      hasgroupquota = false;
    }
  }

  if ((((GetQuota(kGroupBytesTarget, Quota::gProjectId,
		  false)) - (GetQuota(kGroupBytesIs, Quota::gProjectId,
				      false))) > (long long)(desired_space)) &&
      (((GetQuota(kGroupFilesTarget, Quota::gProjectId,
		  false)) - (GetQuota(kGroupFilesIs, Quota::gProjectId, false))) > (inodes)))
  {
    hasprojectquota = true;
  }

  if (!userquota && !groupquota)
    projectquota = true;

  eos_static_info("userquota=%d groupquota=%d userquota=%d groupquota=%d userinodequota=%d uservolumequota=%d projectquota=%d hasprojectquota=%d\n",
		  userquota, groupquota, hasuserquota, hasgroupquota, userinodequota,
		  uservolumequota, projectquota, hasprojectquota);

  if ((userquota) && (groupquota))
  {
    // both are defined, we need to have both
    hasquota = hasuserquota & hasgroupquota;
  }
  else
  {
    hasquota = hasuserquota || hasgroupquota;
  }

  if (projectquota && hasprojectquota)
  {
    hasquota = true;
  }

  if (uid == 0)
  {
    // root does not need any quota
    hasquota = true;
  }

  return hasquota;
}

//------------------------------------------------------------------------------
// Write placement routine checking for quota and calling the scheduler
//------------------------------------------------------------------------------
int
SpaceQuota::FilePlacement(const char* path,
			  eos::common::Mapping::VirtualIdentity_t& vid,
			  const char* grouptag, unsigned long lid,
			  std::vector<unsigned int>& alreadyused_filesystems,
			  std::vector<unsigned int>& selected_filesystems,
			  tPlctPolicy plctpolicy, const std::string& plctTrgGeotag,
			  bool truncate, int forced_scheduling_group_index,
			  unsigned long long bookingsize)
{
  // 0 = 1 replica !
  unsigned int nfilesystems = eos::common::LayoutId::GetStripeNumber(lid) + 1;
  bool hasquota = false;
  uid_t uid = vid.uid;
  gid_t gid = vid.gid;
  // First figure out how many filesystems we need
  eos_static_debug("uid=%u gid=%u grouptag=%s place filesystems=%u", uid, gid,
		   grouptag, nfilesystems);

  // Check if the uid/gid has enough quota configured to place in this space
  if (IsEnabled())
  {
    // we have physical spacequota and namespace spacequota
    SpaceQuota* namespacequota = Quota::GetResponsibleSpaceQuota(path);

    if (namespacequota)
    {
      hasquota = namespacequota->CheckWriteQuota(uid, gid,
		 1ll * nfilesystems * bookingsize, nfilesystems);

      if (!hasquota)
      {
	eos_static_debug("uid=%u gid=%u grouptag=%s place filesystems=%u "
			 "has no quota left!", uid, gid, grouptag, nfilesystems);
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
    eos_static_debug("quota is disabled in space=%s", GetSpaceName());
  }

  std::string spacename = SpaceName.c_str();

  if (!FsView::gFsView.mSpaceGroupView.count(spacename))
  {
    eos_static_err("msg=\"no filesystem in space\" space=\"%s\"",
		   spacename.c_str());
    selected_filesystems.clear();
    return ENOSPC;
  }

  // Call the scheduler implementation
  return Scheduler::FilePlacement(path, vid, grouptag, lid,
				  alreadyused_filesystems,
				  selected_filesystems, plctpolicy, plctTrgGeotag,
				  truncate, forced_scheduling_group_index, bookingsize);
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
// gQuotaMutex.
//------------------------------------------------------------------------------
SpaceQuota*
Quota::GetSpaceQuota(const char* cpath, bool nocreate)
{
  std::string path = cpath;
  SpaceQuota* space_quota = 0;

  // Allow sloppy guys to skip typing '/' at the end
  if ((path[0] == '/') && (path[path.length() - 1]!= '/'))
      path += "/";

  if ((!pMapQuota.count(path)) || !(space_quota = pMapQuota[path]))
  {
    if (nocreate)
      return NULL;

    do
    {
      // This is a dangerous way if any other mutex was used from the caller
      // after gQuotaMutex.UnLockRead() => take care not do to that!
      gQuotaMutex.UnLockRead();
      gQuotaMutex.LockWrite();
      space_quota = new SpaceQuota(path.c_str());
      pMapQuota[path] = space_quota;
      gQuotaMutex.UnLockWrite();
      gQuotaMutex.LockRead();
    }
    while ((!pMapQuota.count(path) && (!(space_quota = pMapQuota[path]))));
  }

  return space_quota;
}

//------------------------------------------------------------------------------
// Get space quota object responsible for path (find best match) - caller has
// to have a read lock on gQuotaMutex.
//------------------------------------------------------------------------------
SpaceQuota*
Quota::GetResponsibleSpaceQuota(const char* cpath)
{
  XrdOucString matchpath = cpath;
  SpaceQuota* spacequota = 0;

  for (auto it = pMapQuota.begin(); it != pMapQuota.end(); it++)
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
// Get individual quota
//------------------------------------------------------------------------------
void
Quota::GetIndividualQuota(eos::common::Mapping::VirtualIdentity_t& vid,
			  const char* path, long long& max_bytes,
			  long long& free_bytes)
{
  eos::common::RWMutexReadLock lock(Quota::gQuotaMutex);
  SpaceQuota* space = Quota::GetResponsibleSpaceQuota(path);

  if (space)
  {
    long long max_bytes_usr, max_bytes_grp, max_bytes_prj;
    long long free_bytes_usr, free_bytes_grp, free_bytes_prj;
    free_bytes_usr = free_bytes_grp = free_bytes_prj = 0;
    max_bytes_usr = max_bytes_grp = max_bytes_prj = 0;
    space->Refresh();
    max_bytes_usr  = space->GetQuota(SpaceQuota::kUserBytesTarget, vid.uid);
    max_bytes_grp = space->GetQuota(SpaceQuota::kGroupBytesTarget, vid.gid);
    max_bytes_prj = space->GetQuota(SpaceQuota::kGroupBytesTarget, Quota::gProjectId);
    free_bytes_usr = max_bytes_usr - space->GetQuota(SpaceQuota::kUserBytesIs,
		     vid.uid);
    free_bytes_grp = max_bytes_grp - space->GetQuota(SpaceQuota::kGroupBytesIs,
		      vid.gid);
    free_bytes_prj = max_bytes_prj - space->GetQuota(
			  SpaceQuota::kGroupBytesIs, Quota::gProjectId);

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

  eos::common::RWMutexReadLock rd_lock(gQuotaMutex);
  SpaceQuota* space_quota = GetSpaceQuota(path.c_str());

  if (!space_quota)
  {
    oss_msg << "error: no quota space defined for node " << path << std::endl;
    msg = oss_msg.str();
    return false;
  }

  space_quota->SetQuota(quota_tag, id, value);
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
  eos::common::RWMutexReadLock rd_lock(gQuotaMutex);
  SpaceQuota* space_quota = GetSpaceQuota(space.c_str());

  if (space_quota)
  {
    space_quota->SetQuota(spaceq_type, id, value);
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

  eos::common::RWMutexReadLock lock(gQuotaMutex);
  SpaceQuota* space_quota = GetSpaceQuota(path.c_str());

  if (!space_quota)
  {
    oss_msg << "error: no quota space defined for node " << path << std::endl;
    msg = oss_msg.str();
    return false;
  }

  if (space_quota->RmQuota(quota_tag, id))
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
Quota::RmSpaceQuota(const std::string& space, std::string& msg, int& retc)
{
  eos_static_debug("space=%s", space.c_str());
  eos::common::RWMutexWriteLock lock(gQuotaMutex);
  SpaceQuota* spacequota = GetSpaceQuota(space.c_str(), true);

  if (!spacequota)
  {
    retc = EINVAL;
    msg = "error: there is no quota node under path ";
    msg += space;
    return false;
  }
  else
  {
    msg = "success: removed space quota for ";
    msg += space;
    spacequota->RemoveQuotaNode(msg, retc);
    pMapQuota.erase(space.c_str());
    // Remove all configuration entries
    std::string match = space;
    match += ":";
    gOFS->ConfEngine->DeleteConfigValueByMatch("quota", match.c_str());

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
  eos::common::RWMutexReadLock rd_lock(gQuotaMutex);
  SpaceQuota* space_quota = GetSpaceQuota(space.c_str());

  if (space_quota)
  {
    space_quota->RmQuota(spaceq_type, id);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Callback function for the namespace to calculate how much space a file occupies
//------------------------------------------------------------------------------
uint64_t
Quota::MapSizeCB(const eos::IFileMD* file)
{
  if (!file)
    return 0;

  eos::IFileMD::layoutId_t lid = file->getLayoutId();
  return (unsigned long long)
	 file->getSize() * eos::common::LayoutId::GetSizeFactor(lid);
}

//------------------------------------------------------------------------------
// Load nodes
//------------------------------------------------------------------------------
void
Quota::LoadNodes()
{
  // Iterate over the defined quota nodes and make them visible as SpaceQuota
  eos::common::RWMutexReadLock lock(gQuotaMutex);
  eos::IQuotaStats::NodeMap::iterator it;
  std::vector<std::string> createQuota;
  // Load all known nodes
  {
    eos::common::RWMutexReadLock nslock(gOFS->eosViewRWMutex);

    for (it = gOFS->eosView->getQuotaStats()->nodesBegin();
	 it != gOFS->eosView->getQuotaStats()->nodesEnd(); it++)
    {
      try
      {
	eos::IContainerMD::id_t id = it->first;
	eos::IContainerMD* container = gOFS->eosDirectoryService->getContainerMD(id);
	std::string quotapath = gOFS->eosView->getUri(container);
	SpaceQuota* spacequota = Quota::GetSpaceQuota(quotapath.c_str(), true);

	if (!spacequota)
	  createQuota.push_back(quotapath);
      }
      catch (eos::MDException& e)
      {
	errno = e.getErrno();
	eos_static_err("msg=\"exception\" ec=%d emsg=\"%s\"\n",
		       e.getErrno(), e.getMessage().str().c_str());
      }
    }
  }

  // Create missing nodes without namespace mutex held
  for (size_t i = 0; i < createQuota.size(); ++i)
  {
    SpaceQuota* spacequota = Quota::GetSpaceQuota(createQuota[i].c_str(), false);
    spacequota = Quota::GetSpaceQuota(createQuota[i].c_str(), false);

    if (spacequota)
      eos_static_notice("Created space for quota node: %s", createQuota[i].c_str());
    else
      eos_static_err("Failed to create space for quota node: %s\n",
		     createQuota[i].c_str());
  }
}

//------------------------------------------------------------------------------
// Map nodes to space quota
//------------------------------------------------------------------------------
void
Quota::NodesToSpaceQuota()
{
  eos::common::RWMutexReadLock locker(gQuotaMutex);
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  // inserts the current state of the quota nodes into SpaceQuota's
  eos::IQuotaStats::NodeMap::iterator it;

  for (it = gOFS->eosView->getQuotaStats()->nodesBegin();
       it != gOFS->eosView->getQuotaStats()->nodesEnd(); it++)
  {
    try
    {
      eos::IContainerMD::id_t id = it->first;
      eos::IContainerMD* container = gOFS->eosDirectoryService->getContainerMD(id);
      std::string quotapath = gOFS->eosView->getUri(container);
      NodeToSpaceQuota(quotapath.c_str());
    }
    catch (eos::MDException& e)
    {
      errno = e.getErrno();
      eos_static_err("msg=\"exception\" ec=%d emsg=\"%s\"\n",
		     e.getErrno(), e.getMessage().str().c_str());
    }
  }
}

//------------------------------------------------------------------------------
// Node to space quota by path
//------------------------------------------------------------------------------
void
Quota::NodeToSpaceQuota(const char* name)
{
  // Has to be called with gQuotaMutex read-locked and eosViewMutexRW locked
  if (!name)
    return;

  SpaceQuota* space_quota = Quota::GetSpaceQuota(name, false);

  if (space_quota && space_quota->UpdateQuotaNodeAddress()
      && space_quota->GetQuotaNode())
  {
    // Insert current state of a single quota node into a SpaceQuota
    eos::IQuotaNode::UserMap::const_iterator itu;
    eos::IQuotaNode::GroupMap::const_iterator itg;
    space_quota->ResetQuota(SpaceQuota::kGroupBytesIs, gProjectId);
    space_quota->ResetQuota(SpaceQuota::kGroupFilesIs, gProjectId);
    space_quota->ResetQuota(SpaceQuota::kGroupLogicalBytesIs, gProjectId);

    // Loop over users
    for (itu = space_quota->GetQuotaNode()->userUsageBegin();
	 itu != space_quota->GetQuotaNode()->userUsageEnd(); itu++)
    {
      space_quota->ResetQuota(SpaceQuota::kUserBytesIs, itu->first);
      space_quota->AddQuota(SpaceQuota::kUserBytesIs, itu->first,
			    itu->second.physicalSpace);
      space_quota->ResetQuota(SpaceQuota::kUserFilesIs, itu->first);
      space_quota->AddQuota(SpaceQuota::kUserFilesIs, itu->first, itu->second.files);
      space_quota->ResetQuota(SpaceQuota::kUserLogicalBytesIs, itu->first);
      space_quota->AddQuota(SpaceQuota::kUserLogicalBytesIs, itu->first,
			    itu->second.space);

      if (space_quota->GetQuota(SpaceQuota::kGroupBytesTarget, gProjectId) > 0)
      {
	// Only account in project quota nodes
	space_quota->AddQuota(SpaceQuota::kGroupBytesIs, gProjectId,
			      itu->second.physicalSpace);
	space_quota->AddQuota(SpaceQuota::kGroupLogicalBytesIs, gProjectId,
			      itu->second.space);
	space_quota->AddQuota(SpaceQuota::kGroupFilesIs, gProjectId, itu->second.files);
      }
    }

    for (itg = space_quota->GetQuotaNode()->groupUsageBegin();
	 itg != space_quota->GetQuotaNode()->groupUsageEnd(); itg++)
    {
      // dont' update the project quota directory from the quota
      if (itg->first == gProjectId)
	continue;

      space_quota->ResetQuota(SpaceQuota::kGroupBytesIs, itg->first);
      space_quota->AddQuota(SpaceQuota::kGroupBytesIs, itg->first,
			    itg->second.physicalSpace);
      space_quota->ResetQuota(SpaceQuota::kGroupFilesIs, itg->first);
      space_quota->AddQuota(SpaceQuota::kGroupFilesIs, itg->first, itg->second.files);
      space_quota->ResetQuota(SpaceQuota::kGroupLogicalBytesIs, itg->first);
      space_quota->AddQuota(SpaceQuota::kGroupLogicalBytesIs, itg->first,
			    itg->second.space);
    }
  }
}

//------------------------------------------------------------------------------
// Print out quota information
//------------------------------------------------------------------------------
void
Quota::PrintOut(const char* space, XrdOucString& output, long uid_sel,
		long gid_sel, bool monitoring, bool translate_ids)
{
  // Add this to have all quota nodes visible even if they are not in
  // the configuration file
  LoadNodes();
  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
  eos::common::RWMutexReadLock lock(gQuotaMutex);
  output = "";
  XrdOucString spacenames = "";

  if (space == 0)
  {
    // make sure all configured spaces exist In the quota views
    std::map<std::string, FsSpace*>::const_iterator sit;

    for (sit = FsView::gFsView.mSpaceView.begin();
	 sit != FsView::gFsView.mSpaceView.end(); sit++)
    {
      Quota::GetSpaceQuota(sit->second->GetMember("name").c_str());
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
    std::string sspace = space;
    SpaceQuota* spacequota = GetResponsibleSpaceQuota(space);

    if (spacequota)
    {
      spacequota->Refresh();
      spacequota->PrintOut(output, uid_sel, gid_sel, monitoring, translate_ids);
    }
  }
}

//------------------------------------------------------------------------------
// Clean-up all space quotas by deleting them and clearing the map
//------------------------------------------------------------------------------
void
Quota::CleanUp()
{
  eos::common::RWMutexWriteLock wr_lock(gQuotaMutex);

  for (auto it = pMapQuota.begin(); it != pMapQuota.end(); ++it)
    delete it->second;

  pMapQuota.clear();
}

EOSMGMNAMESPACE_END
