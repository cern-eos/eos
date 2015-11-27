// ----------------------------------------------------------------------
// File: proc/user/Quota.cc
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Acl.hh"
#include "mgm/Quota.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Quota ()
{
  using eos::common::Mapping;
  using eos::common::StringConversion;

  int errc;
  long id = 0;
  Quota::IdT id_type = Quota::IdT::kUid;
  Quota::Type quota_type = Quota::Type::kUnknown;
  std::string space = "";

  if (pOpaque->Get("mgm.quota.space"))
    space = pOpaque->Get("mgm.quota.space");

  gOFS->MgmStats.Add("Quota", pVid->uid, pVid->gid, 1);

  if (!space.empty())
  {
    // evt. correct the space variable to be a directory path (+/)
    struct stat buf;
    std::string sspace = space;
    if (sspace[sspace.length() - 1] != '/')
      sspace += '/';

    if (!gOFS->_stat(sspace.c_str(), &buf, *mError, *pVid, 0))
    {
      // this exists, so we rewrite space as asspace
      space = sspace;
    }
  }

  if (mSubCmd == "lsuser")
  {
    XrdOucString monitoring = pOpaque->Get("mgm.quota.format");
    bool monitor = false;

    if (monitoring == "m")
    {
      monitor = true;
    }

    eos_notice("quota ls (user)");
    XrdOucString out1 = "";
    XrdOucString out2 = "";
    if (!monitor) stdOut += "By user ...\n";
    Quota::PrintOut(space, out1, pVid->uid, -1, monitor, true);
    stdOut += out1;
    if (!monitor)stdOut += "By group ...\n";
    Quota::PrintOut(space, out2, -1, pVid->gid, monitor, true);
    stdOut += out2;
    mDoSort = false;
    return SFS_OK;
  }

  bool canQuota = false;

  if ((!vid.uid) || (Mapping::HasUid(3, vid.uid_list)) ||
      (Mapping::HasGid(4, vid.gid_list)))
  {
    // root and admin can set quota
    canQuota = true;
  }
  else
  {
    // figure out if the authenticated user is a quota admin
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    eos::IContainerMD::XAttrMap attrmap;
    if (space[0] != '/')
    {
      // take the proc directory
      space = gOFS->MgmProcPath.c_str();
    }

    // ACL and permission check
    Acl acl(space.c_str(), *mError, *pVid, attrmap, false);
    canQuota = acl.CanSetQuota();
  }

  if (canQuota)
  {
    if (mSubCmd == "ls")
    {
      eos_notice("quota ls");

      XrdOucString uid_sel = pOpaque->Get("mgm.quota.uid");
      XrdOucString gid_sel = pOpaque->Get("mgm.quota.gid");
      XrdOucString monitoring = pOpaque->Get("mgm.quota.format");
      XrdOucString printid = pOpaque->Get("mgm.quota.printid");

      std::string suid = (uid_sel.length()) ? uid_sel.c_str() : "0";
      std::string sgid = (gid_sel.length()) ? gid_sel.c_str() : "0";
      long uid = Mapping::UserNameToUid(suid, errc);
      long gid = Mapping::GroupNameToGid(sgid, errc);

      bool monitor = false;
      bool translate = true;

      if (monitoring == "m")
	monitor = true;

      if (printid == "n")
	translate = false;

      XrdOucString out1 = "";
      XrdOucString out2 = "";

      if ((!uid_sel.length() && (!gid_sel.length())))
      {
	Quota::PrintOut(space, stdOut, -1, -1, monitor, translate);
      }
      else
      {
	if (uid_sel.length())
	{
	  Quota::PrintOut(space, out1, uid, -1, monitor, translate);
	  stdOut += out1;
	}

	if (gid_sel.length())
	{
	  Quota::PrintOut(space, out2, -1, gid, monitor, translate);
	  stdOut += out2;
	}
      }
    }

    if (mSubCmd == "set")
    {
      if ( (pVid->prot != "sss") || (Mapping::IsLocalhost(*pVid)) )
      {
	eos_notice("quota set");
	std::string msg {""};
	XrdOucString uid_sel = pOpaque->Get("mgm.quota.uid");
	XrdOucString gid_sel = pOpaque->Get("mgm.quota.gid");
	XrdOucString svolume = pOpaque->Get("mgm.quota.maxbytes");
	XrdOucString sinodes = pOpaque->Get("mgm.quota.maxinodes");

	if (space.empty())
	{
	  stdErr = "error: command not properly formatted";
	  retc = EINVAL;
	  return SFS_OK;
	}

	if (uid_sel.length() && gid_sel.length())
	{
	  stdErr = "error: specify either a uid or a gid - not both!";
	  retc = EINVAL;
	  return SFS_OK;
	}

	if (uid_sel.length())
	{
	  id_type = Quota::IdT::kUid;
	  id = Mapping::UserNameToUid(uid_sel.c_str(), errc);
	}
	else if (gid_sel.length())
	{
	  id_type = Quota::IdT::kGid;
	  id = Mapping::GroupNameToGid(gid_sel.c_str(), errc);
	}
	else
	{
	  stdErr = "error: no uid/gid specified for quota set";
	  retc = EINVAL;
	  return SFS_OK;
	}

	// Deal with volume quota
	unsigned long long size = StringConversion::GetSizeFromString(svolume);

	if (svolume.length() && (errno == EINVAL))
	{
	  stdErr = "error: the volume quota you specified is not a valid number";
	  retc = EINVAL;
	  return SFS_OK;
	}
	else if (svolume.length())
	{
	  // Set volume quota
	  if (Quota::SetQuotaTypeForId(space, id, id_type, Quota::Type::kVolume,
				       size, msg, retc))
	  {
	    stdOut = msg.c_str();
	  }
	  else
	  {
	    stdErr = msg.c_str();
	    return SFS_OK;
	  }
	}

	// Deal with inode quota
	unsigned long long inodes = StringConversion::GetSizeFromString(sinodes);

	if (sinodes.length() && (errno == EINVAL))
	{
	  stdErr = "error: the inode quota you specified are not a valid number";
	  retc = EINVAL;
	  return SFS_OK;
	}
	else if (sinodes.length())
	{
	  // Set inode quota
	  if (Quota::SetQuotaTypeForId(space, id, id_type, Quota::Type::kInode,
				       inodes, msg, retc))
	  {
	    stdOut += msg.c_str();
	  }
	  else
	  {
	    stdErr += msg.c_str();
	    return SFS_OK;
	  }
	}

	if ((!svolume.length()) && (!sinodes.length()))
	{
	  stdErr = "error: max. bytes or max. inodes values have to be defined";
	  retc = EINVAL;
	  return SFS_OK;
	}
      }
      else
      {
	retc = EPERM;
	stdErr = "error: you cannot set quota from storage node with 'sss' "
	  "authentication!";
      }
    }

    if (mSubCmd == "rm")
    {
      eos_notice("quota rm");
      if ((pVid->prot != "sss") || (Mapping::IsLocalhost(*pVid)) )
      {
	int errc;
	XrdOucString uid_sel = pOpaque->Get("mgm.quota.uid");
	XrdOucString gid_sel = pOpaque->Get("mgm.quota.gid");

	if (space.empty())
	{
	  stdErr = "error: command not properly formatted";
	  retc = EINVAL;
	  return SFS_OK;
	}

	if (uid_sel.length() && gid_sel.length())
	{
	  stdErr = "error: you can either specify a uid or a gid - not both!";
	  retc = EINVAL;
	  return SFS_OK;
	}

	if (uid_sel.length())
	{
	  id_type = Quota::IdT::kUid;
	  id = Mapping::UserNameToUid(uid_sel.c_str(), errc);

	  if (errc == EINVAL)
	  {
	    stdErr = "error: unable to translate uid=";
	    stdErr += gid_sel;
	    retc = EINVAL;
	    return SFS_OK;
	  }
	}
	else if (gid_sel.length())
	{
	  id_type = Quota::IdT::kGid;
	  id = Mapping::GroupNameToGid(gid_sel.c_str(), errc);

	  if (errc == EINVAL)
	  {
	    stdErr = "error: unable to translate gid=";
	    stdErr += gid_sel;
	    retc = EINVAL;
	    return SFS_OK;
	  }
	}
	else
	{
	  stdErr = "error: no uid/gid specified for quota remove";
	  retc = EINVAL;
	  return SFS_OK;
	}

	std::string msg{""};
	XrdOucString qtype  = pOpaque->Get("mgm.quota.type");

	// Get type of quota
	if (qtype.length() == 0)
	  quota_type = Quota::Type::kAll;
	else if (qtype == "inode")
	  quota_type = Quota::Type::kInode;
	else if (qtype == "volume")
	  quota_type = Quota::Type::kVolume;

	if (quota_type == Quota::Type::kUnknown)
	{
	  retc = EINVAL;
	  stdErr = "error: unknown quota type ";
	  stdErr += qtype.c_str();
	}
	else if (quota_type == Quota::Type::kAll)
	{
	  if (Quota::RmQuotaForId(space, id, id_type, msg, retc))
	    stdOut = msg.c_str();
	  else
	    stdErr = msg.c_str();
	}
	else
	{
	  if (Quota::RmQuotaTypeForId(space, id, id_type, quota_type, msg, retc))
	    stdOut = msg.c_str();
	  else
	    stdErr = msg.c_str();
	}
      }
      else
      {
	retc = EPERM;
	stdErr = "error: you cannot remove quota from a storage node using "
	  "'sss' authentication!";
      }
    }
  }
  else
  {
    retc = EPERM;
    stdErr = "error: you are not a quota administrator!";
  }
  return SFS_OK;
}

EOSMGMNAMESPACE_END
