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
  XrdOucString space = pOpaque->Get("mgm.quota.space");
  gOFS->MgmStats.Add("Quota", pVid->uid, pVid->gid, 1);

  if (space.length())
  {
    // evt. correct the space variable to be a directory path (+/)
    struct stat buf;
    XrdOucString sspace = space;
    if (!space.endswith("/"))
      sspace += "/";

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
    Quota::PrintOut(space.c_str(), out1, pVid->uid, -1, monitor, true);
    stdOut += out1;
    if (!monitor)stdOut += "By group ...\n";
    Quota::PrintOut(space.c_str(), out2, -1, pVid->gid, monitor, true);
    stdOut += out2;
    mDoSort = false;
    return SFS_OK;
  }

  bool canQuota = false;

  if ((!vid.uid) ||
      (eos::common::Mapping::HasUid(3, vid.uid_list)) ||
      (eos::common::Mapping::HasGid(4, vid.gid_list)))
  {
    // root and admin can set quota
    canQuota = true;
  }
  else
  {
    // figure out if the authenticated user is a quota admin
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    eos::ContainerMD* dh;
    eos::ContainerMD::XAttrMap attrmap;
    if (!space.beginswith("/"))
    {
      // take the proc directory
      space = gOFS->MgmProcPath;
    }

    try
    {
      dh = gOFS->eosView->getContainer(space.c_str());
      // get attributes
      eos::ContainerMD::XAttrMap::const_iterator it;
      for (it = dh->attributesBegin(); it != dh->attributesEnd(); ++it)
      {
        attrmap[it->first] = it->second;
      }
      // ACL and permission check
    }
    catch (eos::MDException &e)
    {
      ;
    }

    Acl acl(attrmap.count("sys.acl") ? attrmap["sys.acl"] : std::string(""), attrmap.count("user.acl") ? attrmap["user.acl"] : std::string(""), vid);
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
      int errc;
      long uid = eos::common::Mapping::UserNameToUid(suid, errc);
      long gid = eos::common::Mapping::GroupNameToGid(sgid, errc);

      bool monitor = false;
      bool translate = true;
      if (monitoring == "m")
      {
        monitor = true;
      }
      if (printid == "n")
      {
        translate = false;
      }

      XrdOucString out1 = "";
      XrdOucString out2 = "";

      if ((!uid_sel.length() && (!gid_sel.length())))
      {
        Quota::PrintOut(space.c_str(), stdOut, -1, -1, monitor, translate);
      }
      else
      {
        if (uid_sel.length())
        {
          Quota::PrintOut(space.c_str(), out1, uid, -1, monitor, translate);
          stdOut += out1;
        }

        if (gid_sel.length())
        {
          Quota::PrintOut(space.c_str(), out2, -1, gid, monitor, translate);
          stdOut += out2;
        }
      }
    }

    if (mSubCmd == "set")
    {
      if (pVid->prot != "sss")
      {
        eos_notice("quota set");
        XrdOucString uid_sel = pOpaque->Get("mgm.quota.uid");
        XrdOucString gid_sel = pOpaque->Get("mgm.quota.gid");
        XrdOucString svolume = pOpaque->Get("mgm.quota.maxbytes");
        XrdOucString sinodes = pOpaque->Get("mgm.quota.maxinodes");

        if (uid_sel.length() && gid_sel.length())
        {
          stdErr = "error: you either specify a uid or a gid - not both!";
          retc = EINVAL;
        }
        else
        {
          unsigned long long size = eos::common::StringConversion::GetSizeFromString(svolume);
          if ((svolume.length()) && (errno == EINVAL))
          {
            stdErr = "error: the size you specified is not a valid number!";
            retc = EINVAL;
          }
          else
          {
            unsigned long long inodes = eos::common::StringConversion::GetSizeFromString(sinodes);
            if ((sinodes.length()) && (errno == EINVAL))
            {
              stdErr = "error: the inodes you specified are not a valid number!";
              retc = EINVAL;
            }
            else
            {
              if ((!svolume.length()) && (!sinodes.length()))
              {
                stdErr = "error: quota set - max. bytes or max. inodes have to be defined!";
                retc = EINVAL;
              }
              else
              {
                XrdOucString msg = "";
                std::string suid = (uid_sel.length()) ? uid_sel.c_str() : "0";
                std::string sgid = (gid_sel.length()) ? gid_sel.c_str() : "0";
                int errc;
                long uid = eos::common::Mapping::UserNameToUid(suid, errc);
                long gid = eos::common::Mapping::GroupNameToGid(sgid, errc);
                if (!Quota::SetQuota(space, uid_sel.length() ? uid : -1, gid_sel.length() ? gid : -1, svolume.length() ? size : -2, sinodes.length() ? inodes : -2, msg, retc))
                {
                  stdErr = msg;
                }
                else
                {
                  stdOut = msg;
                }
              }
            }
          }
        }
      }
      else
      {
        retc = EPERM;
        stdErr = "error: you cannot set quota from storage node with 'sss' authentication!";
      }
    }

    if (mSubCmd == "rm")
    {
      eos_notice("quota rm");
      if (pVid->prot != "sss")
      {
        XrdOucString uid_sel = pOpaque->Get("mgm.quota.uid");
        XrdOucString gid_sel = pOpaque->Get("mgm.quota.gid");

        std::string suid = (uid_sel.length()) ? uid_sel.c_str() : "0";
        std::string sgid = (gid_sel.length()) ? gid_sel.c_str() : "0";
        int errc;
        long uid = eos::common::Mapping::UserNameToUid(suid, errc);
        long gid = eos::common::Mapping::GroupNameToGid(sgid, errc);

        XrdOucString msg = "";
        if (!Quota::RmQuota(space, uid_sel.length() ? uid : -1, gid_sel.length() ? gid : -1, msg, retc))
        {
          stdErr = msg;
        }
        else
        {
          stdOut = msg;
        }
      }
      else
      {
        retc = EPERM;
        stdErr = "error: you cannot remove quota from a storage node using 'sss' authentication!";
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
