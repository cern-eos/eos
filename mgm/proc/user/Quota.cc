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
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Acl.hh"
#include "mgm/Quota.hh"
#include "mgm/Stat.hh"

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::UserQuota()
{
  using eos::common::Mapping;
  using eos::common::StringConversion;
  int errc = 0;
  long id = 0;
  Quota::IdT id_type = Quota::IdT::kUid;
  Quota::Type quota_type = Quota::Type::kUnknown;
  std::string space = "";

  if (pOpaque->Get("mgm.quota.space")) {
    space = pOpaque->Get("mgm.quota.space");
  }

  gOFS->MgmStats.Add("Quota", pVid->uid, pVid->gid, 1);

  if (!space.empty()) {
    // evt. correct the space variable to be a directory path (+/)
    struct stat buf;
    std::string sspace = space;

    if (sspace[sspace.length() - 1] != '/') {
      sspace += '/';
    }

    if (!gOFS->_stat(sspace.c_str(), &buf, *mError, *pVid, 0)) {
      space = sspace;
    }
  }

  if (mSubCmd == "lsuser") {
    XrdOucString monitoring = pOpaque->Get("mgm.quota.format");
    bool monitor = false;

    if (monitoring == "m") {
      monitor = true;
    }

    eos_notice("quota ls (user)");
    XrdOucString out = "";
    bool is_ok = Quota::PrintOut(space, out, pVid->uid, -1, monitor, true);

    if (is_ok && out != "") {
      if (!monitor) {
        stdOut += "\nBy user:";
        stdOut += out;
      } else {
        stdOut += out;
      }
    } else {
      if (!is_ok) {
        stdErr += out;
        retc = EINVAL;
      }
    }

    out = "";
    is_ok = Quota::PrintOut(space, out, -1, pVid->gid, monitor, true);
    mDoSort = false;

    if (is_ok && out != "") {
      if (!monitor) {
        stdOut += "\nBy group:";
        stdOut += out;
      } else {
        stdOut += out;
      }
    } else {
      if (!is_ok) {
        stdErr += out;
        retc = EINVAL;
      }
    }

    return SFS_OK;
  }

  bool canQuota = false;

  if ((!vid.uid) || vid.hasUid(3) || vid.hasGid(4)) {
    // root and admin can set quota
    canQuota = true;
  } else {
    // figure out if the authenticated user is a quota admin
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
    eos::IContainerMD::XAttrMap attrmap;

    if (space[0] != '/') {
      // take the proc directory
      space = gOFS->MgmProcPath.c_str();
    } else {
      // effectively check ACLs on the quota node directory if it can be retrieved
      std::string quota_node_path = Quota::GetResponsibleSpaceQuotaPath(space);

      if (quota_node_path.length()) {
        space = quota_node_path;
      }
    }

    // ACL and permission check
    Acl acl(space.c_str(), *mError, *pVid, attrmap, false);
    canQuota = acl.CanSetQuota();
  }

  if (canQuota) {
    std::string uid_sel = (pOpaque->Get("mgm.quota.uid") ?
                           pOpaque->Get("mgm.quota.uid") : "");
    std::string gid_sel = (pOpaque->Get("mgm.quota.gid") ?
                           pOpaque->Get("mgm.quota.gid") : "");

    if (mSubCmd == "ls") {
      eos_notice("quota ls");
      XrdOucString monitoring = pOpaque->Get("mgm.quota.format");
      XrdOucString printid = pOpaque->Get("mgm.quota.printid");
      long long int uid = (uid_sel.empty() ? -1LL :
                           Mapping::UserNameToUid(uid_sel, errc));
      long long int gid = (gid_sel.empty() ? -1LL :
                           Mapping::GroupNameToGid(gid_sel, errc));
      bool monitor = false;
      bool translate = true;

      if (monitoring == "m") {
        monitor = true;
      }

      if (printid == "n") {
        translate = false;
      }

      XrdOucString out1 = "";

      if ((uid != -1LL) && (gid != -1LL)) {
        // Print both uid and gid info
        if (!Quota::PrintOut(space, out1, uid, -1LL, monitor, translate)) {
          stdOut = "";
          stdErr = out1.c_str();
          retc = EINVAL;
        } else {
          XrdOucString out2 = "";

          if (!Quota::PrintOut(space, out2, -1LL, gid, monitor, translate)) {
            stdOut = "";
            stdErr = out2.c_str();
            retc = EINVAL;
          } else {
            out1 += out2;
            stdOut = out1.c_str();
          }
        }
      } else {
        // Either uid or gid is printed
        if (Quota::PrintOut(space, out1, uid, gid, monitor, translate)) {
          stdOut = out1;
        } else {
          stdOut = "";
          stdErr = out1.c_str();
          retc = EINVAL;
        }
      }
    }

    if (mSubCmd == "set") {
      if ((pVid->prot != "sss") || pVid->isLocalhost()) {
        eos_notice("quota set");
        std::string msg {""};
        XrdOucString svolume = pOpaque->Get("mgm.quota.maxbytes");
        XrdOucString sinodes = pOpaque->Get("mgm.quota.maxinodes");

        if (space.empty()) {
          stdErr = "error: command not properly formatted";
          retc = EINVAL;
          return SFS_OK;
        }

        if (uid_sel.length() && gid_sel.length()) {
          stdErr = "error: you need specify either a uid or a gid";
          retc = EINVAL;
          return SFS_OK;
        }

        if (uid_sel.length()) {
          id_type = Quota::IdT::kUid;
          id = Mapping::UserNameToUid(uid_sel.c_str(), errc);

          if (errc == EINVAL) {
            stdErr = "error: unable to translate uid=";
            stdErr += uid_sel.c_str();
            retc = EINVAL;
            return SFS_OK;
          }
        } else if (gid_sel.length()) {
          id_type = Quota::IdT::kGid;
          id = Mapping::GroupNameToGid(gid_sel.c_str(), errc);

          if (errc == EINVAL) {
            stdErr = "error: unable to translate gid=";
            stdErr += gid_sel.c_str();
            retc = EINVAL;
            return SFS_OK;
          }
        } else {
          stdErr = "error: no uid/gid specified for quota set";
          retc = EINVAL;
          return SFS_OK;
        }

        // Deal with volume quota
        unsigned long long size = StringConversion::GetDataSizeFromString(svolume);

        if (svolume.length() && ((errno == EINVAL) || (errno == ERANGE))) {
          stdErr = "error: the volume quota you specified is not a valid number";
          retc = EINVAL;
          return SFS_OK;
        } else if (svolume.length()) {
          // Set volume quota
          if (Quota::SetQuotaTypeForId(space, id, id_type, Quota::Type::kVolume,
                                       size, msg, retc)) {
            stdOut = msg.c_str();
          } else {
            stdErr = msg.c_str();
            return SFS_OK;
          }
        }

        // Deal with inode quota
        unsigned long long inodes = StringConversion::GetSizeFromString(sinodes);

        if (sinodes.length() && (errno == EINVAL)) {
          stdErr = "error: the inode quota you specified are not a valid number";
          retc = EINVAL;
          return SFS_OK;
        } else if (sinodes.length()) {
          // Set inode quota
          if (Quota::SetQuotaTypeForId(space, id, id_type, Quota::Type::kInode,
                                       inodes, msg, retc)) {
            stdOut += msg.c_str();
          } else {
            stdErr += msg.c_str();
            return SFS_OK;
          }
        }

        if ((!svolume.length()) && (!sinodes.length())) {
          stdErr = "error: max. bytes or max. inodes values have to be defined";
          retc = EINVAL;
          return SFS_OK;
        }
      } else {
        retc = EPERM;
        stdErr = "error: you cannot set quota from storage node with 'sss' "
                 "authentication!";
      }
    }

    if (mSubCmd == "rm") {
      eos_notice("quota rm");

      if ((pVid->prot != "sss") || pVid->isLocalhost()) {
        int errc;

        if (space.empty()) {
          stdErr = "error: command not properly formatted";
          retc = EINVAL;
          return SFS_OK;
        }

        if (uid_sel.length() && gid_sel.length()) {
          stdErr = "error: you need specify either a uid or a gid";
          retc = EINVAL;
          return SFS_OK;
        }

        if (uid_sel.length()) {
          id_type = Quota::IdT::kUid;
          id = Mapping::UserNameToUid(uid_sel.c_str(), errc);

          if (errc == EINVAL) {
            stdErr = "error: unable to translate uid=";
            stdErr += uid_sel.c_str();
            retc = EINVAL;
            return SFS_OK;
          }
        } else if (gid_sel.length()) {
          id_type = Quota::IdT::kGid;
          id = Mapping::GroupNameToGid(gid_sel.c_str(), errc);

          if (errc == EINVAL) {
            stdErr = "error: unable to translate gid=";
            stdErr += gid_sel.c_str();
            retc = EINVAL;
            return SFS_OK;
          }
        } else {
          stdErr = "error: no uid/gid specified for quota remove";
          retc = EINVAL;
          return SFS_OK;
        }

        std::string msg{""};
        XrdOucString qtype  = pOpaque->Get("mgm.quota.type");

        // Get type of quota
        if (qtype.length() == 0) {
          quota_type = Quota::Type::kAll;
        } else if (qtype == "inode") {
          quota_type = Quota::Type::kInode;
        } else if (qtype == "volume") {
          quota_type = Quota::Type::kVolume;
        }

        if (quota_type == Quota::Type::kUnknown) {
          retc = EINVAL;
          stdErr = "error: unknown quota type ";
          stdErr += qtype.c_str();
        } else if (quota_type == Quota::Type::kAll) {
          if (Quota::RmQuotaForId(space, id, id_type, msg, retc)) {
            stdOut = msg.c_str();
          } else {
            stdErr = msg.c_str();
          }
        } else {
          if (Quota::RmQuotaTypeForId(space, id, id_type, quota_type, msg, retc)) {
            stdOut = msg.c_str();
          } else {
            stdErr = msg.c_str();
          }
        }
      } else {
        retc = EPERM;
        stdErr = "error: you cannot remove quota from a storage node using "
                 "'sss' authentication!";
      }
    }
  } else {
    retc = EPERM;
    stdErr = "error: you are not a quota administrator!";
  }

  return SFS_OK;
}

EOSMGMNAMESPACE_END
