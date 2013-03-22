// ----------------------------------------------------------------------
// File: proc_fs.cc
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
#include "mgm/proc/proc_fs.hh"
/*----------------------------------------------------------------------------*/
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/Mapping.hh"
#include "common/StringConversion.hh"
#include "common/Path.hh"
#include "mgm/Access.hh"
#include "mgm/FileSystem.hh"
#include "mgm/Policy.hh"
#include "mgm/Vid.hh"
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Quota.hh"
#include "mgm/FsView.hh"

/*----------------------------------------------------------------------------*/


EOSMGMNAMESPACE_BEGIN
/*----------------------------------------------------------------------------*/

int
proc_fs_dumpmd (std::string &fsidst, XrdOucString &option, XrdOucString &dp, XrdOucString &df, XrdOucString &ds, XrdOucString &stdOut, XrdOucString &stdErr, std::string &tident, eos::common::Mapping::VirtualIdentity &vid_in, size_t &entries)
{
  int retc = 0;
  bool dumppath = false;
  bool dumpfid = false;
  bool dumpsize = false;
  bool monitor = false;

  if (option != "m")
  {
    if (dp == "1")
    {
      dumppath = true;
    }
    if (df == "1")
    {
      dumpfid = true;
    }

    if (ds == "1")
    {
      dumpsize = true;
    }
  }
  else
  {
    monitor = true;
  }

  int fsid = 0;

  entries = 0;

  if (!fsidst.length())
  {
    stdErr = "error: illegal parameters";
    retc = EINVAL;
  }
  else
  {
    fsid = atoi(fsidst.c_str());
    eos::common::RWMutexReadLock nslock(gOFS->eosViewRWMutex);
    try
    {
      eos::FileMD* fmd = 0;
      eos::FileSystemView::FileList filelist = gOFS->eosFsView->getFileList(fsid);
      eos::FileSystemView::FileIterator it;
      for (it = filelist.begin(); it != filelist.end(); ++it)
      {
        std::string env;
        fmd = gOFS->eosFileService->getFileMD(*it);
        if (fmd)
        {
          entries++;
          if ((!dumppath) && (!dumpfid) && (!dumpsize))
          {
            fmd->getEnv(env);
            stdOut += env.c_str();
            if (monitor)
            {
              std::string fullpath = gOFS->eosView->getUri(fmd);
              eos::common::Path cPath(fullpath.c_str());
              stdOut += "&container=";
              stdOut += cPath.GetParentPath();
            }
            stdOut += "\n";
          }
          else
          {
            if (dumppath)
            {
              std::string fullpath = gOFS->eosView->getUri(fmd);
              stdOut += "path=";
              stdOut += fullpath.c_str();
            }
            if (dumpfid)
            {
              if (dumppath) stdOut += " ";
              char sfid[40];
              snprintf(sfid, 40, "fid=%llu", (unsigned long long) fmd->getId());
              stdOut += sfid;
            }
            if (dumpsize)
            {
              if (dumppath || dumpfid) stdOut += " ";
              char ssize[40];
              snprintf(ssize, 40, "size=%llu", (unsigned long long) fmd->getSize());
              stdOut += ssize;
            }

            stdOut += "\n";
          }
        }
      }

      if (monitor)
      {
        // also add files which have still to be unlinked
        eos::FileSystemView::FileList unlinkedfilelist = gOFS->eosFsView->getUnlinkedFileList(fsid);
        for (it = unlinkedfilelist.begin(); it != unlinkedfilelist.end(); ++it)
        {
          std::string env;
          fmd = gOFS->eosFileService->getFileMD(*it);
          if (fmd)
          {
            entries++;
            fmd->getEnv(env);
            stdOut += env.c_str();
            stdOut += "&container=-\n";
          }
        }
      }
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
      eos_static_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
    }
  }
  return retc;
}

int
proc_fs_config (std::string &identifier, std::string &key, std::string &value, XrdOucString &stdOut, XrdOucString &stdErr, std::string &tident, eos::common::Mapping::VirtualIdentity &vid_in)
{
  int retc = 0;
  eos::common::FileSystem::fsid_t fsid = atoi(identifier.c_str());
  if (!identifier.length() || !key.length() || !value.length())
  {
    stdErr = "error: illegal parameters";
    retc = EINVAL;
  }
  else
  {
    eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex);

    FileSystem* fs = 0;
    if (fsid && FsView::gFsView.mIdView.count(fsid))
    {
      // by filesystem id
      fs = FsView::gFsView.mIdView[fsid];
    }
    else
    {
      // by filesystem uuid
      if (FsView::gFsView.GetMapping(identifier))
      {
        if (FsView::gFsView.mIdView.count(FsView::gFsView.GetMapping(identifier)))
        {
          fs = FsView::gFsView.mIdView[FsView::gFsView.GetMapping(identifier)];
        }
      }
      else
      {
        // by host:port:data name
        std::string path = identifier;
        size_t slashpos = identifier.find("/");
        if (slashpos != std::string::npos)
        {
          path.erase(0, slashpos);
          identifier.erase(slashpos);
          if ((identifier.find(":") == std::string::npos))
          {
            identifier += ":1095"; // default eos fst port
          }
          if ((identifier.find("/eos/") == std::string::npos))
          {
            identifier.insert(0, "/eos/");
            identifier.append("/fst");
          }

          if (FsView::gFsView.mNodeView.count(identifier))
          {
            std::set<eos::common::FileSystem::fsid_t>::iterator it;
            for (it = FsView::gFsView.mNodeView[identifier]->begin(); it != FsView::gFsView.mNodeView[identifier]->end(); it++)
            {
              if (FsView::gFsView.mIdView.count(*it))
              {
                if (FsView::gFsView.mIdView[*it]->GetPath() == path)
                {
                  // this is the filesystem
                  fs = FsView::gFsView.mIdView[*it];
                }
              }
            }
          }
        }
      }
    }
    if (fs)
    {
      // check the allowed strings
      if (((key == "configstatus") && (eos::common::FileSystem::GetConfigStatusFromString(value.c_str()) != eos::common::FileSystem::kUnknown)) ||
          (((key == "headroom") || (key == "scaninterval") || (key == "graceperiod") || (key == "drainperiod"))))
      {

        std::string nodename = fs->GetString("host");
        size_t dpos = 0;

        if ((dpos = nodename.find(".")) != std::string::npos)
        {
          nodename.erase(dpos);
        }

        if ((vid_in.uid != 0) && ((vid_in.prot != "sss") || tident.compare(0, tident.length(), nodename, 0, tident.length())))
        {
          stdErr = "error: filesystems can only be configured as 'root' or from the server mounting them using sss protocol\n";
          retc = EPERM;
        }
        else
        {
          if ((key == "headroom") || (key == "scaninterval") || (key == "graceperiod") || (key == "drainperiod"))
          {
            fs->SetLongLong(key.c_str(), eos::common::StringConversion::GetSizeFromString(value.c_str()));
            FsView::gFsView.StoreFsConfig(fs);
          }
          else
          {
            if ((key == "configstatus") && (value == "empty"))
            {
              bool isempty = true;
              // check if this file system is really empty
              try
              {
                eos::FileSystemView::FileList filelist = gOFS->eosFsView->getFileList(fs->GetId());
                if (filelist.size())
                {
                  isempty = false;
                }
              }
              catch (eos::MDException &e)
              {
                isempty = true;
              }
              if (!isempty)
              {
                stdErr = "error: the filesystem is not empty, therefore it can't be removed\n";
                stdErr += "# -------------------------------------------------------------------\n";
                stdErr += "# You can inspect the registered files via the command:\n";
                stdErr += "# [eos] fs dumpmd ";
                stdErr += (int) fs->GetId();
                stdErr += " -path\n";
                stdErr += "# -------------------------------------------------------------------\n";
                stdErr += "# You can drain the filesystem if it is still operational via the command:\n";
                stdErr += "# [eos] fs config ";
                stdErr += (int) fs->GetId();
                stdErr += " configstatus=drain\n";
                stdErr += "# -------------------------------------------------------------------\n";
                stdErr += "# You can drain the filesystem if it is unusable via the command:\n";
                stdErr += "# [eos] fs config ";
                stdErr += (int) fs->GetId();
                stdErr += " configstatus=draindead\n";
                stdErr += "# -------------------------------------------------------------------\n";
                stdErr += "# You can force to remove these files via the command:\n";
                stdErr += "# [eos] fs dropfiles ";
                stdErr += (int) fs->GetId();
                stdErr += "\n";
                stdErr += "# -------------------------------------------------------------------\n";
                stdErr += "# You can force to drop these files (brute force) via the command:\n";
                stdErr += "# [eos] fs dropfiles ";
                stdErr += (int) fs->GetId();
                stdErr += "-f \n";
                stdErr += "# -------------------------------------------------------------------\n";
                stdErr += "# [eos] = 'eos -b' on MGM or 'eosadmin' on storage nodes\n";
                retc = EPERM;
              }
              else
              {
                fs->SetString(key.c_str(), value.c_str());
                FsView::gFsView.StoreFsConfig(fs);
              }
            }
            else
            {
              fs->SetString(key.c_str(), value.c_str());
              FsView::gFsView.StoreFsConfig(fs);
            }
          }
        }
      }
      else
      {
        stdErr += "error: not an allowed parameter <";
        stdErr += key.c_str();
        stdErr += ">";
        retc = EINVAL;
      }
    }
    else
    {
      stdErr += "error: cannot identify the filesystem by <";
      stdErr += identifier.c_str();
      stdErr += ">";
      retc = EINVAL;
    }
  }
  return retc;
}

/*----------------------------------------------------------------------------*/
int
proc_fs_add (std::string &sfsid, std::string &uuid, std::string &nodename, std::string &mountpoint, std::string &space, std::string &configstatus, XrdOucString &stdOut, XrdOucString &stdErr, std::string &tident, eos::common::Mapping::VirtualIdentity &vid_in)
{
  int retc = 0;
  eos::common::FileSystem::fsid_t fsid = atoi(sfsid.c_str());

  if ((!nodename.length()) || (!mountpoint.length()) || (!space.length()) || (!configstatus.length()) ||
      (configstatus.length() && (eos::common::FileSystem::GetConfigStatusFromString(configstatus.c_str()) < eos::common::FileSystem::kOff)))
  {
    stdErr += "error: illegal parameters";
    retc = EINVAL;
  }
  else
  {
    // node name comes as /eos/<host>..../, we just need <host> without domain
    std::string rnodename = nodename;
    rnodename.erase(0, 5);
    size_t dpos;

    if ((dpos = rnodename.find(".")) != std::string::npos)
    {
      rnodename.erase(dpos);
    }

    // ========> ViewMutex WRITELOCK
    FsView::gFsView.ViewMutex.LockWrite();

    // rough check that the filesystem is added from a host with the same tident ... anyway we should have configured 'sss' security
    if ((vid_in.uid != 0) && ((vid_in.prot != "sss") || tident.compare(0, tident.length(), rnodename, 0, tident.length())))
    {
      stdErr += "error: filesystems can only be added as 'root' or from the server mounting them using sss protocol\n";
      retc = EPERM;
    }
    else
    {
      // queuepath = /eos/<host:port><path>
      std::string queuepath = nodename;
      queuepath += mountpoint;

      // check if this filesystem exists already ....
      if (!FsView::gFsView.ExistsQueue(nodename, queuepath))
      {
        // check if there is a mapping for 'uuid'
        if (FsView::gFsView.GetMapping(uuid) || ((fsid > 0) && (FsView::gFsView.HasMapping(fsid))))
        {
          if (fsid)
          {
            stdErr += "error: filesystem identified by uuid='";
            stdErr += uuid.c_str();
            stdErr += "' id='";
            stdErr += sfsid.c_str();
            stdErr += "' already exists!";
          }
          else
          {
            stdErr += "error: filesystem identified by '";
            stdErr += uuid.c_str();
            stdErr += "' already exists!";
          }
          retc = EEXIST;
        }
        else
        {
          FileSystem* fs = 0;

          if (fsid)
          {
            if (!FsView::gFsView.ProvideMapping(uuid, fsid))
            {
              stdErr += "error: conflict adding your uuid & id mapping";
              retc = EINVAL;
            }
            else
            {
              fs = new FileSystem(queuepath.c_str(), nodename.c_str(), &gOFS->ObjectManager);
            }
          }
          else
          {
            fsid = FsView::gFsView.CreateMapping(uuid);
            fs = new FileSystem(queuepath.c_str(), nodename.c_str(), &gOFS->ObjectManager);
          }

          // we want one atomic update with all the parameters defined
          fs->OpenTransaction();

          XrdOucString sizestring;

          stdOut += "success:   mapped '";
          stdOut += uuid.c_str();
          stdOut += "' <=> fsid=";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) fsid);
          if (fs)
          {
            fs->SetId(fsid);
            fs->SetString("uuid", uuid.c_str());
            fs->SetString("configstatus", configstatus.c_str());
            //              fs->SetDrainStatus(eos::common::FileSystem::kNoDrain);
            std::string splitspace = "";
            std::string splitgroup = "";

            unsigned int groupsize = 0;
            unsigned int groupmod = 0;
            unsigned int subgroup = 0;

            bool dorandom = false;

            {
              // logic to automatically adjust scheduling subgroups
              eos::common::StringConversion::SplitByPoint(space, splitspace, splitgroup);
              if (FsView::gFsView.mSpaceView.count(splitspace))
              {
                groupsize = atoi(FsView::gFsView.mSpaceView[splitspace]->GetMember(std::string("cfg.groupsize")).c_str());
                groupmod = atoi(FsView::gFsView.mSpaceView[splitspace]->GetMember(std::string("cfg.groupmod")).c_str());
              }


              if (splitgroup.length())
              {
                // we have to check if the desired group is already full, in case we add to the next group by increasing the number by <groupmod>
                subgroup = atoi(splitgroup.c_str());
                if (splitgroup == "random")
                {
                  dorandom = true;
                  subgroup = (int) ((random()*1.0 / RAND_MAX) * groupmod);

                }

                int j = 0;
                size_t nnotfound = 0;
                for (j = 0; j < 1000; j++)
                {
                  char newgroup[1024];
                  snprintf(newgroup, sizeof (newgroup) - 1, "%s.%u", splitspace.c_str(), subgroup);
                  std::string snewgroup = newgroup;
                  if (!FsView::gFsView.mGroupView.count(snewgroup))
                  {
                    // great, this is still empty
                    splitgroup = newgroup;
                    break;
                  }
                  else
                  {
                    bool exists = false;
                    std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
                    // check if this node has not already a filesystem in this group
                    for (it = FsView::gFsView.mGroupView[snewgroup]->begin(); it != FsView::gFsView.mGroupView[snewgroup]->end(); it++)
                    {
                      if (FsView::gFsView.mIdView[*it]->GetString("host") == fs->GetString("host"))
                      {
                        // this subgroup has already this host
                        exists = true;
                      }
                    }
                    if ((!exists) && (((FsView::gFsView.mGroupView[snewgroup]->size()) < groupsize) || (groupsize == 0)))
                    {
                      // great, there is still space here
                      splitgroup = newgroup;
                      break;
                    }
                    else
                    {
                      if (dorandom)
                      {
                        nnotfound++;
                        if (nnotfound >= groupmod)
                        {
                          subgroup += groupmod;
                          nnotfound = 0;
                        }
                        else
                        {
                          int offset = subgroup / groupmod;
                          subgroup++;
                          subgroup = (offset * groupmod) + (subgroup % groupmod);
                        }
                      }
                      else
                      {
                        subgroup += groupmod;
                      }
                    }
                  }
                }

                if (j == 1000)
                {
                  eos_static_crit("infinite loop detected finding available scheduling group!");
                  stdErr += "error: infinite loop detected finding available scheduling group!";
                  retc = EFAULT;
                }
              }
              else
              {
                splitgroup = splitspace;
              }
            }

            if (!retc)
            {
              fs->SetString("schedgroup", splitgroup.c_str());
              if (!FsView::gFsView.Register(fs))
              {
                // remove mapping
                if (FsView::gFsView.RemoveMapping(fsid, uuid))
                {
                  // ok
                  stdOut += "\nsuccess: unmapped '";
                  stdOut += uuid.c_str();
                  stdOut += "' <!> fsid=";
                  stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) fsid);
                }
                else
                {
                  stdErr += "error: cannot remove mapping - this can be fatal!\n";
                }
                // remove filesystem object
                //delete fs;
                stdErr += "error: cannot register filesystem - check for path duplication!";
                retc = EINVAL;
              }
              // set all the space related default parameters
              if (FsView::gFsView.mSpaceView.count(space))
              {
                if (FsView::gFsView.mSpaceView[space]->ApplySpaceDefaultParameters(fs), true)
                {
                  // store the modifications
                  FsView::gFsView.StoreFsConfig(fs);
                }
              }

            }
            else
            {
              stdErr += "error: cannot allocate filesystem object";
              retc = ENOMEM;
            }
            fs->CloseTransaction(); // close all the definitions and broadcast
          }
        }
      }
      else
      {
        stdErr += "error: cannot register filesystem - is already existing!";
        retc = EEXIST;
      }
    }

    // ========> ViewMutex WRITEUnLOCK
    FsView::gFsView.ViewMutex.UnLockWrite();
  }
  return retc;
}

/*----------------------------------------------------------------------------*/
std::string
proc_fs_target (std::string target_group)

{
  // find's the scheduling group which needs the most a new filesystem
  std::string new_target = "";

  std::string splitspace = "";
  std::string splitgroup = "";

  eos::common::StringConversion::SplitByPoint(target_group, splitspace, splitgroup);

  // if we have a fully specified target, we just return that
  if (splitgroup.length())
    return target_group;

  // find the 'best' group e.g. the one with least filesystems
  std::map<std::string, FsGroup*>::const_iterator it;
  std::vector<std::string> mingroups;
  int minfs = 99999999;
  for (it = FsView::gFsView.mGroupView.begin(); it != FsView::gFsView.mGroupView.end(); it++)
  {
    std::string groupname = it->first;
    std::string groupspace = "";
    std::string groupgroup = "";
    eos::common::StringConversion::SplitByPoint(groupname, groupspace, groupgroup);
    if (groupspace != splitspace)
    {
      // this group is not in our space
      continue;
    }
    int groupfilesystems = it->second->SumLongLong("<n>?configstatus@rw", false);
    if (groupfilesystems < minfs)
    {
      mingroups.clear();
      mingroups.push_back(it->first);
      minfs = groupfilesystems;
    }
    if (groupfilesystems == minfs)
    {
      mingroups.push_back(it->first);
    }
  }

  if (!mingroups.size())
    return target_group;

  int randomgroup = ((1.0 * random()) / RAND_MAX) * mingroups.size();
  return mingroups[randomgroup];
}

/*----------------------------------------------------------------------------*/
FileSystem*
proc_fs_source (std::string source_group, std::string target_group)
{
  // find's a filesystem on nodes in source_group, which are not part of target_group

  std::string splitspace = "";
  std::string splitgroup = "";

  eos::common::StringConversion::SplitByPoint(source_group, splitspace, splitgroup);

  if (!FsView::gFsView.mGroupView.count(target_group))
    return 0;

  if (splitgroup.length())
  {
    // there is a mSelection of a group as source

    // if there is no source group, we can't do anything
    if (!FsView::gFsView.mGroupView.count(source_group))
      return 0;

    std::set<eos::common::FileSystem::fsid_t>::const_iterator its;
    std::set<eos::common::FileSystem::fsid_t>::const_iterator itt;
    // loop over all filesystems and check if the corrensponding node is already part of target_group
    for (its = FsView::gFsView.mGroupView[source_group]->begin(); its != FsView::gFsView.mGroupView[source_group]->end(); its++)
    {
      std::string sourcequeue = FsView::gFsView.mIdView[*its]->GetQueue();
      bool exists = false;
      // loop over all filesystems in target_group and check if they have already sourcequeue
      for (itt = FsView::gFsView.mGroupView[target_group]->begin(); itt != FsView::gFsView.mGroupView[target_group]->end(); itt++)
      {
        std::string targetqueue = FsView::gFsView.mIdView[*itt]->GetQueue();
        if (sourcequeue == targetqueue)
          exists = true;
      }
      if (!exists)
        return FsView::gFsView.mIdView[*its];
    }
  }
  else
  {
    // if there is no source space, we can't do anything
    if (!FsView::gFsView.mSpaceView.count(splitspace))
      return 0;

    std::set<eos::common::FileSystem::fsid_t>::const_iterator its;
    std::set<eos::common::FileSystem::fsid_t>::const_iterator itt;
    // loop over all filesystems and check if the corrensponding node is already part of target_group
    for (its = FsView::gFsView.mSpaceView[splitspace]->begin(); its != FsView::gFsView.mSpaceView[splitspace]->end(); its++)
    {
      std::string sourcequeue = FsView::gFsView.mIdView[*its]->GetQueue();
      bool exists = false;
      // loop over all filesystems in target_group and check if they have already sourcequeue
      for (itt = FsView::gFsView.mGroupView[target_group]->begin(); itt != FsView::gFsView.mGroupView[target_group]->end(); itt++)
      {
        std::string targetqueue = FsView::gFsView.mIdView[*itt]->GetQueue();
        if (sourcequeue == targetqueue)
          exists = true;
      }
      // check if this filesystem is in RW mode (this we have to streamline with the query tags to apply to ro,wo & rw !
      bool isRW = (FsView::gFsView.mIdView[*its]->GetConfigStatus() >= eos::common::FileSystem::kRW) ? true : false;

      bool isActive = (FsView::gFsView.mIdView[*its]->GetActiveStatus() == eos::common::FileSystem::kOnline);
      if ((!exists) && (isRW) && (isActive))
        return FsView::gFsView.mIdView[*its];
    }
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
int
proc_fs_mv (std::string &sfsid, std::string &space, XrdOucString &stdOut, XrdOucString &stdErr, std::string &tident, eos::common::Mapping::VirtualIdentity &vid_in)
{
  int retc = 0;
  eos::common::FileSystem::fsid_t fsid = 0;
  eos::common::FileSystem::fs_snapshot_t snapshot;

  FileSystem* fs = 0;
  fsid = atoi(sfsid.c_str());

  if (!fsid)
  {
    // this is not a filesystem id, but a space where to pick one

    // now we look in the target space, where we need urgently a filesystem
    space = proc_fs_target(space);

    // now we look for a fitting source filesystem
    fs = proc_fs_source(sfsid, space);
  }
  else
  {
    // get this filesystem
    if (FsView::gFsView.mIdView.count(fsid))
    {
      fs = FsView::gFsView.mIdView[fsid];
    }
  }

  if (!fs)
  {
    if (fsid)
    {
      stdErr = "error: no filesystem with id=";
      stdErr += (int) fsid;
      retc = ENOENT;
    }
    else
    {
      stdErr = "error: cannot move according to your request";
      retc = EINVAL;
    }
  }
  else
  {
    fs->SnapShotFileSystem(snapshot);
    if (space == snapshot.mSpace)
    {
      stdErr = "error: filesystem is already in space=";
      stdErr += snapshot.mSpace.c_str();
      retc = EINVAL;
    }
    else
    {
      std::string rspace = space;
      size_t dpos = 0;
      // before 'space' is a group, so we remove the group specific part
      if ((dpos = rspace.find(".")) != std::string::npos)
      {
        rspace.erase(dpos);
      }

      // set all the space related default parameters
      if (FsView::gFsView.mSpaceView.count(rspace))
      {
        FsView::gFsView.mSpaceView[rspace]->ApplySpaceDefaultParameters(fs, true);
      }

      if (FsView::gFsView.MoveGroup(fs, space))
      {
        retc = 0;
        stdOut = "success: moved filesystem ";
        stdOut += (int) fs->GetId();
        stdOut += " into space ";
        stdOut += space.c_str();
      }
      else
      {
        retc = EIO;
        stdErr = "error: failed to move filesystem ";
        stdErr += (int) snapshot.mId;
        stdErr += " into space ";
        stdErr += space.c_str();
      }
    }
  }
  return retc;
}

/*----------------------------------------------------------------------------*/
int
proc_fs_rm (std::string &nodename, std::string &mountpoint, std::string &id, XrdOucString &stdOut, XrdOucString &stdErr, std::string &tident, eos::common::Mapping::VirtualIdentity &vid_in)
{
  int retc = 0;
  eos::common::FileSystem::fsid_t fsid = 0;

  if (id.length())
    fsid = atoi(id.c_str());


  FileSystem* fs = 0;

  if (id.length())
  {
    // find by id
    if (FsView::gFsView.mIdView.count(fsid))
    {
      fs = FsView::gFsView.mIdView[fsid];
    }
  }
  else
  {
    if (mountpoint.length() && nodename.length())
    {
      std::string queuepath = nodename;
      queuepath += mountpoint;
      fs = FsView::gFsView.FindByQueuePath(queuepath);
    }
  }

  if (fs)
  {
    std::string nodename = fs->GetString("host");
    size_t dpos = 0;

    std::string cstate = fs->GetString("configstatus");

    if ((dpos = nodename.find(".")) != std::string::npos)
    {
      nodename.erase(dpos);
    }
    if ((vid_in.uid != 0) && ((vid_in.prot != "sss") || tident.compare(0, tident.length(), nodename, 0, tident.length())))
    {
      stdErr = "error: filesystems can only be removed as 'root' or from the server mounting them using sss protocol\n";
      retc = EPERM;
    }
    else
    {
      if (cstate != "empty")
      {
        stdErr = "error: you can only  remove file systems which are in 'empty' status";
        retc = EINVAL;
      }
      else
      {
        if (!FsView::gFsView.RemoveMapping(fsid))
        {
          stdErr = "error: couldn't remove mapping of filesystem defined by ";
          stdErr += nodename.c_str();
          stdErr += "/";
          stdErr += mountpoint.c_str();
          stdErr += "/";
          stdErr += id.c_str();
          stdErr += " ";
        }

        if (!FsView::gFsView.UnRegister(fs))
        {
          stdErr = "error: couldn't unregister the filesystem ";
          stdErr += nodename.c_str();
          stdErr += " ";
          stdErr += mountpoint.c_str();
          stdErr += " ";
          stdErr += id.c_str();
          stdErr += "from the FsView";
          retc = EFAULT;
        }
        else
        {
          stdOut = "success: unregistered ";
          stdOut += nodename.c_str();
          stdOut += " ";
          stdOut += mountpoint.c_str();
          stdOut += " ";
          stdOut += id.c_str();
          stdOut += " from the FsView";
        }
      }
    }
  }
  else
  {
    stdErr = "error: there is no filesystem defined by ";
    stdErr += nodename.c_str();
    stdErr += " ";
    stdErr += mountpoint.c_str();
    stdErr += " ";
    stdErr += id.c_str();
    stdErr += " ";
    retc = EINVAL;
  }

  return retc;
}

/*----------------------------------------------------------------------------*/
int
proc_fs_dropdeletion (std::string &id, XrdOucString &stdOut, XrdOucString &stdErr, std::string &tident, eos::common::Mapping::VirtualIdentity &vid_in)
{
  int retc = 0;
  eos::common::FileSystem::fsid_t fsid = 0;

  if (id.length())
    fsid = atoi(id.c_str());

  if (fsid)
  {
    if ((vid_in.uid != 0))
    {
      stdErr = "error: filesystems can only be removed as 'root'\n";
      retc = EPERM;
    }
    else
    {
      eos::common::RWMutexWriteLock nslock(gOFS->eosViewRWMutex);
      try
      {
        eos::FileSystemView::FileList unlinklist = gOFS->eosFsView->getUnlinkedFileList(fsid);
        unlinklist.clear();
        unlinklist.resize(0);
        stdOut += "success: dropped deletions on fsid=";
        stdOut += id.c_str();
      }
      catch (eos::MDException &e)
      {
        stdErr = "error: there is no deletion list for fsid=";
        stdErr += id.c_str();
      }
    }
  }
  else
  {
    stdErr = "error: there is no filesystem defined with fsid=";
    stdErr += id.c_str();
    stdErr += " ";
    retc = EINVAL;
  }
  return retc;
}

EOSMGMNAMESPACE_END
