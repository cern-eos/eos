//------------------------------------------------------------------------------
// File: proc_fs.cc
// Author: Andreas-Joachim Peters - CERN & Ivan Arizanovic - Comtrade
//------------------------------------------------------------------------------

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

#include "mgm/proc/proc_fs.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/StringConversion.hh"
#include "common/Path.hh"
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/FsView.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Dump metadata information
//------------------------------------------------------------------------------
int
proc_fs_dumpmd(std::string& fsidst, XrdOucString& option, XrdOucString& dp,
               XrdOucString& df, XrdOucString& ds, XrdOucString& stdOut,
               XrdOucString& stdErr, std::string& tident,
               eos::common::Mapping::VirtualIdentity& vid_in, size_t& entries)
{
  entries = 0;
  int retc = 0;
  bool dumppath = false;
  bool dumpfid = false;
  bool dumpsize = false;
  bool monitor = false;

  if (option != "m") {
    if (dp == "1") {
      dumppath = true;
    }

    if (df == "1") {
      dumpfid = true;
    }

    if (ds == "1") {
      dumpsize = true;
    }
  } else {
    monitor = true;
  }

  if (!fsidst.length()) {
    stdErr = "error: illegal parameters";
    retc = EINVAL;
  } else {
    int fsid = atoi(fsidst.c_str());
    eos::common::RWMutexReadLock nslock(gOFS->eosViewRWMutex);

    try {
      std::shared_ptr<eos::IFileMD> fmd;
      const eos::IFsView::FileList& filelist = gOFS->eosFsView->getFileList(fsid);

      for (auto it = filelist.begin(); it != filelist.end(); ++it) {
        fmd = gOFS->eosFileService->getFileMD(*it);

        if (fmd) {
          entries++;

          if ((!dumppath) && (!dumpfid) && (!dumpsize)) {
            std::string env;
            fmd->getEnv(env, true);
            XrdOucString senv = env.c_str();

            if (senv.endswith("checksum=")) {
              senv.replace("checksum=", "checksum=none");
            }

            stdOut += senv.c_str();

            if (monitor) {
              std::string fullpath = gOFS->eosView->getUri(fmd.get());
              eos::common::Path cPath(fullpath.c_str());
              stdOut += "&container=";
              XrdOucString safepath = cPath.GetParentPath();

              while (safepath.replace("&", "#AND#")) {}

              stdOut += safepath;
            }

            stdOut += "\n";
          } else {
            if (dumppath) {
              std::string fullpath = gOFS->eosView->getUri(fmd.get());
              XrdOucString safepath = fullpath.c_str();

              while (safepath.replace("&", "#AND#")) {}

              stdOut += "path=";
              stdOut += safepath.c_str();
            }

            if (dumpfid) {
              if (dumppath) {
                stdOut += " ";
              }

              char sfid[40];
              snprintf(sfid, 40, "fid=%llu", (unsigned long long) fmd->getId());
              stdOut += sfid;
            }

            if (dumpsize) {
              if (dumppath || dumpfid) {
                stdOut += " ";
              }

              char ssize[40];
              snprintf(ssize, 40, "size=%llu", (unsigned long long) fmd->getSize());
              stdOut += ssize;
            }

            stdOut += "\n";
          }
        }
      }

      if (monitor) {
        // Also add files which have yet to be unlinked
        const eos::IFsView::FileList& unlinked =
          gOFS->eosFsView->getUnlinkedFileList(fsid);

        for (auto it = unlinked.begin(); it != unlinked.end(); ++it) {
          fmd = gOFS->eosFileService->getFileMD(*it);

          if (fmd) {
            entries++;
            std::string env;
            fmd->getEnv(env, true);
            XrdOucString senv = env.c_str();
            senv.replace("checksum=&", "checksum=none&");
            stdOut += senv.c_str();
            stdOut += "&container=-\n";
          }
        }
      }
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_static_debug("caught exception %d %s\n", e.getErrno(),
                       e.getMessage().str().c_str());
    }
  }

  return retc;
}

//------------------------------------------------------------------------------
// Configure filesystem
//------------------------------------------------------------------------------
int
proc_fs_config(std::string& identifier, std::string& key, std::string& value,
               XrdOucString& stdOut, XrdOucString& stdErr, std::string& tident,
               eos::common::Mapping::VirtualIdentity& vid_in)
{
  int retc = 0;
  eos::common::FileSystem::fsid_t fsid = atoi(identifier.c_str());

  if (!identifier.length() || !key.length() || !value.length()) {
    stdErr = "error: illegal parameters";
    retc = EINVAL;
  } else {
    eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex);
    FileSystem* fs = 0;

    if (fsid && FsView::gFsView.mIdView.count(fsid)) {
      // by filesystem id
      fs = FsView::gFsView.mIdView[fsid];
    } else {
      // by filesystem uuid
      if (FsView::gFsView.GetMapping(identifier)) {
        if (FsView::gFsView.mIdView.count(FsView::gFsView.GetMapping(identifier))) {
          fs = FsView::gFsView.mIdView[FsView::gFsView.GetMapping(identifier)];
        }
      } else {
        // by host:port:data name
        std::string path = identifier;
        size_t slashpos = identifier.find("/");

        if (slashpos != std::string::npos) {
          path.erase(0, slashpos);
          identifier.erase(slashpos);

          if ((identifier.find(":") == std::string::npos)) {
            identifier += ":1095"; // default eos fst port
          }

          if ((identifier.find("/eos/") == std::string::npos)) {
            identifier.insert(0, "/eos/");
            identifier.append("/fst");
          }

          if (FsView::gFsView.mNodeView.count(identifier)) {
            eos::mgm::BaseView::const_iterator it;

            for (it = FsView::gFsView.mNodeView[identifier]->begin();
                 it != FsView::gFsView.mNodeView[identifier]->end(); it++) {
              if (FsView::gFsView.mIdView.count(*it)) {
                // This is the filesystem
                if (FsView::gFsView.mIdView[*it]->GetPath() == path) {
                  fs = FsView::gFsView.mIdView[*it];
                }
              }
            }
          }
        }
      }
    }

    if (fs) {
      // Check the allowed strings
      if (((key == "configstatus") &&
           (eos::common::FileSystem::GetConfigStatusFromString(value.c_str()) !=
            eos::common::FileSystem::kUnknown)) ||
          (((key == "headroom") || (key == "scaninterval") || (key == "graceperiod") ||
            (key == "drainperiod") || (key == "proxygroup") ||
            (key == "filestickyproxydepth") || (key == "forcegeotag")))) {
        std::string nodename = fs->GetString("host");
        size_t dpos = 0;

        if ((dpos = nodename.find(".")) != std::string::npos) {
          nodename.erase(dpos);
        }

        if ((vid_in.uid != 0) && ((vid_in.prot != "sss") ||
                                  tident.compare(0, tident.length(), nodename,
                                      0, tident.length()))) {
          stdErr = "error: filesystems can only be configured as 'root' or "
                   "from the server mounting them using sss protocol\n";
          retc = EPERM;
        } else {
          if ((key == "headroom") || (key == "scaninterval") ||
              (key == "graceperiod") || (key == "drainperiod")) {
            fs->SetLongLong(key.c_str(),
                            eos::common::StringConversion::GetSizeFromString(value.c_str()));
            FsView::gFsView.StoreFsConfig(fs);
          } else {
            if ((key == "configstatus") && (value == "empty")) {
              bool isempty = true;

              // Check if this filesystem is really empty
              try {
                const eos::IFsView::FileList& filelist = gOFS->eosFsView->getFileList(
                      fs->GetId());

                if (filelist.size()) {
                  isempty = false;
                }
              } catch (eos::MDException& e) {
                isempty = true;
              }

              if (!isempty) {
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
              } else {
                fs->SetString(key.c_str(), value.c_str());
                FsView::gFsView.StoreFsConfig(fs);
              }
            } else {
              fs->SetString(key.c_str(), value.c_str());
              FsView::gFsView.StoreFsConfig(fs);
            }
          }
        }
      } else {
        stdErr += "error: not an allowed parameter <";
        stdErr += key.c_str();
        stdErr += ">";
        retc = EINVAL;
      }
    } else {
      stdErr += "error: cannot identify the filesystem by <";
      stdErr += identifier.c_str();
      stdErr += ">";
      retc = EINVAL;
    }
  }

  return retc;
}

//------------------------------------------------------------------------------
// Add filesystem
//------------------------------------------------------------------------------
int
proc_fs_add(std::string& sfsid, std::string& uuid, std::string& nodename,
            std::string& mountpoint, std::string& space, std::string& configstatus,
            XrdOucString& stdOut, XrdOucString& stdErr, std::string& tident,
            eos::common::Mapping::VirtualIdentity& vid_in)
{
  int retc = 0;
  eos::common::FileSystem::fsid_t fsid = atoi(sfsid.c_str());

  if ((!nodename.length()) || (!mountpoint.length()) || (!space.length()) ||
      (!configstatus.length()) ||
      (configstatus.length() &&
       (eos::common::FileSystem::GetConfigStatusFromString(configstatus.c_str()) <
        eos::common::FileSystem::kOff))) {
    stdErr += "error: illegal parameters";
    retc = EINVAL;
  } else {
    // Node name comes as /eos/<host>..../, we just need <host> without domain
    std::string rnodename = nodename;
    rnodename.erase(0, 5);
    size_t dpos;

    if ((dpos = rnodename.find(".")) != std::string::npos) {
      rnodename.erase(dpos);
    }

    // ========> ViewMutex WRITELOCK
    FsView::gFsView.ViewMutex.LockWrite();

    // Rough check that the filesystem is added from a host with the same
    // tident ... anyway we should have configured 'sss' security
    if ((vid_in.uid != 0) &&
        ((vid_in.prot != "sss") || tident.compare(0, rnodename.length(),
            rnodename, 0, rnodename.length()))) {
      stdErr += "error: filesystems can only be added as 'root' or from the "
                "server mounting them using sss protocol\n";
      retc = EPERM;
    } else {
      // queuepath = /eos/<host:port><path>
      std::string queuepath = nodename;
      queuepath += mountpoint;

      // Check if this filesystem exists already ....
      if (!FsView::gFsView.ExistsQueue(nodename, queuepath)) {
        // Check if there is a mapping for 'uuid'
        if (FsView::gFsView.GetMapping(uuid) || ((fsid > 0) &&
            (FsView::gFsView.HasMapping(fsid)))) {
          if (fsid) {
            stdErr += "error: filesystem identified by uuid='";
            stdErr += uuid.c_str();
            stdErr += "' id='";
            stdErr += sfsid.c_str();
            stdErr += "' already exists!";
          } else {
            stdErr += "error: filesystem identified by '";
            stdErr += uuid.c_str();
            stdErr += "' already exists!";
          }

          retc = EEXIST;
        } else {
          FileSystem* fs = 0;

          if (fsid) {
            if (!FsView::gFsView.ProvideMapping(uuid, fsid)) {
              stdErr += "error: conflict adding your uuid & id mapping";
              retc = EINVAL;
            } else {
              fs = new FileSystem(queuepath.c_str(), nodename.c_str(), &gOFS->ObjectManager);
            }
          } else {
            fsid = FsView::gFsView.CreateMapping(uuid);
            fs = new FileSystem(queuepath.c_str(), nodename.c_str(), &gOFS->ObjectManager);
          }

          XrdOucString sizestring;
          stdOut += "success:   mapped '";
          stdOut += uuid.c_str();
          stdOut += "' <=> fsid=";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long) fsid);

          if (fs) {
            // We want one atomic update with all the parameters defined
            fs->OpenTransaction();
            fs->SetId(fsid);
            fs->SetString("uuid", uuid.c_str());
            fs->SetString("configstatus", configstatus.c_str());
            std::string splitspace = "";
            std::string splitgroup = "";
            unsigned int groupsize = 0;
            unsigned int groupmod = 0;
            unsigned int subgroup = 0;
            bool dorandom = false;
            // Logic to automatically adjust scheduling subgroups
            eos::common::StringConversion::SplitByPoint(space, splitspace, splitgroup);

            if (FsView::gFsView.mSpaceView.count(splitspace)) {
              groupsize = atoi(FsView::gFsView.mSpaceView[splitspace]->GetMember(
                                 std::string("cfg.groupsize")).c_str());
              groupmod = atoi(FsView::gFsView.mSpaceView[splitspace]->GetMember(
                                std::string("cfg.groupmod")).c_str());
            }

            if (splitgroup.length()) {
              // We have to check if the desired group is already full, in
              // case we add to the next group by increasing the number by
              // <groupmod>.
              subgroup = atoi(splitgroup.c_str());

              if (splitgroup == "random") {
                dorandom = true;
                subgroup = (int)((random() * 1.0 / RAND_MAX) * groupmod);
              }

              int j = 0;
              size_t nnotfound = 0;

              for (j = 0; j < 1000; j++) {
                char newgroup[1024];
                snprintf(newgroup, sizeof(newgroup) - 1, "%s.%u",
                         splitspace.c_str(), subgroup);
                std::string snewgroup = newgroup;

                if (!FsView::gFsView.mGroupView.count(snewgroup)) {
                  // Great, this is still empty
                  splitgroup = newgroup;
                  break;
                } else {
                  bool exists = false;
                  eos::mgm::BaseView::const_iterator it;

                  // Check if this node doesn't already have a filesystem in this group
                  for (it = FsView::gFsView.mGroupView[snewgroup]->begin();
                       it != FsView::gFsView.mGroupView[snewgroup]->end(); it++) {
                    if (FsView::gFsView.mIdView[*it]->GetString("host") ==
                        fs->GetString("host")) {
                      // This subgroup has already this host
                      exists = true;
                    }
                  }

                  if ((!exists) &&
                      (((FsView::gFsView.mGroupView[snewgroup]->size()) < groupsize) ||
                       (groupsize == 0))) {
                    // Great, there is still space here
                    splitgroup = newgroup;
                    break;
                  } else {
                    if (dorandom) {
                      nnotfound++;

                      if (nnotfound >= groupmod) {
                        subgroup += groupmod;
                        nnotfound = 0;
                      } else {
                        int offset = subgroup / groupmod;
                        subgroup++;
                        subgroup = (offset * groupmod) + (subgroup % groupmod);
                      }
                    } else {
                      subgroup += groupmod;
                    }
                  }
                }
              }

              if (j == 1000) {
                eos_static_crit("infinite loop detected finding available scheduling group!");
                stdErr += "error: infinite loop detected finding available scheduling group!";
                retc = EFAULT;
              }
            } else {
              splitgroup = splitspace;
              splitgroup += ".0";
            }

            if (!retc) {
              fs->SetString("schedgroup", splitgroup.c_str());

              if (!FsView::gFsView.Register(fs)) {
                // Remove mapping
                if (FsView::gFsView.RemoveMapping(fsid, uuid)) {
                  stdOut += "\nsuccess: unmapped '";
                  stdOut += uuid.c_str();
                  stdOut += "' <!> fsid=";
                  stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                            (unsigned long long) fsid);
                } else {
                  stdErr += "error: cannot remove mapping - this can be fatal!\n";
                }

                // Remove filesystem object
                stdErr += "error: cannot register filesystem - check for path duplication!";
                retc = EINVAL;
              }

              // Set all the space related default parameters
              if (FsView::gFsView.mSpaceView.count(space)) {
                if (FsView::gFsView.mSpaceView[space]->ApplySpaceDefaultParameters(fs)) {
                  // Store the modifications
                  FsView::gFsView.StoreFsConfig(fs);
                }
              }
            } else {
              stdErr += "error: cannot allocate filesystem object";
              retc = ENOMEM;
            }

            fs->CloseTransaction(); // close all the definitions and broadcast
          }
        }
      } else {
        stdErr += "error: cannot register filesystem - it already exists!";
        retc = EEXIST;
      }
    }

    // ========> ViewMutex WRITEUnLOCK
    FsView::gFsView.ViewMutex.UnLockWrite();
  }

  return retc;
}

//------------------------------------------------------------------------------
// Find best suited scheduling group for a new filesystem
//------------------------------------------------------------------------------
std::string
proc_fs_target(std::string target_group)
{
  std::string new_target = "";
  std::string splitspace = "";
  std::string splitgroup = "";
  eos::common::StringConversion::SplitByPoint(target_group, splitspace,
      splitgroup);

  // If we have a fully specified target, we just return that
  if (splitgroup.length()) {
    return target_group;
  }

  // Find the 'best' group e.g. the one with least filesystems
  std::vector<std::string> mingroups;
  int minfs = 99999999;

  for (auto it = FsView::gFsView.mGroupView.begin();
       it != FsView::gFsView.mGroupView.end(); ++it) {
    std::string groupname = it->first;
    std::string groupspace = "";
    std::string groupgroup = "";
    eos::common::StringConversion::SplitByPoint(groupname, groupspace, groupgroup);

    // Skip if group is not in our space
    if (groupspace != splitspace) {
      continue;
    }

    int groupfilesystems = it->second->SumLongLong("<n>?configstatus@rw", false);

    if (groupfilesystems < minfs) {
      mingroups.clear();
      mingroups.push_back(it->first);
      minfs = groupfilesystems;
    }

    if (groupfilesystems == minfs) {
      mingroups.push_back(it->first);
    }
  }

  if (!mingroups.size()) {
    return target_group;
  }

  int randomgroup = ((1.0 * random()) / RAND_MAX) * mingroups.size();
  return mingroups[randomgroup];
}

//------------------------------------------------------------------------------
// Find a filesystem in the source_group which is not part of the target_group
//------------------------------------------------------------------------------
FileSystem*
proc_fs_source(std::string source_group, std::string target_group)
{
  std::string splitspace = "";
  std::string splitgroup = "";
  eos::common::StringConversion::SplitByPoint(source_group, splitspace,
      splitgroup);

  if (!FsView::gFsView.mGroupView.count(target_group)) {
    return 0;
  }

  if (splitgroup.length()) {
    // This is a selection of a group as source. If there is no source group,
    // we can't do anything
    if (!FsView::gFsView.mGroupView.count(source_group)) {
      return 0;
    }

    // Loop over all filesystems and check if the corresponding node is already
    // part of the target_group
    for (auto its = FsView::gFsView.mGroupView[source_group]->begin();
         its != FsView::gFsView.mGroupView[source_group]->end(); its++) {
      std::string sourcequeue = FsView::gFsView.mIdView[*its]->GetQueue();
      bool exists = false;

      // Loop over all filesystems in target_group and check if they have
      // already a sourcequeue
      for (auto itt = FsView::gFsView.mGroupView[target_group]->begin();
           itt != FsView::gFsView.mGroupView[target_group]->end(); itt++) {
        std::string targetqueue = FsView::gFsView.mIdView[*itt]->GetQueue();

        if (sourcequeue == targetqueue) {
          exists = true;
          break;
        }
      }

      if (!exists) {
        return FsView::gFsView.mIdView[*its];
      }
    }
  } else {
    // If there is no source space, we can't do anything
    if (!FsView::gFsView.mSpaceView.count(splitspace)) {
      return 0;
    }

    // Loop over all filesystems and check if the corresponding node is already
    // part of target_group
    for (auto its = FsView::gFsView.mSpaceView[splitspace]->begin();
         its != FsView::gFsView.mSpaceView[splitspace]->end(); ++its) {
      std::string sourcequeue = FsView::gFsView.mIdView[*its]->GetQueue();
      bool exists = false;

      // Loop over all filesystems in target_group and check if they have already sourcequeue
      for (auto itt = FsView::gFsView.mGroupView[target_group]->begin();
           itt != FsView::gFsView.mGroupView[target_group]->end(); ++itt) {
        std::string targetqueue = FsView::gFsView.mIdView[*itt]->GetQueue();

        if (sourcequeue == targetqueue) {
          exists = true;
          break;
        }
      }

      // Check if this filesystem is in RW mode - this we have to streamline
      // with the query tags to apply to ro,wo & rw !
      bool isRW = (FsView::gFsView.mIdView[*its]->GetConfigStatus() >=
                   eos::common::FileSystem::kRW) ? true : false;
      bool isActive = (FsView::gFsView.mIdView[*its]->GetActiveStatus() ==
                       eos::common::FileSystem::kOnline);

      if ((!exists) && isRW && isActive) {
        return FsView::gFsView.mIdView[*its];
      }
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// Move filesystem to the best group possible - the one with the least FSTs
//------------------------------------------------------------------------------
std::string
proc_fs_mv_bestgroup(FileSystem* fs, std::string space)
{
  bool check_node = false;
  bool best_grp_exists = false;
  eos::common::FileSystem::fs_snapshot_t snapshot;
  // Check if space has specified group and get group and space
  std::string space_space = space.substr(0, space.find("."));
  std::string space_group = "";
  bool space_group_exist = false;

  if (space.find(".") != std::string::npos) {
    space_group_exist = true;
    space_group = space.substr(space.find(".") + 1);
  }

  // List of filesystems per group
  std::map<std::string, std::vector<FileSystem*>> group_fs_view;

  for (auto item = FsView::gFsView.mIdView.begin();
       item != FsView::gFsView.mIdView.end(); ++item) {
    item->second->SnapShotFileSystem(snapshot);
    group_fs_view[snapshot.mGroup].push_back(item->second);
  }

  // Get groupsize and groupmode for space and check if space exist
  bool space_exist = false;
  int space_groupsize = 0;
  int space_groupmode = 0;

  for (auto item = FsView::gFsView.mSpaceView.begin();
       item != FsView::gFsView.mSpaceView.end(); ++item) {
    if (item->first == space_space) {
      space_groupsize = std::atoi(item->second->GetConfigMember("groupsize").c_str());
      space_groupmode = std::atoi(item->second->GetConfigMember("groupmod").c_str());
      space_exist = true;
      break;
    }
  }

  // Create space, if space doesn't exist
  if (!space_exist && !space_group_exist) {
    space += ".0";
  }

  // Move FS to space without specified group
  if (space_exist && !space_group_exist) {
    // If group doesn't exist
    std::vector<int> space_count;

    for (auto item = FsView::gFsView.mGroupView.begin();
         item != FsView::gFsView.mGroupView.end(); ++item) {
      std::string space_best = item->first.substr(0, item->first.find("."));
      std::string group_best = item->first.substr(item->first.find(".") + 1);

      if (space_best == space) {
        space_count.push_back(std::atoi(group_best.c_str()));
      }
    }

    for (int i = 0; i < space_groupmode; i++) {
      if (std::find(space_count.begin(), space_count.end(), i) == space_count.end()) {
        best_grp_exists = true;
        space += ".";
        space += std::to_string((long long int)i);
        break;
      }
    }

    // If group already exist
    fs->SnapShotFileSystem(snapshot);

    if (!best_grp_exists) {
      for (auto item = group_fs_view.begin(); item != group_fs_view.end(); ++item) {
        std::string space_best = item->first.substr(0, item->first.find("."));
        std::string group_best = item->first.substr(item->first.find(".") + 1);

        if (space_best == space) {
          // Check is same FST in same group
          check_node = false;
          eos::common::FileSystem::fs_snapshot_t snapshot2;

          for (auto item2 = group_fs_view[item->first].begin();
               item2 != group_fs_view[item->first].end(); ++item2) {
            (*item2)->SnapShotFileSystem(snapshot2);

            if (snapshot2.mHostPort == snapshot.mHostPort) {
              check_node = true;
            }
          }

          if (space_best == space && item->second.size() &&
              (int)item->second.size() < space_groupsize &&
              std::atoi(group_best.c_str()) < space_groupmode &&
              !check_node) {
            best_grp_exists = true;
            space += ".";
            space += group_best.c_str();
            break;
          }
        }
      }
    }
  }

  // Move FS to space with specified group
  if (space_exist && space_group_exist) {
    bool group_exist = false;
    fs->SnapShotFileSystem(snapshot);

    for (auto item = group_fs_view.begin(); item != group_fs_view.end(); ++item) {
      if (item->first == space) {
        group_exist = true;
        // Check if same FST is in same group
        check_node = false;
        eos::common::FileSystem::fs_snapshot_t snapshot2;

        for (auto item2 = group_fs_view[item->first].begin();
             item2 != group_fs_view[item->first].end(); ++item2) {
          (*item2)->SnapShotFileSystem(snapshot2);

          if (snapshot2.mHostPort == snapshot.mHostPort) {
            check_node = true;
            break;
          }
        }

        // Best group exists if:
        //  - no other filesystem from the same node is in this group
        //  - the current group number is smaller than the space groupmod
        //  - if the space groupsize is 0 - we can add as many filesystems as
        //    we want
        //  - the space group size is != 0 - we need to check the constraint
        //    still holds
        if (!check_node &&
            (!space_groupsize ||
             ((space_groupsize && ((int)item->second.size() < space_groupsize)))) &&
            std::atoi(space_group.c_str()) < space_groupmode) {
          best_grp_exists = true;
          break;
        }
      }
    }

    // If group doesn't exist
    if (!group_exist && std::atoi(space_group.c_str()) < space_groupmode) {
      best_grp_exists = true;
    }
  }

  space += "..";
  space += (best_grp_exists ? "1" : "0");
  space += ".";
  space += (check_node ? "1" : "0");
  return space;
}

//------------------------------------------------------------------------------
// Move a filesystem/group/space to a group/space
//------------------------------------------------------------------------------
int
proc_fs_mv(std::string& sfsid, std::string& space, XrdOucString& stdOut,
           XrdOucString& stdErr, std::string& tident,
           eos::common::Mapping::VirtualIdentity& vid_in)
{
  int retc = 0;
  eos::common::FileSystem::fsid_t fsid = 0;
  eos::common::FileSystem::fs_snapshot_t snapshot;
  FileSystem* fs = 0;
  bool best_grp_exists = false;
  bool check_node = false;
  // Check if space has specified group and get group and spece
  std::string space_space = space.substr(0, space.find("."));
  std::string space_group = "";
  bool space_group_exist = false;

  if (space.find(".") != std::string::npos) {
    space_group_exist = true;
    space_group = space.substr(space.find(".") + 1);
  }

  // Check if space is correctly specified
  if (space_space.find_first_not_of("0123456789") == std::string::npos) {
    retc = EINVAL;
    stdErr = "error: Space ";
    stdErr += space_space.c_str();
    stdErr += " is number. Space can't be created only with number.";
    return retc;
  }

  // Check if group is correctly specified
  if (space_group_exist &&
      space_group.find_first_not_of("0123456789") != std::string::npos) {
    retc = EINVAL;
    stdErr = "error: Group ";
    stdErr += space_group.c_str();
    stdErr += " is not a number and doesn't exist in space ";
    stdErr += space_space.c_str();
    return retc;
  }

  // Get groupsize and groupmode for space and check if space exist
  bool space_exist = false;
  int space_groupsize = 0;
  int space_groupmode = 0;
  // MoveGroup can trigger a deletion of a space/group but this needs to happen
  // outside the gFsView.ViewMutex lock due to the balancer/converter and other
  // async threads which also take this lock.
  std::list<FsSpace*> spaces_to_del;
  std::list<FsGroup*> groups_to_del;
  {
    // Lock FsView::ViewMutex
    eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);

    for (auto item = FsView::gFsView.mSpaceView.begin();
         item != FsView::gFsView.mSpaceView.end(); ++item) {
      if (item->first == space_space) {
        space_groupsize = std::atoi(item->second->GetConfigMember("groupsize").c_str());
        space_groupmode = std::atoi(item->second->GetConfigMember("groupmod").c_str());
        space_exist = true;
        break;
      }
    }

    // Move ID to space
    // Check if first parameter (sfsid) is ID or Space
    if (sfsid.find_first_not_of("0123456789") == std::string::npos) {
      fsid = std::atoi(sfsid.c_str());

      if (!fsid) {
        // This is not a filesystem id, but a space where to pick one. Now
        // we look in the target space, where we need urgently a filesystem.
        space = proc_fs_target(space);
        // Look for a suitable source filesystem
        fs = proc_fs_source(sfsid, space);
      } else {
        // Get this filesystem
        if (FsView::gFsView.mIdView.count(fsid)) {
          fs = FsView::gFsView.mIdView[fsid];
        }
      }

      if (!fs) {
        if (fsid) {
          retc = ENOENT;
          stdErr = "error: There is no filesystem with id ";
          stdErr += (int) fsid;
        } else {
          retc = EINVAL;
          stdErr = "error: Can't move according to your request";
        }

        return retc;
      }

      // Find the best group
      std::string best = proc_fs_mv_bestgroup(fs, space);
      space = best.substr(0, best.find(".."));
      space_space = space.substr(0, space.find("."));
      space_group = space.substr(space.find(".") + 1);
      std::string space2 = best.substr(best.find("..") + 2);
      best_grp_exists = (space2.substr(0, space2.find(".")) == "1") ? true : false;
      check_node = (space2.substr(space2.find(".") + 1) == "1") ? true : false;

      if (!space_exist) {
        if (std::atoi(space_group.c_str()) < space_groupmode ||
            std::atoi(space_group.c_str()) == 0) {
          if (FsView::gFsView.MoveGroup(fs, space, spaces_to_del,
                                        groups_to_del)) {
            retc = 0;
            stdOut += "success: Filesystem ";
            stdOut += (int) fs->GetId();
            stdOut += " moved to created space ";
            stdOut += space.c_str();
          }
        } else {
          retc = EIO;
          stdErr = "error: You can't create space ";
          stdErr += space_space.c_str();
          stdErr += " with group ";
          stdErr += space_group.c_str();
          stdErr += ". Please try with 'fs mv ";
          stdErr += (int) fs->GetId();
          stdErr += " ";
          stdErr += space_space.c_str();
          stdErr += "' or with 'fs mv ";
          stdErr += (int) fs->GetId();
          stdErr += " ";
          stdErr += space_space.c_str();
          stdErr += ".0'";
        }
      } else {
        fs->SnapShotFileSystem(snapshot);

        if (best_grp_exists && !check_node) {
          if (FsView::gFsView.MoveGroup(fs, space, spaces_to_del,
                                        groups_to_del)) {
            retc = 0;
            stdOut += "success: Filesystem ";
            stdOut += (int) fs->GetId();
            stdOut += " moved to space ";
            stdOut += space.c_str();
          }
        }

        if (!best_grp_exists) {
          retc = EIO;
          stdErr = "error: Filesystem ";
          stdErr += (int) snapshot.mId;
          stdErr += " can't be moved to space ";
          stdErr += space.c_str();
          stdErr += ".\nSpace ";
          stdErr += space.c_str();
          stdErr += " is full or not defined. You can change groupsize (";
          stdErr += space_groupsize;
          stdErr += ") and groupmode (";
          stdErr += space_groupmode;
          stdErr += ") with:'space define <space-name> [<groupsize> "
                    "[<groupmode>]]'";
        }

        if (check_node) {
          retc = EIO;
          stdErr = "error: There is already filesystem from ";
          stdErr += snapshot.mHostPort.c_str();
          stdErr += " node in the group ";
          stdErr += space.c_str();
        }
      }
    }
    // Move space to space
    else {
      if (sfsid == space) {
        retc = EINVAL;
        stdErr = "error: Can't move space ";
        stdErr += sfsid.c_str();
        stdErr += " to space ";
        stdErr += space.c_str();
      } else {
        bool fsid_exist = false;
        // Loop through all IDs in space or in specified group (first parameter)
        std::map<std::string, std::vector<FileSystem*>> fsid_list;

        for (auto item = FsView::gFsView.mIdView.begin();
             item != FsView::gFsView.mIdView.end(); ++item) {
          item->second->SnapShotFileSystem(snapshot);
          fsid_list[snapshot.mGroup].push_back(item->second);
        }

        for (auto item = fsid_list.begin(); item != fsid_list.end(); ++item) {
          if ((item->first == sfsid) ||
              (item->first.substr(0, item->first.find(".")) == sfsid)) {
            for (auto item2 = item->second.begin(); item2 != item->second.end(); ++item2) {
              (*item2)->SnapShotFileSystem(snapshot);
              best_grp_exists = false;
              fsid_exist = true;
              std::string sfsid2 = std::to_string((long long int)snapshot.mId);
              fsid = std::atoi(sfsid2.c_str());
              fs = FsView::gFsView.mIdView[fsid];

              // If we don't have specified a group in the space
              if (!space_group_exist && space.find(".") != string::npos) {
                space.resize(space.find("."));
              }

              // Find the best group
              std::string best = proc_fs_mv_bestgroup(fs, space);
              space = best.substr(0, best.find(".."));
              space_space = space.substr(0, space.find("."));
              space_group = space.substr(space.find(".") + 1);
              std::string space2 = best.substr(best.find("..") + 2);
              best_grp_exists = (space2.substr(0, space2.find(".")) == "1") ? true : false;
              check_node = (space2.substr(space2.find(".") + 1) == "1") ? true : false;
              // Get groupsize and groupmode for space and check if space exist.
              space_exist = false;

              for (auto item = FsView::gFsView.mSpaceView.begin();
                   item != FsView::gFsView.mSpaceView.end(); ++item) {
                if (item->first == space_space) {
                  space_groupsize = std::atoi(item->second->GetConfigMember("groupsize").c_str());
                  space_groupmode = std::atoi(item->second->GetConfigMember("groupmod").c_str());
                  space_exist = true;
                }
              }

              // Messages
              if (!space_exist) {
                if (std::atoi(space_group.c_str()) < space_groupmode ||
                    std::atoi(space_group.c_str()) == 0) {
                  if (FsView::gFsView.MoveGroup(fs, space, spaces_to_del,
                                                groups_to_del)) {
                    stdOut += "ID=";
                    stdOut += (int) fs->GetId();
                    stdOut += "\tsuccess: Filesystem ";
                    stdOut += (int) fs->GetId();
                    stdOut += " moved to created space ";
                    stdOut += space.c_str();
                    stdOut += "\n";
                  }
                } else {
                  retc = EIO;
                  stdErr = "error: You can't create space ";
                  stdErr += space_space.c_str();
                  stdErr += " with group ";
                  stdErr += space_group.c_str();
                  stdErr += ". Please try with 'fs mv ";
                  stdErr += sfsid.c_str();
                  stdErr += " ";
                  stdErr += space_space.c_str();
                  stdErr += "' or with 'fs mv ";
                  stdErr += sfsid.c_str();
                  stdErr += " ";
                  stdErr += space_space.c_str();
                  stdErr += ".0'.\n";
                }
              } else {
                fs->SnapShotFileSystem(snapshot);

                if (!check_node && best_grp_exists) {
                  if (FsView::gFsView.MoveGroup(fs, space, spaces_to_del,
                                                groups_to_del)) {
                    stdOut += "ID=";
                    stdOut += (int) fs->GetId();
                    stdOut += "\tsuccess: Filesystem ";
                    stdOut += (int) fs->GetId();
                    stdOut += " moved to space ";
                    stdOut += space.c_str();
                    stdOut += "\n";
                  }
                }

                if ((!check_node && !best_grp_exists) ||
                    (check_node && !space_group_exist)) {
                  retc = EIO;
                  stdErr += "ID=";
                  stdErr += (int) fs->GetId();
                  stdErr += "\t\error: Filesystem ";
                  stdErr += (int) snapshot.mId;
                  stdErr += " can't be moved to space ";
                  stdErr += space.c_str();
                  stdErr += "\n";
                }

                if (check_node && space_group_exist) {
                  retc = EIO;
                  stdErr += "ID=";
                  stdErr += (int) fs->GetId();
                  stdErr += "\terror: Filesystem ";
                  stdErr += (int) snapshot.mId;
                  stdErr += " can't be moved to space ";
                  stdErr += space.c_str();
                  stdErr += ". There is already filesystem from ";
                  stdErr += snapshot.mHostPort.c_str();
                  stdErr += " node in group ";
                  stdErr += space.c_str();
                  stdErr += "\n";
                }
              }
            }

            // Break, if we move filesystems from specified group
            if (sfsid.find(".") != string::npos) {
              break;
            }
          }
        }

        if (retc == EIO) {
          stdErr += "Space ";

          if (!space_group_exist) {
            stdErr += space_space.c_str();
          } else {
            stdErr += space.c_str();
          }

          stdErr += " is full or not defined. You can change groupsize (";
          stdErr += space_groupsize;
          stdErr += ") and groupmode (";
          stdErr += space_groupmode;
          stdErr += ") with: 'space define <space-name> [<groupsize> "
                    "[<groupmode>]]'";
        }

        if (!fsid_exist) {
          retc = EIO;
          stdErr += "error: No filesystem exists in space ";
          stdErr += sfsid.c_str();
        }
      }
    }
  }

  // Delete required spaces and groups
  for (auto it = spaces_to_del.begin(); it != spaces_to_del.end(); ++it) {
    delete *it;
  }

  for (auto it = groups_to_del.begin(); it != groups_to_del.end(); ++it) {
    delete *it;
  }

  return retc;
}

//------------------------------------------------------------------------------
// Remove filesystem
//------------------------------------------------------------------------------
int
proc_fs_rm(std::string& nodename, std::string& mountpoint, std::string& id,
           XrdOucString& stdOut, XrdOucString& stdErr, std::string& tident,
           eos::common::Mapping::VirtualIdentity& vid_in)
{
  int retc = 0;
  eos::common::FileSystem::fsid_t fsid = 0;

  if (id.length()) {
    fsid = atoi(id.c_str());
  }

  FileSystem* fs = 0;

  if (id.length()) {
    // find by id
    if (FsView::gFsView.mIdView.count(fsid)) {
      fs = FsView::gFsView.mIdView[fsid];
    }
  } else {
    if (mountpoint.length() && nodename.length()) {
      std::string queuepath = nodename;
      queuepath += mountpoint;
      fs = FsView::gFsView.FindByQueuePath(queuepath);
    }
  }

  if (fs) {
    std::string nodename = fs->GetString("host");
    size_t dpos = 0;
    std::string cstate = fs->GetString("configstatus");

    if ((dpos = nodename.find(".")) != std::string::npos) {
      nodename.erase(dpos);
    }

    if ((vid_in.uid != 0) &&
        ((vid_in.prot != "sss") || tident.compare(0, tident.length(), nodename,
            0, tident.length()))) {
      stdErr = "error: filesystems can only be removed as 'root' or from the"
               " server mounting them using sss protocol\n";
      retc = EPERM;
    } else {
      if (cstate != "empty") {
        stdErr = "error: you can only remove file systems which are in 'empty' status";
        retc = EINVAL;
      } else {
        if (!FsView::gFsView.RemoveMapping(fsid)) {
          stdErr = "error: couldn't remove mapping of filesystem defined by ";
          stdErr += nodename.c_str();
          stdErr += "/";
          stdErr += mountpoint.c_str();
          stdErr += "/";
          stdErr += id.c_str();
          stdErr += " ";
        }

        if (!FsView::gFsView.UnRegister(fs)) {
          stdErr = "error: couldn't unregister the filesystem ";
          stdErr += nodename.c_str();
          stdErr += " ";
          stdErr += mountpoint.c_str();
          stdErr += " ";
          stdErr += id.c_str();
          stdErr += "from the FsView";
          retc = EFAULT;
        } else {
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
  } else {
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

//-------------------------------------------------------------------------------
// Clean unlinked files from filesystem
//-------------------------------------------------------------------------------
int
proc_fs_dropdeletion(std::string& id, XrdOucString& stdOut,
                     XrdOucString& stdErr, std::string& tident,
                     eos::common::Mapping::VirtualIdentity& vid_in)
{
  int retc = 0;
  eos::common::FileSystem::fsid_t fsid = 0;

  if (id.length()) {
    fsid = atoi(id.c_str());
  }

  if (fsid) {
    if ((vid_in.uid != 0)) {
      stdErr = "error: filesystems can only be removed as 'root'\n";
      retc = EPERM;
    } else {
      eos::common::RWMutexWriteLock nslock(gOFS->eosViewRWMutex);

      if (gOFS->eosFsView->clearUnlinkedFileList(fsid)) {
        stdOut += "success: dropped deletions on fsid=";
        stdOut += id.c_str();
      } else {
        stdErr = "error: there is no deletion list for fsid=";
        stdErr += id.c_str();
      }
    }
  } else {
    stdErr = "error: there is no filesystem defined with fsid=";
    stdErr += id.c_str();
    stdErr += " ";
    retc = EINVAL;
  }

  return retc;
}

EOSMGMNAMESPACE_END
