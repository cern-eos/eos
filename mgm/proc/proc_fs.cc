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
#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/interface/IView.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Get type of entity used in a fs mv operation.
//------------------------------------------------------------------------------
EntityType get_entity_type(const std::string& input, XrdOucString& stdOut,
                           XrdOucString& stdErr)
{
  std::ostringstream oss;
  EntityType ret = EntityType::UNKNOWN;
  size_t pos = input.find('.');

  if (pos == std::string::npos) {
    if (input.find_first_not_of("0123456789") == std::string::npos) {
      // This is an fs id
      errno = 0;
      (void) strtol(input.c_str(), nullptr, 10);

      if (errno) {
        eos_static_err("input fsid: %s must be a numeric value", input.c_str());
        oss << "fsid: " << input << " must be a numeric value";
        stdErr = oss.str().c_str();
      } else {
        ret = EntityType::FS;
      }
    } else {
      // This is a space
      ret = EntityType::SPACE;
    }
  } else {
    // This is group definition, make sure the space and group tokens are correct
    std::string space = input.substr(0, pos);
    std::string group = input.substr(pos + 1);

    if (space.find_first_not_of("0123456789") == std::string::npos) {
      eos_static_err("input space.group: %s must contain a string value for space",
                     input.c_str());
      oss << "space.group: " << input << " must contain a string value for space";
      stdErr = oss.str().c_str();
    } else {
      // Group must be a numeric value
      if (group.find_first_not_of("0123456789") != std::string::npos) {
        eos_static_err("input space.group: %s must contain a numeric value for group",
                       input.c_str());
        oss << "space.group: " << input << " must contain a numeric value for group";
        stdErr = oss.str().c_str();
      } else {
        ret = EntityType::GROUP;
      }
    }
  }

  return ret;
}

//------------------------------------------------------------------------------
// Get operation type based on the input entity types
//------------------------------------------------------------------------------
MvOpType get_operation_type(const std::string& in1, const std::string& in2,
                            XrdOucString& stdOut, XrdOucString& stdErr)
{
  // Do them individually to get proper error messages
  EntityType in1_type = get_entity_type(in1, stdOut, stdErr);

  if (in1_type == EntityType::UNKNOWN) {
    return MvOpType::UNKNOWN;
  }

  EntityType in2_type = get_entity_type(in2, stdOut, stdErr);

  if (in2_type == EntityType::UNKNOWN) {
    return MvOpType::UNKNOWN;
  }

  if (((in1_type == EntityType::FS) && (in2_type == EntityType::SPACE)) ||
      ((in1_type == EntityType::FS) && (in2_type == EntityType::GROUP)) ||
      ((in1_type == EntityType::GROUP) && (in2_type == EntityType::SPACE)) ||
      ((in1_type == EntityType::SPACE) && (in2_type == EntityType::SPACE))) {
    return static_cast<MvOpType>((in1_type << 2) | in2_type);
  }

  return MvOpType::UNKNOWN;
}

//------------------------------------------------------------------------------
// Dump metadata information
//------------------------------------------------------------------------------
int
proc_fs_dumpmd(std::string& fsidst, XrdOucString& option, XrdOucString& dp,
               XrdOucString& df, XrdOucString& ds, XrdOucString& stdOut,
               XrdOucString& stdErr,
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
    std::shared_ptr<eos::IFileMD> fmd;
    eos::common::RWMutexReadLock ns_rd_lock;
    ns_rd_lock.Grab(gOFS->eosViewRWMutex);

    for (auto it_fid = gOFS->eosFsView->getFileList(fsid);
         (it_fid && it_fid->valid()); it_fid->next()) {
      try {
        fmd = gOFS->eosFileService->getFileMD(it_fid->getElement());

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
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_static_err("Couldn't retrieve meta data for file id: %u. Error "
                       "code: %d, message: %s", it_fid->getElement(),
                       e.getErrno(), e.getMessage().str().c_str());
      }

      // Release the lock from time to time to let writers progress
      if (entries % 1024 == 0) {
        ns_rd_lock.Release();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        ns_rd_lock.Grab(gOFS->eosViewRWMutex);
      }
    }

    if (monitor) {
      // Also add files which have yet to be unlinked
      for (auto it_fid = gOFS->eosFsView->getUnlinkedFileList(fsid);
           (it_fid && it_fid->valid()); it_fid->next()) {
        try {
          fmd = gOFS->eosFileService->getFileMD(it_fid->getElement());

          if (fmd) {
            entries++;
            std::string env;
            fmd->getEnv(env, true);
            XrdOucString senv = env.c_str();
            senv.replace("checksum=&", "checksum=none&");
            stdOut += senv.c_str();
            stdOut += "&container=-\n";

            // Release the lock from time to time to let writers progress
            if (entries % 1024 == 0) {
              ns_rd_lock.Release();
              std::this_thread::sleep_for(std::chrono::milliseconds(100));
              ns_rd_lock.Grab(gOFS->eosViewRWMutex);
            }
          }
        } catch (eos::MDException& e) {
          errno = e.getErrno();
          eos_static_err("Couldn't retrieve meta data for file id: %u. Error "
                         "code: %d, message: %s", it_fid->getElement(),
                         e.getErrno(), e.getMessage().str().c_str());
        }
      }
    }
  }

  return retc;
}

//------------------------------------------------------------------------------
// Configure filesystem
//------------------------------------------------------------------------------
int
proc_fs_config(std::string& identifier, std::string& key, std::string& value,
               XrdOucString& stdOut, XrdOucString& stdErr,
               eos::common::Mapping::VirtualIdentity& vid_in)
{
  int retc = 0;
  const std::string vid_hostname = vid_in.host;
  eos::common::FileSystem::fsid_t fsid = atoi(identifier.c_str());

  if (!identifier.length() || !key.length() || !value.length()) {
    stdErr = "error: illegal parameters";
    retc = EINVAL;
  } else {
    eos::common::RWMutexReadLock viewLock(FsView::gFsView.ViewMutex);
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
        size_t slashpos = identifier.find('/');

        if (slashpos != std::string::npos) {
          path.erase(0, slashpos);
          identifier.erase(slashpos);

          if ((identifier.find(':') == std::string::npos)) {
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

        if ((dpos = nodename.find('.')) != std::string::npos) {
          nodename.erase(dpos);
        }

        if ((vid_in.uid != 0) &&
            ((vid_in.prot != "sss") ||
             vid_hostname.compare(0, nodename.length(),
                                  nodename, 0, nodename.length()))) {
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
              if (gOFS->eosFsView->getNumFilesOnFs(fs->GetId())) {
                isempty = false;
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
            XrdOucString& stdOut, XrdOucString& stdErr,
            eos::common::Mapping::VirtualIdentity& vid_in)
{
  int retc = 0;
  const std::string vid_hostname = vid_in.host;
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

    if ((dpos = rnodename.find('.')) != std::string::npos) {
      rnodename.erase(dpos);
    }

    // ========> ViewMutex WRITELOCK
    FsView::gFsView.ViewMutex.LockWrite();

    // Rough check that the filesystem is added from a host with the same
    // hostname ... anyway we should have configured 'sss' security
    if ((vid_in.uid != 0) &&
        ((vid_in.prot != "sss") ||
         vid_hostname.compare(0, rnodename.length(),
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
            std::string splitspace;
            std::string splitgroup;
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
              if (splitspace != "spare") {
                splitgroup = splitspace;
                splitgroup += ".0";
              } else {
                splitgroup = splitspace;
              }
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

    if (retc == 0) {
      for(auto& fsckDir : XrdMgmOfs::MgmFsckDirs) {
        gOFS->CreateContainer(gOFS->MgmProcFsckPath + "/" + fsckDir.c_str() + "/" + std::to_string(fsid).c_str());
      }
    }

    // ========> ViewMutex WRITEUnLOCK
    FsView::gFsView.ViewMutex.UnLockWrite();
  }

  return retc;
}

//------------------------------------------------------------------------------
// Move a filesystem/group/space to a group/space
//------------------------------------------------------------------------------
int
proc_fs_mv(std::string& src, std::string& dst, XrdOucString& stdOut,
           XrdOucString& stdErr,
           eos::common::Mapping::VirtualIdentity& vid_in)
{
  int retc = 0;
  MvOpType operation = get_operation_type(src, dst, stdOut, stdErr);
  eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);

  switch (operation) {
  case MvOpType::FS_2_GROUP:
    retc = proc_mv_fs_group(FsView::gFsView, src, dst, stdOut, stdErr);
    break;

  case MvOpType::FS_2_SPACE:
    retc = proc_mv_fs_space(FsView::gFsView, src, dst, stdOut, stdErr);
    break;

  case MvOpType::GRP_2_SPACE:
    retc = proc_mv_grp_space(FsView::gFsView, src, dst, stdOut, stdErr);
    break;

  case MvOpType::SPC_2_SPACE:
    retc = proc_mv_space_space(FsView::gFsView, src, dst, stdOut, stdErr);
    break;

  default:
    stdErr = "error: operation not supported";
    retc = EINVAL;
    break;
  }

  return retc;
}

//------------------------------------------------------------------------------
// Check if a file system can be moved. It needs to be active and in RW mode.
//------------------------------------------------------------------------------
bool proc_fs_can_mv(eos::mgm::FileSystem* fs, const std::string& dst,
                    XrdOucString& stdOut, XrdOucString& stdErr)
{
  std::ostringstream oss;
  FileSystem::fs_snapshot snapshot;

  if (fs->SnapShotFileSystem(snapshot)) {
    if (dst.find('.') != std::string::npos) {
      if (snapshot.mGroup == dst) {
        oss << "error: file system " << snapshot.mId << " is already in "
            << "group "  << dst << std::endl;
        stdOut = oss.str().c_str();
        return false;
      }
    } else {
      if (snapshot.mSpace == dst) {
        oss << "error:: file system " << snapshot.mId << " is already in "
            << "space " << dst << std::endl;
        stdOut = oss.str().c_str();
        return false;
      }
    }

    // File system must be in RW mode and active for the move to work
    bool is_empty = (fs->GetConfigStatus() == eos::common::FileSystem::kEmpty);
    bool is_active = (fs->GetActiveStatus() == eos::common::FileSystem::kOnline);

    if (!(is_empty && is_active)) {
      eos_static_err("fsid %i is not empty or is not active", snapshot.mId);
      oss << "error: file system " << snapshot.mId << " is not empty or "
          << "is not active" << std::endl;
      stdErr = oss.str().c_str();
      return false;
    }
  } else {
    eos_static_err("failed to snapshot file system");
    oss << "error: failed to snapshot files system" << std::endl;
    stdErr = oss.str().c_str();
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Move a filesystem to a group
//------------------------------------------------------------------------------
int proc_mv_fs_group(FsView& fs_view, const std::string& src,
                     const std::string& dst, XrdOucString& stdOut,
                     XrdOucString& stdErr)
{
  using eos::mgm::FsView;
  using eos::mgm::FileSystem;
  int pos = dst.find('.');
  eos::common::FileSystem::fsid_t fsid = atoi(src.c_str());
  std::string space = dst.substr(0, pos);
  std::string group = dst.substr(pos + 1);
  size_t grp_size = 0;
  size_t grp_mod = 0;
  std::ostringstream oss;
  // Check if space exists and get groupsize and groupmod
  auto it_space = fs_view.mSpaceView.find(space);

  if (it_space != fs_view.mSpaceView.end()) {
    grp_size = std::strtoul(it_space->second->GetConfigMember("groupsize").c_str(),
                            nullptr, 10);
    grp_mod = std::strtoul(it_space->second->GetConfigMember("groupmod").c_str(),
                           nullptr, 10);
  } else {
    eos_static_err("requested space %s does not exist", space.c_str());
    oss << "error: space " << space << " does not exist" << std::endl;
    stdErr = oss.str().c_str();
    return EINVAL;
  }

  // Get this filesystem
  FileSystem* fs = nullptr;

  if (fs_view.mIdView.count(fsid)) {
    fs = fs_view.mIdView[fsid];

    if (!proc_fs_can_mv(fs, dst, stdOut, stdErr)) {
      return EINVAL;
    }
  } else {
    eos_static_err("no such fsid: %i", fsid);
    oss << "error: no such fsid: " << fsid << std::endl;
    stdErr = oss.str().c_str();
    return EINVAL;
  }

  // Get the target group
  if (dst != "spare") {
    auto const iter = fs_view.mGroupView.find(dst);

    if (iter != fs_view.mGroupView.end()) {
      FsGroup* grp = iter->second;
      size_t grp_num_fs = grp->size();

      // Check that we can still add file systems to this group
      if (grp_num_fs > grp_size) {
        eos_static_err("reached maximum number of fs for group: %s", dst.c_str());
        oss << "error: reached maximum number of file systems for group "
            << dst.c_str() << std::endl;
        stdErr = oss.str().c_str();
        return EINVAL;
      }

      // Check that there is no other file system from the same node in this group
      bool is_forbidden = false;
      std::string qnode;
      std::string fs_qnode = fs->GetQueue();

      for (auto it = grp->begin(); it != grp->end(); ++it) {
        qnode = fs_view.mIdView[*it]->GetQueue();

        if (fs_qnode == qnode) {
          is_forbidden = true;
          break;
        }
      }

      if (is_forbidden) {
        eos_static_err("group %s already contains an fs from the same node",
                       dst.c_str());
        oss << "error: group " << dst << " already contains a file system from "
            << "the same node" << std::endl;
        stdErr = oss.str().c_str();
        return EINVAL;
      }
    } else {
      // A new group will be created, check that it respects the groupmod param
      size_t grp_indx = std::strtoul(group.c_str(), nullptr, 10);

      if (grp_indx >= grp_mod) {
        eos_static_err("group %s is not respecting the groupmod value of %u",
                       dst.c_str(), grp_mod);
        oss << "error: group " << dst.c_str() << " is not respecting the groupmod"
            << " value of " << grp_mod << " for this space" << std::endl;
        stdErr = oss.str().c_str();
        return EINVAL;
      }

      eos_static_debug("group %s will be created", dst.c_str());
    }
  } else {
    // This is a special case when we "park" file systems in the spare space
    eos_static_debug("fsid %s will be \"parked\" in space spare", src.c_str());
  }

  if (fs_view.MoveGroup(fs, dst)) {
    oss << "success: filesystem " << (int) fs->GetId() << " moved to group "
        << dst << std::endl;
    stdOut = oss.str().c_str();
  } else {
    eos_static_err("failed to move fsid: %i to group: %s", fsid, dst.c_str());
    oss << "error: failed to move filesystem " << fsid << " to group "
        << dst << std::endl;
    stdErr = oss.str().c_str();
    return EINVAL;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Move a filesystem to a space
//------------------------------------------------------------------------------
int proc_mv_fs_space(FsView& fs_view, const std::string& src,
                     const std::string& dst, XrdOucString& stdOut,
                     XrdOucString& stdErr)
{
  using eos::mgm::FsView;
  std::ostringstream oss;
  // Check if file system is not already in this space
  FileSystem::fsid_t fsid = strtol(src.c_str(), nullptr, 10);
  FileSystem* fs = nullptr;

  if (fs_view.mIdView.count(fsid)) {
    fs = fs_view.mIdView[fsid];

    if (!proc_fs_can_mv(fs, dst, stdOut, stdErr)) {
      return EINVAL;
    }
  } else {
    eos_static_err("no such fsid: %i", fsid);
    oss << "error: no such fsid: " << fsid << std::endl;
    stdErr = oss.str().c_str();
    return EINVAL;
  }

  auto it_space = fs_view.mSpaceView.find(dst);

  if (it_space == fs_view.mSpaceView.end()) {
    eos_static_info("creating space %s", dst.c_str());
    FsSpace* new_space = new FsSpace(dst.c_str());
    fs_view.mSpaceView[dst] = new_space;
    it_space = fs_view.mSpaceView.find(dst);
  }

  int grp_size = std::atoi(it_space->second->GetConfigMember
                           ("groupsize").c_str());
  int grp_mod = std::atoi(it_space->second->GetConfigMember
                          ("groupmod").c_str());

  if ((dst == "spare") && grp_mod) {
    eos_static_err("space \"spare\" must have groupmod 0");
    oss << "error: space \"spare\" must have groupmod 0. Please update the "
        << "space configuration using \"eos space define <space> <size> <mod>"
        << std::endl;
    stdErr = oss.str().c_str();
    stdOut.erase();
    return EINVAL;
  }

  std::list<std::string> sorted_grps;

  if (grp_mod) {
    sorted_grps = proc_sort_groups_by_priority(fs_view, dst, grp_size, grp_mod);
  } else {
    // Special case for spare space which doesn't have groups
    sorted_grps.emplace_back("spare");
  }

  bool done = false;

  for (const auto& grp : sorted_grps) {
    if (proc_mv_fs_group(fs_view, src, grp, stdOut, stdErr) == 0) {
      stdErr = "";
      done = true;
      break;
    }
  }

  if (!done) {
    eos_static_err("failed to add fs %s to space %s", src.c_str(), dst.c_str());
    std::ostringstream oss;
    oss << "error: failed to add file system " << src.c_str() << " to space "
        << dst.c_str() << " - no suitable group found" << std::endl;
    stdOut.erase();
    stdErr = oss.str().c_str();
    return EINVAL;
  }

  return 0;
}
//------------------------------------------------------------------------------
// Sort the groups in a space by priority - the first ones are the ones that
// are most suitable to add a new file system to them.
//------------------------------------------------------------------------------
std::list<std::string>
proc_sort_groups_by_priority(FsView& fs_view, const std::string& space,
                             size_t grp_size, size_t grp_mod)
{
  using eos::mgm::FsView;
  struct cmp_grp {
    bool operator()(FsGroup* a, FsGroup* b)
    {
      return (a->size() < b->size());
    }
  };
  // All groups in space are possible candidats
  std::list<FsGroup*> grps; // first - highest priority, last - lowest
  std::set<std::string> set_grps;
  std::string node;

  for (std::uint32_t  i = 0; i < grp_mod; ++i) {
    node = space;
    node += ".";
    node += std::to_string(i);
    set_grps.insert(node);
  }

  // Get all groups belonging to the current space and that have less than the
  // max number of file systems
  for (auto it = fs_view.mGroupView.begin();
       it != fs_view.mGroupView.end(); ++it) {
    if (it->first.find(space) == 0) {
      set_grps.erase(it->first);

      if (it->second->size() < grp_size) {
        grps.push_back(it->second);
      }
    }
  }

  grps.sort(cmp_grp());
  // Any groups left in the initial set represent completely new groups
  // without any file systems i.e highest priority
  std::list<std::string> ret_grps(set_grps.begin(), set_grps.end());

  for (auto && grp : grps) {
    ret_grps.push_back(grp->mName);
  }

  return ret_grps;
}
//------------------------------------------------------------------------------
// Move a group to a space
//------------------------------------------------------------------------------
int proc_mv_grp_space(FsView& fs_view, const std::string& src,
                      const std::string& dst, XrdOucString& stdOut,
                      XrdOucString& stdErr)
{
  using eos::mgm::FsView;
  std::ostringstream oss;
  std::list<std::string> failed_fs; // file systems that couldn't be moved
  auto it_grp = fs_view.mGroupView.find(src);

  if (it_grp == fs_view.mGroupView.end()) {
    eos_static_err("group %s does not exist", src.c_str());
    oss << "error: group " << src << " does not exist";
    stdErr = oss.str().c_str();
    return EINVAL;
  }

  // Get all file systems from the group since iterators will be invalidated
  // when we start moving them to the new destination
  FsGroup* grp = it_grp->second;
  std::list<std::string> lst_fsids;

  for (auto it = grp->begin(); it != grp->end(); ++it) {
    lst_fsids.push_back(std::to_string(*it));
  }

  for (const auto& sfsid : lst_fsids) {
    if (proc_mv_fs_space(fs_view, sfsid, dst, stdOut, stdErr)) {
      failed_fs.push_back(sfsid);
    }
  }

  if (!failed_fs.empty()) {
    oss << "warning: the following file systems could not be moved ";

    for (auto && elem : failed_fs) {
      oss << elem << " ";
    }

    oss << std::endl;
    stdOut.erase();
    stdErr = oss.str().c_str();;
    return EINVAL;
  } else {
    oss << "success: all file systems in group " << src << " have been "
        << "moved to space " << dst << std::endl;
    stdOut = oss.str().c_str();
    stdErr.erase();
  }

  return 0;
}
//------------------------------------------------------------------------------
// Move space to space
//------------------------------------------------------------------------------
int proc_mv_space_space(FsView& fs_view, const std::string& src,
                        const std::string& dst, XrdOucString& stdOut,
                        XrdOucString& stdErr)
{
  using eos::mgm::FsView;
  std::ostringstream oss;
  std::list<std::string> failed_fs; // file systems that couldn't be moved
  auto it_space1 = fs_view.mSpaceView.find(src);

  if (it_space1 == fs_view.mSpaceView.end()) {
    eos_static_err("space %s does not exist", src.c_str());
    oss << "error: space " << src << " does not exist";
    stdErr = oss.str().c_str();
    return EINVAL;
  }

  auto it_space2 = fs_view.mSpaceView.find(dst);

  if (it_space2 == fs_view.mSpaceView.end()) {
    eos_static_err("space %s does not exist", dst.c_str());
    oss << "error: space " << dst << " does not exist";
    stdErr = oss.str().c_str();
    return EINVAL;
  }

  // Get all file systems from the space since iterators will be invalidated
  // when we start moving them to the new destination
  FsSpace* space1 = it_space1->second;
  std::list<std::string> lst_fsids;

  for (auto it = space1->begin(); it != space1->end(); ++it) {
    lst_fsids.push_back(std::to_string(*it));
  }

  for (const auto& sfsid : lst_fsids) {
    if (proc_mv_fs_space(fs_view, sfsid, dst, stdOut, stdErr)) {
      failed_fs.push_back(sfsid);
    }
  }

  if (!failed_fs.empty()) {
    oss << "warning: the following file systems could not be moved ";

    for (auto && elem : failed_fs) {
      oss << elem << " ";
    }

    oss << std::endl;
    stdOut.erase();
    stdErr = oss.str().c_str();;
    return EINVAL;
  } else {
    oss << "success: all file systems in space " << src << " have been "
        << " moved to space " << dst << std::endl;
    stdOut = oss.str().c_str();
    stdErr.erase();
  }

  return 0;
}
//------------------------------------------------------------------------------
// Remove filesystem
//------------------------------------------------------------------------------
int
proc_fs_rm(std::string& nodename, std::string& mountpoint, std::string& id,
           XrdOucString& stdOut, XrdOucString& stdErr,
           eos::common::Mapping::VirtualIdentity& vid_in)
{
  int retc = 0;
  const std::string vid_hostname = vid_in.host;
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

    if ((dpos = nodename.find('.')) != std::string::npos) {
      nodename.erase(dpos);
    }

    if ((vid_in.uid != 0) &&
        ((vid_in.prot != "sss") ||
         vid_hostname.compare(0, nodename.length(),
                              nodename, 0, nodename.length()))) {
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

  if (retc == 0) {
    for(auto& fsckDir : XrdMgmOfs::MgmFsckDirs) {
      gOFS->RemoveContainer(gOFS->MgmProcFsckPath + "/" + fsckDir.c_str() + "/" + std::to_string(fsid).c_str());
    }
  }

  return retc;
}
//-------------------------------------------------------------------------------
// Clean unlinked files from filesystem
//-------------------------------------------------------------------------------
int
proc_fs_dropdeletion(const std::string& id, XrdOucString& stdOut,
                     XrdOucString& stdErr,
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
