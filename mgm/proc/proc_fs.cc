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

#include "mgm/FsView.hh"
#include "mgm/proc/proc_fs.hh"
#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/interface/IView.hh"
#include "namespace/Prefetcher.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "common/Constants.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Get type of entity used in a fs mv operation.
//------------------------------------------------------------------------------
EntityType get_entity_type(const std::string& input, XrdOucString& stdOut,
                           XrdOucString& stdErr)
{
  std::ostringstream oss;
  EntityType ret = EntityType::UNKNOWN;
  // check for nodes
  size_t ppos = input.find(":");

  if (ppos != std::string::npos) {
    // this is a node with port
    ret = EntityType::NODE;
    return ret;
  }

  // check for fs,group,space
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
      ((in1_type == EntityType::SPACE) && (in2_type == EntityType::SPACE)) ||
      ((in1_type == EntityType::FS) && (in2_type == EntityType::NODE))) {
    return static_cast<MvOpType>((in1_type << 2) | in2_type);
  }

  return MvOpType::UNKNOWN;
}

//------------------------------------------------------------------------------
// Dump metadata information
//------------------------------------------------------------------------------
int
proc_fs_dumpmd(std::string& sfsid, XrdOucString& option, XrdOucString& dp,
               XrdOucString& df, XrdOucString& ds, XrdOucString& stdOut,
               XrdOucString& stdErr,
               eos::common::VirtualIdentity& vid_in, size_t& entries)
{
  entries = 0;
  int retc = 0;
  bool monitor = false;
  bool dumppath = false;
  bool dumpfid = false;
  bool dumpsize = false;
  bool processPath = false;
  std::ostringstream out;
  std::ostringstream err;
  std::ostringstream warn;
  out << std::setfill('0');
  warn << std::setfill('0') << std::hex;

  if (option == "m") {
    monitor = true;
  } else {
    if (dp == "1") {
      dumppath = true;
    }

    if (df == "1") {
      dumpfid = true;
    }

    if (ds == "1") {
      dumpsize = true;
    }
  }

  processPath = monitor || dumppath;

  if (!sfsid.length()) {
    err << "error: no <fsid> provided";
    retc = EINVAL;
  } else {
    int fsid = atoi(sfsid.c_str());
    eos::Prefetcher::prefetchFilesystemFileListWithFileMDsAndParentsAndWait(
      gOFS->eosView, gOFS->eosFsView, fsid);

    if (monitor) {
      eos::Prefetcher::prefetchFilesystemUnlinkedFileListWithFileMDsAndWait(
        gOFS->eosView, gOFS->eosFsView, fsid);
    }

    eos::common::RWMutexReadLock ns_rd_lock;
    ns_rd_lock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);

    for (auto it_fid = gOFS->eosFsView->getFileList(fsid);
         (it_fid && it_fid->valid()); it_fid->next()) {
      std::shared_ptr<eos::IFileMD> fmd;
      std::string containerpath;
      std::string fullpath;

      try {
        fmd = gOFS->eosFileService->getFileMD(it_fid->getElement());

        if (fmd) {
          entries++;

          if (processPath) {
            try {
              std::string spath = gOFS->eosView->getUri(fmd.get());
              XrdOucString safepath = spath.c_str();

              while (safepath.replace("&", "#AND#")) {}

              fullpath = safepath.c_str();
              safepath = eos::common::Path{spath.c_str()} .GetParentPath();

              while (safepath.replace("&", "#AND#")) {}

              containerpath = safepath.c_str();
            } catch (eos::MDException& e) {
              errno = e.getErrno();
              eos_static_err("Couldn't retrieve path for fxid=%08llx "
                             "errc=%d emsg=\"%s\"", it_fid->getElement(),
                             e.getErrno(), e.getMessage().str().c_str());
            }
          }

          if ((!dumppath) && (!dumpfid) && (!dumpsize)) {
            std::string env;
            fmd->getEnv(env, true);
            XrdOucString senv = env.c_str();

            if (senv.endswith("checksum=")) {
              senv.replace("checksum=", "checksum=none");
            }

            out << senv.c_str();

            if (monitor) {
              out << "&container="
                  << (containerpath.size() ? containerpath.c_str() : "(null)");
            }
          } else {
            if (dumppath) {
              out << "path="
                  << (fullpath.size() ? fullpath.c_str() : "(null)");
            }

            if (dumpfid) {
              // @todo(esindril) this should use the fxid tag but for the
              // moment we don't change it to avoid breaking scripts.
              out << (dumppath ? " " : "") << "fid=" << std::setw(8)
                  << std::hex << fmd->getId() << std::dec;
            }

            if (dumpsize) {
              out << ((dumppath || dumpfid) ? " " : "")
                  << "size=" << fmd->getSize();
            }
          }

          out << endl;
        }
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_static_err("Couldn't retrieve meta data for fxid=%llx errc=%d "
                       "emsg=\"%s\"", (unsigned long long) it_fid->getElement(),
                       e.getErrno(), e.getMessage().str().c_str());
      }

      if (!fmd) {
        warn << "# warning: ghost entry fxid=" << std::setw(8) << std::hex
             << it_fid->getElement() << std::dec << endl;
        retc = EIDRM;
      } else if (processPath && containerpath.empty()) {
        warn << "# warning: missing container for fxid=" << std::setw(8)
             << std::hex << fmd->getId() << std::dec << endl;
        retc = EIDRM;
      }
    }

    if (monitor) {
      // Also add files which have yet to be unlinked
      for (auto it_fid = gOFS->eosFsView->getUnlinkedFileList(fsid);
           (it_fid && it_fid->valid()); it_fid->next()) {
        std::shared_ptr<eos::IFileMD> fmd;

        try {
          fmd = gOFS->eosFileService->getFileMD(it_fid->getElement());

          if (fmd) {
            entries++;
            std::string env;
            fmd->getEnv(env, true);
            XrdOucString senv = env.c_str();
            senv.replace("checksum=&", "checksum=none&");
            out << senv.c_str() << "&container=(null)" << endl;
          }
        } catch (eos::MDException& e) {
          errno = e.getErrno();
          eos_static_err("Couldn't retrieve meta data for fxid=%08llx errc=%d "
                         "emsg=\"%s\"", it_fid->getElement(), e.getErrno(),
                         e.getMessage().str().c_str());
        }
      }
    }
  }

  if (retc == EIDRM) {
    out << warn.str().c_str();
    err << "# error: filesystem contains problematic entries" << endl;
  }

  stdOut += out.str().c_str();
  stdErr = err.str().c_str();
  return retc;
}

//------------------------------------------------------------------------------
// Configure filesystem
//------------------------------------------------------------------------------
int
proc_fs_config(std::string& identifier, std::string& key, std::string& value,
               XrdOucString& stdOut, XrdOucString& stdErr,
               eos::common::VirtualIdentity& vid_in,
               const std::string& statusComment)
{
  int retc = 0;
  const std::string vid_hostname = vid_in.host;
  eos::common::FileSystem::fsid_t fsid = 0;

  // Check if identifier is fsid (must be pure numeric)
  if (identifier.find_first_not_of("0123456789") == std::string::npos) {
    fsid = atoi(identifier.c_str());
  }

  if (!identifier.length() || !key.length() || !value.length()) {
    stdErr = "error: illegal parameters";
    retc = EINVAL;
  } else {
    FileSystem* fs = nullptr;
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

    if (fsid) {
      // by filesystem id
      fs = FsView::gFsView.mIdView.lookupByID(fsid);
    }

    if (!fs && FsView::gFsView.GetMapping(identifier)) {
      // by filesystem uuid
      fs = FsView::gFsView.mIdView.lookupByID(FsView::gFsView.GetMapping(identifier));
    }

    if (!fs) {
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
          for (auto it = FsView::gFsView.mNodeView[identifier]->begin();
               it != FsView::gFsView.mNodeView[identifier]->end(); ++it) {
            FileSystem* candidate = FsView::gFsView.mIdView.lookupByID(*it);

            if (candidate && candidate->GetPath() == path) {
              fs = candidate;
            }
          }
        }
      }
    }

    if (fs) {
      // Check the allowed strings
      if (((key == "configstatus") &&
           (eos::common::FileSystem::GetConfigStatusFromString(value.c_str()) !=
            eos::common::ConfigStatus::kUnknown)) ||
          (((key == eos::common::SCAN_IO_RATE_NAME) ||
            (key == eos::common::SCAN_ENTRY_INTERVAL_NAME) ||
            (key == eos::common::SCAN_DISK_INTERVAL_NAME) ||
            (key == eos::common::SCAN_NS_INTERVAL_NAME) ||
            (key == eos::common::SCAN_NS_RATE_NAME) ||
	    (key == "max.ropen" || (key == "max.wopen")) ||
            (key == "headroom") || (key == "graceperiod") ||
            (key == "drainperiod") || (key == "proxygroup") ||
            (key == "filestickyproxydepth") || (key == "forcegeotag") ||
            (key == "s3credentials")))) {
        // Check permissions
        size_t dpos = 0;
        std::string nodename = fs->GetString("host");

        if ((dpos = nodename.find('.')) != std::string::npos) {
          nodename.erase(dpos);
        }

        // If EOS_SKIP_SSS_HOSTNAME_MATCH env variable is set then we skip
        // the check below as this currently breaks the Kubernetes setup.
        bool skip_hostname_match = false;

        if (getenv("EOS_SKIP_SSS_HOSTNAME_MATCH")) {
          skip_hostname_match = true;
        }

        if ((vid_in.uid == 0) || (vid_in.prot == "sss")) {
          if ((vid_in.prot == "sss") && (vid_in.uid != 0)) {
            if (!skip_hostname_match &&
                vid_hostname.compare(0, nodename.length(),
                                     nodename, 0, nodename.length())) {
              stdErr = "error: filesystems can only be configured as 'root' or "
                       "from the server mounting them using sss protocol (1)\n";
              retc = EPERM;
              return retc;
            }
          }
        } else {
          stdErr = "error: filesystems can only be configured as 'root' or "
                   "from the server mounting them using sss protocol (2)\n";
          retc = EPERM;
          return retc;
        }

        if ((key == eos::common::SCAN_IO_RATE_NAME) ||
            (key == eos::common::SCAN_ENTRY_INTERVAL_NAME) ||
            (key == eos::common::SCAN_DISK_INTERVAL_NAME) ||
            (key == eos::common::SCAN_NS_INTERVAL_NAME) ||
            (key == eos::common::SCAN_NS_RATE_NAME) ||
            (key == "headroom") || (key == "graceperiod") ||
            (key == "drainperiod")) {
          fs->SetLongLong(key.c_str(),
                          eos::common::StringConversion::GetSizeFromString(value.c_str()));
          FsView::gFsView.StoreFsConfig(fs);
        } else if (key == "configstatus") {
          if (value == "empty") {
            // Check if this filesystem is really empty
            if (gOFS->eosFsView->getNumFilesOnFs(fs->GetId()) != 0) {
              std::ostringstream oss;
              oss << "error: the filesystem is not empty, therefore it can't be removed\n"
                  << "# -------------------------------------------------------------------\n"
                  << "# You can inspect the registered files via the command:\n"
                  << "# [eos] fs dumpmd " << fs->GetId() << " -path\n"
                  << "# -------------------------------------------------------------------\n"
                  << "# You can drain the filesystem if it is still operational via the command:\n"
                  << "# [eos] fs config " << fs->GetId() << " configstatus=drain\n"
                  << "# -------------------------------------------------------------------\n"
                  << "# You can force to remove these files via the command:\n"
                  << "# [eos] fs dropfiles " << fs->GetId() << "\n"
                  << "# -------------------------------------------------------------------\n"
                  << "# You can force to drop these files (brute force) via the command:\n"
                  << "# [eos] fs dropfiles " << fs->GetId() << " -f \n"
                  << "# -------------------------------------------------------------------\n"
                  << "# [eos] = 'eos -b' on MGM or 'eosadmin' on storage nodes\n";
              stdErr = oss.str().c_str();
              retc = EPERM;
              return retc;
            }
          }

          if (!fs->SetString(key.c_str(), value.c_str())) {
            stdErr = "error: failed to apply configuration change";
            retc = EINVAL;
            return retc;
          }

          std::string operation;
          bool success;

          if (statusComment.empty()) {
            success = fs->RemoveKey("statuscomment");
            operation = "remove";
          } else {
            success = fs->SetString("statuscomment", statusComment.c_str());
            operation = "save";
          }

          if (!success) {
            eos_static_warning("failed to %s config status comment "
                               "fs_identifier=%s comment=%s", operation.c_str(),
                               identifier.c_str(), statusComment.c_str());
          }

          FsView::gFsView.StoreFsConfig(fs);
        } else if (key == "s3credentials") {
          // Validate S3 credentials string
          if (std::count(value.begin(), value.end(), ':') != 1) {
            stdErr += "error: invalid S3 credentials string";
            retc = EINVAL;
            return retc;
          } else {
            size_t pos = value.find(':');

            if (pos == 0 || (pos + 1) == value.length()) {
              stdErr += "error: S3 credentials string is missing ";
              stdErr += (pos == 0)  ? "<accesskey>" : "<secretkey>";
              retc = EINVAL;
              return retc;
            }
          }

          fs->SetString(key.c_str(), value.c_str());
          FsView::gFsView.StoreFsConfig(fs);
        } else if (key == "forcegeotag") {
          const int max_tag_size = 8;
          char node_geotag [value.size()];
          strcpy(node_geotag, value.c_str());
          char* gtag = strtok(node_geotag, "::");

          while (gtag != NULL) {
            if (strlen(gtag) > max_tag_size) {
              stdErr += "error: the forcegeotag value contains a tag longer "
                        "than the 8 chars maximum allowed";
              retc = EINVAL;
              return retc;
            }

            gtag = strtok(NULL, "::");
          }

          fs->SetString(key.c_str(), value.c_str());
          FsView::gFsView.StoreFsConfig(fs);
        } else {
          // Other proxy* key set
          fs->SetString(key.c_str(), value.c_str());
          FsView::gFsView.StoreFsConfig(fs);
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
proc_fs_add(mq::MessagingRealm* realm, std::string& sfsid, std::string& uuid,
            std::string& nodename, std::string& mountpoint, std::string& space,
            std::string& configstatusStr, XrdOucString& stdOut,
            XrdOucString& stdErr, eos::common::VirtualIdentity& vid_in)
{
  using eos::common::StringConversion;
  const std::string vid_hostname = vid_in.host;
  common::FileSystem::fsid_t fsid = atoi(sfsid.c_str());
  common::ConfigStatus configStatus =
    common::FileSystem::GetConfigStatusFromString(configstatusStr.c_str());

  if ((!nodename.length()) || (!mountpoint.length()) || (!space.length()) ||
      (!configstatusStr.length()) ||
      (configStatus < eos::common::ConfigStatus::kOff)) {
    stdErr += "error: illegal parameters";
    return EINVAL;
  }

  // Node name comes as /eos/<host>..../, we just need <host> without domain
  std::string rnodename = nodename;
  rnodename.erase(0, 5);
  size_t dpos;

  if ((dpos = rnodename.find('.')) != std::string::npos) {
    rnodename.erase(dpos);
  }

  // If EOS_SKIP_SSS_HOSTNAME_MATCH env variable is set then we skip
  // the check below as this currently breaks the Kubernetes setup.
  bool skip_hostname_match = (getenv("EOS_SKIP_SSS_HOSTNAME_MATCH") ?
                              true : false);

  // Rough check that the filesystem is added from a host with the same
  // hostname ... anyway we should have configured 'sss' security
  if ((vid_in.uid == 0) || (vid_in.prot == "sss")) {
    if ((vid_in.prot == "sss") && (vid_in.uid != 0)) {
      if (!skip_hostname_match &&
          vid_hostname.compare(0, rnodename.length(),
                               rnodename, 0, rnodename.length())) {
        stdErr = "error: filesystems can only be configured as 'root' or "
                 "from the server mounting them using sss protocol (1)";
        return EPERM;
      }
    }
  } else {
    stdErr = "error: filesystems can only be configured as 'root' or "
             "from the server mounting them using sss protocol (2)";
    return EPERM;
  }

  eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);
  // queuepath = /eos/<host:port><path>
  std::string queuepath = nodename;
  queuepath += mountpoint;
  common::FileSystemLocator locator;

  if (!common::FileSystemLocator::fromQueuePath(queuepath, locator)) {
    eos_static_err("msg=\"could not parse queue path\" queue=\"%s\"",
                   queuepath.c_str());
    stdErr += "error: could not parse queue path qeueue='";
    stdErr += queuepath.c_str();
    stdErr += "'";
    return EINVAL;
  }

  // Check if this filesystem exists already ....
  if (FsView::gFsView.ExistsQueue(nodename, queuepath)) {
    eos_static_err("msg=\"file system already registered\" queue=\"%s\"",
                   queuepath.c_str());
    stdErr += "error: cannot register filesystem - it already exists!";
    return EEXIST;
  }

  // Check if there is a mapping for 'uuid'
  if (FsView::gFsView.GetMapping(uuid) ||
      ((fsid > 0) && (FsView::gFsView.HasMapping(fsid)))) {
    eos_static_err("msg=\"file system already registered\" uuid=%s fsid=%lu",
                   uuid.c_str(), fsid);
    stdErr = SSTR("error: file system identified by uuid=" <<
                  uuid << " id=" << sfsid << " already exists").c_str();
    return EEXIST;
  }

  // We want one atomic update with all the parameters defined
  std::string splitspace;
  std::string splitgroup;
  unsigned int groupsize = 0;
  unsigned int groupmod = 0;
  // Logic to automatically adjust scheduling subgroups
  StringConversion::SplitByPoint(space, splitspace, splitgroup);

  if (FsView::gFsView.mSpaceView.count(splitspace)) {
    groupsize = atoi(FsView::gFsView.mSpaceView[splitspace]->GetMember
                     (std::string("cfg.groupsize")).c_str());
    groupmod = atoi(FsView::gFsView.mSpaceView[splitspace]->GetMember
                    (std::string("cfg.groupmod")).c_str());
  } else {
    if (splitspace != eos::common::EOS_SPARE_GROUP) {
      eos_static_err("msg=\"no such space\" space=%s", splitspace.c_str());
      stdErr = SSTR("error: not such space \"" << splitspace << "\"").c_str();
      return EINVAL;
    }
  }

  // Groups where we attempt to insert the current file system
  std::set<int> target_grps;

  // Case when specific group is requested by the user
  if (splitgroup.length()) {
    try {
      int id = std::stoi(splitgroup);

      if (id >= (int)groupmod) {
        stdErr = SSTR("error: requested group " << id
                      << " bigger than groupmod").c_str();
        return EINVAL;
      }

      target_grps.insert(id);
    } catch (...) {
      eos_static_err("msg=\"invalid group requested\" group=\"%s\"",
                     splitgroup.c_str());
      stdErr = SSTR("error: invalid group requested \""
                    << splitgroup << "\"").c_str();
      return EINVAL;
    }
  } else {
    // Otherwise, try out all the groups in the space
    for (int i = 0; i < (int)groupmod; ++i) {
      target_grps.insert(i);
    }
  }

  // Final group will be decided later on
  splitgroup.clear();

  // Handle special case for "spare" space that does not have any groups
  if (splitspace == eos::common::EOS_SPARE_GROUP) {
    target_grps.clear();
  }

  for (auto grp_id : target_grps) {
    std::string schedgroup = SSTR(splitspace << "." << grp_id);

    // All good if group is not yet created
    if (FsView::gFsView.mGroupView.count(schedgroup) == 0) {
      splitgroup = std::to_string(grp_id);
      break;
    }

    FsGroup* group = FsView::gFsView.mGroupView[schedgroup];

    // Skip if group is already full
    if (group->size() > groupsize) {
      if (target_grps.size() == 1) {
        stdErr += SSTR("error: scheduling group " << splitspace << "." << grp_id
                       << " is full" << std::endl).c_str();
      }

      continue;
    }

    // Skip if group already contains an fs from the current node.
    // Allow disabling this check in development clusters through the envvar
    bool exists = false;

    if (!getenv("EOS_ALLOW_SAME_HOST_IN_GROUP")) {
      for (auto it = group->begin(); it != group->end(); ++it) {
        FileSystem* entry = FsView::gFsView.mIdView.lookupByID(*it);

        if (entry && (entry->GetString("host") == locator.getHost())) {
          exists = true;
          break;
        }
      }
    }

    if (exists) {
      continue;
    }

    splitgroup = std::to_string(grp_id);
    break;
  }

  if ((splitspace != eos::common::EOS_SPARE_GROUP) && splitgroup.empty()) {
    eos_static_err("msg=\"no group available for file system\" fsid=%lu"
                   " queue=%s", fsid, queuepath.c_str());
    stdErr += "error: no group available for file system";
    return EINVAL;
  }

  FileSystem* fs = nullptr;

  if (fsid) {
    if (!FsView::gFsView.ProvideMapping(uuid, fsid)) {
      eos_static_err("msg=\"conflict registering file system uuid/id\""
                     "uuid=%s fsid=%lu", uuid.c_str(), fsid);
      stdErr += "error: conflict adding your uuid/fsid mapping";
      return EINVAL;
    } else {
      fs = new FileSystem(locator, realm);
    }
  } else {
    fsid = FsView::gFsView.CreateMapping(uuid);
    fs = new FileSystem(locator, realm);
  }

  stdOut += SSTR("success: mapped '" << uuid <<  "' <=> fsid=" << fsid).c_str();
  common::GroupLocator groupLocator;
  std::string description = ((splitspace == eos::common::EOS_SPARE_GROUP) ?
                             splitspace : SSTR(splitspace << "." << splitgroup));
  common::GroupLocator::parseGroup(description, groupLocator);
  common::FileSystemCoreParams coreParams(fsid, locator, groupLocator, uuid,
                                          configStatus);

  if (FsView::gFsView.Register(fs, coreParams)) {
    // Set all the space related default parameters
    if (FsView::gFsView.mSpaceView.count(space)) {
      if (FsView::gFsView.mSpaceView[space]->ApplySpaceDefaultParameters(fs)) {
        // Store the modifications
        FsView::gFsView.StoreFsConfig(fs);
      }
    }
  } else {
    // Remove mapping
    if (FsView::gFsView.RemoveMapping(fsid, uuid)) {
      stdErr += SSTR("\ninfo: unmapped '" << uuid << "' <!> fsid=" << fsid).c_str();
    } else {
      stdErr += "error: cannot remove mapping - this can be fatal!";
    }

    // Remove filesystem object
    stdErr += "error: cannot register filesystem - check for path duplication!";
    return EINVAL;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Move a filesystem/group/space to a group/space
//------------------------------------------------------------------------------
int
proc_fs_mv(std::string& src, std::string& dst, XrdOucString& stdOut,
           XrdOucString& stdErr, eos::common::VirtualIdentity& vid_in,
           bool force, mq::MessagingRealm* realm)
{
  int retc = 0;
  MvOpType operation = get_operation_type(src, dst, stdOut, stdErr);
  eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);

  switch (operation) {
  case MvOpType::FS_2_GROUP:
    retc = proc_mv_fs_group(FsView::gFsView, src, dst, stdOut, stdErr, force);
    break;

  case MvOpType::FS_2_SPACE:
    retc = proc_mv_fs_space(FsView::gFsView, src, dst, stdOut, stdErr, force);
    break;

  case MvOpType::GRP_2_SPACE:
    retc = proc_mv_grp_space(FsView::gFsView, src, dst, stdOut, stdErr, force);
    break;

  case MvOpType::SPC_2_SPACE:
    retc = proc_mv_space_space(FsView::gFsView, src, dst, stdOut, stdErr, force);
    break;

  case MvOpType::FS_2_NODE:
    retc = proc_mv_fs_node(FsView::gFsView, src, dst, stdOut, stdErr, force, vid_in,
                           realm);
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
                    XrdOucString& stdOut, XrdOucString& stdErr, bool force)
{
  std::ostringstream oss;
  FileSystem::fs_snapshot_t snapshot;

  if (fs->SnapShotFileSystem(snapshot)) {
    if (force) {
      return true;
    }

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
    bool is_empty = (fs->GetConfigStatus() == eos::common::ConfigStatus::kEmpty);
    bool is_active = (fs->GetActiveStatus() == eos::common::ActiveStatus::kOnline);

    if (!(is_empty && is_active)) {
      eos_static_err("msg=\"file system is not empty or is not active\" "
                     "fsid=%lu", snapshot.mId);
      oss << "error: file system " << snapshot.mId << " is not empty or "
          << "is not active" << std::endl;
      stdErr = oss.str().c_str();
      return false;
    }
  } else {
    eos_static_err("%s", "msg=\"failed to snapshot file system\"");
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
                     XrdOucString& stdErr, bool force)
{
  using eos::mgm::FsView;
  using eos::mgm::FileSystem;
  using eos::common::StringConversion;
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
    eos_static_err("msg=\"requested space %s does not exist\"", space.c_str());
    oss << "error: space " << space << " does not exist" << std::endl;
    stdErr = oss.str().c_str();
    return EINVAL;
  }

  // Get this filesystem
  FileSystem* fs = fs_view.mIdView.lookupByID(fsid);

  if (fs) {
    if (!proc_fs_can_mv(fs, dst, stdOut, stdErr, force)) {
      return EINVAL;
    }
  } else {
    eos_static_err("no such fsid: %i", fsid);
    oss << "error: no such fsid: " << fsid << std::endl;
    stdErr = oss.str().c_str();
    return EINVAL;
  }

  // Get the target group
  if (dst != eos::common::EOS_SPARE_GROUP) {
    auto const iter = fs_view.mGroupView.find(dst);

    if (iter != fs_view.mGroupView.end()) {
      FsGroup* grp = iter->second;
      size_t grp_num_fs = grp->size();

      // Check that we can still add file systems to this group
      if (!force && (grp_num_fs > grp_size)) {
        eos_static_err("msg=\"reached maximum number of fs for group %s\"",
                       dst.c_str());
        oss << "error: reached maximum number of file systems for group "
            << dst.c_str() << std::endl;
        stdErr = oss.str().c_str();
        return EINVAL;
      }

      // Check that there is no other file system from the same node
      // in this group
      bool is_forbidden = false;
      std::string host;
      std::string fs_host = fs->GetHost();

      for (auto it = grp->begin(); it != grp->end(); ++it) {
        FileSystem* entry = fs_view.mIdView.lookupByID(*it);

        if (entry) {
          host = entry->GetHost();

          if (fs_host == host) {
            is_forbidden = true;
            break;
          }
        }
      }

      if (!force && is_forbidden) {
        eos_static_err("msg=\"group %s already contains an fs from the "
                       "same node\"", dst.c_str());
        oss << "error: group " << dst
            << " already contains a file system from the same node"
            << std::endl;
        stdErr = oss.str().c_str();
        return EINVAL;
      }
    } else {
      // A new group will be created, check that it respects the groupmod param
      size_t grp_indx = std::strtoul(group.c_str(), nullptr, 10);

      if (!force && (grp_indx >= grp_mod)) {
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
    // Apply defaults from the new space
    std::set<std::string> paramlist;
    paramlist.insert("scaninterval");
    paramlist.insert("scanrate");
    paramlist.insert("headroom");
    paramlist.insert("drainperiod");
    paramlist.insert("graceperiod");

    for (auto it = paramlist.begin(); it != paramlist.end(); ++it) {
      std::string value = it_space->second->GetConfigMember(*it);

      if (value.length()) {
        int64_t uvalue = StringConversion::GetSizeFromString(value.c_str());
        fs->SetLongLong(it->c_str(), uvalue);
        FsView::gFsView.StoreFsConfig(fs);
        oss << "info: applying space config " << *it << "=" << value << std::endl;
      }
    }

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
                     XrdOucString& stdErr, bool force)
{
  using eos::mgm::FsView;
  std::ostringstream oss;
  // Check if file system is not already in this space
  FileSystem::fsid_t fsid = strtol(src.c_str(), nullptr, 10);
  FileSystem* fs = fs_view.mIdView.lookupByID(fsid);

  if (fs) {
    if (!proc_fs_can_mv(fs, dst, stdOut, stdErr, force)) {
      return EINVAL;
    }
  } else {
    eos_static_err("msg=\"no such file system\" fsid=%lu", fsid);
    oss << "error: no such fsid: " << fsid << std::endl;
    stdErr = oss.str().c_str();
    return EINVAL;
  }

  auto it_space = fs_view.mSpaceView.find(dst);

  if (it_space == fs_view.mSpaceView.end()) {
    eos_static_info("msg=\"creating space %s\"", dst.c_str());
    FsSpace* new_space = new FsSpace(dst.c_str());
    fs_view.mSpaceView[dst] = new_space;
    it_space = fs_view.mSpaceView.find(dst);
  }

  int grp_size = std::atoi(it_space->second->GetConfigMember
                           ("groupsize").c_str());
  int grp_mod = std::atoi(it_space->second->GetConfigMember
                          ("groupmod").c_str());

  if ((dst == eos::common::EOS_SPARE_GROUP) && grp_mod) {
    eos_static_err("%s", "msg=\"space spare must have groupmod 0\"");
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
    sorted_grps.emplace_back(eos::common::EOS_SPARE_GROUP);
  }

  bool done = false;

  for (const auto& grp : sorted_grps) {
    if (proc_mv_fs_group(fs_view, src, grp, stdOut, stdErr, force) == 0) {
      stdErr = "";
      done = true;
      break;
    }
  }

  if (!done) {
    eos_static_err("msg=\"failed to add fs %s to space %s\"",
                   src.c_str(), dst.c_str());
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
  // All groups in space are possible candidates
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
                      XrdOucString& stdErr, bool force)
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
    if (proc_mv_fs_space(fs_view, sfsid, dst, stdOut, stdErr, force)) {
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
                        XrdOucString& stdErr, bool force)
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
    if (proc_mv_fs_space(fs_view, sfsid, dst, stdOut, stdErr, force)) {
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
// Move FS to a new node
//------------------------------------------------------------------------------
int
proc_mv_fs_node(FsView& fs_view, const std::string& src,
                const std::string& dst, XrdOucString& stdOut,
                XrdOucString& stdErr, bool force,
                eos::common::VirtualIdentity& vid_in,
                mq::MessagingRealm* realm)
{
  std::ostringstream oss;
  eos::common::FileSystem::fsid_t fsid = stoi(src.c_str());
  FileSystem* fs = fs_view.mIdView.lookupByID(fsid);

  if (fs) {
    FileSystem::fs_snapshot_t snapshot;

    if (fs->SnapShotFileSystem(snapshot)) {
      // pretend this is empty
      fs->SetString("configstatus", "empty");
      std::string a;
      std::string b;
      std::string id = src;
      std::string uuid = snapshot.mUuid;
      std::string hostport = snapshot.mHostPort;
      std::string path = snapshot.mPath;
      std::string space = snapshot.mSpace;
      std::string group = snapshot.mGroup;
      std::string configstatus = eos::common::FileSystem::GetConfigStatusAsString(
                                   snapshot.mConfigStatus);
      int rc = proc_fs_rm(a , b, id, stdOut, stdErr, vid_in);
      FsView::gFsView.ViewMutex.UnLockWrite();

      if (!rc) {
        std::string nodename = "/eos/";
        nodename += dst;
        nodename += "/fst";
        int rc = proc_fs_add(realm, id, uuid, nodename, path,
                             getenv("EOS_ALLOW_SAME_HOST_IN_GROUP") ? group : space,
                             configstatus, stdOut, stdErr, vid_in);

        if (rc) {
          oss << "error: failed to reinsert filesystem with id='" << fsid <<
              "' - this is really really bad!!!" << std::endl;
          stdErr += oss.str().c_str();
          stdOut.erase();
        }
      } else {
        oss << "error: failed ot snapshot filesystem with id='" << fsid << "'" <<
            std::endl;
        stdErr = oss.str().c_str();
        stdOut.erase();
      }

      FsView::gFsView.ViewMutex.LockWrite();
    } else {
      oss << "error: failed ot snapshot filesystem with id='" << fsid << "'" <<
          std::endl;
      stdErr = oss.str().c_str();
      stdOut.erase();
    }
  } else {
    oss << "error: no such filesystem with id='" << fsid << "'" << std::endl;
    stdErr = oss.str().c_str();
    stdOut.erase();
  }

  return 0;
}



//------------------------------------------------------------------------------
// Remove filesystem
//------------------------------------------------------------------------------
int
proc_fs_rm(std::string& nodename, std::string& mountpoint, std::string& id,
           XrdOucString& stdOut, XrdOucString& stdErr,
           eos::common::VirtualIdentity& vid_in)
{
  int retc = 0;
  const std::string vid_hostname = vid_in.host;
  eos::common::FileSystem::fsid_t fsid = 0;

  if (id.length()) {
    fsid = stoi(id);
  }

  FileSystem* fs = 0;

  if (id.length()) {
    // find by id
    fs = FsView::gFsView.mIdView.lookupByID(fsid);
  } else {
    if (mountpoint.length() && nodename.length()) {
      std::string queuepath = nodename;
      queuepath += mountpoint;
      fs = FsView::gFsView.FindByQueuePath(queuepath);
    }
  }

  if (fs) {
    std::string nodename = fs->GetString("host");
    std::string cstate = fs->GetString("configstatus");
    size_t dpos = 0;

    if ((dpos = nodename.find('.')) != std::string::npos) {
      nodename.erase(dpos);
    }

    // If EOS_SKIP_SSS_HOSTNAME_MATCH env variable is set then we skip
    // the check below as this currently breaks the Kubernetes setup.
    bool skip_hostname_match = false;

    if (getenv("EOS_SKIP_SSS_HOSTNAME_MATCH")) {
      skip_hostname_match = true;
    }

    if ((vid_in.uid == 0) || (vid_in.prot == "sss")) {
      if ((vid_in.prot == "sss") && (vid_in.uid != 0)) {
        if (!skip_hostname_match &&
            vid_hostname.compare(0, nodename.length(),
                                 nodename, 0, nodename.length())) {
          stdErr = "error: filesystems can only be configured as 'root' or "
                   "from the server mounting them using sss protocol (1)\n";
          retc = EPERM;
          return retc;
        }
      }
    } else {
      stdErr = "error: filesystems can only be configured as 'root' or "
               "from the server mounting them using sss protocol (2)\n";
      retc = EPERM;
      return retc;
    }

    if (cstate != "empty") {
      stdErr = "error: you can only remove file systems which are in 'empty' status";
      retc = EINVAL;
    } else {
      if (!FsView::gFsView.UnRegister(fs, true, true)) {
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
proc_fs_dropdeletion(const eos::common::FileSystem::fsid_t& fsid,
                     const eos::common::VirtualIdentity& vid_in,
                     std::string& out, std::string& err)
{
  if (fsid == 0ul) {
    err = "error: no such filesystem fsid=0";
    return EINVAL;
  }

  if ((vid_in.uid != 0)) {
    err = "error: command can only be executed by 'root'";
    return EPERM;
  }

  eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex, __FUNCTION__,
      __LINE__, __FILE__);

  if (gOFS->eosFsView->clearUnlinkedFileList(fsid)) {
    out = SSTR("success: dropped deletions on fsid=" << fsid).c_str();
  } else {
    out = SSTR("note: there is no deletion list for fsid=" << fsid).c_str();
  }

  return 0;
}

//-------------------------------------------------------------------------------
// Clean ghost entries in a filesystem view
//-------------------------------------------------------------------------------
int
proc_fs_dropghosts(const eos::common::FileSystem::fsid_t& fsid,
                   const std::set<eos::IFileMD::id_t>& set_fids,
                   const eos::common::VirtualIdentity& vid_in,
                   std::string& out, std::string& err)
{
  if (fsid == 0ul) {
    err = "error: no such filesystem fsid=0";
    return EINVAL;
  }

  if ((vid_in.uid != 0)) {
    err = "error: command can only be executed by 'root'";
    return EPERM;
  }

  std::set<eos::IFileMD::id_t> to_delete;

  if (set_fids.empty()) {
    // We check all the files on that filesystem
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                            __LINE__, __FILE__);

    for (auto it_fid = gOFS->eosFsView->getFileList(fsid);
         (it_fid && it_fid->valid()); it_fid->next()) {
      try {
        auto fmd = gOFS->eosFileService->getFileMD(it_fid->getElement());
      } catch (eos::MDException& e) {
        if (e.getErrno() == ENOENT) {
          out += SSTR("# removing id: " << it_fid->getElement() << "\n").c_str();
          to_delete.insert(it_fid->getElement());
        }
      }
    }
  } else {
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                            __LINE__, __FILE__);

    for (const auto& fid : set_fids) {
      try {
        auto fmd = gOFS->eosFileService->getFileMD(fid);
      } catch (eos::MDException& e) {
        if (e.getErrno() == ENOENT) {
          out += SSTR("# removing id: " << fid << "\n").c_str();
          to_delete.insert(fid);
        }
      }
    }
  }

  eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex, __FUNCTION__,
      __LINE__, __FILE__);

  for (const auto& fid : to_delete) {
    gOFS->eosFsView->eraseEntry(fsid, fid);
  }

  out += SSTR("success: dropped " << to_delete.size()
              << " ghost entries from fsid=" << fsid).c_str();
  return 0;
}

EOSMGMNAMESPACE_END
