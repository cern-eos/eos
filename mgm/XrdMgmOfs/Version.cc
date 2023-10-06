// ----------------------------------------------------------------------
// File: Version.cc
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

// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

/*----------------------------------------------------------------------------*/
/*
 * @brief handles file versioning for fid
 *
 * @param fid id of the file to version
 * @param error object
 * @param vid virtual identity of the caller
 * @param max_versions the maximum number of version to keep
 * @param versionedpath return variable for the full path to the latest version file
 * @return SFS_OK if successfully send otherwise SFS_ERROR
 *
 * Versions are kept in a hidden directory .<name>/<ctime>owned by root
 */
/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::Version(eos::common::FileId::fileid_t fid,
                   XrdOucErrInfo& error,
                   eos::common::VirtualIdentity& vid,
                   int max_versions,
                   XrdOucString* versionedpath,
                   bool simulate)
{
  static const char* epname = "version";
  EXEC_TIMING_BEGIN("Version");
  gOFS->MgmStats.Add("Versioning", vid.uid, vid.gid, 1);
  std::shared_ptr<eos::IFileMD> fmd;
  std::string path;
  std::string vpath;
  std::string bname;
  std::string versionpath;
  eos::common::VirtualIdentity fidvid = vid;
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  time_t filectime = 0;
  {
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

    try {
      fmd = gOFS->eosFileService->getFileMD(fid);
      path = gOFS->eosView->getUri(fmd.get()).c_str();
      eos::common::Path cPath(path.c_str());
      bool noversion;
      cPath.DecodeAtomicPath(noversion);
      vpath = cPath.GetParentPath();
      bname = cPath.GetName();
      fidvid.uid = fmd->getCUid();
      fidvid.gid = fmd->getCGid();
      fidvid.allowed_gids.insert(fidvid.gid);
      eos::IFileMD::ctime_t ctime;
      fmd->getCTime(ctime);
      filectime = (time_t) ctime.tv_sec;
    } catch (eos::MDException& e) {
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                e.getMessage().str().c_str());
      errno = e.getErrno();
      std::string errmsg = "translate file id - ";
      errmsg = +e.getMessage().str().c_str();
      return Emsg(epname, error, errno, errmsg.c_str(), path.c_str());
    }
  }

  if ((fidvid.uid != vid.uid) && (vid.uid)) {
    return Emsg(epname, error, EPERM,
                "create version - you are not the owner of this file", path.c_str());
  }

  vpath += ".sys.v#.";
  vpath += bname;
  versionpath = vpath;
  versionpath += "/";
  {
    char vci[128];
    snprintf(vci, sizeof(vci) - 1, "%llu.%08llx", (unsigned long long) filectime,
             (unsigned long long) fid);
    versionpath += vci;

    // return the latest version name
    if (versionedpath) {
      *versionedpath = versionpath.c_str();
    }
  }
  // Check if .version directory exists, if not create it
  struct stat buf;

  if (gOFS->_stat(vpath.c_str(), &buf, error, fidvid, 0, 0)) {
    eos_info("msg=\"creating version directory\" version-directory=\"%s\"",
             vpath.c_str());

    if (gOFS->_mkdir(vpath.c_str(), 0, error, fidvid, (const char*) 0, nullptr, true)) {
      return Emsg(epname, error, errno, "create version directory", vpath.c_str());
    }

    if (gOFS->_stat(vpath.c_str(), &buf, error, fidvid, 0, 0)) {
      return Emsg(epname, error, errno, "stat version directory", vpath.c_str());
    }

    // make sure, the owner can write into the version directory
    XrdSfsMode chmod_mode = buf.st_mode | S_IRWXU;

    if (gOFS->_chmod(vpath.c_str(), chmod_mode, error, rootvid, (const char*) 0)) {
      return Emsg(epname, error, errno, "chmod version directory", vpath.c_str());
    }
  }

  // Rename to the version directory target
  if ((!gOFS->_stat(vpath.c_str(), &buf, error, fidvid, 0, 0)) && (!simulate) &&
      gOFS->_rename(path.c_str(), versionpath.c_str(), error, fidvid, 0, 0, false,
                    false)) {
    return Emsg(epname, error, errno, "version file", path.c_str());
  }

  // Purge versions according to policy
  if (max_versions > 0) {
    if (gOFS->PurgeVersion(vpath.c_str(), error, max_versions)) {
      return Emsg(epname, error, errno, "purge versions", path.c_str());
    }
  }

  if (!simulate) {
    eos_info("msg=\"new version created\" previous-path=\"%s\" version-path=\"%s\"",
             path.c_str(), versionpath.c_str());
  } else {
    eos_info("msg=\"new version simulated\" previous-path=\"%s\" version-path=\"%s\"",
             path.c_str(), versionpath.c_str());
  }

  EXEC_TIMING_END("Versioning");
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
/*
 * @brief purge oldest versions exceeding max_versions
 *
 * @param versiondir directory where versions live
 * @param max_versions maximum number of versions to keep
 *
 * @return SFS_OK if success otherwise SFS_ERROR and might set errno
 *
 * If max_versions=0 it will remove all versions and the version directory!
 * If max_versions=-1 it will read the attribute sys.versioning of the parent
 * directory and apply the setting.
 * If max_versions=-2 it will read the attribute sys.versioning of the parent
 * directory and apply the setting-1 .
 *
 * The caller needs to have the quota mutex read locked (gQuoatMutex).
 */
/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::PurgeVersion(const char* versiondir,
                        XrdOucErrInfo& error,
                        int max_versions)
{
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  eos_info("version-dir=%s max-versions=%d", versiondir, max_versions);

  if (!versiondir) {
    errno = EINVAL;
    return SFS_ERROR;
  }

  std::string path = versiondir;

  if (max_versions < 0) {
    // Indicates that we should read the max version depth from the parent attributes
    eos::common::Path cPath(versiondir);
    // Get the attributes and call the verify function
    eos::IContainerMD::XAttrMap map;

    if (gOFS->_attr_ls(cPath.GetParentPath(), error, rootvid, (const char*) 0,
                       map)) {
      return SFS_ERROR;
    }

    if (map.count("sys.versioning")) {
      max_versions = atoi(map["sys.versioning"].c_str());
    } else {
      return SFS_OK;
    }
  }

  XrdMgmOfsDirectory directory;
  int listrc = directory.open(versiondir, rootvid, (const char*) 0);
  eos_info("listrc=%d max-version=%d", listrc, max_versions);

  if (!listrc && !max_versions) {
    // We use the rm -r proc function to do the clean-up to have the recycle
    // functionality involved for version directories
    ProcCommand Cmd;
    //info=eos.rgid=0&eos.ruid=0&mgm.cmd=rm&mgm.option=r&mgm.path=
    XrdOucString info = "mgm.cmd=rm&mgm.option=r&mgm.path=";
    info += path.c_str();
    Cmd.open("/proc/user", info.c_str(), rootvid, &error);
    Cmd.close();

    if (Cmd.GetRetc()) {
      return SFS_ERROR;
    } else {
      return SFS_OK;
    }
  }

  int success = 0;
  std::map<uint64_t, std::string> version_by_age;
  std::vector<std::string> versions;

  if (!listrc) {
    const char* val = 0;
    time_t now = time(NULL);

    while ((val = directory.nextEntry())) {
      std::string entryname = val;

      if ((entryname == ".") || (entryname == "..")) {
        continue;
      }

      versions.push_back(entryname);
      // get age information for this entry
      std::string mtime, inode;
      eos::common::StringConversion::SplitKeyValue(entryname, mtime, inode, ".");
      uint64_t stmtime = strtoull(mtime.c_str(), 0, 10);
      ssize_t age = (ssize_t)(now) - stmtime;

      if (age > 0) {
        version_by_age[age] = entryname;
      }
    }

    const uint64_t age_bins[12] = { 0, 86400 * 1, 86400 * 2, 86400 * 3, 86400 * 4, 86400 * 5, 86400 * 6, 86400 * 7,
                                    86400 * 14, 86400 * 21, 86400 * 28, 0xffffffffffffffff
                                  };
    std::map<int, int> age_map;
    std::set<std::string> keep_set;

    for (auto x = version_by_age.rbegin();  x != version_by_age.rend(); x++) {
      for (size_t i = 0; i < 12 ; ++i) {
        if (EOS_LOGS_DEBUG) {
          eos_static_debug("bin %llu", age_bins[i]);
        }

        if ((x->first >= age_bins[i]) &&
            (x->first < age_bins[i + 1])) {
          if (EOS_LOGS_DEBUG) {
            eos_static_info("map %lu %lu", x->first, i);
          }

          if (age_map[i] == 0) {
            // mark the olderst version in a bin to keep
            keep_set.insert(x->second);
          }

          age_map[i]++;
        }
      }
    }

    if (EOS_LOGS_DEBUG) {
      for (size_t i = 0 ; i < 12 - 1 ; ++i) {
        eos_static_info("age: < %lu days : %lu", age_bins[i + 1] / 86400, age_map[i]);
      }
    }

    // if we have more versions than defined, clean the ones, which can be cleaned
    if ((int) versions.size() > max_versions) {
      for (size_t i = 0; i < (versions.size() - max_versions); ++i) {
        if (keep_set.count(versions[i])) {
          continue;
        }

        std::string deletionpath = path;
        deletionpath += "/";
        deletionpath += versions[i];
        success |= gOFS->_rem(deletionpath.c_str(), error, rootvid, (const char*) 0,
                              false, false);
      }
    }

    if (success == SFS_OK) {
      eos_info("dir=\"%s\" msg=\"purging ok\" old-versions=%d new-versions=%d",
               versiondir, versions.size(), max_versions);
    } else {
      eos_err("dir=\"%s\" msg=\"purging failed\" versions=%d", versiondir,
              versions.size());
    }
  } else {
    success = SFS_ERROR;
  }

  return success;
}
