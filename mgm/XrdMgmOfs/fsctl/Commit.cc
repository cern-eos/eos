// ----------------------------------------------------------------------
// File: Commit.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Macros.hh"
#include "mgm/tracker/ReplicationTracker.hh"
#include "mgm/XrdMgmOfs/fsctl/CommitHelper.hh"

#include <XrdOuc/XrdOucEnv.hh>
#include <openssl/sha.h>

//----------------------------------------------------------------------------
// Commit a replica
//----------------------------------------------------------------------------
int
XrdMgmOfs::Commit(const char* path,
                  const char* ininfo,
                  XrdOucEnv& env,
                  XrdOucErrInfo& error,
                  eos::common::VirtualIdentity& vid,
                  const XrdSecEntity* client)
{
  static const char* epname = "Commit";
  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  EXEC_TIMING_BEGIN("Commit");
  // Checksum string
  char binchecksum[SHA_DIGEST_LENGTH];
  // Process CGI parameters
  CommitHelper::cgi_t cgi;
  CommitHelper::grab_cgi(env, cgi);

  // Initialize logging
  if (cgi.count("logid")) {
    tlLogId.SetLogId(cgi["logid"].c_str(), error.getErrUser());
  }

  // OC parameters
  CommitHelper::param_t params;
  params["oc_n"] = 0;
  params["oc_max"] = 0;
  // Selected options
  CommitHelper::option_t option;
  CommitHelper::set_options(option, cgi);
  // Check 'path' parameter
  CommitHelper::path_t paths;
  paths["atomic"] = std::string("");

  if (cgi.count("path")) {
    paths["commit"] = cgi["path"];
  }

  // Extract all OC upload relevant parameters
  CommitHelper::init_oc(env, cgi, option, params);

  if (CommitHelper::is_reconstruction(option)) {
    // Remove checksum in case of a chunk reconstruction
    // (they have to be ignored)
    cgi["checksum"] = "";
  }

  if (cgi["checksum"].length()) {
    // Compute binary checksum
    CommitHelper::hex2bin_checksum(cgi["checksum"], binchecksum);
  }

  // Check all commit required parameters are defined
  if (CommitHelper::check_commit_params(cgi)) {
    // Convert the main CGI parameters into numbers
    unsigned long long size = std::stoull(cgi["size"]);
    unsigned long long fid = strtoull(cgi["fid"].c_str(), 0, 16);
    unsigned long fsid = std::stoul(cgi["fsid"]);
    unsigned long mtime = std::stoul(cgi["mtime"]);
    unsigned long mtimens = std::stoul(cgi["mtimensec"]);
    std::string emsg;
    CommitHelper::log_info(vid, tlLogId, cgi, option, params);
    int rc = CommitHelper::check_filesystem(vid, fsid, cgi, option,
                                            params, emsg);

    if (rc) {
      return Emsg(epname, error, rc, emsg.c_str(), "");
    }

    // Create a checksum buffer object
    eos::Buffer checksumbuffer;
    checksumbuffer.putData(binchecksum, SHA_DIGEST_LENGTH);
    // Attempt file meta data retrieval
    std::shared_ptr<eos::IFileMD> fmd;
    eos::IContainerMD::id_t cid = 0;
    std::string fmdname;
    {
      // Keep the lock order View => Namespace => Quota
      eos::common::RWMutexWriteLock nslock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
      errno = 0;

      try {
        fmd = gOFS->eosFileService->getFileMD(fid);
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"",
                         e.getErrno(), e.getMessage().str().c_str());
        emsg = "retc=";
        emsg += e.getErrno();
        emsg += " msg=";
        emsg += e.getMessage().str().c_str();
      }

      if (!fmd) {
        if (errno == ENOENT) {
          return Emsg(epname, error, ENOENT,
                      "commit filesize change - file is already removed [EIDRM]", "");
        }

        emsg.insert(0, "commit filesize change [EIO]");
        return Emsg(epname, error, errno, emsg.c_str(), cgi["path"].c_str());
      }

      unsigned long lid = fmd->getLayoutId();

      // Check if fsid and fid are ok
      if (fmd->getId() != fid) {
        eos_thread_notice("commit for fxid=%08llx != fmd_fxid=%08llx",
                          fid, fmd->getId());
        gOFS->MgmStats.Add("CommitFailedFid", 0, 0, 1);
        return Emsg(epname, error, EINVAL,
                    "commit filesize change - file id is wrong [EINVAL]",
                    cgi["path"].c_str());
      }

      // Check if file is already unlinked from the visible namespace
      if (!(cid = fmd->getContainerId())) {
        eos_thread_debug("commit for fxid=%08llx but file is disconnected "
                         "from any container", fmd->getId());
        gOFS->MgmStats.Add("CommitFailedUnlinked", 0, 0, 1);
        return Emsg(epname, error, EIDRM,
                    "commit filesize change - file is already removed [EIDRM]", "");
      }

      // Check if we have this replica in the unlink list or not linked list, if yes, the commit has to be suppressed
      if (option["fusex"] &&
          (fmd->hasUnlinkedLocation((unsigned int) fsid) ||
           (!fmd->hasLocation((unsigned int) fsid)))) {
        eos_thread_err("suppressing possible recovery replica for fxid=%08llx "
                       "on unlinked/not linked fsid=%llu - rejecting replica",
                       fmd->getId(), fsid);
        // This happens when a FUSEX recovery has been triggered.
        // To avoid to reattach replicas, we clean them up here
        return Emsg(epname, error, EBADE,
                    "commit replica - file size is wrong [EBADE] "
                    "- suppressing recovery replica", "");
      }

      // Check if commit comes from a replication procedure
      // and if the size/checksum is ok
      if (option["replication"]) {
        CommitHelper::remove_scheduler(fid);

        if (eos::common::LayoutId::GetLayoutType(lid) ==
            eos::common::LayoutId::kReplica) {
          // We check filesize and the checksum only for replica layouts
          eos_thread_debug("fmd_size=%llu, size=%lli", fmd->getSize(), size);

          // Validate size parameters
          if (!CommitHelper::validate_size(vid, fmd, fsid, size, option)) {
            return Emsg(epname, error, EBADE,
                        "commit replica - file size is wrong [EBADE]", "");
          }

          // Validate checksum parameters
          if (option["verifychecksum"] &&
              !CommitHelper::validate_checksum(vid, fmd, checksumbuffer,
                                               fsid, option)) {
            return Emsg(epname, error, EBADR,
                        "commit replica - file checksum is wrong [EBADR]", "");
          }
        }
      }

      if (option["verifysize"]) {
        // Check if a file size change was detected
        if (fmd->getSize() != size) {
          eos_thread_err("commit for fxid=%08llx gave a file size change after "
                         "verification on fsid=%llu", fmd->getId(), fsid);
        }
      }

      if (option["verifychecksum"]) {
        CommitHelper::log_verifychecksum(vid, fmd, checksumbuffer, fsid,
                                         cgi, option);
      }

      if (!CommitHelper::handle_location(vid, cid, fmd, fsid, size,
                                         cgi, option)) {
        return Emsg(epname, error, EIDRM,
                    "commit file, parent container removed [EIDRM]", "");
      }

      // Advance oc upload parameters if concerned
      CommitHelper::handle_occhunk(vid, fmd, option, params);
      // Set checksum if concerned
      CommitHelper::handle_checksum(vid, fmd, option, checksumbuffer);
      fmdname = fmd->getName();
      paths["atomic"].Init(fmdname.c_str());
      paths["atomic"].DecodeAtomicPath(option["versioning"]);
      option["atomic"] = (paths["atomic"].GetName() != fmdname);

      if (option["commitverify"]) {
        // disable atomic and versioning functionality for commits originated by "verify --commitxyz"
        option["atomic"] = false;
        option["versioning"] = false;
      }

      if (option["update"] && mtime) {
        // Update the modification time only if the file contents changed and
        // mtime != 0
        // - FUSE clients will commit mtime=0 to indicate they call utimes anyway
        // - OC clients set the mtime during a commit
        if (!option["atomic"] || option["occhunk"]) {
          eos::IFileMD::ctime_t mt;
          mt.tv_sec = mtime;
          mt.tv_nsec = mtimens;
          fmd->setMTime(mt);
        }
      }

      eos_thread_debug("commit: setting size to %llu", fmd->getSize());

      if (!CommitHelper::commit_fmd(vid, cid, fmd, size, option, emsg)) {
        return Emsg(epname, error, errno, "commit filesize change", emsg.c_str());
      }

      if (option["update"]) {
        // broadcast file md
        gOFS->FuseXCastRefresh(fmd->getIdentifier(),
                               eos::ContainerIdentifier(fmd->getContainerId()));
      }
    }
    {
      eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
      // Path of a previous version existing before an atomic/versioning upload
      std::string delete_path = "";
      eos_thread_info("commitsize=%d n1=%s n2=%s occhunk=%d ocdone=%d",
                      option["commitsize"],
                      fmdname.c_str(), paths["atomic"].GetName(),
                      option["occhunk"], option["ocdone"]);

      // -----------------------------------------------------------------------
      // We are asked to commit the size and this commit changes the current
      // atomic name to the final name and we are not an OC upload
      // -----------------------------------------------------------------------
      if ((option["commitsize"]) && (fmdname != paths["atomic"].GetName()) &&
          (!option["occhunk"] || option["ocdone"])) {
        eos_thread_info("commit: de-atomize file %s => %s",
                        fmdname.c_str(), paths["atomic"].GetName());
        unsigned long long vfid =
          CommitHelper::get_version_fid(vid, fid, paths, option);

        // Check for versioning request
        if (option["versioning"]) {
          eos_static_info("checked %s%s vfxid=%08llx",
                          paths["versiondir"].GetParentPath(),
                          paths["atomic"].GetPath(), vfid);

          // We purged the versions before during open, so we just simulate
          // a new one and do the final rename in a transaction
          if (vfid) {
            XrdOucString versionedname = "";

            if (gOFS->Version(vfid, error, rootvid, 0xffff, &versionedname, true)) {
              eos_static_crit("versioning failed %s/%s vfxid=%08lxx",
                              paths["versiondir"].GetParentPath(),
                              paths["atomic"].GetPath(), vfid);
              const char* errmsg = "commit - versioning failed";
              return Emsg(epname, error, EREMCHG, errmsg, paths["atomic"].GetName());
            } else {
              paths["version"].Init(versionedname.c_str());
            }
          }
        }

        CommitHelper::handle_versioning(vid, fid, paths,
                                        option, delete_path);
      }

      gOFS->mReplicationTracker->Commit(fmd);

      // -----------------------------------------------------------------------
      // If there was a previous target file we have to delete the renamed
      // atomic left-over
      // -----------------------------------------------------------------------
      if (delete_path.length()) {
        delete_path.insert(0, paths["versiondir"].GetParentPath());
        eos_thread_info("msg=\"delete path\" path=%s", delete_path.c_str());

        if (gOFS->_rem(delete_path.c_str(), error, rootvid, "")) {
          eos_thread_err("msg=\"failed to remove atomic left-over\" path=%s",
                         delete_path.c_str());
        }
      }

      if (option["abort"]) {
        return Emsg(epname, error, EREMCHG, "commit replica - overlapping "
                    "atomic upload - discarding atomic upload [EREMCHG]", "");
      }
    }
  } else {
    int envlen = 0;
    eos_thread_err("commit message does not contain all meta information: %s",
                   env.Env(envlen));
    gOFS->MgmStats.Add("CommitFailedParameters", 0, 0, 1);
    const char* errtarget = "unknown";
    const char* errmsg =
      "commit filesize change - size, fid, fsid, mtime, path not complete";

    if (cgi.count("path")) {
      errmsg = "commit filesize change - size, fid, fsid, mtime not complete";
      errtarget = cgi["path"].c_str();
    }

    return Emsg(epname, error, EINVAL, errmsg, errtarget);
  }

  gOFS->MgmStats.Add("Commit", 0, 0, 1);
  const char* ok = "OK";
  error.setErrInfo(strlen(ok) + 1, ok);
  EXEC_TIMING_END("Commit");
  return SFS_DATA;
}
