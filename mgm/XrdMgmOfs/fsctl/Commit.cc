// ----------------------------------------------------------------------
// File: Commit.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/********************A***************************************************
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

{

  static const char* epname = "commit";
  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  EXEC_TIMING_BEGIN("Commit");

  // checksums
  char binchecksum[SHA_DIGEST_LENGTH];

  XrdOucString oc_uuid = "";

  // cgi parameters
  CommitHelper::cgi_t cgi;
  CommitHelper::grab_cgi(env, cgi);

  // initialize logging
  if (cgi.count("logid"))
  {
    ThreadLogId.SetLogId(cgi["logid"].c_str(), tident);
  }

  // OC parameters
  CommitHelper::param_t params;
  params["oc_n"] = 0;
  params["oc_max"] = 0;

  // selected options
  CommitHelper::option_t option;
  CommitHelper::set_options(option, cgi);

  // has parameters
  CommitHelper::path_t paths;
  paths["atomic"] = std::string("");

  if (cgi.count("path"))
  {
    paths["commit"] = cgi["path"];
  }

  // extract all OC upload relevant parameters
  // populates also cgi["ocuuid"]
  CommitHelper::init_oc(env, cgi, option, params);

  if (CommitHelper::is_reconstruction(option))
  {
    // remove checksum in case of a chunk reconstruction, they have to be ignored
    cgi["checksum"] = "";
  }

  if (cgi["checksum"].length())
  {
    // compute binary checksum
    CommitHelper::hex2bin_checksum(cgi["checksum"], binchecksum);
  }

  // check that all required parameters for a commit are defined
  if (CommitHelper::check_commit_params(cgi))
  {
    // convert the main cgi parameters into numbers
    unsigned long long size = std::stoull(cgi["size"]);
    unsigned long long fid = strtoull(cgi["fid"].c_str(), 0, 16);
    unsigned long fsid = std::stoul(cgi["fsid"]);
    unsigned long mtime = std::stoul(cgi["mtime"]);
    unsigned long mtimens = std::stoul(cgi["mtimensec"]);
    std::string emsg;

    if ((errno = CommitHelper::check_filesystem(vid, ThreadLogId, fsid, cgi, option,
         params, emsg))) {
      return Emsg(epname, error, errno, emsg.c_str(), "");
    }

    // create a checksum buffer object
    eos::Buffer checksumbuffer;
    checksumbuffer.putData(binchecksum, SHA_DIGEST_LENGTH);
    CommitHelper::log_info(vid, ThreadLogId, cgi, option, params);
    // get the file meta data if exists
    std::shared_ptr<eos::IFileMD> fmd;
    eos::IContainerMD::id_t cid = 0;
    std::string fmdname;
    {
      // Keep the lock order View=>Namespace=>Quota
      eos::common::RWMutexWriteLock nslock(gOFS->eosViewRWMutex);
      XrdOucString emsg = "";

      try {
        fmd = gOFS->eosFileService->getFileMD(fid);
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                         e.getMessage().str().c_str());
        emsg = "retc=";
        emsg += e.getErrno();
        emsg += " msg=";
        emsg += e.getMessage().str().c_str();
      }

      if (!fmd) {
        // uups, no such file anymore
        if (errno == ENOENT) {
          return Emsg(epname, error, ENOENT,
                      "commit filesize change - file is already removed [EIDRM]", "");
        } else {
          emsg.insert("commit filesize change [EIO] ", 0);
          return Emsg(epname, error, errno, emsg.c_str(), cgi["path"].c_str());
        }
      } else {
        unsigned long lid = fmd->getLayoutId();

        // check if fsid and fid are ok
        if (fmd->getId() != fid) {
          eos_thread_notice("commit for fid=%lu but fid=%lu", fmd->getId(), fid);
          gOFS->MgmStats.Add("CommitFailedFid", 0, 0, 1);
          return Emsg(epname, error, EINVAL,
                      "commit filesize change - file id is wrong [EINVAL]", cgi["path"].c_str());
        }

        // check if this file is already unlinked from the visible namespace
        if (!(cid = fmd->getContainerId())) {
          eos_thread_debug("commit for fid=%lu but file is disconnected from any container",
                           fmd->getId());
          gOFS->MgmStats.Add("CommitFailedUnlinked", 0, 0, 1);
          return Emsg(epname, error, EIDRM,
                      "commit filesize change - file is already removed [EIDRM]", "");
        }

        // check if this commit comes from a transfer and if the size/checksum is ok
        if (option["replication"]) {
          CommitHelper::remove_scheduler(fid);

          // check if we have this replica in the unlink list
          if (option["fusex"] && fmd->hasUnlinkedLocation((unsigned short) fsid)) {
            eos_thread_err("suppressing possible recovery replica for fid=%lu on unlinked fsid=%llu- rejecting replica",
                           fmd->getId(), fsid);
            // this happens when a FUSEX recovery has been triggered, to avoid to reattach replicas,
            // we clean them up here
            return Emsg(epname, error, EBADE,
                        "commit replica - file size is wrong [EBADE] - suppressing recovery replica",
                        "");
          }

          if (eos::common::LayoutId::GetLayoutType(lid) ==
              eos::common::LayoutId::kReplica) {
            // we check filesize and the checksum only for replica layouts
            eos_thread_debug("fmd size=%lli, size=%lli", fmd->getSize(), size);

            // validate the correct size parameters
            if (!CommitHelper::validate_size(vid, ThreadLogId, fmd, fsid, size)) {
              return Emsg(epname, error, EBADE, "commit replica - file size is wrong [EBADE]",
                          "");
            }

            // validate the correct checksum parameters
            if (option["verifychecksum"] &&
                !CommitHelper::validate_checksum(vid, ThreadLogId, fmd, checksumbuffer,
                                                 fsid)) {
              return Emsg(epname, error, EBADR,
                          "commit replica - file checksum is wrong [EBADR]", "");
            }
          }
        }

        if (option["verifysize"]) {
          // check if we saw a file size change or checksum change
          if (fmd->getSize() != size) {
            eos_thread_err("commit for fid=%lu gave a file size change after "
                           "verification on fsid=%llu", fmd->getId(), fsid);
          }
        }

        if (option["verifychecksum"]) {
          CommitHelper::log_verifychecksum(vid, ThreadLogId, fmd, checksumbuffer, fsid,
                                           cgi, option);
        }

        if (!CommitHelper::handle_location(vid, ThreadLogId, cid, fmd, fsid, size,
                                           cgi, option)) {
          return Emsg(epname, error, EIDRM,
                      "commit file, parent contrainer removed [EIDRM]", "");
        }

        fmdname = fmd->getName();
        // advance oc upload parameters if concerned
        CommitHelper::handle_occhunk(vid, ThreadLogId, fmd, option, params);
        // set checksum if concerned
        CommitHelper::handle_checksum(vid, ThreadLogId, fmd, option, checksumbuffer);
        paths["atomic"].Init(fmd->getName().c_str());
        paths["atomic"].DecodeAtomicPath(option["versioning"]);
        option["atomic"] = (paths["atomic"].GetName() != fmd->getName());

        if (option["update"] && mtime) {
          // Update the modification time only if the file contents changed and
          // mtime != 0 (FUSE clients will commit mtime=0 to indicated that they
          // call utimes anyway
          // OC clients set the mtime during a commit!
          if (!option["atomic"] || option["occhunk"]) {
            eos::IFileMD::ctime_t mt;
            mt.tv_sec = mtime;
            mt.tv_nsec = mtimens;
            fmd->setMTime(mt);
          }
        }

        eos_thread_debug("commit: setting size to %llu", fmd->getSize());
        std::string errmsg;

        if (!CommitHelper::commit_fmd(vid, ThreadLogId, cid, fmd, option, errmsg)) {
          return Emsg(epname, error, errno, "commit filesize change",
                      errmsg.c_str());
        }
      }
    }
    {
      eos::common::Mapping::VirtualIdentity rootvid;
      eos::common::Mapping::Root(rootvid);
      // Path of a previous version existing before an atomic/versioning upload
      std::string delete_path = "";
      eos_thread_info("commitsize=%d n1=%s n2=%s occhunk=%d ocdone=%d",
                      option["commitsize"],
                      fmdname.c_str(), paths["atomic"].GetName(),
                      option["occhunk"], option["ocdone"]);

      // -----------------------------------------------------------------------
      // we are asked to commit the size and this commit changes the current
      // atomic name to the final name and we are not an OC upload
      // -----------------------------------------------------------------------
      if ((option["commitsize"]) && (fmdname != paths["atomic"].GetName()) &&
          (!option["occhunk"] || option["ocdone"])) {
        eos_thread_info("commit: de-atomize file %s => %s", fmdname.c_str(),
                        paths["atomic"].GetName());
        XrdOucString versionedname = "";
        unsigned long long vfid = CommitHelper::get_version_fid(vid, ThreadLogId, fmd,
                                  paths, option);

        // check if we want versioning
        if (option["versioning"]) {
          eos_static_info("checked  %s%s vfid=%llu",
                          paths["versiondir"].GetParentPath(),
                          paths["atomic"].GetPath(),
                          vfid);

          // We purged the versions before during open, so we just simulate a new
          // one and do the final rename in a transaction
          if (vfid) {
            gOFS->Version(vfid, error, rootvid, 0xffff, &versionedname, true);
            paths["version"].Init(versionedname.c_str());
          }
        }

        CommitHelper::handle_versioning(vid, ThreadLogId, fid, paths, option,
                                        delete_path);
      }

      // -----------------------------------------------------------------------
      // If there was a previous target file we have to delete the renamed
      // atomic left-over
      // -----------------------------------------------------------------------
      if (delete_path.length()) {
        delete_path.insert(0, paths["versiondir"].GetParentPath());
        eos_thread_info("msg=\"delete path\" %s", delete_path.c_str());

        if (gOFS->_rem(delete_path.c_str(), error, rootvid, "")) {
          eos_thread_err("msg=\"failed to remove atomic left-over\" path=%s",
                         delete_path.c_str());
        }
      }

      if (option["abort"]) {
        return Emsg(epname, error, EREMCHG, "commit replica - overlapping "
                    "atomic upload [EREMCHG] - discarding atomic upload", "");
      }
    }
  } else
  {
    int envlen = 0;
    eos_thread_err("commit message does not contain all meta information: %s",
                   env.Env(envlen));
    gOFS->MgmStats.Add("CommitFailedParameters", 0, 0, 1);

    if (cgi["path"].length()) {
      return Emsg(epname, error, EINVAL,
                  "commit filesize change - size,fid,fsid,mtime not complete",
                  cgi["path"].c_str());
    } else {
      return Emsg(epname, error, EINVAL,
                  "commit filesize change - size,fid,fsid,mtime,path not complete", "unknown");
    }
  }

  gOFS->MgmStats.Add("Commit", 0, 0, 1);
  const char* ok = "OK";
  error.setErrInfo(strlen(ok) + 1, ok);
  EXEC_TIMING_END("Commit");
  return SFS_DATA;
}
