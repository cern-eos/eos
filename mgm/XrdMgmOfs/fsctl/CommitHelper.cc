// ----------------------------------------------------------------------
// File: CommitHelper.cc
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

#include "mgm/XrdMgmOfs/fsctl/CommitHelper.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/FsView.hh"
#include "mgm/Stat.hh"
#include "common/http/OwnCloud.hh"
#include "common/LayoutId.hh"
#include "namespace/interface/IQuota.hh"
#include "namespace/interface/IView.hh"

EOSMGMNAMESPACE_BEGIN

// Initialize static variables
thread_local eos::common::LogId CommitHelper::tlLogId;

//------------------------------------------------------------------------------
// convert hex to binary checksum
//------------------------------------------------------------------------------
void
CommitHelper::hex2bin_checksum(std::string& checksum, char* binchecksum)
{
  // hex2binary conversion
  memset(binchecksum, 0, SHA_DIGEST_LENGTH);

  for (unsigned int i = 0; i < checksum.length(); i += 2) {
    char hex[3];
    hex[0] = checksum.at(i);
    hex[1] = checksum.at(i + 1);
    hex[2] = 0;
    binchecksum[i / 2] = strtol(hex, 0, 16);
  }
}

//------------------------------------------------------------------------------
// check if the filesystem to commit to is writable state
//------------------------------------------------------------------------------

int
CommitHelper::check_filesystem(eos::common::VirtualIdentity& vid,
                               unsigned long fsid,
                               CommitHelper::cgi_t& cgi,
                               CommitHelper::option_t& option,
                               CommitHelper::param_t& params,
                               std::string& emsg)
{
  // Check that the file system is still allowed to accept replica's
  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
  eos::mgm::FileSystem* fs = FsView::gFsView.mIdView.lookupByID(fsid);

  if ((!fs) || (fs->GetConfigStatus() < eos::common::ConfigStatus::kDrain)) {
    eos_thread_err("msg=\"commit suppressed\" configstatus=%s subcmd=commit "
                   "path=%s size=%s fxid=%s fsid=%s dropfsid=%s checksum=%s"
                   " mtime=%s mtime.nsec=%s oc-chunk=%d oc-n=%d oc-max=%d "
                   "oc-uuid=%s",
                   (fs ? eos::common::FileSystem::GetConfigStatusAsString(
                      fs->GetConfigStatus())
                    : "deleted"),
                   cgi["path"].c_str(),
                   cgi["size"].c_str(),
                   cgi["fid"].c_str(),
                   cgi["fsid"].c_str(),
                   cgi["dropfsid"].c_str(),
                   cgi["checksum"].c_str(),
                   cgi["mtime"].c_str(),
                   cgi["mtimensec"].c_str(),
                   option["occhunk"], params["oc_n"],
                   params["oc_max"], cgi["oc_uuid"].c_str());
    emsg = "commit file metadata - "
           "filesystem is in non-operational state [EIO]";
    return EIO;
  }

  return 0;
}

//------------------------------------------------------------------------------
// extract all CGI KV pairs
//------------------------------------------------------------------------------

void
CommitHelper::grab_cgi(XrdOucEnv& env, CommitHelper::cgi_t& cgi)
{
  if (env.Get("mgm.size")) {
    cgi["size"] = env.Get("mgm.size");
  }

  if (env.Get("mgm.path")) {
    cgi["path"] = env.Get("mgm.path");
  }

  if (env.Get("mgm.fid")) {
    cgi["fid"] = env.Get("mgm.fid");
  }

  if (env.Get("mgm.add.fsid")) {
    cgi["fsid"] = env.Get("mgm.add.fsid");
  }

  if (env.Get("mgm.mtime")) {
    cgi["mtime"] = env.Get("mgm.mtime");
  }

  if (env.Get("mgm.mtime_ns")) {
    cgi["mtimensec"] = env.Get("mgm.mtime_ns");
  }

  if (env.Get("mgm.logid")) {
    cgi["logid"] = env.Get("mgm.logid");
  }

  if (env.Get("mgm.verify.checksum")) {
    cgi["verifychecksum"] = env.Get("mgm.verify.checksum");
  }

  if (env.Get("mgm.commit.checksum")) {
    cgi["commitchecksum"] = env.Get("mgm.commit.checksum");
  }

  if (env.Get("mgm.commit.verify")) {
    cgi["commitverify"] = env.Get("mgm.commit.verify");
  }

  if (env.Get("mgm.verify.size")) {
    cgi["verifysize"] = env.Get("mgm.verify.size");
  }

  if (env.Get("mgm.commit.size")) {
    cgi["commitsize"] = env.Get("mgm.commit.size");
  }

  if (env.Get("mgm.drop.fsid")) {
    cgi["dropfsid"] = env.Get("mgm.drop.fsid");
  }

  if (env.Get("mgm.replication")) {
    cgi["replication"] = env.Get("mgm.replication");
  }

  if (env.Get("mgm.reconstruction")) {
    cgi["reconstruction"] = env.Get("mgm.reconstruction");
  }

  if (env.Get("mgm.modified")) {
    cgi["ismodified"] = env.Get("mgm.modified");
  }

  if (env.Get("mgm.fusex")) {
    cgi["fusex"] = env.Get("mgm.fusex");
  }

  if (env.Get("mgm.checksum")) {
    cgi["checksum"] = env.Get("mgm.checksum");
  }
}

//------------------------------------------------------------------------------
// log commit information
//------------------------------------------------------------------------------

void
CommitHelper::log_info(eos::common::VirtualIdentity& vid,
                       const eos::common::LogId& thread_logid,
                       CommitHelper::cgi_t& cgi,
                       CommitHelper::option_t& option,
                       CommitHelper::param_t& params)
{
  // Set the thread local variable that will be used when calling eos_thread_*
  tlLogId = thread_logid;

  if (cgi["checksum"].length()) {
    eos_thread_info("subcmd=commit path=%s size=%s fxid=%s fsid=%s dropfsid=%s "
                    "checksum=%s mtime=%s mtime.nsec=%s oc-chunk=%d oc-n=%d "
                    "oc-max=%d oc-uuid=%s",
                    cgi["path"].c_str(),
                    cgi["size"].c_str(),
                    cgi["fid"].c_str(),
                    cgi["fsid"].c_str(),
                    cgi["dropfsid"].c_str(),
                    cgi["checksum"].c_str(),
                    cgi["mtime"].c_str(),
                    cgi["mtimensec"].c_str(),
                    option["occhunk"],
                    params["oc_n"],
                    params["oc_max"],
                    cgi["ocuuid"].c_str());
  } else {
    eos_thread_info("subcmd=commit path=%s size=%s fxid=%s fsid=%s dropfsid=%s "
                    "mtime=%s mtime.nsec=%s oc-chunk=%d oc-n=%d "
                    "oc-max=%d oc-uuid=%s",
                    cgi["path"].c_str(),
                    cgi["size"].c_str(),
                    cgi["fid"].c_str(),
                    cgi["fsid"].c_str(),
                    cgi["dropfsid"].c_str(),
                    cgi["mtime"].c_str(),
                    cgi["mtimensec"].c_str(),
                    option["occhunk"],
                    params["oc_n"],
                    params["oc_max"],
                    cgi["ocuuid"].c_str());
  }
}

//------------------------------------------------------------------------------
// extract options from CGI
//------------------------------------------------------------------------------

void
CommitHelper::set_options(CommitHelper::option_t& option,
                          CommitHelper::cgi_t& cgi)
{
  option["verifychecksum"] = (cgi["verifychecksum"] == "1");
  option["commitchecksum"] = (cgi["commitchecksum"] == "1");
  option["commitsize"] = (cgi["commitsize"] == "1");
  option["commitverify"] = (cgi["commitverify"] == "1");
  option["verifysize"] = (cgi["verifysize"] == "1");
  option["replication"] = (cgi["replication"] == "1");
  option["reconstruction"] = (cgi["reconstruction"] == "1");
  option["modified"] = (cgi["ismodified"] == "1");
  option["fusex"] = (cgi["fusex"] == "1");
  option["abort"] = false; // indicate to abort a commit
  option["versioning"] = false; // indicate versioning
  option["atomic"] = false; // indicate an atomic upload
  option["occhunk"] = false; // indicate owncloud chunking
  option["ocdone"] =
    false; // indicate when the last chunk of a chunked OC upload has been committed
}

//------------------------------------------------------------------------------
// initialize OC commit parameters
//------------------------------------------------------------------------------

void
CommitHelper::init_oc(XrdOucEnv& env, CommitHelper::cgi_t& cgi,
                      CommitHelper::option_t& option,
                      CommitHelper::param_t& params)
{
  int envlen;
  int oc_n = 0;
  int oc_max = 0;
  XrdOucString oc_uuid = "";
  option["occhunk"] = eos::common::OwnCloud::GetChunkInfo(env.Env(envlen), oc_n,
                      oc_max, oc_uuid);
  cgi["ocuuid"] = oc_uuid.c_str();
  params["oc_n"] = oc_n;
  params["oc_max"] = oc_max;
}

//------------------------------------------------------------------------------
// check for a reconstruction commit
//------------------------------------------------------------------------------

bool
CommitHelper::is_reconstruction(CommitHelper::option_t& option)
{
  if (option["reconstruction"]) {
    option["verifysize"] = false;
    option["verifychecksum"] = false;
    option["commitsize"] = false;
    option["commitchecksum"] = false;
    option["commitverify"] = false;
    option["replication"] = false;
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// check proper commit parameter
//------------------------------------------------------------------------------

bool
CommitHelper::check_commit_params(CommitHelper::cgi_t& cgi)
{
  if (cgi["size"].length() && cgi["fid"].length() && cgi["path"].length() &&
      cgi["fsid"].length() && cgi["mtime"].length() && cgi["mtimensec"].length()) {
    return true;
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// Remove fid from the trakced maps
//------------------------------------------------------------------------------
void
CommitHelper::remove_scheduler(unsigned long long fid)
{
  gOFS->mDrainTracker.RemoveEntry(fid);
  gOFS->mBalancingTracker.RemoveEntry(fid);
}

//------------------------------------------------------------------------------
// validate the given size information
//------------------------------------------------------------------------------

bool
CommitHelper::validate_size(eos::common::VirtualIdentity& vid,
                            std::shared_ptr<eos::IFileMD> fmd,
                            unsigned long fsid,
                            unsigned long long size,
                            CommitHelper::option_t& option)
{
  if (fmd->getSize() != size) {
    eos_thread_err("replication for fxid=%08llx resulted in a different file "
                   "size on fsid=%llu - %llu vs %llu - rejecting replica", fmd->getId(), fsid,
                   fmd->getSize(), size);
    gOFS->MgmStats.Add("ReplicaFailedSize", 0, 0, 1);

    // -----------------------------------------------------------
    // if we come via FUSE, we have to remove this replica
    // -----------------------------------------------------------
    if (option["fusex"]) {
      if (fmd->hasLocation((unsigned short) fsid)) {
        fmd->unlinkLocation((unsigned short) fsid);
        fmd->removeLocation((unsigned short) fsid);

        try {
          gOFS->eosView->updateFileStore(fmd.get());
          // this call is not needed, since it is just a new replica location
          // gOFS->FuseXCastFile(fmd->getIdentifier());
        } catch (eos::MDException& e) {
          errno = e.getErrno();
          std::string errmsg = e.getMessage().str();
          eos_thread_crit("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                          e.getErrno(), e.getMessage().str().c_str());
        }
      }
    }

    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// validate the given checksum
//------------------------------------------------------------------------------

bool
CommitHelper::validate_checksum(eos::common::VirtualIdentity& vid,
                                std::shared_ptr<eos::IFileMD> fmd,
                                eos::Buffer& checksumbuffer,
                                unsigned long long fsid,
                                CommitHelper::option_t& option)
{
  bool cxError = false;
  size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId());

  for (size_t i = 0; i < cxlen; i++) {
    if (fmd->getChecksum().getDataPadded(i) != checksumbuffer.getDataPadded(i)) {
      cxError = true;
    }
  }

  if (cxError) {
    eos_thread_err("replication for fxid=%08llx resulted in a different checksum "
                   "on fsid=%llu - rejecting replica", fmd->getId(), fsid);
    gOFS->MgmStats.Add("ReplicaFailedChecksum", 0, 0, 1);

    // -----------------------------------------------------------
    // if we don't come via FUSEX, we have to remove this replica
    // -----------------------------------------------------------
    if (!option["fusex"]) {
      if (fmd->hasLocation((unsigned short) fsid)) {
        fmd->unlinkLocation((unsigned short) fsid);
        fmd->removeLocation((unsigned short) fsid);
        eos_thread_err("replication for fxid=%08llx resulted in a different checksum "
                       "on fsid=%llu - dropping replica", fmd->getId(), fsid);

        try {
          gOFS->eosView->updateFileStore(fmd.get());
        } catch (eos::MDException& e) {
          errno = e.getErrno();
          std::string errmsg = e.getMessage().str();
          eos_thread_crit("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                          e.getErrno(), e.getMessage().str().c_str());
        }
      }
    }

    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// log checksum verification
//------------------------------------------------------------------------------

void
CommitHelper::log_verifychecksum(eos::common::VirtualIdentity& vid,
                                 std::shared_ptr<eos::IFileMD>fmd,
                                 eos::Buffer& checksumbuffer,
                                 unsigned long fsid,
                                 CommitHelper::cgi_t& cgi,
                                 CommitHelper::option_t& option)
{
  if (cgi["checksum"].length()) {
    if (option["verifychecksum"]) {
      bool cxError = false;
      size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId());

      for (size_t i = 0; i < cxlen; i++) {
        if (fmd->getChecksum().getDataPadded(i) != checksumbuffer.getDataPadded(i)) {
          cxError = true;
        }
      }

      if (cxError) {
        eos_thread_err("commit for fxid=%08llx gave a different checksum after "
                       "verification on fsid=%llu", fmd->getId(), fsid);
      }
    }
  }
}

//------------------------------------------------------------------------------
// handle replica location updates
//------------------------------------------------------------------------------

bool
CommitHelper::handle_location(eos::common::VirtualIdentity& vid,
                              unsigned long cid,
                              std::shared_ptr<eos::IFileMD> fmd,
                              unsigned long fsid,
                              unsigned long long size,
                              CommitHelper::cgi_t& cgi,
                              CommitHelper::option_t& option)
{
  // For changing the modification time we have to figure out if we
  // just attach a new replica or if we have a change of the contents
  std::shared_ptr<eos::IContainerMD> dir;

  try {
    dir = gOFS->eosDirectoryService->getContainerMD(cid);
  } catch (eos::MDException& e) {
    eos_thread_err("parent_id=%llu not found", cid);
    gOFS->MgmStats.Add("CommitFailedUnlinked", 0, 0, 1);
    return false;
  }

  eos::IQuotaNode* ns_quota = nullptr;

  try {
    ns_quota = gOFS->eosView->getQuotaNode(dir.get());
  } catch (const eos::MDException& e) {
    // empty
  }

  // Free previous quota
  if (ns_quota) {
    ns_quota->removeFile(fmd.get());
  }

  fmd->addLocation(fsid);

  // If fsid is in the deletion list, we try to remove it if there
  // is something in the deletion list
  if (fmd->getNumUnlinkedLocation()) {
    fmd->removeLocation(fsid);
  }

  if (cgi["dropfsid"].length()) {
    unsigned long dropfsid = std::stoul(cgi["dropfsid"]);
    eos_thread_debug("commit: dropping replica on fs %lu", dropfsid);
    fmd->unlinkLocation((unsigned short) dropfsid);
  }

  option["update"] = false;

  if (option["commitsize"]) {
    if ((fmd->getSize() != size) || option["modified"]) {
      eos_thread_debug("size difference forces mtime %lld %lld or "
                       "ismodified=%d", fmd->getSize(), size, option.count("modified"));
      option["update"] = true;
    }

    fmd->setSize(size);
  }

  if (ns_quota) {
    ns_quota->addFile(fmd.get());
  }

  return true;
}

//------------------------------------------------------------------------------
// handle OC chunk uploads
//------------------------------------------------------------------------------

void
CommitHelper::handle_occhunk(eos::common::VirtualIdentity& vid,
                             std::shared_ptr<eos::IFileMD>& fmd,
                             CommitHelper::option_t& option,
                             CommitHelper::param_t& params)
{
  if (option["occhunk"] && option["commitsize"]) {
    // store the index in flags;
    fmd->setFlags(params["oc_n"] + 1);
    eos_thread_info("subcmd=commit max-chunks=%d committed-chunks=%d",
                    params["oc_max"],
                    fmd->getFlags());

    // The last chunk terminates all
    if (params["oc_max"] == (params["oc_n"] + 1)) {
      // we are done with chunked upload, remove the flags counter
      fmd->setFlags((S_IRWXU | S_IRWXG | S_IRWXO));
      option["ocdone"] = true;
    }
  }
}

//------------------------------------------------------------------------------
// handle new checksums
//------------------------------------------------------------------------------

void
CommitHelper::handle_checksum(eos::common::VirtualIdentity& vid,
                              std::shared_ptr<eos::IFileMD>fmd,
                              CommitHelper::option_t& option,
                              eos::Buffer& checksumbuffer)
{
  if (option["commitchecksum"]) {
    if (!option["update"]) {
      for (int i = 0; i < SHA_DIGEST_LENGTH; i++) {
        if (fmd->getChecksum().getDataPadded(i) != checksumbuffer.getDataPadded(i)) {
          eos_thread_debug("checksum difference forces mtime");
          option["update"] = true;
        }
      }
    }

    fmd->setChecksum(checksumbuffer);
  }
}

//------------------------------------------------------------------------------
// commit new file meta data
//------------------------------------------------------------------------------

bool
CommitHelper::commit_fmd(eos::common::VirtualIdentity& vid,
                         unsigned long cid,
                         std::shared_ptr<eos::IFileMD>fmd,
                         unsigned long long replica_size,
                         CommitHelper::option_t& option,
                         std::string& errmsg)
{
  std::shared_ptr<eos::IContainerMD> cmd;

  try {
    // check for a temporary Etag and remove it
    std::string tmpEtag = "sys.tmp.etag";

    if (fmd->hasAttribute(tmpEtag) && (!option["atomic"] || option["occhunk"])
        && (option["commitsize"] || option["commitchecksum"])) {
      // Drop the tmp etag only if this was not a creation of a 0-size file
      if ((fmd->getSize() != 0) || (replica_size != 0)) {
        fmd->removeAttribute(tmpEtag);
      }
    }

    gOFS->eosView->updateFileStore(fmd.get());
    cmd = gOFS->eosDirectoryService->getContainerMD(cid);

    if (option["update"]) {
      if (cmd->hasAttribute(tmpEtag)) {
        // Drop the tmp etag only if this was not a creation of a 0-size file
        if ((fmd->getSize() != 0) || (replica_size != 0)) {
          cmd->removeAttribute(tmpEtag);
        }
      }

      // update parent mtime
      cmd->setMTimeNow();
      gOFS->eosView->updateContainerStore(cmd.get());

      // Broadcast to the fusex network only if the change has been
      // triggered outside the fusex client network e.g. xrdcp etc.
      if (!option["fusex"]) {
        gOFS->FuseXCastContainer(cmd->getIdentifier());
        gOFS->FuseXCastRefresh(cmd->getIdentifier(), cmd->getParentIdentifier());
      }

      cmd->notifyMTimeChange(gOFS->eosDirectoryService);
    }

    return true;
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    errmsg = e.getMessage().str();
    eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                     e.getErrno(), e.getMessage().str().c_str());
    gOFS->MgmStats.Add("CommitFailedNamespace", 0, 0, 1);
    return false;
  }
}

//------------------------------------------------------------------------------
// identify latest version file id
//------------------------------------------------------------------------------

unsigned long long
CommitHelper::get_version_fid(eos::common::VirtualIdentity& vid,
                              unsigned long long fid,
                              CommitHelper::path_t& paths,
                              CommitHelper::option_t& option)
{
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  std::shared_ptr<eos::IFileMD> versionfmd;

  try {
    auto fmd = gOFS->eosFileService->getFileMD(fid);
    paths["versiondir"] = gOFS->eosView->getUri(fmd.get());

    if (option["versioning"]) {
      versionfmd = gOFS->eosView->getFile(std::string(
                                            paths["versiondir"].GetParentPath())
                                          + std::string(paths["atomic"].GetPath()));
      return versionfmd->getId();
    }
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                     e.getErrno(), e.getMessage().str().c_str());
  }

  return 0;
}

//------------------------------------------------------------------------------
// handle creation of a new version during commit
//------------------------------------------------------------------------------

void
CommitHelper::handle_versioning(eos::common::VirtualIdentity& vid,
                                unsigned long fid,
                                CommitHelper::path_t& paths,
                                CommitHelper::option_t& option,
                                std::string& delete_path)
{
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);

  // We have to de-atomize the fmd name here e.g. make the temporary
  // atomic name a persistent name
  try {
    std::shared_ptr<eos::IFileMD> fmd;
    std::shared_ptr<eos::IContainerMD> dir;
    std::shared_ptr<eos::IContainerMD> versiondir;
    std::shared_ptr<eos::IFileMD> versionfmd;
    dir = gOFS->eosView->getContainer(paths["versiondir"].GetParentPath());
    fmd = gOFS->eosFileService->getFileMD(fid);

    if (fmd->getName() == paths["atomic"].GetName()) {
      // defere version handling for an overlapping secondary commit due to lock-release during commit
      return;
    }

    if (option["versioning"] && (std::string(paths["version"].GetPath()) != "/")) {
      try {
        versiondir = gOFS->eosView->getContainer(paths["version"].GetParentPath());
        // rename the existing path to the version path
        versionfmd = gOFS->eosView->getFile(std::string(
                                              paths["versiondir"].GetParentPath()) + std::string(paths["atomic"].GetPath()));
        dir->removeFile(paths["atomic"].GetName());
        versionfmd->setName(paths["version"].GetName());
        versionfmd->setContainerId(versiondir->getId());
        versiondir->addFile(versionfmd.get());
        versiondir->setMTimeNow();
        gOFS->eosView->updateFileStore(versionfmd.get());
        gOFS->FuseXCastDeletion(dir->getIdentifier(), paths["atomic"].GetName());
        gOFS->FuseXCastRefresh(versionfmd->getIdentifier(),
                               versiondir->getIdentifier());
        gOFS->FuseXCastContainer(versiondir->getIdentifier());
        // Update the ownership and mode of the new file to the original
        // one
        fmd->setCUid(versionfmd->getCUid());
        fmd->setCGid(versionfmd->getCGid());
        fmd->setFlags(versionfmd->getFlags());
        gOFS->eosView->updateFileStore(fmd.get());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_thread_err("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                       e.getErrno(), e.getMessage().str().c_str());
      }
    }

    std::shared_ptr<eos::IFileMD> pfmd;

    // Rename the temporary upload path to the final path
    if ((pfmd = dir->findFile(paths["atomic"].GetName()))) {
      // Check if we are tagged as that 'latest' atomic upload
      std::string atomic_tag;

      try {
        atomic_tag = pfmd->getAttribute("sys.tmp.atomic");
      } catch (eos::MDException& e) {
      }

      if ((!option["ocdone"]) && (atomic_tag != fmd->getName())) {
        // this is not our atomic upload, just abort that and delete the temporary artefact
        delete_path = fmd->getName();
        eos_thread_err("msg=\"we are not the last atomic upload - cleaning %s\"",
                       delete_path.c_str());
        option["abort"] = true;
      } else {
        eos_thread_info("msg=\"found final path\" %s", paths["atomic"].GetName());
        // If the target exists we swap the two and then delete the
        // previous one
        delete_path = fmd->getName();
        delete_path += ".delete";
        gOFS->eosView->renameFile(pfmd.get(), delete_path);
      }
    } else {
      eos_thread_info("msg=\"didn't find path\" %s", paths["atomic"].GetName());
    }

    if (!option["abort"]) {
      gOFS->eosView->renameFile(fmd.get(), paths["atomic"].GetName());
      eos_thread_info("msg=\"de-atomize file\" fxid=%08llx atomic-name=%s "
                      "final-name=%s", fmd->getId(), fmd->getName().c_str(),
                      paths["atomic"].GetName());
    }
  } catch (eos::MDException& e) {
    delete_path = "";
    errno = e.getErrno();
    std::string errmsg = e.getMessage().str();
    eos_thread_err("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                   e.getErrno(), e.getMessage().str().c_str());
  }
}

EOSMGMNAMESPACE_END
