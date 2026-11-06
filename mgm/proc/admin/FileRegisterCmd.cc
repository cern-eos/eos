//------------------------------------------------------------------------------
// @file: QuotaCmd.cc
// @author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#include "mgm/proc/admin/FileRegisterCmd.hh"
#include "QuotaCmd.hh"
#include "common/Constants.hh"
#include "common/Path.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/policy/Policy.hh"
#include "mgm/proc/ProcInterface.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/interface/IQuota.hh"
#include <algorithm>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Method implementing the specific behavior of the command executed by the
// asynchronous thread
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileRegisterCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;

  // ---------------------------------------------------------------------------
  // Defense-in-depth admin gate. The dispatcher in
  // ProcInterface::HandleProtobufRequest already refuses non-admin callers
  // for kRecord, but FileRegisterCmd writes namespace metadata with caller-
  // supplied owner uid/gid, so we re-check here to keep this command safe
  // even if the central gate is ever bypassed or relaxed.
  // ---------------------------------------------------------------------------
  if (!ProcInterface::VidIsAdmin(mVid)) {
    eos_err("msg=\"FileRegisterCmd refused for non-admin\" "
            "uid=%u gid=%u prot=%s host=%s",
            mVid.uid, mVid.gid, mVid.prot.c_str(), mVid.host.c_str());
    reply.set_retc(EPERM);
    reply.set_std_err("error: file registration requires admin privileges");
    return reply;
  }

  eos::console::FileRegisterProto reg = mReqProto.record();
  // Reject malformed checksums before accessing the namespace.
  const auto& checksum = reg.checksum();
  if (checksum.size() > 2 * SHA256_DIGEST_LENGTH || checksum.size() % 2 != 0 ||
      !std::all_of(checksum.begin(), checksum.end(), [](char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
      })) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: checksum must contain at most 64 hex digits in byte pairs");
    return reply;
  }
  eos::common::Path cPath(reg.path());
  std::shared_ptr<eos::IContainerMD> dir;
  std::shared_ptr<eos::IFileMD> fmd;
  eos::IContainerMD::XAttrMap attrmap;
  {
    eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView,
        cPath.GetParentPath());
    // Also the entry, so findItem below does not wait for the backend under
    // the namespace write lock
    eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, cPath.GetPath());
    // The name lookups can be slow (NSS): do them before the namespace lock
    uid_t uid = reg.owner().uid();
    uid_t gid = reg.owner().gid();

    if (reg.owner().username().length()) {
      int errc = 0;
      uid = eos::common::Mapping::UserNameToUid(reg.owner().username().c_str(), errc);
    }

    if (reg.owner().groupname().length()) {
      int errc = 0;
      gid = eos::common::Mapping::GroupNameToGid(reg.owner().groupname().c_str(), errc);
    }

    // The namespace lock in read mode excludes the writers that still take it
    // in write mode, the MD locks below the ones using per-object locks
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

    // Check for the parent directory
    try {
      dir = gOFS->eosView->getContainer(cPath.GetParentPath());
      attrmap = dir->getAttributes();
      std::string name = cPath.GetName();
      // Wait for the entry: value() of a pending future throws
      eos::FileOrContainerMD exists = dir->findItem(name).get();

      if (exists.file || exists.container) {
        if (reg.update() && exists.file) {
          fmd = exists.file;
        } else {
          reply.set_retc(EEXIST);
          reply.set_std_err("file already exists");
          return reply;
        }
      } else {
        if (reg.update()) {
          reply.set_retc(ENOENT);
          reply.set_std_err("no suche file");
          return reply;
        }
      }

      // The quota node lookup walks up the parents and the layout policy
      // takes the FsView locks, so they run before the MD locks
      eos::IQuotaNode* ns_quota = nullptr;

      try {
        ns_quota = gOFS->eosView->getQuotaNode(dir.get());
      } catch (const eos::MDException&) {
        // no quota node
      }

      unsigned long layoutId = reg.layoutid();

      if (!layoutId && !reg.update()) {
        // automatically get a layout id for this registration
        std::string space;
        XrdOucEnv env;
        unsigned long forcedfsid = 0;
        long forcedgroup = 0;
        std::string bandwidth;
        bool schedule = false;
        std::string iopriority;
        std::string iotype;
        Policy::GetLayoutAndSpace(cPath.GetFullPath().c_str(), attrmap, vid, layoutId,
                                  space, env, forcedfsid, forcedgroup, bandwidth,
                                  schedule, iopriority, iotype, true, false);
      }

      eos::MDLocking::BulkMDWriteLock md_locker;
      md_locker.add(dir.get());

      if (fmd) {
        md_locker.add(fmd.get());
      }

      auto md_locks =
          std::make_unique<eos::MDLocking::BulkMDWriteLock::Locks>(md_locker.lockAll());

      // A parallel writer removed the directory or changed the entry after
      // the lookup
      eos::FileOrContainerMD now = dir->findItem(name).get();

      if (dir->isDeleted() || (now.file != fmd) || now.container) {
        reply.set_retc(EAGAIN);
        reply.set_std_err("entry changed during the registration, try again");
        return reply;
      }

      // An update can change owner, layout and size: take the file out of
      // quota before the changes and account it again after them
      if (reg.update() && ns_quota) {
        ns_quota->removeFile(fmd.get());
      }

      if (!reg.update()) {
        // create with given uid/gid, in the locked directory: the conflict
        // check and the insertion of createFile(path) are separate steps
        fmd = gOFS->eosFileService->createFile(0);

        if (!fmd) {
          throw_mdexception(EIO, "File creation failed");
        }

        fmd->setName(name);
        fmd->setCUid(uid);
        fmd->setCGid(gid);
        fmd->setCTimeNow();
        fmd->setATimeNow(0);
        fmd->setMTimeNow();
        fmd->clearChecksum(0);
        dir->addFile(fmd.get());
        gOFS->eosView->updateFileStore(fmd.get());
      } else {
        if (uid) {
          fmd->setCUid(uid);
        }

        if (gid) {
          fmd->setCGid(gid);
        }
      }

      if (reg.mode()) {
        //store mode
        fmd->setFlags(reg.mode());
      }

      if (reg.checksum().length()) {
        //store checksum
        size_t out_sz = 0;
        auto xs_binary = eos::common::StringConversion::Hex2BinDataChar
                         (reg.checksum(), out_sz, SHA256_DIGEST_LENGTH);
        eos::Buffer xs_buff;
        xs_buff.putData(xs_binary.get(), SHA256_DIGEST_LENGTH);
        fmd->setChecksum(xs_buff);
      }

      if (reg.ctime().sec()) {
        // add ctime
        struct timespec tvp;
        tvp.tv_sec = reg.ctime().sec();
        tvp.tv_nsec = reg.ctime().nsec();
        fmd->setCTime(tvp);
      }

      if (reg.mtime().sec()) {
        // add mtime
        struct timespec tvp;
        tvp.tv_sec = reg.mtime().sec();
        tvp.tv_nsec = reg.mtime().nsec();
        fmd->setMTime(tvp);
      }

      if (reg.atime().sec()) {
	// add atime
	struct timespec tvp;
	struct timespec tvnow;
	tvp.tv_sec = reg.atime().sec();
	tvp.tv_nsec = reg.atime().nsec();

	if (reg.atimeifnewer()) {
	  // only update if the input atime is actually newer than the existing one
	  fmd->getATime(tvnow);
	  if ( (tvp.tv_sec > tvnow.tv_sec) ||
	       ( (tvp.tv_sec == tvnow.tv_sec) && (tvp.tv_nsec > tvnow.tv_nsec) ) ) {
	    fmd->setATime(tvp);
	  } else {
	    reply.set_std_out("warning: atime is not newer than existing one");
	  }
	} else {
	  fmd->setATime(tvp);
	}
      }

      if (reg.btime().sec()) {
        // add btime
        char btime[256];
        snprintf(btime, sizeof(btime), "%lu.%lu", reg.btime().sec(),
                 reg.btime().nsec());
        fmd->setAttribute("sys.eos.btime", btime);
      } else {
        // add btime
        char btime[256];
        snprintf(btime, sizeof(btime), "%lu.%lu", time(NULL), 0l);
        fmd->setAttribute("sys.eos.btime", btime);
      }

      // add locations
      for (const auto& fsid : reg.locations()) {
        if ((fsid > 0) && (fsid <= eos::common::TAPE_FS_ID)) {
          fmd->addLocation(fsid);
        }
      }

      // add xattr
      for (const auto& elem : reg.attr()) {
        fmd->setAttribute(elem.first, elem.second);
      }

      if (layoutId) {
        fmd->setLayoutId(layoutId);
      }

      // As for the other fields, an update with no size keeps the size
      if (!reg.update() || reg.size()) {
        fmd->setSize(reg.size());
      }

      if (ns_quota) {
        ns_quota->addFile(fmd.get());
      }

      gOFS->eosView->updateFileStore(fmd.get());
      dir->setMTimeNow();
      gOFS->eosView->updateContainerStore(dir.get());
      md_locks.reset();
      ns_rd_lock.Release();
      dir->notifyMTimeChange(gOFS->eosDirectoryService);
      return reply;
    } catch (eos::MDException& e) {
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
      dir.reset();
      reply.set_retc(e.getErrno());
      reply.set_std_err(e.getMessage().str().c_str());
      return reply;
    }
  }
  reply.set_retc(EINVAL);
  reply.set_std_err("error: not supported");
  return reply;
}

EOSMGMNAMESPACE_END
