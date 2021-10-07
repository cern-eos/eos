//------------------------------------------------------------------------------
//! @file data.cc
//! @author Andreas-Joachim Peters CERN
//! @brief data handling class
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "data/data.hh"
#include "kv/kv.hh"
#include "eosfuse.hh"
#include "data/cachesyncer.hh"
#include "data/journalcache.hh"
#include "data/xrdclproxy.hh"
#include "misc/MacOSXHelper.hh"
#include "misc/fusexrdlogin.hh"
#include "misc/filename.hh"
#include "common/Logging.hh"
#include "common/SymKeys.hh"
#include <iostream>
#include <sstream>


bufferllmanager data::datax::sBufferManager;
std::string data::datax::kInlineAttribute = "sys.file.buffer";
std::string data::datax::kInlineMaxSize = "sys.file.inline.maxsize";
std::string data::datax::kInlineCompressor = "sys.file.inline.compressor";

/* -------------------------------------------------------------------------- */
data::data()
/* -------------------------------------------------------------------------- */
{
}

/* -------------------------------------------------------------------------- */
data::~data()
/* -------------------------------------------------------------------------- */
{
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::init()
/* -------------------------------------------------------------------------- */
{
  // configure the ra,rd,wr buffer sizes
  XrdCl::Proxy::sRaBufferManager.configure(16,
      cachehandler::instance().get_config().default_read_ahead_size,
      cachehandler::instance().get_config().max_inflight_read_ahead_buffer_size);
  XrdCl::Proxy::sWrBufferManager.configure(128,
      128 * 1024,
      cachehandler::instance().get_config().max_inflight_write_buffer_size);
  datamap.run();
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::terminate(uint64_t seconds)
/* -------------------------------------------------------------------------- */
{
  if (datamap.waitflush(seconds)) {
    datamap.join();
  }
}


/* -------------------------------------------------------------------------- */
data::shared_data
/* -------------------------------------------------------------------------- */
data::get(fuse_req_t req,
          fuse_ino_t ino,
          metad::shared_md md)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(datamap);

  if (datamap.count(ino)) {
    shared_data io = datamap[ino];
    io->attach(); // client ref counting
    return io;
  } else {
    // protect against running out of file descriptors
    size_t openfiles = datamap.size();
    size_t openlimit = (EosFuse::Instance().Config().options.fdlimit - 128) / 2;

    while ((openfiles = datamap.size()) > openlimit) {
      datamap.UnLock();
      eos_static_warning("open-files=%lu limit=%lu - waiting for release of file descriptors",
                         openfiles, openlimit);
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      datamap.Lock();
    }

    if (datamap.count(ino)) {
      // might have been created in the meanwhile
      shared_data io = datamap[ino];
      io->attach(); // client ref counting
      return io;
    } else {
      shared_data io = std::make_shared<datax>(md);
      io->set_id(ino, req);
      datamap[(fuse_ino_t) io->id()] = io;
      io->attach();
      return io;
    }
  }
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
data::has(fuse_ino_t ino, bool checkwriteopen)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(datamap);

  if (datamap.count(ino)) {
    if (checkwriteopen) {
      if (datamap[ino]->flags() & (O_RDWR | O_WRONLY)) {
        return true;
      } else {
        return false;
      }
    } else {
      return true;
    }
  } else {
    return false;
  }
}

/* -------------------------------------------------------------------------- */
metad::shared_md
data::retrieve_wr_md(fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  // return the shared_md  boject if this is a writer
  XrdSysMutexHelper mLock(datamap);

  if (datamap.count(ino)) {
    if (datamap[ino]->flags() & (O_RDWR | O_WRONLY)) {
      return datamap[ino]->md();
    }
  }

  return nullptr;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::release(fuse_req_t req,
              fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(datamap);

  if (datamap.count(ino)) {
    shared_data io = datamap[ino];
    io->detach();
    // the object is cleaned by the flush thread
  }

  if (datamap.count(ino + 0xffffffff)) {
    // in case this is an unlinked object
    shared_data io = datamap[ino + 0xffffffff];
    io->detach();
  }
}

void
/* -------------------------------------------------------------------------- */
data::update_cookie(uint64_t ino, std::string& cookie)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(datamap);

  if (datamap.count(ino)) {
    shared_data io = datamap[ino];
    io->attach(); // client ref counting
    io->store_cookie(cookie);
    io->detach();
  }
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::invalidate_cache(fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(datamap);

  if (datamap.count(ino)) {
    shared_data io = datamap[ino];
    io->attach(); // client ref counting
    io->cache_invalidate();
    io->detach();
  }
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::unlink(fuse_req_t req, fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  bool has_data = false;
  shared_data datap;
  {
    XrdSysMutexHelper mLock(datamap);
    has_data = datamap.count(ino);

    if (has_data) {
      datap = datamap[ino];
    }
  }

  if (has_data) {
    {
      XrdSysMutexHelper helper(datap->Locker());
      // wait for open in flight to be done
      datap->WaitOpen();
      datap->unlink(req);
    }
    // put the unlinked inode in a high bucket, will be removed by the flush thread
    {
      XrdSysMutexHelper mLock(datamap);

      if (datamap.count(ino)) {
        datamap[ino + 0xffffffff] = datamap[ino];
        datamap.erase(ino);
        eos_static_info("datacache::unlink size=%lu", datamap.size());
      }
    }
  } else {
    shared_data io = std::make_shared<datax>();
    io->set_id(ino, req);
    io->unlink(req);
  }
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::flush(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  XrdSysMutexHelper lLock(mLock);
  bool flush_wait_open = false;

  if (mFlags & O_CREAT) {
    flush_wait_open = (EosFuse::Instance().Config().options.flush_wait_open ==
                       EosFuse::Instance().Config().options.kWAIT_FLUSH_ON_CREATE) ? true : false;

    if ((!flush_wait_open) &&
        (mMd->size() >= EosFuse::Instance().Config().options.flush_wait_open_size)) {
      flush_wait_open = true;
    }

    if (EosFuse::Instance().Config().options.nowait_flush_executables.size()) {
      if (!filename::matches_suffix(fusexrdlogin::executable(req),
                                    EosFuse::Instance().Config().options.nowait_flush_executables)) {
        eos_notice("flush-wait-open: forced for exec=%s",
                   fusexrdlogin::executable(req).c_str());
        flush_wait_open = true;
      }
    }
  } else {
    flush_wait_open = (EosFuse::Instance().Config().options.flush_wait_open !=
                       EosFuse::Instance().Config().options.kWAIT_FLUSH_NEVER) ? true : false;
  }

  if (EOS_LOGS_DEBUG) {
    eos_notice("flush-wait-open: %d size=%lu exec=%s\n", flush_wait_open,
               mMd->size(), fusexrdlogin::executable(req).c_str());
  }

  return flush_nolock(req, flush_wait_open, false);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::flush_nolock(fuse_req_t req, bool wait_open, bool wait_writes)
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  bool journal_recovery = false;
  errno = 0;

  if (mFile->journal() && mFile->has_xrdiorw(req)) {
    eos_info("flushing journal");
    ssize_t truncate_size = mFile->journal()->get_truncatesize();

    if (wait_open) {
      // wait atleast that we could open that file
      mFile->xrdiorw(req)->WaitOpen();
    }

    if ((truncate_size != -1)
        || (wait_writes && mFile->journal()->size())) {
      // if there is a truncate to be done, we have to wait for the writes and truncate
      // if we are asked to wait for writes (when pwrite sees a journal full) we free the journal
      for (auto it = mFile->get_xrdiorw().begin();
           it != mFile->get_xrdiorw().end(); ++it) {
        XrdCl::XRootDStatus status = it->second->WaitOpen();

        if (!status.IsOK()) {
          if (status.errNo == kXR_overQuota) {
            eos_crit("flush error errno=%d", XrdCl::Proxy::status2errno(status));
            return XrdCl::Proxy::status2errno(status);
          }

          journal_recovery = true;
          eos_err("file not open");
        }

        status = it->second->WaitWrite();

        if (!status.IsOK()) {
          mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                           "status='%s' hint='will TryRecovery'",
                                           status.ToString().c_str()));
          journal_recovery = true;
          eos_err("write error error=%s", status.ToStr().c_str());
        }
      }

      ssize_t truncate_size = mFile->journal()->get_truncatesize();

      if (!journal_recovery && (truncate_size != -1)) {
        // the journal might have a truncation size indicated, so we need to run a sync truncate in the end
        XrdCl::XRootDStatus status = mFile->xrdiorw(req)->Truncate(truncate_size);

        if (!status.IsOK()) {
          mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                           "status='%s' hint='will TryRecovery'",
                                           status.ToString().c_str()));
          journal_recovery = true;
          eos_err("truncation failed");
        }
      }

      if (simulate_write_error_in_flush()) {
        // force a 'fake' repair now for testing purposes
        journal_recovery = true;
      }

      if (journal_recovery) {
        eos_debug("try recovery");
        int rc = 0;

        if ((rc = TryRecovery(req, true))) {
          mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                           "errno='%d' hint='failed TryRecovery'",
                                           rc));
          eos_err("journal-flushing recovery failed rc=%d", rc);
          return rc;
        } else {
          mRecoveryStack.push_back(eos_log(LOG_SILENT, "hint='success TryRecovery'"));

          if ((rc = journalflush(req))) {
            mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                             "errno='%d' hint='failed journalflush'",
                                             rc));
            eos_err("journal-flushing failed rc=%d", rc);
            return rc;
          } else {
            mRecoveryStack.push_back(eos_log(LOG_SILENT, "hint='success journalflush'"));
          }
        }
      }

      // truncate the journal
      if (mFile->journal()->reset()) {
        char msg[1024];
        snprintf(msg, sizeof(msg), "journal reset failed - ino=%#lx errno=%d %s", id(),
                 errno, mFile->journal()->dump().c_str());
        eos_crit("%s", msg);
        throw std::runtime_error(msg);
      }

      mFile->journal()->done_flush();
    }
  }

  // check if the open failed
  XrdCl::Proxy* proxy = mFile->has_xrdiorw(req) ? mFile->xrdiorw(req) : 0;

  if (proxy) {
    if (proxy->stateTS() == XrdCl::Proxy::FAILED) {
      int rc = 0;
      eos_debug("try recovery");
      mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                       "status='XrdCl::Proxy::FAILED' hint='will TryRecovery'"));

      if ((rc = TryRecovery(req, true))) {
        mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                         "errno='%d' hint='failed TryRecovery'",
                                         rc));
        eos_err("remote open failed - returning %d", rc);
        return rc;
      } else {
        mRecoveryStack.push_back(eos_log(LOG_SILENT, "hint='success TryRecovery'"));

        if ((rc = journalflush(req))) {
          mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                           "errno='%d' hint='failed journalflush'",
                                           rc));
          eos_err("journal-flushing failed");
          return rc;
        } else {
          mRecoveryStack.push_back(eos_log(LOG_SILENT, "hint='success journalflush'"));

          // truncate the journal
          if (mFile->journal()->reset()) {
            char msg[1024];
            snprintf(msg, sizeof(msg), "journal reset failed - ino=%#lx errno=%d %s", id(),
                     errno, mFile->journal()->dump().c_str());
            eos_crit("%s", msg);
            throw std::runtime_error(msg);
          }

          mFile->journal()->done_flush();
        }
      }
    }
  }

  eos_info("retc=0");
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::journalflush(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  // call this with a mLock locked
  eos_info("");
  XrdCl::XRootDStatus status = mFile->xrdiorw(req)->WaitOpen();

  // we have to push the journal now
  if (!status.IsOK()) {
    eos_err("async journal-cache-wait-open failed - ino=%#lx", id());
    errno = XrdCl::Proxy::status2errno(status);
    return errno;
  }

  eos_info("syncing cache");
  cachesyncer cachesync(*((XrdCl::File*)mFile->xrdiorw(req)));

  if ((mFile->journal())->remote_sync(cachesync)) {
    eos_err("async journal-cache-sync failed - ino=%#lx", id());
    return EREMOTEIO;
  }

  eos_info("retc=0");
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::journalflush(std::string cid)
/* -------------------------------------------------------------------------- */
{
  // call this with a mLock locked
  eos_info("");
  XrdCl::XRootDStatus status = mFile->xrdiorw(cid)->WaitOpen();

  // we have to push the journal now
  if (!status.IsOK()) {
    eos_err("async journal-cache-wait-open failed - ino=%#lx", id());
    errno = XrdCl::Proxy::status2errno(status);
    return errno;
  }

  if (mFile->journal()) {
    eos_info("syncing cache");
    cachesyncer cachesync(*((XrdCl::File*)mFile->xrdiorw(cid)));

    if ((mFile->journal())->remote_sync(cachesync)) {
      eos_err("async journal-cache-sync failed - ino=%#lx", id());
      return EREMOTEIO;
    }
  }

  eos_info("retc=0");
  return 0;
}


/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
data::datax::is_wopen(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(mLock);
  return mFile->xrdiorw(req)->IsOpen();
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::journalflush_async(std::string cid)
/* -------------------------------------------------------------------------- */
{
  // call this with a mLock locked
  eos_info("");

  // we have to push the journal now
  if (!mFile->xrdiorw(cid)->WaitOpen().IsOK()) {
    eos_err("async journal-cache-wait-open failed - ino=%#lx", id());
    return -1;
  }

  if (mFile->journal()) {
    eos_info("syncing cache asynchronously");

    if ((mFile->journal())->remote_sync_async(mFile->xrdiorw(cid))) {
      eos_err("async journal-cache-sync-async failed - ino=%#lx", id());
      return -1;
    }
  }

  eos_info("retc=0");
  return 0;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::datax::set_id(uint64_t ino, fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(Locker());
  mIno = ino;
  mReq = req;
  mFile = cachehandler::instance().get(ino);
  char lid[64];
  snprintf(lid, sizeof(lid), "logid:ino:%016lx", ino);
  SetLogId(lid);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::attach(fuse_req_t freq, std::string& cookie, int flags)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(mLock);
  bool isRW = false;
  bool add_O_SYNC = false;
  bool add_O_CREAT = false;

  if (mFlags & O_SYNC) {
    // preserve the sync flag
    add_O_SYNC = true;
  }

  if (mFlags & O_CREAT) {
    // preserve the creat flag
    add_O_CREAT = true;
  }

  mFlags = flags;

  if (add_O_SYNC) {
    mFlags |= O_SYNC;
  }

  if (add_O_CREAT) {
    mFlags |= O_CREAT;
  }

  if (mFlags & O_CREAT) {
    mFlags |= O_RDWR;
  }

  // check for file inlining only for the first attach call
  if ((!inline_buffer) && (EosFuse::Instance().Config().inliner.max_size ||
                           mMd->inlinesize())) {
    if (mMd->inlinesize()) {
      mInlineMaxSize = mMd->inlinesize();
    } else {
      mInlineMaxSize = EosFuse::Instance().Config().inliner.max_size;
    }

    auto attrMap = mMd->attr();

    if (attrMap.count(kInlineMaxSize)) {
      mInlineMaxSize = strtoull(attrMap[kInlineMaxSize].c_str(), 0, 10);
    }

    if (attrMap.count(kInlineCompressor)) {
      mInlineCompressor = attrMap[kInlineCompressor];
    } else {
      mInlineCompressor = EosFuse::Instance().Config().inliner.default_compressor;
    }

    eos_debug("inline-size=%llu inline-compressor=%s", mInlineMaxSize,
              mInlineCompressor.c_str());
    // reserve buffer for inlining
    inline_buffer = std::make_shared<bufferll>(mInlineMaxSize,
                    mInlineMaxSize);
    mIsInlined = true;

    if (attrMap.count(kInlineAttribute)) {
      std::string base64_string(attrMap[kInlineAttribute].c_str(),
                                attrMap[kInlineAttribute].size());
      std::string raw_string;
      bool decoding = false;

      if (base64_string.substr(0, 8) == "zbase64:") {
        SymKey::ZDeBase64(base64_string, raw_string);
        decoding = true;
      } else if (base64_string.substr(0, 7) == "base64:") {
        SymKey::DeBase64(base64_string, raw_string);
        decoding = true;
      }

      if (decoding) {
        // decode attribute to buffer
        inline_buffer->writeData(raw_string.c_str(), 0, raw_string.size());

        // in case there is any inconsistency between size and attribute buffer, just ignore this one
        if (raw_string.size() != mMd->size()) {
          inline_buffer = 0;
          // delete the inline buffer
          (mMd->mutable_attr())->erase(kInlineAttribute);
          mIsInlined = false;
        }
      } else {
        mIsInlined = false;
      }
    } else {
      if (mMd->size()) {
        mIsInlined = false;
      }
    }
  }

  if (flags & (O_CREAT | O_RDWR | O_WRONLY)) {
    isRW = true;
  }

  eos_info("cookie=%s flags=%o isrw=%d md-size=%d %s", cookie.c_str(), flags,
           isRW, mMd->size(),
           isRW ? mRemoteUrlRW.c_str() : mRemoteUrlRO.c_str());
  // store the currently known size here
  mSize = mMd->size();

  // set write error simulation flags
  if (mMd->name().find("#err_sim_flush#") != std::string::npos) {
    eos_crit("enabling error simulation on flush");
    mSimulateWriteErrorInFlush = true;
  } else if (mMd->name().find("#err_sim_flusher#") != std::string::npos) {
    eos_crit("enabling error simulation on flusher");
    mSimulateWriteErrorInFlusher = true;
  }

  if ((flags & O_SYNC) ||
      ((time(NULL) - mMd->bc_time()) <
       EosFuse::Instance().Config().options.nocache_graceperiod)) {
    mFile->disable_caches();
  }

  int bcache = mFile->file() ? mFile->file()->attach(freq, cookie, isRW) : 0;
  int jcache = mFile->journal() ? ((isRW ||
                                    (mFlags & O_CACHE)) ? mFile->journal()->attach(freq, cookie,
                                        flags) : 0) : 0;

  if (bcache < 0) {
    char msg[1024];
    snprintf(msg, sizeof(msg), "attach to cache failed - ino=%#lx errno=%d", id(),
             errno);
    eos_crit("%s", msg);
    throw std::runtime_error(msg);
  }

  if (jcache < 0) {
    char msg[1024];
    snprintf(msg, sizeof(msg), "attach to journal failed - ino=%#lx errno=%d", id(),
             errno);
    eos_crit("%s", msg);
    throw std::runtime_error(msg);
  }

  if (isRW) {
    if (!mFile->has_xrdiorw(freq) || mFile->xrdiorw(freq)->IsClosing() ||
        mFile->xrdiorw(freq)->IsClosed()) {
      if (mFile->has_xrdiorw(freq) && (mFile->xrdiorw(freq)->IsClosing() ||
                                       mFile->xrdiorw(freq)->IsClosed())) {
        mFile->xrdiorw(freq)->WaitClose();
        mFile->xrdiorw(freq)->attach();
      } else {
        // attach an rw io object
        mFile->set_xrdiorw(freq, new XrdCl::Proxy());
        mFile->xrdiorw(freq)->attach();
        mFile->xrdiorw(freq)->set_id(id(), req());
      }

      XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update;
      XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW |
                                 XrdCl::Access::UX;
      mFile->xrdiorw(freq)->OpenAsync(mRemoteUrlRW.c_str(), targetFlags, mode, 0);
    } else {
      if (mFile->xrdiorw(freq)->IsWaitWrite()) {
        // re-open the file in the state machine
        mFile->xrdiorw(freq)->set_state_TS(XrdCl::Proxy::OPENED);
      }

      mFile->xrdiorw(freq)->attach();
    }

    // when someone attaches a writer, we have to drop all the read-ahead buffers because we might get stale information in readers
    for (auto fit = mFile->get_xrdioro().begin();
         fit != mFile->get_xrdioro().end(); ++fit) {
      if (fit->second->IsOpen()) {
        fit->second->DropReadAhead();
      }
    }
  } else {
    if (!mFile->has_xrdioro(freq) || mFile->xrdioro(freq)->IsClosing() ||
        mFile->xrdioro(freq)->IsClosed()) {
      if (mFile->has_xrdioro(freq) && (mFile->xrdioro(freq)->IsClosing() ||
                                       mFile->xrdioro(freq)->IsClosed())) {
        mFile->xrdioro(freq)->WaitClose();
        mFile->xrdioro(freq)->attach();
      } else {
        mFile->set_xrdioro(freq, new XrdCl::Proxy());
        mFile->xrdioro(freq)->attach();
        mFile->xrdioro(freq)->set_id(id(), req());

        if (!(flags & O_SYNC)) {
          if (EOS_LOGS_DEBUG)
            eos_debug("readhead: strategy=%s nom:%lu max:%lu sparse-ratio:%.01f",
                      cachehandler::instance().get_config().read_ahead_strategy.c_str(),
                      cachehandler::instance().get_config().default_read_ahead_size,
                      cachehandler::instance().get_config().max_read_ahead_size,
		      cachehandler::instance().get_config().read_ahead_sparse_ratio);

          mFile->xrdioro(freq)->set_readahead_strategy(
            XrdCl::Proxy::readahead_strategy_from_string(
              cachehandler::instance().get_config().read_ahead_strategy),
            4096,
            cachehandler::instance().get_config().default_read_ahead_size,
            cachehandler::instance().get_config().max_read_ahead_size,
            cachehandler::instance().get_config().max_read_ahead_blocks,
	    cachehandler::instance().get_config().read_ahead_sparse_ratio
          );
          mFile->xrdioro(freq)->set_readahead_maximum_position(mSize);
        }
      }

      XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Read;
      XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UX;
      // we might need to wait for a creation to go through
      WaitOpen();
      mFile->xrdioro(freq)->OpenAsync(mRemoteUrlRO.c_str(), targetFlags, mode, 0);
    } else {
      if (mFile->has_xrdiorw(freq)) {
        // we have to drop all existing read-ahead buffers to avoid reading outdated buffers
        mFile->xrdioro(freq)->DropReadAhead();
      }

      mFile->xrdioro(freq)->attach();
    }
  }

  return bcache | jcache;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
data::datax::inline_file(ssize_t size)
{
  XrdSysMutexHelper lLock(mLock);

  if (size == -1) {
    size = mMd->size();
  }

  if (inlined() && inline_buffer) {
    if ((size_t) size <= mInlineMaxSize) {
      // rewrite the extended attribute
      std::string raw_string(inline_buffer->ptr(), size);
      std::string base64_string;

      if (mInlineCompressor == "zlib") {
        SymKey::ZBase64(raw_string, base64_string);
      } else {
        SymKey::Base64(raw_string, base64_string);
      }

      (*(mMd->mutable_attr()))[kInlineAttribute] = base64_string;
      (*(mMd->mutable_attr()))[kInlineMaxSize] = std::to_string(mInlineMaxSize);
      (*(mMd->mutable_attr()))[kInlineCompressor] = mInlineCompressor;
      return true;
    } else {
      // remove the extended attribute
      (mMd->mutable_attr())->erase(kInlineAttribute);
      mIsInlined = false;
      return false;
    }
  }

  return false;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
data::datax::prefetch(fuse_req_t req, bool lock)
/* -------------------------------------------------------------------------- */
{
  size_t file_size = mMd->size();
  eos_info("handler=%d file=%lx size=%lu md-size=%lu", mPrefetchHandler ? 1 : 0,
           mFile ? mFile->file() : 0,
           mFile ? mFile->file() ? mFile->file()->size() : 0 : 0,
           file_size);

  if (mFile && mFile->has_xrdiorw(req)) {
    // never prefetch on a wr open file
    return true;
  }

  if (inlined()) {
    // never prefetch an inlined file
    return true;
  }

  if (lock) {
    mLock.Lock();
  }

  if (!mPrefetchHandler && mFile->file() && !mFile->file()->size() && file_size) {
    XrdCl::Proxy* proxy = mFile->has_xrdioro(req) ? mFile->xrdioro(
                            req) : mFile->xrdiorw(req);

    if (proxy) {
      XrdCl::XRootDStatus status;
      size_t prefetch_size = std::min((size_t) file_size,
                                      (size_t) mFile->file()->prefetch_size());
      // try to send an async read request
      mPrefetchHandler = proxy->ReadAsyncPrepare(0, prefetch_size, false);
      bool nobuffer = false;

      if (mPrefetchHandler->valid()) {
        status = proxy->PreReadAsync(0, prefetch_size, mPrefetchHandler, 0);
      } else {
        // no free IO buffer
        XrdCl::XRootDStatus newstatus(XrdCl::stFatal,
                                      0,
                                      XrdCl::errOSError,
                                      "no free read-ahead buffer"
                                     );
        status = newstatus;
        nobuffer = true;
      }

      if (!status.IsOK()) {
        if (!nobuffer) {
          eos_err("pre-fetch failed error=%s", status.ToStr().c_str());
        }

        mPrefetchHandler = 0;
      } else {
        // instruct the read-ahead handler where to start
        proxy->set_readahead_position(prefetch_size);
      }
    }
  }

  if (lock) {
    mLock.UnLock();
  }

  return mPrefetchHandler ? true : false;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::datax::WaitPrefetch(fuse_req_t req, bool lock)
/* -------------------------------------------------------------------------- */
{
  eos_info("");

  if (lock) {
    mLock.Lock();
  }

  size_t file_size = mMd->size();

  if (mPrefetchHandler && mFile->file()) {
    XrdCl::Proxy* proxy = mFile->has_xrdioro(req) ? mFile->xrdioro(
                            req) : mFile->xrdiorw(req);

    if (mPrefetchHandler && proxy) {
      XrdCl::XRootDStatus status = proxy->WaitRead(mPrefetchHandler);

      if (status.IsOK()) {
        eos_info("pre-read done with size=%lu md-size=%lu",
                 mPrefetchHandler->vbuffer().size(), file_size);

        if ((mPrefetchHandler->vbuffer().size() == file_size) && mFile->file()) {
          ssize_t nwrite = mFile->file()->pwrite(mPrefetchHandler->buffer(),
                                                 mPrefetchHandler->vbuffer().size(), 0);
          eos_debug("nwb=%lu to local cache", nwrite);
        }
      } else {
        eos_err("pre-read failed error=%s", status.ToStr().c_str());
      }
    }
  }

  if (lock) {
    mLock.UnLock();
  }
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::datax::WaitOpen()
{
  // make sure there is no pending remote open.
  // this has to be done to avoid opening a file for read, which is not yet
  // created.
  for (auto fit = mFile->get_xrdiorw().begin();
       fit != mFile->get_xrdiorw().end(); ++fit) {
    if (fit->second->IsOpening()) {
      eos_info("status=pending url=%s", fit->second->url().c_str());
      fit->second->WaitOpen();
      eos_info("status=final url=%s", fit->second->url().c_str());
    } else {
      eos_info("status=final url=%s", fit->second->url().c_str());
    }
  }
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::datax::FlagDeleted()
{
  for (auto fit = mFile->get_xrdiorw().begin();
       fit != mFile->get_xrdiorw().end(); ++fit) {
    fit->second->setDeleted();
  }

  for (auto fit = mFile->get_xrdioro().begin();
       fit != mFile->get_xrdioro().end(); ++fit) {
    fit->second->setDeleted();
  }
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::TryRecovery(fuse_req_t req, bool iswrite)
{
  eos_debug("");

  if (req && fuse_req_interrupted(req)) {
    eos_warning("request interrupted");

    // clean-up pending in-memory requests
    if (iswrite) {
      XrdCl::Proxy* proxy = mFile->xrdiorw(req);

      if (proxy) {
        proxy->CleanWriteQueue();
      }
    }

    return EINTR;
  }

  if (mReadErrorStack.size() > 128) {
    std::string stack_dump;

    // we give up recovery if to many recoveries took place
    for (auto it = mReadErrorStack.begin(); it != mReadErrorStack.end(); ++it) {
      stack_dump += "\n";
      stack_dump += *it;
    }

    eos_err("giving up recovery - error-stack:%s", stack_dump.c_str());
    return EREMOTEIO;
  }

  if (iswrite) {
    // recover write failures
    if (!EosFuse::Instance().Config().recovery.write) { // might be disabled
      eos_warning("write recovery disabled");
      return EREMOTEIO;
    }

    if (!mFile->has_xrdiorw(req)) {
      eos_crit("no proxy object");
      return EFAULT;
    }

    XrdCl::Proxy* proxy = mFile->xrdiorw(req);

    if (proxy->opening_state().IsError() &&
        ! proxy->opening_state_should_retry()) {
      eos_err("unrecoverable error - code=%d errNo=%d",
              proxy->opening_state().code,
              proxy->opening_state().errNo);
      proxy->CleanWriteQueue();
      return XrdCl::Proxy::status2errno(proxy->opening_state());
    }

    switch (proxy->stateTS()) {
    case XrdCl::Proxy::FAILED:
    case XrdCl::Proxy::OPENED:
    case XrdCl::Proxy::WAITWRITE:
    default:
      eos_crit("triggering write recovery state = %d", proxy->stateTS());
      return recover_write(req);
      eos_crit("default action");
    }
  } else {
    // recover read failures
    if (!EosFuse::Instance().Config().recovery.read) { // might be disabled
      return EREMOTEIO;
    }

    // no way to recover
    if (!mFile->has_xrdioro(req)) {
      eos_crit("no proxy object");
      return EFAULT;
    }

    XrdCl::Proxy* proxy = mFile->xrdioro(req);

    if (proxy->opening_state().IsError() &&
        ! proxy->opening_state_should_retry()) {
      eos_err("unrecoverable error - code=%d errNo=%d",
              proxy->opening_state().code,
              proxy->opening_state().errNo);
      proxy->CleanWriteQueue();
      return XrdCl::Proxy::status2errno(proxy->opening_state());
    }

    switch (proxy->stateTS()) {
    case XrdCl::Proxy::FAILED:
      mReadErrorStack.push_back("open-failed");
      return recover_ropen(req);

    case XrdCl::Proxy::OPENED:
      mReadErrorStack.push_back("read-failed");
      return recover_read(req);

    default:
      break;
    }
  }

  return EREMOTEIO;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::recover_ropen(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  XrdCl::Proxy* proxy = 0;
  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts, true);

  while (1) {
    proxy = mFile->xrdioro(req);
    mRecoveryStack.push_back(eos_log(LOG_SILENT, "hint='recover read-open'"));
    eos_warning("recover read-open [%d]",
                EosFuse::Instance().Config().recovery.read_open);

    if (!EosFuse::Instance().Config().recovery.read_open) { // might be disabled
      break;
    }

    XrdCl::XRootDStatus status = proxy->opening_state();

    if (status.errNo == kXR_noserver) {
      eos_crit("recover read-open-noserver [%d]",
               EosFuse::Instance().Config().recovery.read_open_noserver);

      // there is no server to read that file
      if (!EosFuse::Instance().Config().recovery.read_open_noserver) { // might be disabled
        return ENETUNREACH;
      }
    }

    if (status.IsFatal()) {
      // error useless to retry
      eos_crit("recover-ropen failed errno=%d", XrdCl::Proxy::status2errno(status));
      return XrdCl::Proxy::status2errno(status);
    }

    eos_warning("recover reopening file for read");
    XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Read;
    XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UX;
    // retrieve the 'tried' information to apply this for the file-reopening to exclude already known 'bad' locations
    std::string slasturl;
    proxy->GetProperty("LastURL", slasturl);
    XrdCl::URL lasturl(slasturl);
    XrdCl::URL newurl(mRemoteUrlRO);
    XrdCl::URL::ParamsMap last_cgi = lasturl.GetParams();
    XrdCl::URL::ParamsMap new_cgi = newurl.GetParams();
    std::string last_host = lasturl.GetHostName();

    if ((lasturl.GetHostName() != newurl.GetHostName()) &&
        (lasturl.GetPort() != newurl.GetPort())) {
      eos_warning("applying exclusion list: tried=%s,%s", last_host.c_str(),
                  new_cgi["tried"].c_str());
      new_cgi["tried"] = last_host.c_str() + std::string(",") + last_cgi["tried"];
      new_cgi["eos.repairread"] = "1";
      newurl.SetParams(new_cgi);
      mRemoteUrlRO = newurl.GetURL();
    } else {
      new_cgi.erase("tried");
      new_cgi["eos.repairread"] = "1";
      newurl.SetParams(new_cgi);
      mRemoteUrlRO = newurl.GetURL();
    }

    // issue a new open
    XrdCl::Proxy* newproxy = new XrdCl::Proxy();
    newproxy->OpenAsync(mRemoteUrlRO.c_str(), targetFlags, mode, 0);
    // wait this time for completion

    if ((req && fuse_req_interrupted(req)) || (newproxy->WaitOpen(req) == EINTR)) {
      eos_warning("request interrupted");
      return EINTR;
    }

    newproxy->inherit_attached(proxy);
    newproxy->inherit_protocol(proxy);
    // replace the proxy object
    mFile->set_xrdioro(req, newproxy);
    proxy->detach();
    // save the error status of the previous proxy object
    status = proxy->opening_state();

    // once all callbacks are there, this object can destroy itself since we don't track it anymore
    if (!proxy->IsWaitWrite() && !proxy->IsOpening() && !proxy->IsClosing()) {
      proxy->flag_selfdestructionTS();
      proxy->CheckSelfDestruction();
    } else {
      proxy->flag_selfdestructionTS();
    }

    if (newproxy->stateTS() == XrdCl::Proxy::OPENED) { // that worked !
      eos_warning("recover reopened file successfully");
      return 0;
    }

    // that failed again ...

    if (status.errNo == kXR_noserver) {
      double retry_time_sec = 1.0 * eos::common::Timing::GetCoarseAgeInNs(&ts,
                              0) / 1000000000.0;
      eos_warning("recover no server retry window [ %.02f/%lu ]",
                  retry_time_sec,
                  EosFuse::Instance().Config().recovery.read_open_noserver_retrywindow
                 );

      // check how long we are supposed to retry
      if (retry_time_sec <
          EosFuse::Instance().Config().recovery.read_open_noserver_retrywindow) {
        eos_warning("recover no server retry in 5 seconds");

        for (auto i = 0; i < 50; ++i) {
          // sleep for 5s and then try again
          std::this_thread::sleep_for(std::chrono::milliseconds(100));

          if (req && fuse_req_interrupted(req)) {
            eos_warning("request interrupted");
            return EINTR;
          }
        }

        // empty the tried= CGI tag
        new_cgi.erase("tried");
        newurl.SetParams(new_cgi);
        mRemoteUrlRO = newurl.GetURL();
        continue;
      }

      break;
    }

    if (status.IsFatal()) {
      // error useless to retry
      eos_crit("recover-ropen failed errno=%d", XrdCl::Proxy::status2errno(status));
      return XrdCl::Proxy::status2errno(status);
    }

    break;
  }

  return EREMOTEIO;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::try_ropen(fuse_req_t req, XrdCl::Proxy*& proxy,
                       std::string open_url)
{
  mRecoveryStack.push_back(eos_log(LOG_SILENT, "hint='try read-open'"));
  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts, true);
  XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Read;
  XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UX;
  proxy->OpenAsync(open_url, targetFlags, mode, 0);

  // wait this time for completion
  if ((req && fuse_req_interrupted(req)) || (proxy->WaitOpen(req) == EINTR)) {
    eos_warning("request interrupted");
    return EINTR;
  }

  if (proxy->stateTS() == XrdCl::Proxy::OPENED) { // that worked !
    eos_warning("recover read-open succesfull");
    return 0;
  }

  while (1) {
    eos_warning("recover read-open [%d]",
                EosFuse::Instance().Config().recovery.read_open);

    if (!EosFuse::Instance().Config().recovery.read_open) { // might be disabled
      break;
    }

    XrdCl::XRootDStatus status = proxy->opening_state();

    if (status.errNo == kXR_noserver) {
      eos_crit("recover read-open-noserver [%d]",
               EosFuse::Instance().Config().recovery.read_open_noserver);

      // there is no server to read that file
      if (!EosFuse::Instance().Config().recovery.read_open_noserver) { // might be disabled
        return ENETUNREACH;
      }
    }

    if (status.IsFatal()) {
      // error useless to retry
      eos_crit("recover read-open errno=%d", XrdCl::Proxy::status2errno(status));
      return XrdCl::Proxy::status2errno(status);
    }

    if ((status.errNo == kXR_overQuota) ||
        (status.errNo == kXR_NoSpace)) {
      // error useless to retry - this can happen if the open tries to reattach a file without locations and the user is out of quota
      eos_crit("recover read-open errno=%d", XrdCl::Proxy::status2errno(status));
      return XrdCl::Proxy::status2errno(status);
    }

    eos_warning("recover reopening file for read");
    // retrieve the 'tried' information to apply this for the file-reopening to exclude already knowns 'bad' locations
    std::string slasturl;
    proxy->GetProperty("LastURL", slasturl);
    XrdCl::URL lasturl(slasturl);
    XrdCl::URL newurl(open_url);
    XrdCl::URL::ParamsMap last_cgi = lasturl.GetParams();
    XrdCl::URL::ParamsMap new_cgi = newurl.GetParams();
    std::string last_host = lasturl.GetHostName();

    if ((lasturl.GetHostName() != newurl.GetHostName()) ||
        (lasturl.GetPort() != newurl.GetPort())) {
      eos_warning("applying exclusion list: tried=%s,%s", last_host.c_str(),
                  new_cgi["tried"].c_str());
      new_cgi["tried"] = last_host.c_str() + std::string(",") + last_cgi["tried"];
      newurl.SetParams(new_cgi);
      open_url = newurl.GetURL();
    } else {
      new_cgi.erase("tried");
      newurl.SetParams(new_cgi);
      open_url = newurl.GetURL();
    }

    // issue a new open
    XrdCl::Proxy* newproxy = new XrdCl::Proxy();
    newproxy->OpenAsync(open_url.c_str(), targetFlags, mode, 0);
    // wait this time for completion

    if ((req && fuse_req_interrupted(req)) || (newproxy->WaitOpen(req) == EINTR)) {
      eos_warning("request interrupted");
      return EINTR;
    }

    newproxy->inherit_attached(proxy);
    newproxy->inherit_protocol(proxy);

    // once all callbacks are there, this object can destroy itself since we don't track it anymore
    if (!proxy->IsWaitWrite() && !proxy->IsOpening() && !proxy->IsClosing()) {
      proxy->flag_selfdestructionTS();
      proxy->detach();
      proxy->CheckSelfDestruction();
    } else {
      proxy->flag_selfdestructionTS();
      proxy->detach();
    }

    // replace the proxy object
    proxy = newproxy;

    if (newproxy->stateTS() == XrdCl::Proxy::OPENED) { // that worked !
      eos_warning("recover reopened file successfully");
      return 0;
    }

    // that failed again ...
    status = proxy->opening_state();

    if (status.errNo == kXR_noserver) {
      double retry_time_sec = 1.0 * eos::common::Timing::GetCoarseAgeInNs(&ts,
                              0) / 1000000000.0;
      eos_warning("recover no server retry window [ %.02f/%lu ]",
                  retry_time_sec,
                  EosFuse::Instance().Config().recovery.read_open_noserver_retrywindow
                 );

      // check how long we are supposed to retry
      if (retry_time_sec <
          EosFuse::Instance().Config().recovery.read_open_noserver_retrywindow) {
        eos_warning("recover no server retry in 5 seconds");

        for (auto i = 0; i < 50; ++i) {
          // sleep for 5s and then try again
          std::this_thread::sleep_for(std::chrono::milliseconds(100));

          if (req && fuse_req_interrupted(req)) {
            eos_warning("request interrupted");
            return EINTR;
          }
        }

        // empty the tried= CGI tag
        new_cgi.erase("tried");
        newurl.SetParams(new_cgi);
        open_url = newurl.GetURL();
        continue;
      }

      break;
    }

    break;
  }

  eos_warning("recover failed try_ropen");
  return EREMOTEIO;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::try_wopen(fuse_req_t req, XrdCl::Proxy*& proxy,
                       std::string open_url)
{
  mRecoveryStack.push_back(eos_log(LOG_SILENT, "hint='try write-open'"));
  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts, true);
  // try to open this file for writing
  XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update;
  XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW |
                             XrdCl::Access::UX;
  // try to open
  XrdCl::XRootDStatus status = proxy->OpenAsync(open_url.c_str(), targetFlags,
                               mode, 0);

  if (proxy->WaitOpen(req) == EINTR) {
    eos_warning("request interrupted");
    proxy->CleanWriteQueue();
    return EINTR;
  }

  // if that worked we are already fine, otherwise we enter a timebased logic for retries
  if (proxy->stateTS() == XrdCl::Proxy::OPENED) { // that worked !
    eos_warning("re-opening for write succeeded");
    return 0;
  }

  while (1) {
    eos_warning("recover write-open [%d]",
                EosFuse::Instance().Config().recovery.write_open);

    if (!EosFuse::Instance().Config().recovery.write_open) { // might be disabled
      break;
    }

    XrdCl::XRootDStatus status = proxy->opening_state();

    if (status.errNo == kXR_noserver) {
      eos_crit("recover write-open-noserver [%d]",
               EosFuse::Instance().Config().recovery.write_open_noserver);

      // there is no server to read that file
      if (!EosFuse::Instance().Config().recovery.write_open_noserver) { // might be disable
        return ENETUNREACH;
      }
    }

    if (status.IsFatal()) {
      // error useless to retry
      eos_crit("recover write-open-fatal queue=%d errno=%d",
               proxy->WriteQueue().size(), XrdCl::Proxy::status2errno(status));
      return XrdCl::Proxy::status2errno(status);
    }

    if (status.errNo == kXR_overQuota) {
      // error useless to retry - no quota anymore
      eos_crit("recover write-open errno=%d", XrdCl::Proxy::status2errno(status));
      return XrdCl::Proxy::status2errno(status);
    }

    eos_warning("recover reopening file for writing");
    // issue a new open
    XrdCl::Proxy* newproxy = new XrdCl::Proxy();
    newproxy->OpenAsync(open_url.c_str(), targetFlags, mode, 0);
    // wait this time for completion

    if ((req && fuse_req_interrupted(req)) || (newproxy->WaitOpen(req) == EINTR)) {
      eos_warning("request interrupted");
      proxy->CleanWriteQueue();
      return EINTR;
    }

    newproxy->inherit_attached(proxy);
    newproxy->inherit_protocol(proxy);
    newproxy->inherit_writequeue(proxy);
    // once all callbacks are there, this object can destroy itself since we don't track it anymore
    proxy->flag_selfdestructionTS();
    // replace the proxy object
    proxy = newproxy;

    if (newproxy->stateTS() == XrdCl::Proxy::OPENED) { // that worked !
      eos_warning("recover reopened file successfully");
      return 0;
    }

    // that failed again ...
    status = proxy->opening_state();

    if (status.errNo == kXR_noserver) {
      double retry_time_sec = 1.0 * eos::common::Timing::GetCoarseAgeInNs(&ts,
                              0) / 1000000000.0;
      eos_warning("recover no server retry window [ %.02f/%.02f ]",
                  retry_time_sec,
                  EosFuse::Instance().Config().recovery.read_open_noserver_retrywindow
                 );

      // check how long we are supposed to retry
      if (retry_time_sec <
          EosFuse::Instance().Config().recovery.read_open_noserver_retrywindow) {
        eos_warning("recover no server retry in 5 seconds");

        for (auto i = 0; i < 50; ++i) {
          // sleep for 5s and then try again
          std::this_thread::sleep_for(std::chrono::milliseconds(100));

          if (req && fuse_req_interrupted(req)) {
            eos_warning("request interrupted");
            proxy->CleanWriteQueue();
            return EINTR;
          }
        }

        continue;
      }

      break;
    }

    if (status.IsFatal()) {
      // error useless to retry
      eos_crit("recover write-open-fatal errno=%d",
               XrdCl::Proxy::status2errno(status));
      return XrdCl::Proxy::status2errno(status);
    }

    break;
  }

  return EREMOTEIO;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::recover_read(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  mRecoveryStack.push_back(eos_log(LOG_SILENT, "hint='recover read'"));
  XrdCl::Proxy* proxy = mFile->xrdioro(req);
  // recover a pread error
  XrdCl::XRootDStatus status = proxy->read_state();

  if (req && fuse_req_interrupted(req)) {
    eos_warning("request interrupted");
    return EINTR;
  }

  // re-open the file
  int reopen = recover_ropen(req);

  if (!reopen) {
    eos_warning("recover reopened file successfully to re-read");
    // let's try again
    return 0;
  }

  return reopen;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::recover_write(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  mRecoveryStack.push_back(eos_log(LOG_SILENT, "hint='recover write'"));
  eos_debug("");
  XrdCl::Proxy* proxy = mFile->xrdiorw(req);
  // check if we have a problem with the open
  XrdCl::XRootDStatus status = proxy->WaitOpen();

  if (status.IsFatal() ||
      (proxy->opening_state().IsError() &&
       ! proxy->opening_state_should_retry())
     ) {
    // error useless to retry
    proxy->CleanWriteQueue();
    proxy->ChunkMap().clear();
    eos_crit("recover write-open-fatal queue=%d errno=%d",
             proxy->WriteQueue().size(), XrdCl::Proxy::status2errno(status));
    return XrdCl::Proxy::status2errno(status);
  }

  // try to open this file for reading
  bool recover_from_file_cache = false;
  bool recover_truncate = false;

  // check if the file has been created here and is still complete in the local caches
  if ((mFlags & O_CREAT) && mFile->file() &&
      (((mSize <= mFile->file()->prefetch_size()) &&
        (mSize == (ssize_t) mFile->file()->size())) ||
       (mFile->journal() &&
        mFile->journal()->first_flush()))) { // if the journal was not flushed yet and it is a creation, we have still all the data in the journal
    eos_debug("recover from file cache");
    // this file can be recovered from the file start cache
    recover_from_file_cache = true;
    mRecoveryStack.push_back(eos_log(LOG_SILENT, "hint='recover from file cache'"));
  } else {
    // we have to recover this from remote
    eos_debug("recover from remote file");
    recover_from_file_cache = false;
    ssize_t truncate_size = mFile->journal() ? mFile->journal()->get_truncatesize()
                            : -1;

    if (truncate_size == 0) {
      recover_truncate = true;
    }

    mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                     "hint='recover from remote file'"));
  }

  XrdCl::Proxy* aproxy = new XrdCl::Proxy();

  if (!recover_from_file_cache && !recover_truncate) {
    // we need to open this file because it is not complete locally
    int rc = try_ropen(req, aproxy,
                       mRemoteUrlRW + "&eos.checksum=ignore&eos.repairread=1");

    if (rc) {
      mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                       "hint='read-open failed with rc=%d'", rc));
      {
	eos::common::RWMutexWriteLock wLock(XrdCl::Proxy::gDeleteMutex);
	delete aproxy;
      }
      proxy->CleanWriteQueue();
      return rc;
    }
  }

  std::unique_ptr<XrdCl::Proxy> newproxy(aproxy);

  if (mFile->file() || recover_truncate) {
    void* buf = 0;
    std::string stagefile;
    int fd = 0;
    off_t off = 0;
    uint32_t size = 1 * 1024 * 1024;
    bufferllmanager::shared_buffer buffer;

    if (!recover_truncate) {
      mFile->file()->recovery_location(stagefile);
      buffer = sBufferManager.get_buffer(size);
      buf = (void*) buffer->ptr();
      // open local stagefile
      fd = ::open(stagefile.c_str(), O_CREAT | O_RDWR, S_IRWXU);
      ::unlink(stagefile.c_str());

      if (fd < 0) {
        sBufferManager.put_buffer(buffer);
        eos_crit("failed to open local stagefile %s", stagefile.c_str());
        proxy->CleanWriteQueue();
        return EREMOTEIO;
      }
    }

    if (req && begin_flush(req)) {
      eos_warning("failed to signal begin-flush");
    }

    if (recover_from_file_cache) {
      eos_debug("recovering from local start cache into stage file %s",
                stagefile.c_str());
      // make sure the buffer size fits
      buffer->resize(mFile->file()->size());
      buffer->reserve(mFile->file()->size());
      buf = (void*) buffer->ptr();

      // recover file from the local start cache
      if (mFile->file()->pread(buf, mFile->file()->size(), 0) < 0) {
        if (!recover_truncate) {
          sBufferManager.put_buffer(buffer);
        }

        close(fd);
        eos_crit("unable to read file for recovery from local file cache");

        if (req && end_flush(req)) {
          eos_warning("failed to signal end-flush");
        }

        proxy->CleanWriteQueue();
        return EIO;
      }
    } else {
      eos_debug("recovering from remote file into stage file %s", stagefile.c_str());

      if (!recover_truncate) {
        // download all into local stagefile, we don't need to do this, if there is a truncate request
        uint32_t bytesRead = 0;

        do {
          status = newproxy->Read(off, size, buf, bytesRead);
          eos_debug("off=%lu bytesread=%u", off, bytesRead);

          if (!status.IsOK()) {
            sBufferManager.put_buffer(buffer);
            eos_warning("failed to read remote file for recovery msg='%s'",
                        status.ToString().c_str());
            mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                             "status='%s' hint='failed to read remote file for recovery'",
                                             status.ToString().c_str()));
            ::close(fd);

            if (req && end_flush(req)) {
              eos_warning("failed to signal end-flush");
            }

            proxy->CleanWriteQueue();
            return EREMOTEIO;
          } else {
            off += bytesRead;
          }

          ssize_t wr = ::write(fd, buf, bytesRead);

          if (wr != bytesRead) {
            sBufferManager.put_buffer(buffer);
            eos_crit("failed to write to local stage file %s", stagefile.c_str());
            ::close(fd);

            if (req && end_flush(req)) {
              eos_warning("failed to signal end-flush");
            }

            proxy->CleanWriteQueue();
            return EREMOTEIO;
          }
        } while (bytesRead > 0);
      }
    }

    // upload into identical inode using the drop & replace option (repair flag)
    XrdCl::Proxy* uploadproxy = new XrdCl::Proxy();
    uploadproxy->inherit_attached(proxy);
    uploadproxy->inherit_writequeue(proxy);

    // we have to remove the flush otherwise we cannot open this file even ourselfs
    if (req && end_flush(req)) {
      eos_warning("failed to signal begin-flush");
    }

    // add the repair flag to drop existing locations and select new ones
    mRemoteUrlRW += "&eos.repair=1";
    eos_warning("re-opening with repair flag for recovery %s",
                mRemoteUrlRW.c_str());
    int rc = try_wopen(req, uploadproxy, mRemoteUrlRW);
    mRemoteUrlRW.erase(mRemoteUrlRW.length() -
                       std::string("&eos.repair=1").length());

    // put back the flush indicator
    if (req && begin_flush(req)) {
      eos_warning("failed to signal begin-flush");
    }

    if (rc) {
      if (!recover_truncate) {
        sBufferManager.put_buffer(buffer);
      }

      ::close(fd);
      {
	eos::common::RWMutexWriteLock wLock(XrdCl::Proxy::gDeleteMutex);
	delete uploadproxy;
      }

      if (req && end_flush(req)) {
        eos_warning("failed to signal end-flush");
      }

      proxy->CleanWriteQueue();
      return rc;
    }

    off_t upload_offset = 0;

    if (!recover_truncate) {
      ssize_t nr = 0;

      do {
        nr = ::pread(fd, buf, size, upload_offset);

        if (nr < 0) {
          sBufferManager.put_buffer(buffer);
          eos_crit("failed to read from local stagefile");
          ::close(fd);

          if (req && end_flush(req)) {
            eos_warning("failed to signal end-flush");
          }

          sBufferManager.put_buffer(buffer);
	  {
	    eos::common::RWMutexWriteLock wLock(XrdCl::Proxy::gDeleteMutex);
	    delete uploadproxy;
	  }

          if (req && end_flush(req)) {
            eos_warning("failed to signal end-flush");
          }

          proxy->CleanWriteQueue();
          return EREMOTEIO;
        }

        if (nr) {
          // send asynchronous upstream writes
          XrdCl::Proxy::write_handler handler = uploadproxy->WriteAsyncPrepare(nr,
                                                upload_offset, 60);
          uploadproxy->ScheduleWriteAsync(buf, handler);
          upload_offset += nr;
        }
      } while (nr > 0);

      ::close(fd);
      // collect the writes to verify everything is alright now
      uploadproxy->WaitWrite(req);

      if (!uploadproxy->write_state().IsOK()) {
        sBufferManager.put_buffer(buffer);
        eos_crit("got failure when collecting outstanding writes from the upload proxy");
	{
	  eos::common::RWMutexWriteLock wLock(XrdCl::Proxy::gDeleteMutex);
	  delete uploadproxy;
	}

        if (req && end_flush(req)) {
          eos_warning("failed to signal end-flush");
        }

        proxy->CleanWriteQueue();
        return EREMOTEIO;
      }

      mRecoveryStack.push_back(eos_log(LOG_SILENT, "uploaded-bytes=%lu",
                                       upload_offset));
      sBufferManager.put_buffer(buffer);
    }

    eos_notice("finished write recovery successfully");
    // replace the proxy object
    mFile->set_xrdiorw(req, uploadproxy);
    proxy->detach();

    // replay the journal
    if (mFile->journal()) {
      if ((rc = journalflush(req))) {
        mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                         "errno='%d' hint='failed journalflush'",
                                         rc));
        eos_err("journal-flushing failed rc=%d", rc);
        return rc;
      } else {
        mRecoveryStack.push_back(eos_log(LOG_SILENT, "hint='success journalflush'"));
      }
    }

    // re-open the file centrally for access
    if (req && end_flush(req)) {
      eos_warning("failed to signal end-flush");
    }

    // once all callbacks are there, this object can destroy itself since we don't track it anymore
    proxy->flag_selfdestructionTS();

    if (!proxy->IsWaitWrite() && !proxy->IsOpening() && !proxy->IsClosing()) {
      proxy->CheckSelfDestruction();
    }
  } else {
    eos_crit("no local cache data for recovery");
    proxy->CleanWriteQueue();
    return EREMOTEIO;
  }

  return 0;
}

/* -------------------------------------------------------------------------- */
int
data::datax::begin_flush(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  return EosFuse::Instance().mds.begin_flush(req, mMd,
         std::string("repair")); // flag an ongoing flush centrally
}

/* -------------------------------------------------------------------------- */
int
data::datax::end_flush(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  return EosFuse::Instance().mds.end_flush(req, mMd,
         std::string("repair")); // unflag an ongoing flush centrally
}

/* -------------------------------------------------------------------------- */
int
data::datax::detach(fuse_req_t req, std::string& cookie, int flags)
/* -------------------------------------------------------------------------- */
{
  bool isRW = false;

  if (flags & (O_RDWR | O_WRONLY)) {
    isRW = true;
  }

  eos_info("cookie=%s flags=%o isrw=%d", cookie.c_str(), flags, isRW);
  int rflush = flush(req);
  XrdSysMutexHelper lLock(mLock);
  int bcache = mFile->file() ? mFile->file()->detach(cookie) : 0;
  int jcache = mFile->journal() ? ((isRW ||
                                    (mFlags & O_CACHE)) ? mFile->journal()->detach(
                                     cookie) : 0) : 0;
  int xio = 0;

  if (isRW) {
    if (mFile->has_xrdiorw(req)) {
      mFile->xrdiorw(req)->detach();
    }
  } else {
    if (mFile->has_xrdioro(req)) {
      mFile->xrdioro(req)->detach();
    }
  }

  return rflush | bcache | jcache | xio;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::store_cookie(std::string& cookie)
/* -------------------------------------------------------------------------- */
{
  eos_info("cookie=%s", cookie.c_str());
  int bc = mFile->file() ? mFile->file()->set_cookie(cookie) : 0;
  int jc = mFile->journal() ? mFile->journal()->set_cookie(cookie) : 0;
  return bc | jc;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::unlink(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  cachehandler::instance().rm(mIno);
  int bcache = mFile->file() ? mFile->file()->unlink() : 0;
  int jcache = mFile->journal() ? mFile->journal()->unlink() : 0;
  mIsUnlinked = true;
  FlagDeleted();
  return bcache | jcache;
}
// IO bridge interface

/* -------------------------------------------------------------------------- */
ssize_t
/* -------------------------------------------------------------------------- */
data::datax::pread(fuse_req_t req, void* buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  eos_info("offset=%llu count=%lu", offset, count);
  mLock.Lock();

  if (mFile->journal()) {
    ssize_t jts = ((mFile->journal()))->get_truncatesize();

    if (jts >= 0) {
      // reduce reads in case of truncation stored in the journal
      if ((ssize_t) offset > jts) {
        offset = 0;
        count = 0;
      } else {
        if ((ssize_t)(offset + count) > jts) {
          count = jts - offset;
        }
      }
    }
  }

  if (inline_buffer && inlined() &&
      (count + offset) < mInlineMaxSize) {
    // possibly return data from an inlined buffer
    ssize_t avail_bytes = 0;

    if (((size_t) offset < mMd->size())) {
      if ((offset + count) > mMd->size()) {
        avail_bytes = mMd->size() - offset;
      } else {
        avail_bytes = count;
      }
    } else {
      avail_bytes = 0;
    }

    memcpy(buf, inline_buffer->ptr() + offset, avail_bytes);
    return avail_bytes;
  }

  ssize_t br = 0;

  if (mFile->file()) {
    // read from file start cache
    br = mFile->file()->pread(buf, count, offset);
  }

  if (br < 0) {
    mLock.UnLock();
    return br;
  }

  if (br == (ssize_t) count) {
    mLock.UnLock();
    return br;
  }

  if (mFile->file() && (offset < mFile->file()->prefetch_size())) {
    mLock.UnLock();

    if (prefetch(req)) {
      WaitPrefetch(req);
      mLock.Lock();
      ssize_t br = mFile->file()->pread(buf, count, offset);

      if (br < 0) {
        return br;
      }

      if (br == (ssize_t) count) {
        return br;
      }
    } else {
      mLock.Lock();
    }
  }

  ssize_t jr = 0;

  if (mFile->journal()) {
    // read from journal cache
    jr = mFile->journal() ? mFile->journal()->pread((char*) buf + br, count - br,
         offset + br) : 0;
  }

  if (jr < 0) {
    mLock.UnLock();
    return jr;
  }

  if ((br + jr) == (ssize_t) count) {
    mLock.UnLock();
    return (br + jr);
  }

  // read the missing part remote
  XrdCl::Proxy* proxy = mFile->has_xrdioro(req) ? mFile->xrdioro(
                          req) : mFile->xrdiorw(req);
  XrdCl::XRootDStatus status;

  if (proxy) {
    if (proxy->IsOpening()) {
      status = proxy->WaitOpen();
    }

    if (!mFile->is_caching()) {
      // if caching is disabled, we wait for outstanding writes
      status = proxy->WaitWrite();
      // it is not obvious what we should do if there was a write error,
      // we just proceed
    }

    uint32_t bytesRead = 0;

    if (proxy->Read(offset + br,
                    count - br,
                    (char*) buf + br,
                    bytesRead).IsOK()) {
      mLock.UnLock();
      std::vector<journalcache::chunk_t> chunks;

      if (mFile->journal()) {
        // retrieve all journal chunks matching our range
        chunks = ((mFile->journal()))->get_chunks(offset + br, count - br);

        for (auto it = chunks.begin(); it != chunks.end(); ++it) {
          eos_err("offset=%ld count=%lu overlay-chunk offset=%ld size=%lu\n", offset,
                  count, it->offset, it->size);
          // overlay journal contents again over the remote contents
          ssize_t ljr = mFile->journal()->pread((char*) buf + br +
                                                (it->offset - offset - br), it->size, it->offset);

          if (ljr >= 0) {
            // check if the journal contents extends the remote read
            ssize_t chunkread = it->offset + it->size - offset - br;

            if (chunkread > bytesRead) {
              bytesRead = chunkread;
            }
          }
        }

        if (mFile->journal()) {
          eos_info("offset=%ld count=%lu journal-max=%ld\n", offset, count,
                   mFile->journal()->get_max_offset());

          // check if there is a chunk in the journal which extends the file size,
          // so we have to extend the read
          if (mFile->journal()->get_max_offset() > (off_t)(offset + br + bytesRead)) {
            if (mFile->journal()->get_max_offset() > (off_t)(offset  + count)) {
              // the last journal entry extends over the requested range, we got all bytes
              bytesRead = count;
            } else {
              //  this should not be required, because logically we cannot get here
              eos_err("consistency error : max-journal=%ld offset=%ld count=%lu br=%lu bytesread=%lu",
                      mFile->journal()->get_max_offset(), offset, count, br, bytesRead);
              // we don't set bytesread here!
            }
          }
        }
      }

      eos_info("count=%lu read-bytes=%lu", count, br + bytesRead);

      if ((size_t)(br + bytesRead)  > count) {
        return count;
      } else {
        return (br + bytesRead);
      }

      return (br + bytesRead);
    } else {
      mLock.UnLock();
      errno = EREMOTEIO;
      // IO error
      return -1;
    }
  }

  mLock.UnLock();
  errno = EFAULT;
  return -1;
}

/* -------------------------------------------------------------------------- */
ssize_t
/* -------------------------------------------------------------------------- */
data::datax::pwrite(fuse_req_t req, const void* buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(mLock);
  eos_info("offset=%llu count=%lu", offset, count);
  ssize_t dw = 0;

  // inlined-files
  if (inline_buffer) {
    if ((count + offset) < mInlineMaxSize) {
      // copy into file inline buffer
      inline_buffer->writeData(buf, offset, count);
    }
  }

  if (mFile->file()) {
    if (mFile->file()->size() || (mFlags & O_CREAT)) {
      // don't write into the file start cache, if it is currently empty and it is not a newly created file
      dw = mFile->file()->pwrite(buf, count, offset);
    }
  }

  if (dw < 0) {
    return dw;
  } else {
    if (mFile->journal()) {
      if (!mFile->journal()->fits(count)) {
        int rc = flush_nolock(req, true, true);

        if (rc) {
          eos_warning("flush failed with errno=%d", rc);
          errno = rc;
          return -1;
        }
      }

      // now there is space to write for us
      ssize_t jw = mFile->journal()->pwrite(buf, count, offset);

      if (jw < 0) {
        return jw;
      }

      dw = jw;
    }

    {
      // stop sending more writes in case of unrecoverable errors
      XrdCl::Proxy* proxy = mFile->xrdiorw(req);

      // block writes on read-only fds
      if (!proxy) {
        errno = EROFS;
        return -1;
      }

      if (proxy->opening_state().IsError() &&
          ! proxy->opening_state_should_retry()) {
        eos_err("unrecoverable error - code=%d errNo=%d",
                proxy->opening_state().code,
                proxy->opening_state().errNo);
        proxy->CleanWriteQueue();
        errno = XrdCl::Proxy::status2errno(proxy->opening_state());
        return -1;
      }
    }

    // send an asynchronous upstream write, which does not wait for the file open to be done
    XrdCl::Proxy::write_handler handler =
      mFile->xrdiorw(req)->WriteAsyncPrepare(count, offset, 60);
    XrdCl::XRootDStatus status =
      mFile->xrdiorw(req)->ScheduleWriteAsync(buf, handler);
    // test if we switch to xoff mode, where we only write into the journal
    size_t cnt = 0;

    while (mFile->xrdiorw(req)->HasTooManyWritesInFlight()) {
      if (!(cnt % 1000)) {
        eos_debug("doing XOFF");
      }

      EosFuse::instance().datas.set_xoff();
      mXoff = true;
      std::string msg;

      if (mFile->xrdiorw(req)->HadFailures(msg)) {
        eos_err("file state failure during xoff - switching to sync mode msg='%s'",
                msg.c_str());
        // if we had failures we change into synchronous mode to be able to trigger appropriate recovery
        mFlags |= O_SYNC;
        break;
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      cnt++;
    }

    mXoff = false;

    if ((!status.IsOK()) && (!EosFuse::Instance().Config().recovery.write)) {
      errno = XrdCl::Proxy::status2errno(status);
      eos_err("async remote-io failed msg=\"%s\"", status.ToString().c_str());
      return -1;
    }

    if (mFlags & O_SYNC) {
      eos_debug("O_SYNC");
      // make sure the file gets opened
      XrdCl::XRootDStatus status = mFile->xrdiorw(req)->WaitOpen();

      if (!status.IsOK()) {
        mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                         "status='%s' hint='will TryRecovery'",
                                         status.ToString().c_str()));
        int tret = 0;

        if ((tret = TryRecovery(req, true))) {
          errno = XrdCl::Proxy::status2errno(status);
          eos_err("pseudo-sync remote-io failed msg=\"%s\"", status.ToString().c_str());
          mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                           "status='%s' errno='%d' hint='failed TryRecovery'",
                                           status.ToString().c_str(), tret));
          return -1;
        } else {
          mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                           "triggering-status='%s' hint='success TryRecovery'",
                                           status.ToString().c_str()));
          // re-send the write again
          XrdCl::Proxy::write_handler handler =
            mFile->xrdiorw(req)->WriteAsyncPrepare(count, offset, 60);
          XrdCl::XRootDStatus status =
            mFile->xrdiorw(req)->ScheduleWriteAsync(buf, handler);
        }
      }

      // make sure all writes were successful
      status = mFile->xrdiorw(req)->WaitWrite();

      if (!status.IsOK()) {
        mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                         "status='%s' hint='will TryRecovery'",
                                         status.ToString().c_str()));
        int tret = 0;

        if ((tret = TryRecovery(req, true))) {
          errno = XrdCl::Proxy::status2errno(status);
          eos_err("pseudo-sync remote-io failed msg=\"%s\"", status.ToString().c_str());
          mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                           "status='%s' errno='%d' hint='failed TryRecovery'",
                                           status.ToString().c_str(), tret));
          return -1;
        } else {
          mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                           "triggering-status='%s' hint='success TryRecovery'",
                                           status.ToString().c_str()));
          // re-send the write again
          XrdCl::Proxy::write_handler handler =
            mFile->xrdiorw(req)->WriteAsyncPrepare(count, offset, 60);
          XrdCl::XRootDStatus status =
            mFile->xrdiorw(req)->ScheduleWriteAsync(buf, handler);
          status = mFile->xrdiorw(req)->WaitWrite();

          if (!status.IsOK()) {
            errno = XrdCl::Proxy::status2errno(status);
            eos_err("pseudo-sync remote-io failed msg=\"%s\"", status.ToString().c_str());
            mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                             "status='%s' hint='failed resending writes after successful recovery'",
                                             status.ToString().c_str()));
            return -1;
          }
        }
      }
    }
  }

  if ((off_t)(offset + count) > mSize) {
    mSize = count + offset;
  }

  eos_info("offset=%llu count=%lu result=%d", offset, count, dw);
  return dw;
}

/* -------------------------------------------------------------------------- */
ssize_t
/* -------------------------------------------------------------------------- */
data::datax::peek_pread(fuse_req_t req, char*& buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  mLock.Lock();
  eos_info("offset=%llu count=%lu size=%lu", offset, count, mMd->size());

  if (mFile->journal()) {
    ssize_t jts = ((mFile->journal()))->get_truncatesize();

    if (jts >= 0) {
      // reduce reads in case of truncation stored in the journal
      if ((ssize_t) offset > jts) {
        offset = 0;
        count = 0;
      } else {
        if ((ssize_t)(offset + count) > jts) {
          count = jts - offset;
        }
      }
    }
  }

  buffer = sBufferManager.get_buffer(count);
  buf = buffer->ptr();

  if (inline_buffer && inlined()) {
    // possibly return data from an inlined buffer
    ssize_t avail_bytes = 0;

    if (((size_t) offset < mMd->size())) {
      if ((offset + count) > mMd->size()) {
        avail_bytes = mMd->size() - offset;
      } else {
        avail_bytes = count;
      }
    } else {
      avail_bytes = 0;
    }

    if (mMd->size() <= (unsigned long long) inline_buffer->getSize()) {
      memcpy(buf, inline_buffer->ptr() + offset, avail_bytes);
      eos_debug("inline-read byte=%lld inline-buffer-size=%lld", avail_bytes,
                inline_buffer->getSize());
      return avail_bytes;
    }
  }

  ssize_t br = 0;

  if (mFile->file()) {
    br = mFile->file()->pread(buf, count, offset);

    if (EOS_LOGS_DEBUG) {
      eos_debug("disk-read:%ld", br);
    }

    if (br < 0) {
      return br;
    }

    if ((br == (ssize_t) count) || (br == (ssize_t) mMd->size())) {
      return br;
    }
  }

  if (mFile->file() && (offset < mFile->file()->prefetch_size())) {
    if (prefetch(req, false)) {
      WaitPrefetch(req, false);
      ssize_t br = mFile->file()->pread(buf, count, offset);

      if (br < 0) {
        return br;
      }

      if (br == (ssize_t) count) {
        if (mFile->journal() && (mFlags & O_CACHE)) {
          // optionally populate the read journal cache
          mFile->journal()->pwrite(buf, count, offset);
        }

        return br;
      }
    }
  }

  ssize_t jr = 0;

  if (mFile->journal()) {
    jr = mFile->journal() ? mFile->journal()->pread(buf + br, count - br,
         offset + br) : 0;

    if (jr < 0) {
      return jr;
    }

    if ((br + jr) == (ssize_t) count) {
      return (br + jr);
    }
  }

  // read the missing part remote
  XrdCl::Proxy* proxy = mFile->has_xrdioro(req) ? mFile->xrdioro(
                          req) : mFile->xrdiorw(req);
  XrdCl::XRootDStatus status;
  eos_debug("ro=%d offset=%llu count=%lu br=%lu jr=%lu", mFile->has_xrdioro(req),
            offset, count, br, jr);

  if (proxy) {
    if (proxy->IsOpening()) {
      status = proxy->WaitOpen();
    }

    if (mFile->has_xrdiorw(req)) {
      if (!status.IsOK()) {
        mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                         "status='%s' hint='will TryRecovery'",
                                         status.ToString().c_str()));
        int tret = 0;

        // call recovery for an open
        if ((tret = TryRecovery(req, false))) {
          mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                           "status='%s' errno='%d' hint='failed TryRecovery'",
                                           status.ToString().c_str(), tret));
          errno = XrdCl::Proxy::status2errno(status);
          eos_err("sync remote-io failed msg=\"%s\"", status.ToString().c_str());
          return -1;
        } else {
          mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                           "triggering-status='%s' hint='success TryRecovery'",
                                           status.ToString().c_str()));
          // get the new proxy object, the recovery might exchange the file object
          proxy = mFile->has_xrdioro(req) ? mFile->xrdioro(req) : mFile->xrdiorw(
                    req); // recovery might change the proxy object
        }
      }
    }

    if (mFile->has_xrdiorw(req)) {
      XrdCl::Proxy* wproxy = mFile->xrdiorw(req);

      if (wproxy->OutstandingWrites()) {
        status = wproxy->WaitWrite();
      }

      if (!status.IsOK()) {
        errno = XrdCl::Proxy::status2errno(status);
        eos_err("sync remote-io failed msg=\"%s\"", status.ToString().c_str());
        return -1;
      }
    }

    uint32_t bytesRead = 0;
    int recovery = 0;

    while (1) {
      // if the recovery failed already once, we continue to silently return this error
      if (!can_recover_read()) {
        errno = XrdCl::Proxy::status2errno(status);

        if (EOS_LOGS_DEBUG) {
          eos_debug("sync remote-io failed msg=\"%s\" previously - recovery disabled",
                    status.ToString().c_str());
        }

        return -1;
      }

      proxy = mFile->has_xrdioro(req) ? mFile->xrdioro(req) : mFile->xrdiorw(
                req); // recovery might change the proxy object
      status = proxy->Read(offset + br + jr,
                           count - br - jr,
                           (char*) buf + br + jr,
                           bytesRead);

      if (!status.IsOK()) {
        // read failed
        mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                         "status='%s' hint='will TryRecovery'",
                                         status.ToString().c_str()));
        recovery = TryRecovery(req, false);

        if (recovery) {
          // recovery failed
          mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                           "status='%s' errno='%d' hint='failed TryRecovery'",
                                           status.ToString().c_str(), recovery));
          break;
        } else {
          // recovery succeeded
          mRecoveryStack.push_back(eos_log(LOG_SILENT,
                                           "triggering-status='%s' hint='success TryRecovery'",
                                           status.ToString().c_str()));
        }
      } else {
        // read succeeded
        break;
      }
    }

    if (recovery) {
      errno = recovery;
      disable_read_recovery();
      eos_err("sync remote-io recovery failed errno=%d", errno);
      return -1;
    }

    if (status. IsOK()) {
      std::vector<journalcache::chunk_t> chunks;

      if (mFile->journal()) {
        // retrieve all journal chunks matching our range
        chunks = ((mFile->journal()))->get_chunks(offset + br , count - br);

        for (auto it = chunks.begin(); it != chunks.end(); ++it) {
          eos_info("offset=%ld count=%lu overlay-chunk offset=%ld size=%lu", offset,
                   count, it->offset, it->size);
          // overlay journal contents again over the remote contents
          ssize_t ljr = mFile->journal()->pread((char*) buf + br +
                                                (it->offset - offset - br), it->size, it->offset);

          if (ljr >= 0) {
            // check if the journal contents extends the remote read
            ssize_t chunkread = it->offset + it->size - offset - br;

            if (chunkread > bytesRead) {
              bytesRead = chunkread;
            }
          }
        }

        eos_info("offset=%ld count=%lu bytes-read=%lu journal-max=%ld\n", offset, count,
                 bytesRead, mFile->journal()->get_max_offset());

        // check if there is a chunk in the journal which extends the file size,
        // so we have to extend the read
        if (mFile->journal()->get_max_offset() > (off_t)(offset + br + bytesRead)) {
          if (mFile->journal()->get_max_offset() > (off_t)(offset  + count)) {
            // the last journal entry extends over the requested range, we got all bytes
            bytesRead = count;
          } else {
            //  this should not be required, because logically we cannot get here
            bytesRead = mFile->journal()->get_max_offset() - offset;
          }
        }
      }

      eos_info("count=%lu read-bytes=%lu", count, br + bytesRead);

      if (mFile->journal() && (mFlags & O_CACHE)) {
        // optionally populate the read journal cache
        mFile->journal()->pwrite(buf, br + bytesRead, offset);
      }

      if ((size_t)(br + bytesRead)  > count) {
        return count;
      } else {
        return (br + bytesRead);
      }
    } else {
      errno = XrdCl::Proxy::status2errno(status);
      eos_err("sync remote-io failed msg=\"%s\"", status.ToString().c_str());
      return -1;
    }
  }

  return -1;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::datax::release_pread()
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  sBufferManager.put_buffer(buffer);
  buffer.reset();
  mLock.UnLock();
  return;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::truncate(fuse_req_t req, off_t offset)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(mLock);
  eos_info("offset=%llu size=%llu", offset, mSize);
  int dt = 0;

  if (inline_buffer) {
    if (inlined()) {
      if (((size_t) offset) < mInlineMaxSize) {
        // truncate file inline buffer
        inline_buffer->truncateData(offset);
      }
    } else {
      if (offset == 0) {
        // we can re-enable the inlining for such a file
        inline_buffer->truncateData(0);
        mIsInlined = true;
      }
    }
  }

  if (mFile->file()) {
    if (offset <= mFile->file()->prefetch_size()) {
      // if the truncate falls into the file cache size, we have disable it because
      // subsequent writes can stamp a whole inside the file cache
      dt = mFile->file()->truncate(0);
      remove_file_cache();
    }
  }

  // if we have a journal it tracks the truncation size
  int jt = 0;

  if (mFile->journal()) {
    jt = mFile->journal() ? mFile->journal()->truncate(offset) : 0;
  }

  eos_info("dt=%d jt=%d", dt, jt);

  if (!mFile->journal()) {
    if (mFile->has_xrdiorw(req)) {
      if (mFile->xrdiorw(req)->IsOpening()) {
        mFile->xrdiorw(req)->WaitOpen();
      }

      mFile->xrdiorw(req)->WaitWrite();
      // the journal keeps track of truncation, otherwise or for O_SYNC we do it here
      XrdCl::XRootDStatus status = mFile->xrdiorw(req)->Truncate(offset);
      errno = XrdCl::Proxy::status2errno(status);

      if (!status.IsOK()) {
        return -1;
      }
    } else {
      errno = EFAULT;
      return -1;
    }
  }

  if (!(dt | jt)) {
    mSize = offset;
  }

  return dt | jt;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::sync()
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  int ds = 0;

  if (mFile->file()) {
    ds = mFile->file()->sync();
  }

  int js = 0;

  if (mFile->journal()) {
    js = mFile->journal() ? mFile->journal()->sync() : 0;
  }

  bool journal_recovery = false;

  for (auto it = mFile->get_xrdiorw().begin();
       it != mFile->get_xrdiorw().end(); ++it) {
    if (it->second->IsOpening()) {
      it->second->WaitOpen();
    }

    XrdCl::XRootDStatus status = it->second->WaitWrite();

    if (!status.IsOK()) {
      errno = XrdCl::Proxy::status2errno(status);
      journal_recovery = true;
    } else {
      status = it->second->Sync();

      if (!status.IsOK()) {
        errno = XrdCl::Proxy::status2errno(status);
        journal_recovery = true;
      }
    }
  }

  if (journal_recovery) {
    eos_err("syncing failed");
    return -1;
  }

  return ds | js;
}

/* -------------------------------------------------------------------------- */
size_t
/* -------------------------------------------------------------------------- */
data::datax::size()
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  off_t dsize = mFile->file() ? mFile->file()->size() : 0;

  if (mSize > dsize) {
    return mSize;
  }

  return dsize;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::cache_invalidate()
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  XrdSysMutexHelper lLock(mLock);
  // truncate the block cache
  int dt = mFile->file() ? mFile->file()->truncate(0) : 0;
  int jt = mFile->journal() ? mFile->journal()->truncate(0, true) : 0;
  inline_buffer = nullptr;

  for (auto fit = mFile->get_xrdioro().begin();
       fit != mFile->get_xrdioro().end(); ++fit) {
    if (fit->second->IsOpen()) {
      fit->second->DropReadAhead();
    }
  }

  return dt | jt;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::datax::set_remote(const std::string& hostport,
                        const std::string& basename,
                        const uint64_t md_ino,
                        const uint64_t md_pino,
                        fuse_req_t req,
                        bool isRW)
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  std::string remoteurl;
  remoteurl = "root://";
  remoteurl += hostport;
  remoteurl += "//fusex-open";
  remoteurl += "?eos.lfn=";

  if (md_ino) {
    remoteurl += "ino:";
    char sino[128];
    snprintf(sino, sizeof(sino), "%lx", md_ino);
    remoteurl += sino;
  } else {
    remoteurl += "pino:";
    char pino[128];
    snprintf(pino, sizeof(pino), "%lx", md_pino);
    remoteurl += pino;
    remoteurl += "/";
    remoteurl += basename;
  }

  std::string appname;

  if (EosFuse::Instance().mds.supports_appname()) {
    appname =  EosFuse::Instance().Config().appname;
  } else {
    appname = "fuse";
  }

  remoteurl += "&eos.app=";
  remoteurl += appname;
  remoteurl += "&mgm.mtime=0&mgm.fusex=1&eos.bookingsize=0";

  if (!isRW) {
    // we don't check checksums in read, because we might read a file which is open and it does not have
    // a final checksum when we read over the end
    remoteurl += "&eos.checksum=ignore";
  }

  XrdCl::URL url(remoteurl);
  XrdCl::URL::ParamsMap query = url.GetParams();
  fusexrdlogin::loginurl(url, query, req, md_ino);
  url.SetParams(query);
  remoteurl = url.GetURL();

  if (isRW) {
    mRemoteUrlRW = remoteurl;
  } else {
    mRemoteUrlRO = remoteurl;
  }
}

/* -------------------------------------------------------------------------- */
void
data::datax::dump_recovery_stack()
/* -------------------------------------------------------------------------- */
{
  size_t i = 0;
  char n[8];

  if (mRecoveryStack.size()) {
    std::stringstream sdump;
    sdump << "#      -------------------" << std::endl;
    sdump << "#      - recovery record -" << std::endl;
    sdump << "#      -------------------" << std::endl;
    sdump << "#        path := '" << fullpath() << "'" << std::endl;
    sdump << "#        fid  := " << fid() << std::endl;

    for (auto it : mRecoveryStack) {
      snprintf(n, sizeof(n), "%03lu", i);
      sdump << "#        -[ " << n << " ] " << it << std::endl;
      ++i;
    }

    fprintf(stderr, "%s\n", sdump.str().c_str());
    fflush(stderr);
  }
}

/* -------------------------------------------------------------------------- */
const char*
data::datax::Dump(std::string& out)
/* -------------------------------------------------------------------------- */
{
  for (auto fit = mFile->get_xrdioro().begin();
       fit != mFile->get_xrdioro().end(); ++fit) {
    fit->second->Dump(out);
  }

  for (auto fit = mFile->get_xrdiorw().begin();
       fit != mFile->get_xrdiorw().end(); ++fit) {
    fit->second->Dump(out);
  }

  return out.c_str();
}


/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
data::dmap::waitflush(uint64_t seconds)
{
  // wait that all pending data is flushed for 'seconds'
  // if all is flushed, it returns true, otherwise false
  for (uint64_t i = 0; i < seconds; ++i) {
    size_t nattached = 0;
    {
      XrdSysMutexHelper mLock(this);
      nattached = this->size();
    }

    if (nattached) {
      eos_static_warning("[ waiting data to be flushed for %03d io objects] [ %d of %d seconds ]",
                         nattached, i, seconds);
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    } else {
      eos_static_warning("[ all data flushed ]");
      return true;
    }
  }

  eos_static_warning("[ data flush timed out after %d seconds ]", seconds);
  return false;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::dmap::ioflush(ThreadAssistant& assistant)
/* -------------------------------------------------------------------------- */
{
  while (!assistant.terminationRequested()) {
    {
      //eos_static_debug("");
      std::vector<shared_data> data;
      {
        // avoid mutex contention
        XrdSysMutexHelper mLock(this);

        for (auto it = this->begin(); it != this->end(); ++it) {
          if (it->second) {
            data.push_back(it->second);
          }
        }
      }

      for (auto it = data.begin(); it != data.end(); ++it) {
        XrdSysMutexHelper lLock((*it)->Locker());
        eos_static_info("dbmap-in %#lx => %lx", (*it)->id(), &(*it));
      }

      for (auto it = data.begin(); it != data.end(); ++it) {
        {
          XrdSysMutexHelper lLock((*it)->Locker());
          eos_static_info("dbmap-in => ino:%16lx %lx attached=%d", (*it)->id(), &(*it),
                          (*it)->attached_nolock());

          if (!(*it)->attached_nolock()) {
            // files which are detached might need an upstream sync
            bool repeat = true;

            while (repeat) {
              // close all readers in async fashion
              std::map<std::string, XrdCl::Proxy*>& rmap = (*it)->file()->get_xrdioro();

              for (auto fit = rmap.begin();
                   fit != rmap.end();) {
                if (!fit->second) {
                  fit++;
                  continue;
                }

                if (fit->second->IsOpening() || fit->second->IsClosing()) {
                  eos_static_info("skipping xrdclproxyrw state=%d %d", fit->second->stateTS(),
                                  fit->second->IsClosed());
                  // skip files which are opening or closing
                  fit++;
                  continue;
                }

                if (fit->second->IsOpen()) {
                  // close read-only file if longer than 1s open
                  if ((fit->second->state_age() > 1.0)) {
                    // closing read-only file
                    fit->second->CloseAsync();
                    eos_static_info("closing reader");
                    fit++;
                    continue;
                  }
                }

                if (fit->second->IsOpening() || fit->second->IsClosing()) {
                  // skip if its neither opened nor closed
                  fit++;
                  continue;
                }

                if (fit->second->IsClosed()) {
                  if (fit->second->DoneReadAhead()) {
		    {
		      eos::common::RWMutexWriteLock wLock(XrdCl::Proxy::gDeleteMutex);
		      delete fit->second;
		    }
                    fit = (*it)->file()->get_xrdioro().erase(fit);
                    eos_static_info("deleting reader");
                    continue;
                  }
                }

                fit++;
              }

              std::map<std::string, XrdCl::Proxy*>& map = (*it)->file()->get_xrdiorw();

              for (auto fit = map.begin();
                   fit != map.end(); ++fit) {
                if (!fit->second) {
                  continue;
                }

                if (fit->second->IsOpening() || fit->second->IsClosing()) {
                  eos_static_info("skipping xrdclproxyrw state=%d %d", fit->second->stateTS(),
                                  fit->second->IsClosed());
                  // skip files which are opening or closing
                  break;
                }

                if (fit->second->IsOpen()) {
                  eos_static_info("skip flushing journal for req=%s id=%#lx", fit->first.c_str(),
                                  (*it)->id());
                  // flush the journal using an asynchronous thread pool
                  // skipped: (*it)->journalflush_async(fit->first);
                  fit->second->set_state_TS(XrdCl::Proxy::WAITWRITE);
                  eos_static_info("changing to wait write state");
                }

                if (fit->second->IsWaitWrite()) {
                  if (!fit->second->OutstandingWrites()) {
                    if ((fit->second->state_age() > 1.0) &&
                        !EosFuse::Instance().mds.has_flush((*it)->id())) {
                      std::string msg;

                      // check if we need to run a recovery action
                      if ((fit->second->HadFailures(msg) ||
                           ((*it)->simulate_write_error_in_flusher()))) {
                        (*it)->recoverystack().push_back
                        (eos_static_log(LOG_SILENT, "status='%s' hint='will TryRecovery'",
                                        msg.c_str()));
                        int tret = 0;

                        if (!(tret = (*it)->TryRecovery(0, true))) {
                          (*it)->recoverystack().push_back
                          (eos_static_log(LOG_SILENT, "hint='success TryRecovery'"));
                          int jret = 0;

                          if ((jret = (*it)->journalflush(fit->first))) {
                            eos_static_err("ino:%16lx recovery failed", (*it)->id());
                            (*it)->recoverystack().push_back
                            (eos_static_log(LOG_SILENT, "errno='%d' hint='failed journalflush'", jret));
                          } else {
                            (*it)->recoverystack().push_back
                            (eos_static_log(LOG_SILENT, "hint='success journalflush'"));
                          }
                        } else {
                          (*it)->recoverystack().push_back
                          (eos_static_log(LOG_SILENT, "errno='%d' hint='failed TryRecovery", tret));
                        }
                      }

                      eos_static_info("changing to close async state - age = %f ino:%16lx has-flush=%s",
                                      fit->second->state_age(), (*it)->id(),
                                      EosFuse::Instance().mds.has_flush((*it)->id()) ? "true" : "false");
                      fit->second->CloseAsync();
                      break;
                    } else {
                      if (fit->second->state_age() < 1.0) {
                        eos_static_info("waiting for right age before async close - age = %f ino:%16lx has-flush=%s",
                                        fit->second->state_age(), (*it)->id(),
                                        EosFuse::Instance().mds.has_flush((*it)->id()) ? "true" : "false");
                      } else {
                        eos_static_info("waiting for flush before async close - age = %f ino:%16lx has-flush=%s",
                                        fit->second->state_age(), (*it)->id(),
                                        EosFuse::Instance().mds.has_flush((*it)->id()) ? "true" : "false");
                      }

                      break;
                    }
                  }
                }

                if (!fit->second->IsClosed()) {
                  break;
                }

                {
                  std::string msg;

                  if ((!(*it)->unlinked()) && fit->second->HadFailures(msg)) {
                    // let's see if the initial OpenAsync got a timeout, this we should retry always
                    XrdCl::XRootDStatus status = fit->second->opening_state();
                    bool rescue = true;

                    if (
                      (status.code == XrdCl::errConnectionError) ||
                      (status.code == XrdCl::errSocketTimeout) ||
                      (status.code == XrdCl::errOperationExpired) ||
                      (status.code == XrdCl::errSocketDisconnected)) {
                      // retry the open
                      eos_static_warning("re-issuing OpenAsync request after timeout - ino:%16lx err-code:%d",
                                         (*it)->id(), status.code);
                      // to recover this errors XRootD requires new XrdCl::File object ... sigh ...
                      XrdCl::Proxy* newproxy = new XrdCl::Proxy();
                      newproxy->OpenAsync(fit->second->url(), fit->second->flags(),
                                          fit->second->mode(), 0);
                      newproxy->inherit_attached(fit->second);
                      newproxy->inherit_protocol(fit->second);
		      {
			eos::common::RWMutexWriteLock wLock(XrdCl::Proxy::gDeleteMutex);
			delete(fit->second);
		      }
                      map[fit->first] = newproxy;
                      continue;
                    } else {
                      eos_static_warning("OpenAsync failed - trying recovery - ino:%16lx err-code:%d",
                                         (*it)->id(), status.code);

                      if (status.errNo == kXR_noserver) {
                        int tret = 0;

                        if (!(tret = (*it)->TryRecovery(0, true))) {
                          (*it)->recoverystack().push_back
                          (eos_static_log(LOG_SILENT, "hint='success TryRecovery'"));
                          int jret = 0;

                          if ((jret = (*it)->journalflush(fit->first))) {
                            eos_static_err("ino:%16lx recovery failed", (*it)->id());
                            (*it)->recoverystack().push_back
                            (eos_static_log(LOG_SILENT, "errno='%d' hint='failed journalflush'", jret));
                          } else {
                            (*it)->recoverystack().push_back
                            (eos_static_log(LOG_SILENT, "hint='success journalflush'"));
                            continue;
                          }
                        } else {
                          (*it)->recoverystack().push_back
                          (eos_static_log(LOG_SILENT, "errno='%d' hint='failed TryRecovery", tret));
                        }
                      }

                      eos_static_warning("giving up OpenAsync request - ino:%16lx err-code:%d",
                                         (*it)->id(), status.code);

                      if (status.errNo == kXR_overQuota) {
                        // don't preserve these files, they got an application error beforehand
                        rescue = false;
                      }
                    }

                    // ---------------------------------------------------------
                    // we really have to avoid this to happen, but
                    // we can put everything we have cached in a save place for
                    // manual recovery and tag the error message
                    // ---------------------------------------------------------

                    if (rescue) {
                      std::string file_rescue_location;
                      std::string journal_rescue_location;
                      int dt = (*it)->file()->file() ? (*it)->file()->file()->rescue(
                                 file_rescue_location) : 0;
                      int jt = (*it)->file()->journal() ? (*it)->file()->journal()->rescue(
                                 journal_rescue_location) : 0;

                      if (!dt || !jt) {
                        const char* cmsg =
                          eos_static_log(LOG_CRIT,
                                         "ino:%16lx msg=%s file-recovery=%s journal-recovery=%s",
                                         (*it)->id(),
                                         msg.c_str(),
                                         (!dt) ? file_rescue_location.c_str() : "<none>",
                                         (!jt) ? journal_rescue_location.c_str() : "<none>");
                        (*it)->recoverystack().push_back(cmsg);
                      }
                    }
                  }

                  eos_static_info("deleting xrdclproxyrw state=%d %d", fit->second->stateTS(),
                                  fit->second->IsClosed());
		  {
		    eos::common::RWMutexWriteLock wLock(XrdCl::Proxy::gDeleteMutex);
		    delete fit->second;
		  }
                  (*it)->file()->get_xrdiorw().erase(fit);
                  break;
                }
              }

              repeat = false;
            }
          }
        }
        XrdSysMutexHelper mLock(this);
        XrdSysMutexHelper lLock((*it)->Locker());

        // re-check that nobody is attached
        if (!(*it)->attached_nolock() && !(*it)->file()->get_xrdiorw().size() &&
            !(*it)->file()->get_xrdioro().size()) {
          eos_static_info("dropping one");
          // here we make the data object unreachable for new clients
          (*it)->detach_nolock();
          cachehandler::instance().rm((*it)->id());
          this->erase((*it)->id());
          this->erase((*it)->id() + 0xffffffff);
        }
      }

      assistant.wait_for(std::chrono::milliseconds(128));
    }
  }
}
