//------------------------------------------------------------------------------
//! @file xrdclproxy.cc
//! @author Andreas-Joachim Peters CERN
//! @brief XrdCl proxy class
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

#include "xrdclproxy.hh"
#include "fusex/misc/fusexrdlogin.hh"
#include "common/Logging.hh"
#include "common/Path.hh"
#include <XrdCl/XrdClXRootDResponses.hh>

using namespace XrdCl;


ssize_t XrdCl::Proxy::sChunkTimeout = 300;

XrdCl::BufferManager XrdCl::Proxy::sWrBufferManager;
XrdCl::BufferManager XrdCl::Proxy::sRaBufferManager;

std::atomic<int> XrdCl::Proxy::sProxy;

std::mutex XrdCl::Proxy::WriteAsyncHandler::gBuffReferenceMutex;
std::map<std::string, uint64_t>
XrdCl::Proxy::WriteAsyncHandler::gBufferReference;

std::mutex XrdCl::Proxy::ReadAsyncHandler::gExpiredChunksMutex;
std::vector<XrdCl::Proxy::read_handler>
XrdCl::Proxy::ReadAsyncHandler::gExpiredChunks;

//------------------------------------------------------------------------------
// Method to crate a new XrdCl::Proxy object with the option to request the
// creation of a new TCP connection for the underlying XrdC::File object.
//------------------------------------------------------------------------------
XrdCl::shared_proxy
XrdCl::Proxy::Factory(const fuse_ctx* ctx, const fuse_id* id)
{
  // If fusex ctx or fuse_id present then we need to force the creation of a
  // new TCP connection for the underlying proxy object
  if (ctx || id) {
    uid_t uid = (ctx ? ctx->uid : id->uid);
    gid_t gid = (ctx ? ctx->gid : id->gid);
    pid_t pid = (ctx ? ctx->pid : id->pid);
    (void) fusexrdlogin::processCache->retrieve(pid, uid, gid, true);
  }

  shared_proxy sp = std::make_shared<Proxy>();
  return sp;
}

XrdCl::Proxy::ProxyStat XrdCl::Proxy::ProxyStatHandle::sProxyStats;

std::shared_ptr<XrdCl::Proxy::ProxyStatHandle>
XrdCl::Proxy::ProxyStatHandle::Get()
{
  return std::make_shared<XrdCl::Proxy::ProxyStatHandle>();
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::Write(uint64_t offset,
                    uint32_t size,
                    const void* buffer,
                    ResponseHandler* handler,
                    uint16_t timeout)
/* -------------------------------------------------------------------------- */
{
  eos_debug("offset=%lu size=%u", offset, size);
  XRootDStatus status = WaitOpen();

  if (!status.IsOK()) {
    return status;
  }

  return File::Write(offset, size, buffer, handler, timeout);
}

/* -------------------------------------------------------------------------- */
XRootDStatus
XrdCl::Proxy::Read(XrdCl::shared_proxy proxy,
                   uint64_t offset,
                   uint32_t size,
                   void* buffer,
                   uint32_t& bytesRead,
                   uint16_t timeout)
/* -------------------------------------------------------------------------- */
{
  eos_debug("offset=%lu size=%u", offset, size);
  XRootDStatus status = WaitOpen();
  bytesRead = 0;

  if (!status.IsOK()) {
    return status;
  }

  eos_debug("----: read: offset=%lu size=%u", offset, size);
  int readahead_window_hit = 0;
  uint64_t current_offset = offset;
  uint32_t current_size = size;
  bool isEOF = false;
  bool request_next = true;
  std::set<uint64_t> delete_chunk;
  std::set<uint64_t> expired_chunk;
  void* pbuffer = buffer;

  if ((off_t)offset == (off_t)mPosition) {
    mSeqDistance += size;
  } else {
    if (!XReadAheadDisabled) {
      // Offset if unsigned so abs might not work properly in case of underflow
      off_t seek_distance = (offset >= (uint64_t)mPosition ? offset - mPosition :
                             mPosition -
                             offset);
      double sparse_ratio = 1.0 * mSeqDistance / (seek_distance + mSeqDistance);

      if (EOS_LOGS_DEBUG) {
        eos_debug("sparse ratio:= %.02f seq-distance=%lu seek-distance=%lu",
                  sparse_ratio, (off_t)mSeqDistance, (off_t)seek_distance);
      }

      mSeqDistance = size;

      if (sparse_ratio && (sparse_ratio < XReadAheadSparseRatio)) {
        eos_notice("sparse ratio:= %.02f seq-distance=%lu seek-distance=%lu - disabling readahead permanently url:'%s'",
                   sparse_ratio, (off_t)mSeqDistance, (off_t)seek_distance,
                   sparse_ratio, mUrl.c_str());
        XReadAheadDisabled = true;
      }
    }
  }

  if ((XReadAheadStrategy != NONE)) {
    ReadCondVar().Lock();
    XReadAheadBlocksIs = 0;

    if (ChunkRMap().size()) {
      auto last_chunk_before_match = ChunkRMap().begin();

      for (auto it = ChunkRMap().begin(); it != ChunkRMap().end(); ++it) {
        // extract all possible data from the read-ahead map
        off_t match_offset;
        uint32_t match_size;
        XrdSysCondVarHelper lLock(it->second->ReadCondVar());

        if (EOS_LOGS_DEBUG) {
          eos_debug("----: eval offset=%lu chunk-offset=%lu rah-position=%lu", offset,
                    it->second->offset(), mReadAheadPosition);
        }

        if (it->second->matches(current_offset, current_size, match_offset,
                                match_size)) {
          readahead_window_hit++;
          size_t cnt = 0;

          while (!it->second->done()) {
            it->second->ReadCondVar().WaitMS(25);
            cnt++;

            if (!(cnt % 2400)) {
              // every 60 seconds ...
              if (it->second->expired()) {
                eos_crit("read-ahead request expired after %u cycles - now: %lu ctime: %lu",
                         cnt, time(NULL), it->second->creationtime());
                continue;
              }
            }
          }

          status = it->second->Status();

          if (it->second->Status().IsOK()) {
            // the match result can change after the read actually returned
            if (!it->second->matches(current_offset, current_size, match_offset,
                                     match_size)) {
              continue;
            }

            if (EOS_LOGS_DEBUG) {
              eos_debug("----: prefetched offset=%lu m-offset=%lu current-size=%u m-size=%u dim=%ld",
                        current_offset, match_offset, current_size, match_size,
                        (char*) buffer - (char*) pbuffer);
              eos_debug("----: out-buffer=%lx in-buffer=%lx in-buffer-size=%lu",
                        (long unsigned int) buffer, (long unsigned int) it->second->buffer(),
                        it->second->vbuffer().size());
            }

            // just copy what we have
            memcpy(buffer, it->second->buffer() + match_offset - it->second->offset(),
                   match_size);
            bytesRead += match_size;
            mTotalReadAheadHitBytes += match_size;
            buffer = (char*) buffer + match_size;
            current_offset = match_offset + match_size;
            current_size -= match_size;
            isEOF = it->second->eof();

            if (isEOF) {
              eos_info("got EOF in matching chunk %lu (%lu)", it->second->offset(),
                       mPosition);
              request_next = false;
              XReadAheadNom = 0;
              XReadAheadBlocksNom = XReadAheadBlocksMin;
              mReadAheadPosition = 0;
              XReadAheadReenableHits = 0;
              break;
            }
          }
        } else {
          if (!readahead_window_hit) {
            last_chunk_before_match = it;
          } else {
            XReadAheadBlocksIs++;
          }

          isEOF = it->second->eof();

          if (isEOF) {
            eos_info("got EOF in matching chunk %lu (%lu)", it->second->offset(),
                     mPosition);
            request_next = false;
            XReadAheadNom = 0;
            XReadAheadBlocksNom = XReadAheadBlocksMin;
            mReadAheadPosition = 0;
            XReadAheadReenableHits = 0;
          }
        }
      }

      if (readahead_window_hit) {
        // check if we can remove previous prefetched chunks, we keep one block before the current read position
        for (auto it = ChunkRMap().begin(); it != last_chunk_before_match; ++it) {
          XrdSysCondVarHelper lLock(it->second->ReadCondVar());

          if (it->second->expired()) {
            expired_chunk.insert(it->first);
          } else {
            if (it->second->done()) {
              if (EOS_LOGS_DEBUG) {
                eos_debug("----: dropping chunk offset=%lu chunk-offset=%lu", offset,
                          it->second->offset());
              }

              delete_chunk.insert(it->first);
            }
          }
        }
      } else {
        // clean-up all chunks in the read-ahead map
        for (auto it = ChunkRMap().begin(); it != ChunkRMap().end(); ++it) {
          XrdSysCondVarHelper lLock(it->second->ReadCondVar());
          size_t cnt = 0;

          while (!it->second->done()) {
            it->second->ReadCondVar().WaitMS(25);
            cnt++;

            if (!(cnt % 2400)) {
              // every 60 seconds ...
              if (it->second->expired()) {
                eos_crit("read-ahead request expired after %u cycles - now: %lu ctime: %lu",
                         cnt, time(NULL), it->second->creationtime());
                break;
              }
            }
          }

          if (it->second->expired()) {
            expired_chunk.insert(it->first);
          } else {
            delete_chunk.insert(it->first);
          }
        }
      }

      for (auto it = delete_chunk.begin(); it != delete_chunk.end(); ++it) {
        ChunkRMap().erase(*it);
      }

      for (auto it = expired_chunk.begin(); it != expired_chunk.end(); ++it) {
        auto chunk = ChunkRMap()[*it];
        {
          // put on a garbage stack
          std::lock_guard<std::mutex> lock(
            XrdCl::Proxy::ReadAsyncHandler::gExpiredChunksMutex);
          XrdCl::Proxy::ReadAsyncHandler::gExpiredChunks.push_back(chunk);
        }
        {
          // delete form the read-ahead map
          ChunkRMap().erase(*it);
        }
      }
    } else {
      if ((off_t) offset == mPosition) {
        XReadAheadReenableHits++;

        if ((!XReadAheadDisabled) && (XReadAheadReenableHits > 2)) {
          eos_info("re-enabling read-ahead at %lu (%lu)", offset, mPosition);
          // re-enable read-ahead if sequential reading occurs
          request_next = true;

          if (!mReadAheadPosition) {
            set_readahead_position(offset + size);

            // tune the read-ahead size with the read-pattern
            if (size > XReadAheadNom) {
              XReadAheadNom = size;
            }

            if (XReadAheadNom > XReadAheadMax) {
              XReadAheadNom = XReadAheadMax;
            }
          }
        }
      } else {
        XReadAheadReenableHits = 0;
        eos_info("disabling read-ahead at %lu (%lu)", offset, mPosition);
        request_next = false;
        XReadAheadNom = 0;
        ;
        XReadAheadBlocksNom = XReadAheadBlocksMin;
        set_readahead_position(0);
      }
    }

    if (request_next) {
      // dynamic window scaling
      if (readahead_window_hit) {
        if (XReadAheadStrategy == DYNAMIC) {
          // increase the read-ahead window
          XReadAheadNom *= 2;

          if (XReadAheadNom > XReadAheadMax) {
            XReadAheadNom = XReadAheadMax;
          }

          // increase the number of pre-fetched blocks
          XReadAheadBlocksNom *= 2;

          if (XReadAheadBlocksNom > XReadAheadBlocksMax) {
            XReadAheadBlocksNom = XReadAheadBlocksMax;
          }
        }
      }

      if (EOS_LOGS_DEBUG) {
        eos_debug("hit:%d chunks:%d pre-blocks:%d to-fetch:%d", readahead_window_hit,
                  ChunkRMap().size(), XReadAheadBlocksNom,
                  XReadAheadBlocksNom - XReadAheadBlocksIs);
      }

      // pre-fetch missing read-ahead blocks, if there is a window !=0
      size_t blocks_to_fetch = XReadAheadNom ?
                               ((XReadAheadBlocksNom > XReadAheadBlocksIs) ? ((XReadAheadBlocksNom -
                                   XReadAheadBlocksIs)) : 0)
                               : 0;

      for (size_t n_fetch = 0; n_fetch < blocks_to_fetch; n_fetch++) {
        if (EOS_LOGS_DEBUG)
          eos_debug("----: pre-fetch window=%lu pf-offset=%lu block(%d/%d)",
                    XReadAheadNom,
                    (unsigned long) mReadAheadPosition,
                    n_fetch, blocks_to_fetch
                   );

        if (mReadAheadPosition > get_readahead_maximum_position()) {
          eos_debug("----: pre-fetch skipped max-readahead-position=%lu",
                    get_readahead_maximum_position());
        }

        if (!ChunkRMap().count(mReadAheadPosition)) {
          ReadCondVar().UnLock();
          XrdCl::Proxy::read_handler rahread = ReadAsyncPrepare(proxy, mReadAheadPosition,
                                               XReadAheadNom, false);

          if (!rahread->valid()) {
            ReadCondVar().Lock();
            // no buffer available
            break;
          }

          XRootDStatus rstatus = PreReadAsync(mReadAheadPosition, XReadAheadNom,
                                              rahread, timeout);

          if (rstatus.IsOK()) {
            mReadAheadPosition += XReadAheadNom;
            mTotalReadAheadBytes += XReadAheadNom;
          }

          ReadCondVar().Lock();
        }
      }

      ReadCondVar().UnLock();
    } else {
      ReadCondVar().UnLock();
    }
  }

  if (current_size) {
    // do a synchronous read for missing pieces
    uint32_t rbytes_read = 0;
    status = File::Read(current_offset,
                        current_size,
                        buffer, rbytes_read, timeout);

    if (status.IsOK()) {
      if (rbytes_read) {
        if (EOS_LOGS_DEBUG) {
          eos_debug("----: postfetched offset=%lu size=%u rbytes=%d", current_offset,
                    current_size, rbytes_read);
        }
      }

      bytesRead += rbytes_read;
    }
  }

  set_readstate(&status);

  if (status.IsOK()) {
    mPosition = offset + size;
    mTotalBytes += bytesRead;
  }

  return status;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::OpenAsync(XrdCl::shared_proxy proxy,
                        const std::string& url,
                        OpenFlags::Flags flags,
                        Access::Mode mode,
                        uint16_t timeout)
/* -------------------------------------------------------------------------- */
{
  eos_debug("url=%s flags=%x mode=%x", url.c_str(), (int) flags, (int) mode);
  XrdSysCondVarHelper lLock(OpenCondVar());
  int in_state = state();
  mUrl = url;
  mFlags = flags;
  mMode = mode;
  mTimeout = timeout;

  if ((state() == OPENING) ||
      (state() == WAITWRITE)) {
    XRootDStatus status(XrdCl::stError,
                        suAlreadyDone,
                        XrdCl::errInProgress,
                        "in progress"
                       );
    return status;
  }

  if (state() == OPENED) {
    XRootDStatus status(XrdCl::stOK,
                        0,
                        0,
                        "opened"
                       );
    return status;
  }

  if (state() == FAILED) {
    eos_err("url=%s flags=%x mode=%x state=failed", url.c_str(), (int) flags,
            (int) mode);
    return XOpenState;
  }

  // Disable recovery on read and write
#if kXR_PROTOCOLVERSION == 0x00000297
  ((XrdCl::File*)(this))->EnableReadRecovery(false);
  ((XrdCl::File*)(this))->EnableWriteRecovery(false);
#else
  SetProperty("ReadRecovery", "false");
  SetProperty("WriteRecovery", "false");
#endif

  if (EOS_LOGS_DEBUG) {
    eos_debug("this=%x url=%s in-state %d state %d\n", this, url.c_str(), in_state,
              state());
  }

  XrdCl::XRootDStatus status;
  status = fuzzing().OpenAsyncSubmitFuzz();

  if (!status.IsOK()) {
  } else {
    XOpenAsyncHandler.SetProxy(proxy);
    status = Open(url.c_str(),
                  flags,
                  mode,
                  &XOpenAsyncHandler,
                  timeout);
  }

  if (status.IsOK()) {
    set_state(OPENING);
  } else {
    eos_err("url=%s flags=%x mode=%x state=failed errmsg=%s", url.c_str(),
            (int) flags, (int) mode, status.ToString().c_str());
    XOpenAsyncHandler.SetProxy(0);
    set_state(FAILED);
  }

  return XOpenState;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::OpenAsyncHandler::HandleResponseWithHosts(
  XrdCl::XRootDStatus* status,
  XrdCl::AnyObject* response,
  XrdCl::HostList* hostList)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("");
  {
    XrdSysCondVarHelper openLock(proxy()->OpenCondVar());
    XRootDStatus fuzzingstatus = proxy()->fuzzing().OpenAsyncResponseFuzz();

    if (!fuzzingstatus.IsOK()) {
      eos_static_debug("fuzzing open response");
      *status = fuzzingstatus;
    }

    if (status->IsOK()) {
      proxy()->set_state(OPENED);
      proxy()->set_lasturl();
      openLock.UnLock();
      XrdSysCondVarHelper writeLock(proxy()->WriteCondVar());

      while (proxy()->WriteQueue().size()) {
        write_handler handler = proxy()->WriteQueue().front();
        XRootDStatus status;
        eos_static_debug("sending scheduled write request: off=%ld size=%lu timeout=%hu",
                         handler->offset(),
                         handler->vbuffer().size(),
                         handler->timeout());
        writeLock.UnLock();
        status = proxy()->WriteAsync((uint64_t) handler->offset(),
                                     (uint32_t)(handler->vbuffer().size()),
                                     0,
                                     handler,
                                     handler->timeout()
                                    );
        writeLock.Lock(&proxy()->WriteCondVar());
        proxy()->WriteQueue().pop_front();

        if (!status.IsOK()) {
          proxy()->set_writestate(&status);
        }
      }

      writeLock.UnLock();
      openLock.Lock(&proxy()->OpenCondVar());
    } else {
      eos_static_err("state=failed async open returned errmsg=%s",
                     status->ToString().c_str());
      XrdSysCondVarHelper writeLock(proxy()->WriteCondVar());

      if (proxy()->WriteQueue().size()) {
        // if an open failes we clean the write queue
        proxy()->CleanWriteQueue();
      }

      writeLock.UnLock();
      proxy()->set_state(FAILED, status);
    }

    proxy()->OpenCondVar().Signal();
    delete hostList;
    delete status;

    if (response) {
      delete response;
    }
  }
  mProxy = 0;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::ReOpenAsync(XrdCl::shared_proxy proxy)
/* -------------------------------------------------------------------------- */
{
  if (mUrl.length()) {
    set_state_TS(CLOSED);
    return OpenAsync(proxy, mUrl, mFlags, mMode, mTimeout);
  } else {
    XRootDStatus status(XrdCl::stError,
                        suRetry,
                        XrdCl::errUninitialized,
                        "never opened before"
                       );
    eos_err("state=failed reopenasync errmsg=%s", status.ToString().c_str());
    set_state_TS(FAILED, &status);
    return status;
  }
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::CloseAsync(XrdCl::shared_proxy proxy, uint16_t timeout)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");

  // don't close files attached by several clients
  if (mAttached > 1) {
    eos_debug("still attached");
    return XRootDStatus();
  }

  WaitOpen();
  DropReadAhead();
  XrdSysCondVarHelper lLock(OpenCondVar());

  // only an opened file requires a close, otherwise we return the last known state
  if ((state() == OPENED) ||
      (state() == WAITWRITE)) {
    XCloseAsyncHandler.SetProxy(proxy);
    XrdCl::XRootDStatus status = XrdCl::File::Close(&XCloseAsyncHandler,
                                 timeout);

    if (!status.IsOK()) {
      eos_err("state=failed closeasync errms=%s", status.ToString().c_str());
      set_state(FAILED, &status);
      XCloseAsyncHandler.SetProxy(0);
    } else {
      set_state(CLOSING, &status);
    }
  } else {
    eos_crit("%x closing an unopened file state=%d url=%s\n", this, state(),
             mUrl.c_str());
  }

  return XOpenState;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::ScheduleCloseAsync(XrdCl::shared_proxy proxy, uint16_t timeout)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");

  if (mAttached > 1) {
    eos_debug("still attached");
    return XRootDStatus();
  }

  {
    bool no_chunks_left = true;

    if ((state() == OPENING) ||
        (state() == OPENED)) {
      {
        DropReadAhead();
        XrdSysCondVarHelper lLock(WriteCondVar());

        // either we have submitted chunks
        if (ChunkMap().size()) {
          no_chunks_left = false;
        }

        // or we have chunks still to be submitted
        if (WriteQueue().size()) {
          no_chunks_left = false;
        }

        if (!no_chunks_left) {
          // indicate to close this file when the last write-callback arrived
          eos_debug("indicating close-after-write");
          XCloseAfterWrite = true;
          XCloseAfterWriteTimeout = timeout;
        }
      }

      if (no_chunks_left) {
        return CloseAsync(proxy, timeout);
      } else {
        return XOpenState;
      }
    }
  }

  XRootDStatus status(XrdCl::stError,
                      suAlreadyDone,
                      XrdCl::errInvalidOp,
                      "file not open"
                     );
  return status;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::Close(uint16_t timeout)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");

  // don't close files attached by several clients
  if (mAttached > 1) {
    return XRootDStatus();
  }

  WaitOpen();

  if (IsOpen()) {
    Collect();
  }

  DropReadAhead();
  XrdSysCondVarHelper lLock(OpenCondVar());
  XrdCl::XRootDStatus status = XrdCl::File::Close(timeout);
  set_state(CLOSED, &status);
  return status;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::CloseAsyncHandler::HandleResponse(XrdCl::XRootDStatus* status,
    XrdCl::AnyObject* response)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("");
  {
    XrdSysCondVarHelper lLock(mProxy->OpenCondVar());

    if (!status->IsOK()) {
      // if the open failed before, we leave the open failed state here
      if (!mProxy->isDeleted()) {
        if (mProxy->state() != XrdCl::Proxy::FAILED) {
          eos_static_crit("%x current status = %d - setting CLOSEFAILED - msg=%s url=%s\n",
                          mProxy.get(), mProxy->state(), status->ToString().c_str(),
                          mProxy->url().c_str());
          mProxy->set_state(XrdCl::Proxy::CLOSEFAILED, status);
        }
      } else {
        eos_static_info("%x current status = %d - silencing CLOSEFAILED - msg=%s url=%s\n",
                        mProxy.get(), mProxy->state(), status->ToString().c_str(),
                        mProxy->url().c_str());
        // an unlinked file can have a close failure response
        XRootDStatus okstatus;
        mProxy->set_state(XrdCl::Proxy::CLOSED, &okstatus);
      }
    } else {
      mProxy->set_state(XrdCl::Proxy::CLOSED, status);
    }

    mProxy->OpenCondVar().Signal();
    delete response;
    delete status;
  }
  mProxy = 0;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::WaitClose()
/* -------------------------------------------------------------------------- */
{
  eos_debug("");

  if (IsOpen()) {
    Collect();
  }

  XrdSysCondVarHelper lLock(OpenCondVar());

  while (state() == CLOSING) {
    OpenCondVar().WaitMS(25);
  }

  return XOpenState;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::WaitOpen()
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  XrdSysCondVarHelper lLock(OpenCondVar());

  while (state() == OPENING) {
    OpenCondVar().WaitMS(25);
  }

  return XOpenState;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::WaitOpen(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  XrdSysCondVarHelper lLock(OpenCondVar());

  while (state() == OPENING) {
    if (req && fuse_req_interrupted(req)) {
      return EINTR;
    }

    OpenCondVar().WaitMS(25);
  }

  return 0;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::IsOpening()
/* -------------------------------------------------------------------------- */
{
  XrdSysCondVarHelper lLock(OpenCondVar());
  eos_debug("state=%d", state());
  return (state() == OPENING) ? true : false;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::IsClosing()
/* -------------------------------------------------------------------------- */
{
  XrdSysCondVarHelper lLock(OpenCondVar());
  eos_debug("state=%d", state());
  return (state() == CLOSING) ? true : false;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::IsOpen()
/* -------------------------------------------------------------------------- */
{
  XrdSysCondVarHelper lLock(OpenCondVar());
  eos_debug("state=%d", state());
  return (state() == OPENED) ? true : false;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::IsClosed()
/* -------------------------------------------------------------------------- */
{
  XrdSysCondVarHelper lLock(OpenCondVar());
  eos_debug("state=%d", state());
  return ((state() == CLOSED) || (state() == CLOSEFAILED) ||
          (state() == FAILED)) ? true : false;
}

bool

XrdCl::Proxy::IsWaitWrite()
{
  XrdSysCondVarHelper lLock(OpenCondVar());
  eos_debug("state=%d", state());
  return (state() == WAITWRITE) ? true : false;
}

bool
XrdCl::Proxy::HadFailures(std::string& message)
{
  bool ok = true;
  XrdSysCondVarHelper lLock(OpenCondVar());

  if (state() == CLOSEFAILED) {
    message = "file close failed";
    ok = false;
  }

  if (state() == FAILED) {
    message = "file open failed";
    ok = false;
  }

  if (!write_state().IsOK()) {
    message = "file writing failed";
    ok = false;
  }

  eos_debug("state=%d had-failures=%d", state(), !ok);
  return !ok;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::WriteAsyncHandler::HandleResponse(XrdCl::XRootDStatus* status,
    XrdCl::AnyObject* response)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("ino=%llx", mProxy->id());
  bool no_chunks_left = true;
  // for correct destruct order 'myself' must be declared before anything
  // that uses member variables
  write_handler myself;
  shared_proxy proxy;
  {
    std::lock_guard<std::mutex> lock(mDisableProxyMutex);
    proxy = mProxy;
    myself = mDisableKeepalive;
    mDisableKeepalive = 0;
  }
  if (!proxy) {
    delete response;
    delete status;
    return;
  }

  {
    XrdSysCondVarHelper lLock(proxy->WriteCondVar());
    {
      if (!status->IsOK()) {
        proxy->set_writestate(status);
        eos_static_crit("write error '%s'", status->ToString().c_str());
      }

      proxy->WriteCondVar().Signal();
      delete response;
      delete status;

      myself = proxy->ChunkMap()[(int64_t)this];
      proxy->ChunkMap().erase((uint64_t)this);

    }
  }

  std::lock_guard<std::mutex> lock(mDisableProxyMutex);
  if (!mProxy) {
    myself = mDisableKeepalive;
    mDisableKeepalive = 0;
    return;
  }

  if (no_chunks_left) {
    if (mProxy->close_after_write()) {
      eos_static_debug("sending close-after-write");
      // send an asynchronous close now
      XrdCl::XRootDStatus status = mProxy->CloseAsync(mProxy,
                                   mProxy->close_after_write_timeout());
    }
  }
  mProxy = 0;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::WriteAsyncHandler::DumpReferences(std::string& out)
{
  std::lock_guard<std::mutex> lock(gBuffReferenceMutex);

  for (auto it = gBufferReference.begin(); it != gBufferReference.end(); ++it) {
    out += "ref:";
    out += it->first;
    out += " := ";
    out += std::to_string(it->second);
    out += "\n";
  }

  return;
}

/* -------------------------------------------------------------------------- */
XrdCl::Proxy::write_handler
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::WriteAsyncPrepare(XrdCl::shared_proxy proxy, uint32_t size,
                                uint64_t offset,
                                uint16_t timeout)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  write_handler dst = std::make_shared<WriteAsyncHandler>(proxy, size, offset,
                      timeout);
  XrdSysCondVarHelper lLock(WriteCondVar());
  ChunkMap()[(uint64_t) dst.get()] = dst;
  return dst;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::WriteAsync(uint64_t offset,
                         uint32_t size,
                         const void* buffer,
                         XrdCl::Proxy::write_handler handler,
                         uint16_t timeout)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");

  // a buffer indicates, the handler buffer is already filled
  if (buffer) {
    handler->copy(buffer, size);
  }

  XRootDStatus status = Write(static_cast<uint64_t>(offset),
                              static_cast<uint32_t>(size),
                              handler->buffer(), handler.get(), timeout);

  if (!status.IsOK()) {
    // remove failing requests
    XrdSysCondVarHelper lLock(WriteCondVar());
    ChunkMap().erase((uint64_t) handler.get());
  }

  return status;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::ScheduleWriteAsync(
  const void* buffer,
  write_handler handler
)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");

  if (buffer) {
    handler->copy(buffer, handler->vbuffer().size());
  }

  XrdSysCondVarHelper openLock(OpenCondVar());

  if (state() == OPENED) {
    openLock.UnLock();
    eos_debug("direct");
    inc_write_queue_direct_submissions();
    // we can send off the write request
    return WriteAsync(handler->offset(),
                      (size_t) handler->vbuffer().size(),
                      0,
                      handler,
                      handler->timeout());
  }

  if (state() == OPENING) {
    inc_write_queue_scheduled_submissions();
    eos_debug("scheduled");
    // we add this write to the list to be submitted when the open call back arrives
    XrdSysCondVarHelper lLock(WriteCondVar());
    WriteQueue().push_back(handler);
    // we can only say status OK in that case
    XRootDStatus status(XrdCl::stOK,
                        0,
                        XrdCl::errInProgress,
                        "in progress"
                       );
    return status;
  } else {
    // remove requests with failed open
    XrdSysCondVarHelper lLock(WriteCondVar());
    ChunkMap().erase((uint64_t) handler.get());
  }

  return XOpenState;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::WaitWrite()
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  WaitOpen();

  if (state() == WAITWRITE) {
    XrdSysCondVarHelper openLock(OpenCondVar());
    return XOpenState;
  }

  // check if the open failed
  if (state() != OPENED) {
    XrdSysCondVarHelper openLock(OpenCondVar());
    return XOpenState;
  }

  {
    time_t wait_start = time(NULL);
    XrdSysCondVarHelper lLock(WriteCondVar());

    while (ChunkMap().size()) {
      eos_debug("     [..] map-size=%lu", ChunkMap().size());
      WriteCondVar().WaitMS(1000);
      time_t wait_stop = time(NULL);

      if (ChunkMap().size() && ((wait_stop - wait_start) > sChunkTimeout)) {
        eos_err("discarding %d chunks  in-flight for writing", ChunkMap().size());

        for (auto it = ChunkMap().begin(); it != ChunkMap().end(); ++it) {
          it->second->disable(it->second);
        }

        ChunkMap().clear();
        return XRootDStatus(XrdCl::stFatal,
                            suDone,
                            XrdCl::errSocketTimeout,
                            "request timeout"
                           );
      }
    }

    eos_debug(" [..] map-size=%lu", ChunkMap().size());
  }

  {
    XrdSysCondVarHelper writeLock(WriteCondVar());
    return XWriteState;
  }
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::WaitWrite(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  // this waits for all writes to come back and checks for interrupts inbetween
  // this assumes a file is in OPENED state
  {
    XrdSysCondVarHelper lLock(WriteCondVar());

    while (ChunkMap().size()) {
      if (req && fuse_req_interrupted(req)) {
        return EINTR;
      }

      eos_debug("     [..] map-size=%lu", ChunkMap().size());
      WriteCondVar().WaitMS(1000);
    }

    eos_debug(" [..] map-size=%lu", ChunkMap().size());
  }
  return 0;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::CollectWrites()
/* -------------------------------------------------------------------------- */
{
  // this waits for all writes to come back and checks for interrupts inbetween
  // this assumes a file is in OPENED state
  {
    XrdSysCondVarHelper lLock(WriteCondVar());

    while (ChunkMap().size()) {
      eos_debug("     [..] map-size=%lu", ChunkMap().size());
      WriteCondVar().WaitMS(1000);
    }

    eos_debug(" [..] map-size=%lu", ChunkMap().size());
  }
  return XWriteState;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::OutstandingWrites()
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  XrdSysCondVarHelper lLock(WriteCondVar());
  return ChunkMap().size() ? true : false;
}

void
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::ReadAsyncHandler::HandleResponse(XrdCl::XRootDStatus* status,
    XrdCl::AnyObject* response)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("");
  // for correct destruct order 'myself' must be declared before anything
  // that uses member variables
  read_handler myself;
  shared_proxy proxy;
  {
    std::lock_guard<std::mutex> lock(mDisableProxyMutex);
    proxy = mProxy;
    myself = mDisableKeepalive;
    mDisableKeepalive = 0;
  }
  if (!proxy) {
    delete response;
    delete status;
    return;
  }

  {
    XrdSysCondVarHelper lLock(ReadCondVar());
    mStatus = *status;
    bool fuzzing = proxy->fuzzing().ReadAsyncResponseFuzz();

    if (!fuzzing && status->IsOK()) {
      XrdCl::ChunkInfo* chunk = 0;

      if (response) {
        response->Get(chunk);

        if (valid()) {
          if (chunk->length < mBuffer->size()) {
            if (EOS_LOGS_DEBUG) {
              eos_static_debug("handler %x received %lu instead of %lu\n", this,
                               chunk->length, mBuffer->size());
            }

            mBuffer->resize(chunk->length);
          }
        }

        if (!chunk->length) {
          mEOF = true;
        }

        delete response;
      } else {
        mBuffer->resize(0);
      }
    } else {
      if (status->IsOK()) {
        if (valid()) {
          mBuffer->resize(0);
        }

        if (response) {
          delete response;
        }
      }

      // we free the buffer, so it get's back to the buffer handler;
      release_buffer();
    }

    mDone = true;
    delete status;

    std::lock_guard<std::mutex> lock(mDisableProxyMutex);
    if (!mProxy) {
      myself = mDisableKeepalive;
      mDisableKeepalive = 0;
      return;
    }
    mProxy->dec_read_chunks_in_flight();
    ReadCondVar().Signal();
    mProxy = 0;
  }
}

/* -------------------------------------------------------------------------- */
XrdCl::Proxy::read_handler
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::ReadAsyncPrepare(XrdCl::shared_proxy proxy, off_t offset,
                               uint32_t size, bool blocking)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  read_handler src = std::make_shared<ReadAsyncHandler>(proxy, offset, size,
                     blocking);

  if (!src->valid()) {
    // check if an IO buffer was allocated
    return src;
  }

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("handler %x request %lu/%lu non-blocking\n", &(*src), offset,
                     size);
  }

  XrdSysCondVarHelper lLock(ReadCondVar());

  if (!ChunkRMap().count(src->offset())) {
    inc_read_chunks_in_flight();
  }

  ChunkRMap()[(uint64_t) src->offset()] = src;
  ReadCondVar().Signal();
  return src;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::PreReadAsync(uint64_t offset,
                           uint32_t size,
                           read_handler handler,
                           uint16_t timeout)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  XRootDStatus status = WaitOpen();

  if (!status.IsOK()) {
    // remove the allocated chunk buffer
    XrdSysCondVarHelper lLock(ReadCondVar());
    ChunkRMap().erase(offset);
    dec_read_chunks_in_flight();
    return status;
  }

  XRootDStatus rstatus = File::Read(static_cast<uint64_t>(offset),
                                    static_cast<uint32_t>(size),
                                    (void*) handler->buffer(), handler.get(), timeout);

  if (!rstatus.IsOK()) {
    // remove the allocated chunk buffer
    XrdSysCondVarHelper lLock(ReadCondVar());
    ChunkRMap().erase(offset);
    dec_read_chunks_in_flight();
  }

  return rstatus;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::WaitRead(read_handler handler)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  XrdSysCondVarHelper lLock(handler->ReadCondVar());
  time_t wait_start = time(NULL);

  while (!handler->done()) {
    handler->ReadCondVar().WaitMS(1000);
    time_t wait_stop = time(NULL);

    if (((wait_stop - wait_start) > sChunkTimeout)) {
      eos_err("discarding %d chunks  in-flight for reading", ChunkMap().size());

      for (auto it = ChunkRMap().begin(); it != ChunkRMap().end(); ++it) {
        it->second->disable(it->second);
      }

      clear_read_chunks_in_flight();
      ChunkRMap().clear();
      return XRootDStatus(XrdCl::stFatal,
                          suDone,
                          XrdCl::errSocketTimeout,
                          "request timeout"
                         );
    }
  }

  if (handler->valid()) {
    eos_debug(" [..] read-size=%lu", handler->vbuffer().size());
  }

  return handler->Status();
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::ReadAsync(read_handler handler,
                        uint32_t size,
                        void* buffer,
                        uint32_t& bytesRead)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  XRootDStatus status = WaitRead(handler);

  if (!status.IsOK()) {
    return status;
  }

  bytesRead = (size < handler->vbuffer().size()) ? size :
              handler->vbuffer().size();
  memcpy(buffer, handler->buffer(), bytesRead);
  return status;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::DoneAsync(read_handler handler)
{
  eos_debug("");
  XRootDStatus status = WaitRead(handler);
  XrdSysCondVarHelper lLock(ReadCondVar());
  ChunkRMap().erase(handler->offset());
  return true;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::Sync(uint16_t timeout)
{
  eos_debug("");
  return File::Sync(timeout);
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::attach()
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(mAttachedMutex);
  mAttached++;
  eos_debug("attached=%u", mAttached);
  return;
}

/* -------------------------------------------------------------------------- */
size_t
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::detach()
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(mAttachedMutex);
  mAttached--;
  eos_debug("attached=%u", mAttached);
  return mAttached;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::attached()
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(mAttachedMutex);
  return mAttached ? true : false;
}

/* -------------------------------------------------------------------------- */
size_t
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::get_attached()
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(mAttachedMutex);
  return mAttached;
}


/* -------------------------------------------------------------------------- */
int XrdCl::Fuzzing::errors[22] = { 101, 102, 103, 104, 105, 106, 107, 108, 109,
                                   201, 202, 203, 204, 205, 206, 207,
                                   301, 302, 303, 304, 305, 306
                                 };
/* -------------------------------------------------------------------------- */

size_t XrdCl::Fuzzing::non_fatal_errors = 9;
/* -------------------------------------------------------------------------- */
size_t XrdCl::Fuzzing::fatal_errors = 13;
/* -------------------------------------------------------------------------- */
size_t XrdCl::Fuzzing::open_async_submit_scaler = 0;
size_t XrdCl::Fuzzing::open_async_submit_counter = 0;
size_t XrdCl::Fuzzing::open_async_return_scaler = 0;
size_t XrdCl::Fuzzing::open_async_return_counter = 0;
size_t XrdCl::Fuzzing::read_async_return_scaler = 0;
size_t XrdCl::Fuzzing::read_async_return_counter = 0;
bool XrdCl::Fuzzing::open_async_submit_fatal = false;
bool XrdCl::Fuzzing::open_async_return_fatal = false;

/* -------------------------------------------------------------------------- */
XrdCl::XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Fuzzing::OpenAsyncSubmitFuzz()
{
  if (open_async_submit_scaler) {
    if (!(open_async_submit_counter++ % open_async_submit_scaler)) {
      size_t random_error = rand() % (non_fatal_errors + (open_async_submit_fatal ?
                                      fatal_errors : 0));
      eos_static_debug("fuzzing error %d", errors[random_error]);

      if (random_error < non_fatal_errors) {
        XrdCl::XRootDStatus status(XrdCl::stError, errors[random_error], 0);
        return status;
      } else {
        XrdCl::XRootDStatus status(XrdCl::stFatal, errors[random_error], 0);
        return status;
      }
    }
  }

  //  size_t open_async_submit_counter;
  XRootDStatus status(XrdCl::stOK,
                      0,
                      0,
                      "open submitted"
                     );
  return status;
}


/* -------------------------------------------------------------------------- */
XrdCl::XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Fuzzing::OpenAsyncResponseFuzz()
{
  if (open_async_return_scaler) {
    if (!(open_async_return_counter++ % open_async_return_scaler)) {
      size_t random_error = rand() % (non_fatal_errors + (open_async_return_fatal ?
                                      fatal_errors : 0));
      eos_static_debug("fuzzing error %d", errors[random_error]);

      if (random_error < non_fatal_errors) {
        XrdCl::XRootDStatus status(XrdCl::stError, errors[random_error], 0);
        return status;
      } else {
        XrdCl::XRootDStatus status(XrdCl::stFatal, errors[random_error], 0);
        return status;
      }
    }
  }

  eos_static_debug("fuzzing OK");
  //  size_t open_async_return_counter
  XRootDStatus status(XrdCl::stOK,
                      0,
                      0,
                      "open successful"
                     );
  return status;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
XrdCl::Fuzzing::ReadAsyncResponseFuzz()
{
  if (read_async_return_scaler) {
    if (!(read_async_return_counter++ % read_async_return_scaler)) {
      eos_static_debug("fuzzing error");
      return true;
    }
  }

  eos_static_debug("fuzzing OK");
  return false;
}


/* -------------------------------------------------------------------------- */
const char*
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::Dump(std::string& out)
{
  mProtocol.Dump(out);
  return out.c_str();
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::Protocol::Add(std::string s)
{
  XrdSysMutexHelper lock(mMutex);
  mMessages.push_back(std::string("---- " + s + "\n"));
}

/* -------------------------------------------------------------------------- */
const char*
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::Protocol::Dump(std::string& out)
{
  XrdSysMutexHelper lock(mMutex);

  for (auto item = mMessages.begin(); item != mMessages.end(); ++item) {
    out += *item;
  }

  return out.c_str();
}
