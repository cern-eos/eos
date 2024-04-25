//------------------------------------------------------------------------------
//! @file xrdclproxy.hh
//! @author Andreas-Joachim Peters CERN
//! @brief xrootd file proxy class
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

#ifndef FUSE_XRDCLPROXY_HH_
#define FUSE_XRDCLPROXY_HH_

#include <XrdSys/XrdSysPthread.hh>
#include <XrdCl/XrdClFile.hh>
#include <XrdCl/XrdClFileSystem.hh>
#include <XrdCl/XrdClDefaultEnv.hh>
#include "llfusexx.hh"
#include "common/Logging.hh"
#include "common/Timing.hh"
#include "common/RWMutex.hh"
#include <memory>
#include <map>
#include <string>
#include <deque>
#include <queue>
#include <thread>
#include <atomic>
#include <mutex>

namespace XrdCl
{

// ---------------------------------------------------------------------- //
class Fuzzing
{
public:

  Fuzzing()
  {
  }

  static void Configure(size_t _open_async_submit_scaler,
                        size_t _open_async_return_scaler,
                        bool _open_async_submit_fatal,
                        bool _open_async_return_fatal,
                        size_t _read_async_return_scaler)
  {
    open_async_submit_scaler = _open_async_submit_scaler;
    open_async_return_scaler = _open_async_return_scaler;
    open_async_submit_fatal = _open_async_submit_fatal;
    open_async_return_fatal = _open_async_return_fatal;
    read_async_return_scaler = _read_async_return_scaler;
  }

  XRootDStatus OpenAsyncSubmitFuzz();
  XRootDStatus OpenAsyncResponseFuzz();
  bool ReadAsyncResponseFuzz();

  static int errors[22];
  static size_t non_fatal_errors;
  static size_t fatal_errors;

private:
  static size_t open_async_submit_scaler;
  static size_t open_async_submit_counter;
  static size_t open_async_return_scaler;
  static size_t open_async_return_counter;
  static size_t read_async_return_scaler;
  static size_t read_async_return_counter;
  static bool open_async_submit_fatal;
  static bool open_async_return_fatal;
};

// ---------------------------------------------------------------------- //
typedef std::shared_ptr<std::vector<char>> shared_buffer;
class Proxy;
typedef std::shared_ptr<Proxy> shared_proxy;

// ---------------------------------------------------------------------- //

// ---------------------------------------------------------------------- //

class BufferManager : public XrdSysMutex
// ---------------------------------------------------------------------- //
{
public:

  BufferManager(size_t _max = 128, size_t _default_size = 128 * 1024,
                size_t _max_inflight_size = 1 * 1024 * 1024 * 1024)
  {
    max = _max;
    buffersize = _default_size;
    queued_size = 0;
    inflight_size = 0;
    inflight_buffers = 0;
    xoff_cnt = 0;
    nobuf_cnt = 0;
    max_inflight_size = _max_inflight_size;
  }

  virtual ~BufferManager() { }

  void configure(size_t _max, size_t _size,
                 size_t _max_inflight_size = 1 * 1024 * 1024 * 1024)
  {
    max = _max;
    buffersize = _size;
    max_inflight_size = _max_inflight_size;
  }

  void reset()
  {
    XrdSysMutexHelper lLock(this);
    inflight_size = 0;
    inflight_buffers = 0;
  }

  shared_buffer get_buffer(size_t size, bool blocking = true)
  {
    struct timespec ts;
    eos::common::Timing::GetTimeSpec(ts, true);

    // make sure, we don't have more buffers in flight than max_inflight_size
    do {
      size_t cnt = 0;
      {
        XrdSysMutexHelper lLock(this);
        static time_t grace_buffer_time = 0;

        if ((inflight_size < max_inflight_size) &&
            (inflight_buffers < 16384)) { // avoid to trigger XRootD SID exhaustion
          break;
        }

        // a grace buffer period allows to unstuck a getbuffer dead-lock where buffers are referenced by a failing fd
        if (ts.tv_sec < grace_buffer_time) {
          if ((inflight_size < (2 * max_inflight_size) &&
               (inflight_buffers < 16384))) {
            break;
          }
        }

        if (!(cnt % 1000)) {
          if (inflight_size >= max_inflight_size) {
            eos_static_info("inflight-buffer exceeds maximum number of bytes [%ld/%ld]",
                            inflight_size, max_inflight_size);
          }

          if (inflight_buffers >= 16384) {
            eos_static_info("inflight-buffer exceeds maximum number of buffers in flight [%ld/%ld]",
                            inflight_buffers, 16384);
          }
        }

        if (!blocking) {
          nobuf_cnt++;
          // we don't wait for free buffers
          return nullptr;
        }

        xoff_cnt++;
        double exec_time_sec = 1.0 * eos::common::Timing::GetCoarseAgeInNs(&ts,
                               0) / 1000000000.0;

        if (exec_time_sec > 200) {
          // temporarily increase the buffer size to unlock a buffer starvation dead-lock
          grace_buffer_time = time(NULL) + 60;
          // exceptionally grant more buffers to recover from a dead-lock situation
          eos_static_warning("granting grace buffers now=%u until then=%u",
                             ts.tv_sec,
                             grace_buffer_time);
        }
      }
      cnt++;
      // we wait that the situation relaxes
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } while (1);

    XrdSysMutexHelper lLock(this);
    size_t cap_size = size;
    inflight_buffers++;
    shared_buffer buffer;

    if (!queue.size() || (size < buffersize)) {
      inflight_size += cap_size;
      buffer = std::make_shared<std::vector<char>>(cap_size, 0);
    } else {
      buffer = queue.front();
      queued_size -= buffer->capacity();
      buffer->resize(cap_size);
      buffer->reserve(cap_size);
      inflight_size += buffer->capacity();
      queue.pop();
    }

    if (EOS_LOGS_DEBUG) {
      eos_static_debug("get-buffer %lx size %lu", (uint64_t)(&((*buffer)[0])),
                       buffer->capacity());
    }

    return buffer;
  }

  void put_buffer(shared_buffer buffer)
  {
    XrdSysMutexHelper lLock(this);
    inflight_size -= buffer->capacity();

    if (EOS_LOGS_DEBUG) {
      eos_static_debug("put-buffer %lx size %lu", (uint64_t)(&((*buffer)[0])),
                       buffer->capacity());
    }

    inflight_buffers--;

    if ((queue.size() == max) || (buffer->capacity() < buffersize)) {
      return;
    } else {
      queue.push(buffer);
      buffer->resize(buffersize);
      buffer->reserve(buffersize);
      buffer->shrink_to_fit();
      queued_size += buffersize;
      return;
    }
  }

  const size_t queued()
  {
    XrdSysMutexHelper lLock(this);
    return queued_size;
  }

  const size_t inflight()
  {
    XrdSysMutexHelper lLock(this);
    return inflight_size;
  }

  const size_t xoff()
  {
    XrdSysMutexHelper lLock(this);
    return xoff_cnt;
  }

  const size_t nobuf()
  {
    XrdSysMutexHelper lLock(this);
    return nobuf_cnt;
  }

private:
  std::queue<shared_buffer> queue;
  size_t max;
  size_t buffersize;
  size_t queued_size;
  size_t inflight_size;
  size_t inflight_buffers;
  size_t max_inflight_size;
  size_t xoff_cnt;
  size_t nobuf_cnt;
};


// ---------------------------------------------------------------------- //


class Proxy : public XrdCl::File, public eos::common::LogId
// ---------------------------------------------------------------------- //
{
public:

  class ReadAsyncHandler;

  static BufferManager sWrBufferManager; // write buffer manager
  static BufferManager sRaBufferManager; // async read buffer manager

  static XrdCl::shared_proxy Factory();

  // ---------------------------------------------------------------------- //
  XRootDStatus OpenAsync(XrdCl::shared_proxy proxy,
                         const std::string& url,
                         OpenFlags::Flags flags,
                         Access::Mode mode,
                         uint16_t timeout);

  // ---------------------------------------------------------------------- //
  XRootDStatus ReOpenAsync(XrdCl::shared_proxy proxy);

  // ---------------------------------------------------------------------- //
  XRootDStatus WaitOpen();

  int WaitOpen(fuse_req_t); // waiting interrupts

  // ---------------------------------------------------------------------- //
  bool IsOpening();

  // ---------------------------------------------------------------------- //
  bool IsClosing();

  // ---------------------------------------------------------------------- //
  bool IsOpen();

  // ---------------------------------------------------------------------- //
  bool IsClosed();

  // ---------------------------------------------------------------------- //
  XRootDStatus WaitWrite();

  // ---------------------------------------------------------------------- //
  XRootDStatus CollectWrites();

  // ---------------------------------------------------------------------- //
  int WaitWrite(fuse_req_t); // waiting interrupts

  // ---------------------------------------------------------------------- //
  bool IsWaitWrite();

  // ---------------------------------------------------------------------- //
  bool OutstandingWrites();

  // ---------------------------------------------------------------------- //
  bool HadFailures(std::string& message);

  // ---------------------------------------------------------------------- //
  XRootDStatus Write(uint64_t offset,
                     uint32_t size,
                     const void* buffer,
                     ResponseHandler* handler,
                     uint16_t timeout = 0);

  XRootDStatus Write(uint64_t offset,
                     uint32_t size,
                     const void* buffer,
                     uint16_t timeout = 0)
  {
    return File::Write(offset, size, buffer, timeout);
  }

  // ---------------------------------------------------------------------- //
  XRootDStatus Read(XrdCl::shared_proxy proxy,
                    uint64_t offset,
                    uint32_t size,
                    void* buffer,
                    uint32_t& bytesRead,
                    uint16_t timeout = 0);

  // ---------------------------------------------------------------------- //
  XRootDStatus Sync(uint16_t timeout = 0);


  // ---------------------------------------------------------------------- //

  XRootDStatus CloseAsync(XrdCl::shared_proxy proxy, uint16_t timeout = 0);

  XRootDStatus ScheduleCloseAsync(XrdCl::shared_proxy proxy,
                                  uint16_t timeout = 0);


  XRootDStatus WaitClose();

  // ---------------------------------------------------------------------- //
  XRootDStatus Close(uint16_t timeout = 0);

  // ---------------------------------------------------------------------- //
  bool HadFailures();

  // ---------------------------------------------------------------------- //

  void inherit_attached(shared_proxy proxy)
  {
    XrdSysMutex mAttachedMutex;
    mAttached = proxy ? proxy->get_attached() : 1;
  }

  void inherit_writequeue(shared_proxy new_proxy, shared_proxy proxy)
  {
    XWriteQueue = proxy->WriteQueue();
    proxy->WriteQueue().clear();

    for (auto i : XWriteQueue) {
      i->SetProxy(new_proxy);
    }
  }

  void inherit_protocol(shared_proxy proxy)
  {
    mProtocol = proxy->getProtocol();
  }

  // ---------------------------------------------------------------------- //

  static int status2errno(const XRootDStatus& status)
  {
    if (!status.errNo) {
      if (status.IsOK()) {
        return 0;
      } else {
        return EPROTO;
      }
    }

    if (status.errNo < kXR_ArgInvalid) {
      return status.errNo;
    } else {
      return XProtocol::toErrno(status.errNo);
    }
  }

  enum OPEN_STATE {
    CLOSED = 0,
    OPENING = 1,
    OPENED = 2,
    WAITWRITE = 3,
    CLOSING = 4,
    FAILED = 5,
    CLOSEFAILED = 6,
  };

  const char* state_string()
  {
    switch (open_state) {
    case CLOSED:
      return "closed";

    case OPENING:
      return "opening";

    case OPENED:
      return "open";

    case WAITWRITE:
      return "waitwrite";

    case CLOSING:
      return "closing";

    case FAILED:
      return "failed";

    case CLOSEFAILED:
      return "closefailed";

    default:
      return "invalid";
    }
  }

  OPEN_STATE state()
  {
    // lock XOpenAsyncCond from outside
    return open_state;
  }

  XRootDStatus read_state()
  {
    return XReadState;
  }

  XRootDStatus write_state()
  {
    return XWriteState;
  }

  XRootDStatus opening_state()
  {
    return XOpenState;
  }

  bool opening_state_should_retry()
  {
    if ((opening_state().code != XrdCl::errConnectionError) &&
        (opening_state().code != XrdCl::errSocketTimeout) &&
        (opening_state().code != XrdCl::errOperationExpired) &&
        (opening_state().code != XrdCl::errSocketDisconnected) &&
        (opening_state().errNo != kXR_noserver) &&
        (opening_state().errNo != kXR_FSError) &&
        (opening_state().errNo != kXR_IOError)) {
      return false;
    }

    return true;
  }

  void set_state(OPEN_STATE newstate, XRootDStatus* xs = 0)
  {
    // lock XOpenAsyncCond from outside
    open_state = newstate;
    eos::common::Timing::GetTimeSpec(open_state_time);
    mProtocol.Add(eos_static_log(LOG_SILENT, "%s", state_string()));

    if (xs) {
      XOpenState = *xs;
    }
  }


  void set_lasturl()
  {
    this->GetProperty("LastURL", mLastUrl);
    XrdCl::URL newurl(mLastUrl);
    XrdCl::URL::ParamsMap cgi = newurl.GetParams();
    mProtocol.Add(eos_static_log(LOG_SILENT, "host=%s:%d",
                                 newurl.GetHostName().c_str(), newurl.GetPort()));
    mProtocol.Add(eos_static_log(LOG_SILENT, "lfn='%s' app='%s'",
                                 cgi["eos.lfn"].c_str(), cgi["eos.app"].c_str()));
    mProtocol.Add(eos_static_log(LOG_SILENT, "logid=%s", cgi["mgm.logid"].c_str()));
    mProtocol.Add(eos_static_log(LOG_SILENT, "fuse=%s:%s:%s:%s:%s",
                                 cgi["fuse.exe"].c_str(),
                                 cgi["fuse.uid"].c_str(),
                                 cgi["fuse.gid"].c_str(),
                                 cgi["fuse.pid"].c_str(),
                                 cgi["fuse.ver"].c_str()));
    mProtocol.Add(eos_static_log(LOG_SILENT, "xrd=%s:%s:%s:%s",
                                 cgi["xrdcl.requuid"].c_str(),
                                 cgi["xrdcl.secuid"].c_str(),
                                 cgi["xrdcl.sccgid"].c_str(),
                                 cgi["xrdcl.wantprot"].c_str()));
  }

  // TS stands for "thread-safe"

  void set_state_TS(OPEN_STATE newstate, XRootDStatus* xs = 0)
  {
    XrdSysCondVarHelper lLock(OpenCondVar());
    set_state(newstate, xs);
  }

  double state_age()
  {
    return ((double) eos::common::Timing::GetAgeInNs(&open_state_time,
            0) / 1000000000.0);
  }

  void set_readstate(XRootDStatus* xs)
  {
    XReadState = *xs;
  }

  void set_writestate(XRootDStatus* xs)
  {
    XWriteState = *xs;
  }

  enum READAHEAD_STRATEGY {
    NONE = 0,
    STATIC = 1,
    DYNAMIC = 2
  };

  void set_readahead_maximum_position(off_t offset)
  {
    mReadAheadMaximumPosition = offset;
  }

  off_t get_readahead_maximum_position() const
  {
    return mReadAheadMaximumPosition;
  }

  void set_readahead_sparse_ratio(double r)
  {
    XReadAheadSparseRatio = r;
  }

  double get_readhead_sparse_ratio()
  {
    return XReadAheadSparseRatio;
  }

  static READAHEAD_STRATEGY readahead_strategy_from_string(
    const std::string& strategy)
  {
    if (strategy == "dynamic") {
      return DYNAMIC;
    }

    if (strategy == "static") {
      return STATIC;
    }

    return NONE;
  }

  void set_readahead_strategy(READAHEAD_STRATEGY rhs,
                              size_t min, size_t nom, size_t max, size_t rablocks, float sparse_ratio = 0.0)
  {
    XReadAheadStrategy = rhs;
    XReadAheadMin = min;
    XReadAheadNom = nom;
    XReadAheadMax = max;
    XReadAheadBlocksMax = rablocks;
    XReadAheadBlocksNom = 1;
    XReadAheadBlocksMin = 1;
    XReadAheadReenableHits = 0;
    XReadAheadSparseRatio = sparse_ratio;
  }

  float get_readahead_efficiency()
  {
    XrdSysCondVarHelper lLock(ReadCondVar());
    return (mTotalBytes) ? (100.0 * mTotalReadAheadHitBytes / mTotalBytes) : 0.0;
  }

  float get_readahead_volume_efficiency()
  {
    XrdSysCondVarHelper lLock(ReadCondVar());
    return (mTotalReadAheadBytes) ? (100.0 * mTotalReadAheadHitBytes /
                                     mTotalReadAheadBytes)
           : 0.0;
  }

  void set_id(uint64_t ino, fuse_req_t req)
  {
    mIno = ino;
    mReq = req;
    char lid[64];
    snprintf(lid, sizeof(lid), "logid:ino:%016lx", ino);
    SetLogId(lid);
  }

  uint64_t id() const
  {
    return mIno;
  }

  fuse_req_t req() const
  {
    return mReq;
  }

  // ---------------------------------------------------------------------- //

  Proxy() :
    XOpenAsyncCond(0),
    XWriteAsyncCond(0),
    XReadAsyncCond(0),
    XWriteQueueDirectSubmission(0),
    XWriteQueueScheduledSubmission(0),
    XCloseAfterWrite(false),
    XCloseAfterWriteTimeout(0),
    mReq(0), mIno(0), mDeleted(false)
  {
    XrdSysCondVarHelper lLock(XOpenAsyncCond);
    set_state(CLOSED);
    XrdCl::Env* env = XrdCl::DefaultEnv::GetEnv();
    env->PutInt("TimeoutResolution", 1);
    env->PutInt("MetalinkProcessing", 0);
    XReadAheadStrategy = NONE;
    XReadAheadMin = 4 * 1024;
    XReadAheadNom = 256 * 1204;
    XReadAheadMax = 1024 * 1024;
    XReadAheadBlocksMax = 16;
    XReadAheadBlocksNom = 1;
    XReadAheadBlocksMin = 1;
    XReadAheadReenableHits = 0;
    XReadAheadBlocksIs = 0;
    XReadAheadDisabled = false;
    XReadAheadSparseRatio = 0.0;
    mSeqDistance = 0;
    mPosition = 0;
    mReadAheadPosition = 0;
    mTotalBytes = 0;
    mTotalReadAheadHitBytes = 0;
    mTotalReadAheadBytes = 0;
    mAttached = 0;
    mTimeout = 0;
    mRChunksInFlight.store(0, std::memory_order_seq_cst);
    mDeleted = false;
    mReadAheadMaximumPosition = 64 * 1024ll * 1024ll * 1024ll * 1024ll;
    sProxy++;
  }

  void Collect()
  {
    WaitWrite();
    XrdSysCondVarHelper lLock(ReadCondVar());

    for (auto it = ChunkRMap().begin(); it != ChunkRMap().end(); ++it) {
      XrdSysCondVarHelper llLock(it->second->ReadCondVar());

      while (!it->second->done()) {
        it->second->ReadCondVar().WaitMS(25);
      }
    }
  }

  bool HasReadsInFlight()
  {
    return (read_chunks_in_flight()) ? true : false;
  }

  bool HasWritesInFlight()
  {
    // needs WriteCondVar locked
    return ChunkMap().size() ? true : false;
  }

  bool HasTooManyWritesInFlight()
  {
    XrdSysCondVarHelper lLock(WriteCondVar());
    return (ChunkMap().size() > 1024);
  }

  void DropReadAhead()
  {
    WaitWrite();
    XrdSysCondVarHelper lLock(ReadCondVar());

    for (auto it = ChunkRMap().begin(); it != ChunkRMap().end(); ++it) {
      XrdSysCondVarHelper llLock(it->second->ReadCondVar());

      while (!it->second->done()) {
        it->second->ReadCondVar().WaitMS(25);
      }
    }

    ChunkRMap().clear();
  }

  bool DoneReadAhead()
  {
    XrdSysCondVarHelper lLock(ReadCondVar());

    for (auto it = ChunkRMap().begin(); it != ChunkRMap().end(); ++it) {
      XrdSysCondVarHelper llLock(it->second->ReadCondVar());

      if (!it->second->done()) {
        return false;
      }
    }

    ChunkRMap().clear();
    return true;
  }


  // ---------------------------------------------------------------------- //

  virtual ~Proxy()
  {
    sProxy--;
    WaitOpen();

    // collect all pending read requests
    if (IsOpen()) {
      Collect();
    }

    // collect all pending write requests
    if (IsWaitWrite()) {
      CollectWrites();
    }

    eos_notice("ra-efficiency=%f ra-vol-efficiency=%f tot-bytes=%lu ra-bytes=%lu ra-hit-bytes=%lu ",
               get_readahead_efficiency(),
               get_readahead_volume_efficiency(),
               mTotalReadAheadBytes,
               mTotalReadAheadHitBytes);
  }

  // ---------------------------------------------------------------------- //

  class OpenAsyncHandler : public XrdCl::ResponseHandler
  // ---------------------------------------------------------------------- //
  {
  public:

    OpenAsyncHandler() { }

    OpenAsyncHandler(shared_proxy file) :
      mProxy(file) { }

    void SetProxy(shared_proxy file)
    {
      mProxy = file;
    }

    virtual ~OpenAsyncHandler() { }

    virtual void HandleResponseWithHosts(XrdCl::XRootDStatus* status,
                                         XrdCl::AnyObject* response,
                                         XrdCl::HostList* hostList);

    shared_proxy proxy()
    {
      return mProxy;
    }

  private:
    shared_proxy mProxy;
  };

  // ---------------------------------------------------------------------- //

  class CloseAsyncHandler : public XrdCl::ResponseHandler
  // ---------------------------------------------------------------------- //
  {
  public:

    CloseAsyncHandler() { }

    CloseAsyncHandler(shared_proxy file) :
      mProxy(file) { }

    void SetProxy(shared_proxy file)
    {
      mProxy = file;
    }

    virtual ~CloseAsyncHandler() { }

    virtual void HandleResponse(XrdCl::XRootDStatus* pStatus,
                                XrdCl::AnyObject* pResponse);

    shared_proxy proxy()
    {
      return mProxy;
    }

  private:
    shared_proxy mProxy;
  };


  // ---------------------------------------------------------------------- //

  class WriteAsyncHandler : public XrdCl::ResponseHandler
  // ---------------------------------------------------------------------- //
  {
  public:

    WriteAsyncHandler() { }

    WriteAsyncHandler(WriteAsyncHandler* other)
    {
      mProxy = other->proxy();
      *mBuffer = other->vbuffer();
      woffset = other->offset();
      mTimeout = other->timeout();
    }

    WriteAsyncHandler(shared_proxy file, uint32_t size, off_t off = 0,
                      uint16_t timeout = 0) : mProxy(file), woffset(off), mTimeout(timeout)
    {
      mBuffer = sWrBufferManager.get_buffer(size);
      mBuffer->resize(size);

      if (file) {
        std::lock_guard<std::mutex> lock(gBuffReferenceMutex);
        mId = std::to_string((uint64_t) file.get());
        mId += ":open=";
        mId += std::to_string(file->state());
        mId += ":";
        mId += file->url();
        gBufferReference[mId] = size;
      }

      XrdSysCondVarHelper lLock(mProxy->WriteCondVar());
      mProxy->WriteCondVar().Signal();
    }

    virtual ~WriteAsyncHandler()
    {
      {
        sWrBufferManager.put_buffer(mBuffer);
        std::lock_guard<std::mutex> lock(gBuffReferenceMutex);
        gBufferReference.erase(mId);
      }
      mProxy = 0;
    }

    void SetProxy(shared_proxy proxy)
    {
      mProxy = proxy;
    }

    char* buffer()
    {
      return &((*mBuffer)[0]);
    }

    const off_t offset()
    {
      return woffset;
    }

    const uint16_t timeout()
    {
      return mTimeout;
    }

    const std::vector<char>& vbuffer()
    {
      return *mBuffer;
    }

    shared_proxy proxy()
    {
      return mProxy;
    }

    void disable()
    {
      mProxy = 0;
    }

    void copy(const void* cbuffer, size_t size)
    {
      mBuffer->resize(size);
      memcpy(buffer(), cbuffer, size);
    }

    virtual void HandleResponse(XrdCl::XRootDStatus* pStatus,
                                XrdCl::AnyObject* pResponse);

    static std::mutex gBuffReferenceMutex;
    static std::map<std::string, uint64_t> gBufferReference;

    static void DumpReferences(std::string& out);

  private:
    shared_proxy mProxy;
    shared_buffer mBuffer;
    off_t woffset;
    uint16_t mTimeout;
    std::string mId;

  };

  // ---------------------------------------------------------------------- //

  class ReadAsyncHandler : public XrdCl::ResponseHandler
  // ---------------------------------------------------------------------- //
  {
  public:

    ReadAsyncHandler() : mAsyncCond(0) { }

    ReadAsyncHandler(ReadAsyncHandler* other) : mAsyncCond(0)
    {
      mProxy = other->proxy();

      if (other->valid()) {
        *mBuffer = other->vbuffer();
      }

      roffset = other->offset();
      mDone = false;
      mEOF = false;
      mCreationTime = other->creationtime();
    }

    ReadAsyncHandler(shared_proxy file, off_t off, uint32_t size,
                     bool blocking = true) : mProxy(file),
      mAsyncCond(0)
    {
      mBuffer = sRaBufferManager.get_buffer(size, blocking);

      if (valid()) {
        mBuffer->resize(size);
      }

      roffset = off;
      mDone = false;
      mEOF = false;
      mProxy = file;
      mCreationTime = time(NULL);

      if (valid()) {
        eos_static_debug("----: creating chunk offset=%ld size=%u addr=%lx", off, size,
                         this);
      }
    }

    virtual ~ReadAsyncHandler()
    {
      if (valid()) {
        eos_static_debug("----: releasing chunk offset=%d size=%u addr=%lx", roffset,
                         mBuffer->size(), this);
      }

      release_buffer();
    }

    void release_buffer()
    {
      if (valid()) {
        sRaBufferManager.put_buffer(mBuffer);
        mBuffer = 0;
      }
    }

    char* buffer()
    {
      return &((*mBuffer)[0]);
    }

    std::vector<char>& vbuffer()
    {
      return *mBuffer;
    }

    shared_proxy proxy()
    {
      return mProxy;
    }

    time_t creationtime()
    {
      return mCreationTime;
    }

    bool expired()
    {
      // a read should never take that long
      if ((time(NULL) - creationtime()) > 300) {
        return true;
      } else {
        return false;
      }
    }

    off_t offset()
    {
      return roffset;
    }

    size_t size()
    {
      return mBuffer->size();
    }

    bool valid()
    {
      // check for an allocated buffer
      return (mBuffer ? true : false);
    }
    bool matches(off_t off, uint32_t size,
                 off_t& match_offset, uint32_t& match_size)
    {
      if (!mBuffer) {
        return false;
      }

      if ((off >= roffset) &&
          (off < ((off_t)(roffset + mBuffer->size())))) {
        match_offset = off;

        if ((off + size) <= (off_t)(roffset + mBuffer->size())) {
          match_size = size;
        } else {
          match_size = (roffset + mBuffer->size() - off);
        }

        return true;
      }

      return false;
    }

    bool successor(off_t off, uint32_t size)
    {
      off_t match_offset;
      uint32_t match_size;

      if (matches((off_t) off, size, match_offset, match_size)) {
        return true;
      } else {
        return false;
      }
    }

    XrdSysCondVar& ReadCondVar()
    {
      return mAsyncCond;
    }

    XRootDStatus& Status()
    {
      return mStatus;
    }

    bool done()
    {
      return mDone;
    }

    bool eof()
    {
      return mEOF;
    }

    void disable()
    {
      mProxy = 0;
    }

    virtual void HandleResponse(XrdCl::XRootDStatus* pStatus,

                                XrdCl::AnyObject* pResponse);
    static size_t nexpired()
    {
      std::lock_guard<std::mutex> lock(gExpiredChunksMutex);
      return gExpiredChunks.size();
    }

    static std::mutex
    gExpiredChunksMutex; // protecting expired global expired chunks vector
    static std::vector<std::shared_ptr<ReadAsyncHandler>>
        gExpiredChunks;     // global expired chunks vector

  private:
    bool mDone;
    bool mEOF;
    shared_proxy mProxy;
    shared_buffer mBuffer;
    off_t roffset;
    XRootDStatus mStatus;
    XrdSysCondVar mAsyncCond;
    time_t mCreationTime;
  };


  // ---------------------------------------------------------------------- //
  typedef std::shared_ptr<ReadAsyncHandler> read_handler;
  typedef std::shared_ptr<WriteAsyncHandler> write_handler;

  typedef std::map<uint64_t, write_handler> chunk_map;
  typedef std::map<uint64_t, read_handler> chunk_rmap;

  typedef std::vector<write_handler> chunk_vector;
  typedef std::vector<read_handler> chunk_rvector;

  // ---------------------------------------------------------------------- //
  write_handler WriteAsyncPrepare(XrdCl::shared_proxy proxy, uint32_t size,
                                  uint64_t offset = 0,
                                  uint16_t timeout = 0);


  // ---------------------------------------------------------------------- //
  XRootDStatus WriteAsync(uint64_t offset,
                          uint32_t size,
                          const void* buffer,
                          write_handler handler,
                          uint16_t timeout);

  // ---------------------------------------------------------------------- //
  XRootDStatus ScheduleWriteAsync(
    const void* buffer,
    write_handler handler
  );


  // ---------------------------------------------------------------------- //

  XrdSysCondVar& OpenCondVar()
  {
    return XOpenAsyncCond;
  }

  // ---------------------------------------------------------------------- //

  XrdSysCondVar& WriteCondVar()
  {
    return XWriteAsyncCond;
  }

  // ---------------------------------------------------------------------- //

  XrdSysCondVar& ReadCondVar()
  {
    return XReadAsyncCond;
  }

  // ---------------------------------------------------------------------- //

  read_handler ReadAsyncPrepare(XrdCl::shared_proxy proxy,
                                off_t offset, uint32_t size,
                                bool blocking = true);


  // ---------------------------------------------------------------------- //
  XRootDStatus PreReadAsync(uint64_t offset,
                            uint32_t size,
                            read_handler handler,
                            uint16_t timeout);

  // ---------------------------------------------------------------------- //
  XRootDStatus WaitRead(read_handler handler);


  // ---------------------------------------------------------------------- //
  XRootDStatus ReadAsync(read_handler handler,
                         uint32_t size,
                         void* buffer,
                         uint32_t& bytesRead
                        );

  // ---------------------------------------------------------------------- //
  bool DoneAsync(read_handler handler);


  std::deque<write_handler>& WriteQueue()
  {
    return XWriteQueue;
  }

  void CleanWriteQueue()
  {
    XWriteQueueDirectSubmission = 0;
    XWriteQueueScheduledSubmission = 0;

    for (auto it = WriteQueue().begin(); it != WriteQueue().end(); ++it) {
      ChunkMap().erase((uint64_t)it->get());
    }

    WriteQueue().clear();
  }

  void inc_write_queue_direct_submissions()
  {
    XWriteQueueDirectSubmission++;
  }

  void inc_write_queue_scheduled_submissions()
  {
    XWriteQueueScheduledSubmission++;
  }

  float get_scheduled_submission_fraction()
  {
    return (XWriteQueueScheduledSubmission + XWriteQueueDirectSubmission) ? 100.0 *
           XWriteQueueScheduledSubmission / (XWriteQueueScheduledSubmission +
               XWriteQueueDirectSubmission) : 0;
  }

  const bool close_after_write()
  {
    return XCloseAfterWrite;
  }

  const uint16_t close_after_write_timeout()
  {
    return XCloseAfterWriteTimeout;
  }

  chunk_map& ChunkMap()
  {
    return XWriteAsyncChunks;
  }

  chunk_rmap& ChunkRMap()
  {
    return XReadAsyncChunks;
  }

  size_t nominal_read_ahead()
  {
    return XReadAheadNom;
  }

  void set_readahead_position(off_t pos)
  {
    mReadAheadPosition = pos;
  }

  void set_readahead_nominal(size_t size)
  {
    XReadAheadNom = size;
  }

  off_t readahead_position()
  {
    return mReadAheadPosition;
  }

  // ref counting
  void attach();
  size_t detach();
  bool attached();
  size_t get_attached();

  bool isDeleted()
  {
    return mDeleted;
  }

  void setDeleted()
  {
    mDeleted = true;
  }

  std::string url()
  {
    return mUrl;
  }

  OpenFlags::Flags flags()
  {
    return mFlags;
  }


  Access::Mode mode()
  {
    return mMode;
  }

  int read_chunks_in_flight()
  {
    return mRChunksInFlight.load();
  }

  void clear_read_chunks_in_flight()
  {
    mRChunksInFlight.store(0, std::memory_order_seq_cst);
  }

  void inc_read_chunks_in_flight()
  {
    mRChunksInFlight.fetch_add(1, std::memory_order_seq_cst);
  }

  void dec_read_chunks_in_flight()
  {
    mRChunksInFlight.fetch_sub(1, std::memory_order_seq_cst);
  }

  static ssize_t chunk_timeout(ssize_t to = 0)
  {
    if (to) {
      sChunkTimeout = to;
    }

    return sChunkTimeout;
  }
  static ssize_t
  sChunkTimeout; // time after we move an inflight chunk out of a proxy object into the static map

  Fuzzing& fuzzing()
  {
    return mFuzzing;
  }


  const char* Dump(std::string& out);

  class Protocol
  {
  public:
    Protocol() {}
    virtual ~Protocol() {}
    void Add(std::string);
    const char* Dump(std::string& out);
  private:
    XrdSysMutex mMutex;
    std::deque<std::string> mMessages;
  };

  Protocol& getProtocol()
  {
    return mProtocol;
  }

  static std::atomic<int> sProxy;
  static int Proxies()
  {
    return sProxy;
  }

  class ProxyStat : public XrdSysMutex, public std::map<std::string, uint64_t>
  {
  public:
    ProxyStat()
    {
      (*this)["recover:n"] = 0;
      (*this)["recover:read:exceeded"] = 0;
      (*this)["recover:write:disabled"] = 0;
      (*this)["recover:write:noproxy"] = 0;
      (*this)["recover:write:unrecoverable"] = 0;
      (*this)["recover:write:n"] = 0;
      (*this)["recover:read:disabled"] = 0;
      (*this)["recover:read:noproxy"] = 0;
      (*this)["recover:read:unrecoverble"] = 0;
      (*this)["recover:read:reopen:n"] = 0;
      (*this)["recover:read:reread:n"] = 0;
      (*this)["recover:read:reopen:disabled"] = 0;
      (*this)["recover:read:reopen:noserver:disabled"] = 0;
      (*this)["recover:read:reopen:failed"] = 0;
      (*this)["recover:read:reopen:success"] = 0;
      (*this)["recover:read:reopen:noserver:retry"] = 0;
      (*this)["recover:read:reopen:noserver:fatal"] = 0;
      (*this)["recover:write:reopen:n"] = 0;
      (*this)["recover:write:reopen:success"] = 0;
      (*this)["recover:write:reopen:disabled"] = 0;
      (*this)["recover:write:reopen:noserver::retry"] = 0;
      (*this)["recover:write:reopen:noserver::disabled"] = 0;
      (*this)["recover:write:reopen:unrecoverable"] = 0;
      (*this)["recover:write:reopen:overquota"] = 0;
      (*this)["recover:write:reopen:success"] = 0;
      (*this)["recover:write:reopen:nosever"] = 0;
      (*this)["recover:write:reopen:noserver:failed"] = 0;
      (*this)["recover:read:n"] = 0;
      (*this)["recover:read:success"] = 0;
      (*this)["recover:read:failed"] = 0;
      (*this)["recover:write:n"] = 0;
      (*this)["recover:write:unrecoverable"] = 0;
      (*this)["recover:write:fromcache"] = 0;
      (*this)["recover:write:fromremote"] = 0;
      (*this)["recover:write:fromcache:failed"] = 0;
      (*this)["recover:write:fromremote:local:failed"] = 0;
      (*this)["recover:write:fromcache:read:failed"] = 0;
      (*this)["recover:write:fromremote:read:failed"] = 0;
      (*this)["recover:write:fromremote:localwrite:failed"] = 0;
      (*this)["recover:write:fromremote:beginflush:failed"] = 0;
      (*this)["recover:write:fromremote:endflush:failed"] = 0;
      (*this)["recover:write:fromremote:write:failed"] = 0;
      (*this)["recover:write:journalflush:failed"] = 0;
      (*this)["recover:write:journalflush:success"] = 0;
      (*this)["recover:write:nocache:failed"] = 0;
    }
  };

  class ProxyStatHandle
  {
  public:
    static std::shared_ptr<ProxyStatHandle> Get();
    ProxyStatHandle()
    {
      sProxyStats.Lock();
    }

    ~ProxyStatHandle()
    {
      sProxyStats.UnLock();
    }
    ProxyStat& Stats()
    {
      return sProxyStats;
    }
    static ProxyStat sProxyStats;
  };

  std::string getLastUrl()
  {
    return mLastUrl;
  }


private:
  std::atomic<OPEN_STATE> open_state;
  struct timespec open_state_time;
  XRootDStatus XOpenState;
  OpenAsyncHandler XOpenAsyncHandler;
  CloseAsyncHandler XCloseAsyncHandler;
  XrdSysCondVar XOpenAsyncCond;
  XrdSysCondVar XWriteAsyncCond;
  XrdSysCondVar XReadAsyncCond;
  chunk_map XWriteAsyncChunks;
  chunk_rmap XReadAsyncChunks;

  Fuzzing mFuzzing;

  // this static map will take over chunks where we don't see callbacks in a reasonable time
  static chunk_vector sTimeoutWriteAsyncChunks;
  static chunk_rvector sTimeoutReadAsyncChunks;
  static XrdSysMutex sTimeoutAsyncChunksMutex;


  XRootDStatus XReadState;
  XRootDStatus XWriteState;

  std::deque<write_handler> XWriteQueue;
  size_t XWriteQueueDirectSubmission;
  size_t XWriteQueueScheduledSubmission;

  bool XCloseAfterWrite;
  uint16_t XCloseAfterWriteTimeout;

  READAHEAD_STRATEGY XReadAheadStrategy;
  size_t XReadAheadMin; // minimum ra block size when re-enabling
  size_t XReadAheadNom; // nominal ra block size when
  size_t XReadAheadMax; // maximum pow2 scaled window
  size_t XReadAheadBlocksMin; // minimum number of prefetch blocks
  size_t XReadAheadBlocksNom; // nominal number of prefetch blocks
  size_t XReadAheadBlocksMax; // maximum number of prefetch blocks
  size_t XReadAheadBlocksIs; // current blocks in the read-ahead
  size_t XReadAheadReenableHits; // sequential read hits in a row
  bool   XReadAheadDisabled; // one-off disabling of read-ahead
  double XReadAheadSparseRatio; // sparse ratio when we permanently disable read-ahead
  off_t mPosition;
  off_t mReadAheadPosition;
  off_t mTotalBytes;
  off_t mTotalReadAheadHitBytes;
  off_t mTotalReadAheadBytes;
  off_t mReadAheadMaximumPosition;
  off_t mSeqDistance;
  XrdSysMutex mAttachedMutex;
  size_t mAttached;
  fuse_req_t mReq;
  uint64_t mIno;

  std::string mUrl;
  std::string mLastUrl;

  OpenFlags::Flags mFlags;
  Access::Mode mMode;
  uint16_t mTimeout;

  std::atomic<int> mRChunksInFlight;

  Protocol mProtocol;
  bool mDeleted;
};

}

#endif /* FUSE_XRDCLPROXY_HH_ */
