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

#include "XrdSys/XrdSysPthread.hh"
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
#include "llfusexx.hh"
#include "common/Logging.hh"

#include <memory>
#include <map>
#include <string>
#include <deque>
#include <queue>
#include <thread>

// need some redefines for XRootD v3
#ifndef EOSCITRINE

#define kXR_overQuota (kXR_inProgress+1)
#define kXR_SigVerErr (kXR_inProgress+2)
#define kXR_DecryptErr (kXR_inProgress+3)
#define kXR_Overloaded (kXR_inProgress+4)

static int XtoErrno( int xerr )
{
  switch(xerr)
    {case kXR_ArgInvalid:    return EINVAL;
    case kXR_ArgMissing:    return EINVAL;
    case kXR_ArgTooLong:    return ENAMETOOLONG;
    case kXR_FileLocked:    return EDEADLK;
    case kXR_FileNotOpen:   return EBADF;
    case kXR_FSError:       return EIO;
    case kXR_InvalidRequest:return EEXIST;
    case kXR_IOError:       return EIO;
    case kXR_NoMemory:      return ENOMEM;
    case kXR_NoSpace:       return ENOSPC;
    case kXR_NotAuthorized: return EACCES;
    case kXR_NotFound:      return ENOENT;
    case kXR_ServerError:   return ENOMSG;
    case kXR_Unsupported:   return ENOSYS;
    case kXR_noserver:      return EHOSTUNREACH;
    case kXR_NotFile:       return ENOTBLK;
    case kXR_isDirectory:   return EISDIR;
    case kXR_Cancelled:     return ECANCELED;
    case kXR_ChkLenErr:     return EDOM;
    case kXR_ChkSumErr:     return EDOM;
    case kXR_inProgress:    return EINPROGRESS;
    case kXR_overQuota:     return EDQUOT;
    case kXR_SigVerErr:     return EILSEQ;
    case kXR_DecryptErr:    return ERANGE;
    case kXR_Overloaded:    return EUSERS;
    default:                return ENOMSG;
    }
}

#endif

namespace XrdCl
{
  // ---------------------------------------------------------------------- //
  typedef std::shared_ptr<std::vector<char>> shared_buffer;
  // ---------------------------------------------------------------------- //

  // ---------------------------------------------------------------------- //
  class BufferManager : public XrdSysMutex
  // ---------------------------------------------------------------------- //
  {
  public:

    BufferManager(size_t _max = 128, size_t _default_size = 128 * 1024, size_t _max_inflight_size = 1*1024*1024*1024)
    {
      max = _max;
      buffersize = _default_size;
      queued_size = 0;
      inflight_size = 0;
      max_inflight_size = _max_inflight_size;
    }

    virtual ~BufferManager() {}

    void configure(size_t _max, size_t _size)
    {
      max = _max;
      buffersize = _size;
    }

    shared_buffer get_buffer(size_t size)
    {
      // make sure, we don't have more buffers in flight than max_inflight_size
      do {
	size_t cnt=0;
	{
	  XrdSysMutexHelper lLock(this);
	  if (inflight_size < max_inflight_size)
	    break;
	}
	// we wait that the situation relaxes
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	if (!(cnt%50))
	  eos_static_warning("inflight-buffer exceeds maximum of %lu bytes", max_inflight_size);
	cnt++;
      } while (1);

      XrdSysMutexHelper lLock(this);

      size_t cap_size = (size > buffersize)?size : buffersize;

      if (!queue.size()) {
	inflight_size += cap_size;
	return std::make_shared<std::vector<char>>( cap_size , 0);
      } else {
	shared_buffer buffer = queue.front();
	queued_size -= buffer->capacity();
	buffer->resize(cap_size);
	buffer->reserve(cap_size);
	inflight_size += buffer->capacity();

	queue.pop();
	return buffer;
      }
    }

    void put_buffer(shared_buffer buffer)
    {
      XrdSysMutexHelper lLock(this);
      inflight_size -= buffer->capacity();

      if (queue.size() == max) {
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
    
  private:
    std::queue<shared_buffer> queue;
    size_t max;
    size_t buffersize;
    size_t queued_size;
    size_t inflight_size;
    size_t max_inflight_size;
  } ;


  // ---------------------------------------------------------------------- //
  class Proxy : public XrdCl::File, public eos::common::LogId
  // ---------------------------------------------------------------------- //
  {
  public:

    static BufferManager sWrBufferManager; // write buffer manager
    static BufferManager sRaBufferManager; // async read buffer manager

    // ---------------------------------------------------------------------- //
    XRootDStatus OpenAsync( const std::string &url,
                           OpenFlags::Flags   flags,
                           Access::Mode       mode,
                           uint16_t         timeout);

    // ---------------------------------------------------------------------- //
    XRootDStatus WaitOpen();

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
    bool IsWaitWrite();

    // ---------------------------------------------------------------------- //
    bool OutstandingWrites();

    // ---------------------------------------------------------------------- //
    bool HadFailures(std::string &message);

    // ---------------------------------------------------------------------- //
    XRootDStatus Write( uint64_t         offset,
                       uint32_t         size,
                       const void      *buffer,
                       ResponseHandler *handler,
                       uint16_t         timeout = 0 );

    XRootDStatus Write( uint64_t         offset,
                       uint32_t         size,
                       const void      *buffer,
                       uint16_t         timeout = 0 )
    {
      return File::Write(offset, size, buffer, timeout);
    }

    // ---------------------------------------------------------------------- //
    XRootDStatus Read( uint64_t  offset,
                      uint32_t  size,
                      void     *buffer,
                      uint32_t &bytesRead,
                      uint16_t  timeout = 0 );

    // ---------------------------------------------------------------------- //
    XRootDStatus Sync( uint16_t timeout = 0 );


    // ---------------------------------------------------------------------- //

    XRootDStatus CloseAsync(uint16_t         timeout = 0 );

    XRootDStatus ScheduleCloseAsync(uint16_t timeout = 0 );


    XRootDStatus WaitClose();

    // ---------------------------------------------------------------------- //
    XRootDStatus Close(uint16_t         timeout = 0);

    // ---------------------------------------------------------------------- //
    bool HadFailures();

    // ---------------------------------------------------------------------- //

    static int status2errno(const XRootDStatus& status)
    {
      if (!status.errNo)
	return 0;

      if (status.errNo < kXR_ArgInvalid)
      {
	return status.errNo;
      }
      else
      {
#ifndef EOSCITRINE
	return XtoErrno(status.errNo);
#else
	return XProtocol::toErrno(status.errNo);
#endif
      }
    }

    enum OPEN_STATE
    {
      CLOSED = 0,
      OPENING = 1,
      OPENED = 2,
      WAITWRITE = 3,
      CLOSING = 4,
      FAILED = 5,
      CLOSEFAILED = 6,
    } ;

    OPEN_STATE state()
    {
      // lock XOpenAsyncCond from outside
      return open_state;
    }

    OPEN_STATE stateTS() {
      XrdSysCondVarHelper lLock(OpenCondVar());
      return state();
    }

    XRootDStatus write_state()
    {
      return XWriteState;
    }

    void set_state(OPEN_STATE newstate, XRootDStatus* xs=0)
    {
      // lock XOpenAsyncCond from outside
      open_state = newstate;
      eos::common::Timing::GetTimeSpec(open_state_time);
      if (xs)
      {
        XOpenState = *xs;
      }
    }

    // TS stands for "thread-safe"
    void set_state_TS(OPEN_STATE newstate, XRootDStatus* xs=0) {
      XrdSysCondVarHelper lLock(OpenCondVar());
      set_state(newstate, xs);
    }

    double state_age()
    {
      return ((double) eos::common::Timing::GetAgeInNs(&open_state_time, 0) / 1000000000.0);
    }

    void set_writestate(XRootDStatus* xs)
    {
      XWriteState = *xs;
    }

    enum READAHEAD_STRATEGY
    {
      NONE = 0,
      STATIC = 1,
      DYNAMIC = 2
    } ;


    static READAHEAD_STRATEGY readahead_strategy_from_string(const std::string& strategy)
    {
      if (strategy == "dynamic")
	return DYNAMIC;
      if (strategy == "static")
	return STATIC;
      return NONE;
    }

    void set_readahead_strategy(READAHEAD_STRATEGY rhs,
                                size_t min, size_t nom, size_t max)
    {
      XReadAheadStrategy = rhs;
      XReadAheadMin = min;
      XReadAheadNom = nom;
      XReadAheadMax = max;
    }

    float get_readahead_efficiency()
    {
      XrdSysCondVarHelper lLock(ReadCondVar());
      return (mTotalBytes) ? (100.0 * mTotalReadAheadHitBytes / mTotalBytes) : 0.0;
    }

    off_t aligned_offset(off_t offset) const
    {
      return offset / XReadAheadNom*XReadAheadNom;
    }

    void set_id( uint64_t ino, fuse_req_t req)
    {
      mIno = ino;
      mReq = req;
      char lid[64];
      snprintf(lid, sizeof (lid), "logid:ino:%016lx", ino);
      SetLogId(lid);
    }

    uint64_t id () const
    {
      return mIno;
    }

    fuse_req_t req () const
    {
      return mReq;
    }

    // ---------------------------------------------------------------------- //

    Proxy() : XOpenAsyncHandler(this),
	      XCloseAsyncHandler(this),
	      XOpenAsyncCond(0),
	      XWriteAsyncCond(0),
	      XReadAsyncCond(0),
	      XWriteQueueDirectSubmission(0),
	      XWriteQueueScheduledSubmission(0),
	      XCloseAfterWrite(false),
	      XCloseAfterWriteTimeout(0),
	      mReq(0), mIno(0)
    {
      XrdSysCondVarHelper lLock(XOpenAsyncCond);
      set_state(CLOSED);
      XrdCl::Env* env = XrdCl::DefaultEnv::GetEnv();
      env->PutInt( "TimeoutResolution", 1 );
      XReadAheadStrategy = NONE;
      XReadAheadMin = 4 * 1024;
      XReadAheadNom = 256 * 1204;
      XReadAheadMax = 1024 * 1024;
      mPosition = 0;
      mTotalBytes = 0;
      mTotalReadAheadHitBytes  = 0;
      mAttached = 0;
    }

    void Collect()
    {
      WaitWrite();
      XrdSysCondVarHelper lLock(ReadCondVar());
      for (auto it = ChunkRMap().begin(); it != ChunkRMap().end(); ++it)
      {
        XrdSysCondVarHelper llLock(it->second->ReadCondVar());
        while ( !it->second->done() )
          it->second->ReadCondVar().WaitMS(25);
      }
    }

    void DropReadAhead()
    {
      WaitWrite();
      XrdSysCondVarHelper lLock(ReadCondVar());
      for (auto it = ChunkRMap().begin(); it != ChunkRMap().end(); ++it)
      {
        XrdSysCondVarHelper llLock(it->second->ReadCondVar());
        while ( !it->second->done() )
          it->second->ReadCondVar().WaitMS(25);
      }
      ChunkRMap().clear();
    }

    
    // ---------------------------------------------------------------------- //

    virtual ~Proxy()
    {
      Collect();
      eos_notice("ra-efficiency=%f", get_readahead_efficiency());
    }

    // ---------------------------------------------------------------------- //

    class OpenAsyncHandler : public XrdCl::ResponseHandler
    // ---------------------------------------------------------------------- //
    {
    public:

      OpenAsyncHandler()
      {
      }

      OpenAsyncHandler(Proxy* file) :
      mProxy(file)
      {
      }

      virtual ~OpenAsyncHandler()
      {
      }

      virtual void HandleResponseWithHosts(XrdCl::XRootDStatus* status,
                                           XrdCl::AnyObject* response,
                                           XrdCl::HostList* hostList);

      Proxy* proxy()
      {
        return mProxy;
      }

    private:
      Proxy* mProxy;
    } ;

    // ---------------------------------------------------------------------- //

    class CloseAsyncHandler : public XrdCl::ResponseHandler
    // ---------------------------------------------------------------------- //
    {
    public:

      CloseAsyncHandler()
      {
      }

      CloseAsyncHandler(Proxy* file) :
      mProxy(file)
      {
      }

      virtual ~CloseAsyncHandler()
      {
      }

      virtual void HandleResponse (XrdCl::XRootDStatus* pStatus,
                                   XrdCl::AnyObject* pResponse);

      Proxy* proxy()
      {
        return mProxy;
      }

    private:
      Proxy* mProxy;
    } ;


    // ---------------------------------------------------------------------- //

    class WriteAsyncHandler : public XrdCl::ResponseHandler
    // ---------------------------------------------------------------------- //
    {
    public:

      WriteAsyncHandler()
      {
      }

      WriteAsyncHandler(WriteAsyncHandler* other )
      {
        mProxy = other->proxy();
        *mBuffer = other->vbuffer();
	woffset = other->offset();
	mTimeout = other->timeout();
      }

      WriteAsyncHandler(Proxy* file, uint32_t size, off_t off=0, uint16_t timeout=0)  : mProxy(file), woffset(off), mTimeout(timeout)
      {
        mBuffer = sWrBufferManager.get_buffer(size);
	mBuffer->resize(size);
        XrdSysCondVarHelper lLock(mProxy->WriteCondVar());
        mProxy->WriteCondVar().Signal();
      }

      virtual ~WriteAsyncHandler()
      {
        sWrBufferManager.put_buffer(mBuffer);
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

      Proxy* proxy()
      {
        return mProxy;
      }

      void copy(const void* cbuffer, size_t size)
      {
        mBuffer->resize(size);
        memcpy(buffer(), cbuffer, size);
      }

      virtual void HandleResponse (XrdCl::XRootDStatus* pStatus,
                                   XrdCl::AnyObject* pResponse);

    private:
      Proxy* mProxy;
      shared_buffer mBuffer;
      off_t woffset;
      uint16_t mTimeout;
    } ;

    // ---------------------------------------------------------------------- //

    class ReadAsyncHandler : public XrdCl::ResponseHandler
    // ---------------------------------------------------------------------- //
    {
    public:

      ReadAsyncHandler() : mAsyncCond(0)
      {
      }

      ReadAsyncHandler(ReadAsyncHandler* other ) : mAsyncCond(0)
      {
        mProxy = other->proxy();
        *mBuffer = other->vbuffer();
        roffset = other->offset();
        mDone = false;
        mEOF = false;
      }

      ReadAsyncHandler(Proxy* file, off_t off, uint32_t size)  : mProxy(file), mAsyncCond(0)
      {
        mBuffer = sRaBufferManager.get_buffer(size);
	mBuffer->resize(size);
        roffset = off;
        mDone = false;
        mEOF = false;
        mProxy = file;
        eos_static_debug("----: creating chunk offset=%ld", off);
      }

      virtual ~ReadAsyncHandler()
      {
        eos_static_debug("----: releasing chunk offset=%d size=%u addr=%lx", roffset, mBuffer->size(), this);
	sRaBufferManager.put_buffer(mBuffer);
      }

      char* buffer()
      {
        return &((*mBuffer)[0]);
      }

      std::vector<char>& vbuffer()
      {
        return *mBuffer;
      }

      Proxy* proxy()
      {
        return mProxy;
      }

      off_t offset()
      {
        return roffset;
      }

      size_t size()
      {
	return mBuffer->size();
      }

      bool matches(off_t off, uint32_t size,
                   off_t& match_offset, uint32_t& match_size)
      {
        if ( (off >= roffset) &&
            (off < ((off_t) (roffset + mBuffer->size()) )) )
        {
          match_offset = off;
          if ( (off + size) <= (off_t) (roffset + mBuffer->size()) )
            match_size = size;
          else
            match_size = (roffset + mBuffer->size() - off);
          return true;
        }

        return false;
      }

      bool successor(off_t off, uint32_t size)
      {
        off_t match_offset;
        uint32_t match_size;
        if (matches((off_t) (off + proxy()->nominal_read_ahead()), size, match_offset, match_size))
        {
          return true;
        }
        else
        {
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

      virtual void HandleResponse (XrdCl::XRootDStatus* pStatus,
                                   XrdCl::AnyObject* pResponse);

    private:
      bool mDone;
      bool mEOF;
      Proxy* mProxy;
      shared_buffer mBuffer;
      off_t roffset;
      XRootDStatus mStatus;
      XrdSysCondVar mAsyncCond;
    } ;


    // ---------------------------------------------------------------------- //
    typedef std::shared_ptr<ReadAsyncHandler> read_handler;
    typedef std::shared_ptr<WriteAsyncHandler> write_handler;

    typedef std::map<uint64_t, write_handler> chunk_map;
    typedef std::map<uint64_t, read_handler> chunk_rmap;

    // ---------------------------------------------------------------------- //
    write_handler WriteAsyncPrepare(uint32_t size, uint64_t offset=0, uint16_t timeout=0);


    // ---------------------------------------------------------------------- //
    XRootDStatus WriteAsync( uint64_t          offset,
                             uint32_t           size,
                             const void        *buffer,
                             write_handler      handler,
                             uint16_t           timeout);

    // ---------------------------------------------------------------------- //
    XRootDStatus ScheduleWriteAsync( 
 				     const void   *buffer,
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

    read_handler ReadAsyncPrepare(off_t offset, uint32_t size);


    // ---------------------------------------------------------------------- //
    XRootDStatus PreReadAsync( uint64_t          offset,
                              uint32_t           size,
                              read_handler       handler,
                              uint16_t           timeout);

    // ---------------------------------------------------------------------- //
    XRootDStatus WaitRead( read_handler handler );


    // ---------------------------------------------------------------------- //
    XRootDStatus ReadAsync( read_handler  handler,
                           uint32_t  size,
                           void     *buffer,
                           uint32_t &bytesRead
                           );

    std::deque<write_handler>& WriteQueue() 
    {
      return XWriteQueue;
    }

    void inc_write_queue_direct_submissions() { XWriteQueueDirectSubmission++; }

    void inc_write_queue_scheduled_submissions() { XWriteQueueScheduledSubmission++; }

    float get_scheduled_submission_fraction() { return (XWriteQueueScheduledSubmission + XWriteQueueDirectSubmission)? 100.0 * XWriteQueueScheduledSubmission / (XWriteQueueScheduledSubmission + XWriteQueueDirectSubmission) : 0; }


    const bool close_after_write() { return XCloseAfterWrite; }

    const uint16_t close_after_write_timeout() { return XCloseAfterWriteTimeout; }

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

    // ref counting
    void attach();
    size_t detach();
    bool attached();

    std::string url()
    {
      return mUrl;
    }

  private:
    OPEN_STATE open_state;
    struct timespec open_state_time;
    XRootDStatus XOpenState;
    OpenAsyncHandler XOpenAsyncHandler;
    CloseAsyncHandler XCloseAsyncHandler;
    XrdSysCondVar XOpenAsyncCond;
    XrdSysCondVar XWriteAsyncCond;
    XrdSysCondVar XReadAsyncCond;
    chunk_map XWriteAsyncChunks;
    chunk_rmap XReadAsyncChunks;

    XRootDStatus XWriteState;

    std::deque<write_handler> XWriteQueue;
    size_t XWriteQueueDirectSubmission;
    size_t XWriteQueueScheduledSubmission;

    bool XCloseAfterWrite;
    uint16_t XCloseAfterWriteTimeout;

    READAHEAD_STRATEGY XReadAheadStrategy;
    size_t XReadAheadMin;
    size_t XReadAheadNom;
    size_t XReadAheadMax;

    off_t mPosition;
    off_t mTotalBytes;
    off_t mTotalReadAheadHitBytes;

    XrdSysMutex mAttachedMutex;
    size_t mAttached;
    fuse_req_t mReq;
    uint64_t mIno;

    std::string mUrl;
  } ;
}

#endif /* FUSE_XRDCLPROXY_HH_ */
