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

namespace XrdCl
{

  class Proxy : public XrdCl::File, public eos::common::LogId
  {
  public:
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

    XRootDStatus WaitClose();

    // ---------------------------------------------------------------------- //
    XRootDStatus Close(uint16_t         timeout = 0);

    // ---------------------------------------------------------------------- //
    bool HadFailures();

    // ---------------------------------------------------------------------- //

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

    double state_age()
    {
      return ((double) eos::common::Timing::GetCoarseAgeInNs(&open_state_time, 0) / 1000000000.0);
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
        while ( !it->second->done() )
          it->second->ReadCondVar().WaitMS(25);
      }
    }

    // ---------------------------------------------------------------------- //

    virtual ~Proxy()
    {
      Collect();
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
        mBuffer = other->vbuffer();
      }

      WriteAsyncHandler(Proxy* file, uint32_t size)  : mProxy(file)
      {
        XrdSysCondVarHelper lLock(mProxy->WriteCondVar());
        mBuffer.resize(size);
        mProxy->WriteCondVar().Signal();

      }

      virtual ~WriteAsyncHandler()
      {
      }

      const char* buffer()
      {
        return &mBuffer[0];
      }

      const std::vector<char>& vbuffer()
      {
        return mBuffer;
      }

      Proxy* proxy()
      {
        return mProxy;
      }

      void copy(const void* buffer, size_t size)
      {
        mBuffer.resize(size);
        memcpy(&mBuffer[0], buffer, size);
      }

      virtual void HandleResponse (XrdCl::XRootDStatus* pStatus,
                                   XrdCl::AnyObject* pResponse);

    private:
      Proxy* mProxy;
      std::vector<char> mBuffer;
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
        mBuffer = other->vbuffer();
        roffset = other->offset();
        mDone = false;
        mEOF = false;
      }

      ReadAsyncHandler(Proxy* file, off_t off, uint32_t size)  : mProxy(file), mAsyncCond(0)
      {
        mBuffer.resize(size);
        roffset = off;
        mDone = false;
        mEOF = false;
        mProxy = file;
        eos_static_debug("----: creating chunk offset=%ld", off);
      }

      virtual ~ReadAsyncHandler()
      {
      }

      char* buffer()
      {
        return &mBuffer[0];
      }

      std::vector<char>& vbuffer()
      {
        return mBuffer;
      }

      Proxy* proxy()
      {
        return mProxy;
      }

      off_t offset()
      {
        return roffset;
      }

      bool matches(off_t off, uint32_t size,
                   off_t& match_offset, uint32_t& match_size)
      {
        if ( (off >= roffset) &&
            (off < ((off_t) (roffset + mBuffer.size()) )) )
        {
          match_offset = off;
          if ( (off + size) <= (off_t) (roffset + mBuffer.size()) )
            match_size = size;
          else
            match_size = (roffset + mBuffer.size() - off);
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
      std::vector<char> mBuffer;
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
    write_handler WriteAsyncPrepare(uint32_t size);


    // ---------------------------------------------------------------------- //
    XRootDStatus WriteAsync( uint64_t          offset,
                            uint32_t           size,
                            const void        *buffer,
                            write_handler      handler,
                            uint16_t           timeout);

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
