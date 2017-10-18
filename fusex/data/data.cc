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
#include "data/cachesyncer.hh"
#include "data/journalcache.hh"
#include "misc/MacOSXHelper.hh"
#include "misc/fusexrdlogin.hh"
#include "common/Logging.hh"
#include <iostream>
#include <sstream>

bufferllmanager data::datax::sBufferManager;

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
  datamap.run();
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
  if (datamap.count(ino))
  {
    shared_data io = datamap[ino];
    io->attach(); // client ref counting

    return io;
  }
  else
  {
    shared_data io = std::make_shared<datax>(md);
    io->set_id(ino, req);
    datamap[(fuse_ino_t) io->id()] = io;
    io->attach();
    return io;
  }
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::release(fuse_req_t req,
              fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(datamap);
  if (datamap.count(ino))
  {
    shared_data io = datamap[ino];
    io->detach();
    // the object is cleaned by the flush thread
  }
  if (datamap.count(ino + 0xffffffff))
  {
    // in case this is an unlinked object
    shared_data io = datamap[ino + 0xffffffff];
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
  if (datamap.count(ino))
  {
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
  XrdSysMutexHelper mLock(datamap);
  if (datamap.count(ino))
  {
    // wait for open in flight to be done
    datamap[ino]->WaitOpen();
    datamap[ino]->unlink(req);
    // put the unlinked inode in a high bucket, will be removed by the flush thread
    datamap[ino + 0xffffffff] = datamap[ino];
    datamap.erase(ino);
    eos_static_info("datacache::unlink size=%lu", datamap.size());
  }
  else
  {
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

  return flush_nolock(req);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::flush_nolock(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  eos_info("");

  if (mFile->journal())
  {
    if ( (ssize_t) mFile->journal()->size() > (ssize_t) mFile->file()->prefetch_size())
    {
      eos_debug("no attach jsize=%ld psize=%ld", (ssize_t) mFile->journal()->size(), (ssize_t) mFile->file()->prefetch_size());
      if (mWaitForOpen)
      {
        if (journalflush(req))
        {
          errno = EREMOTEIO ;
          return -1;
        }
        mWaitForOpen = false;
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

  // we have to push the journal now
  if (!mFile->xrdiorw(req)->WaitOpen().IsOK())
  {
    eos_err("async journal-cache-wait-open failed - ino=%08lx", id());
    return -1;
  }

  eos_info("syncing cache");
  cachesyncer cachesync(*((XrdCl::File*)mFile->xrdiorw(req)));
  if (((journalcache*) mFile->journal())->remote_sync(cachesync))
  {
    eos_err("async journal-cache-sync failed - ino=%08lx", id());
    return -1;
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

  // we have to push the journal now
  if (!mFile->xrdiorw(cid)->WaitOpen().IsOK())
  {
    eos_err("async journal-cache-wait-open failed - ino=%08lx", id());
    return -1;
  }

  if (mFile->journal())
  {
    eos_info("syncing cache");
    cachesyncer cachesync(*((XrdCl::File*)mFile->xrdiorw(cid)));
    if (((journalcache*) mFile->journal())->remote_sync(cachesync))
    {
      eos_err("async journal-cache-sync failed - ino=%08lx", id());
      return -1;
    }
  }
  eos_info("retc=0");
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::attach(fuse_req_t freq, std::string& cookie, int flags)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(mLock);

  bool isRW = false;

  mFlags = flags;

  if (flags & (O_RDWR | O_WRONLY) )
    isRW = true;

  eos_info("cookie=%s flags=%o isrw=%d md-size=%d %s", cookie.c_str(), flags,
           isRW, mMd->size(),
           isRW?mRemoteUrlRW.c_str() : mRemoteUrlRO.c_str());


  if ( flags & O_SYNC )
  {
    mFile->disable_caches();
  }

  int bcache = mFile->file() ? mFile->file()->attach(freq, cookie, isRW) : 0;
  int jcache = mFile->journal() ? mFile->journal()->attach(freq, cookie, isRW) : 0;

  if (bcache < 0)
  {
    char msg[1024];
    snprintf(msg, sizeof (msg), "attach to cache failed - ino=%08lx", id());
    throw std::runtime_error(msg);
  }

  if (jcache < 0)
  {
    char msg[1024];
    snprintf(msg, sizeof (msg), "attach to journal failed - ino=%08lx", id());
    throw std::runtime_error(msg);
  }


  if (isRW)
  {
    if (!mFile->xrdiorw(freq) || mFile->xrdiorw(freq)->IsClosing() || mFile->xrdiorw(freq)->IsClosed())
    {
      if (mFile->xrdiorw(freq) && (mFile->xrdiorw(freq)->IsClosing() || mFile->xrdiorw(freq)->IsClosed()))
      {
        mFile->xrdiorw(freq)->WaitClose();
      }
      else
      {
        // attach an rw io object
        mFile->set_xrdiorw(freq, new XrdCl::Proxy());
        mFile->xrdiorw(freq)->attach();
        mFile->xrdiorw(freq)->set_id(id(), req());
      }
      XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update;
      XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW | XrdCl::Access::UX;
      mFile->xrdiorw(freq)->OpenAsync(mRemoteUrlRW.c_str(), targetFlags, mode, 0);
    }
    else
    {
      mFile->xrdiorw(freq)->attach();
    }
  }
  else
  {
    if (!mFile->xrdioro(freq) || mFile->xrdioro(freq)->IsClosing() || mFile->xrdioro(freq)->IsClosed())
    {
      if (mFile->xrdioro(freq) && (mFile->xrdioro(freq)->IsClosing() || mFile->xrdioro(freq)->IsClosed()))
      {
        mFile->xrdioro(freq)->WaitClose();
      }
      else
      {
        mFile->set_xrdioro(freq, new XrdCl::Proxy());
        mFile->xrdioro(freq)->attach();
        mFile->xrdioro(freq)->set_id(id(), req());

        if ( ! (flags & O_SYNC))
        {
          mFile->xrdioro(freq)->set_readahead_strategy(XrdCl::Proxy::DYNAMIC, 4096, 1 * 1024 * 1024, 8 * 1024 * 1024);
        }
      }
      XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Read;
      XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UX;

      // we might need to wait for a creation to go through
      WaitOpen();

      mFile->xrdioro(freq)->OpenAsync(mRemoteUrlRO.c_str(), targetFlags, mode, 0);
    }
    else
    {
      mFile->xrdioro(freq)->attach();
    }
  }

  return bcache | jcache;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
data::datax::prefetch(fuse_req_t req, bool lock)
/* -------------------------------------------------------------------------- */
{
  eos_info("handler=%d file=%lx size=%lu md-size=%lu", mPrefetchHandler ? 1 : 0,
           mFile ? mFile->file() : 0,
           mFile ? mFile->file() ? mFile->file()->size() : 0 : 0,
           mMd->size());
  
  if (lock)
    mLock.Lock();
  
  if (!mPrefetchHandler && mFile->file() && !mFile->file()->size() && mMd->size())
  {
    XrdCl::Proxy* proxy = mFile->xrdioro(req) ? mFile->xrdioro(req) : mFile->xrdiorw(req);

    if (proxy)
    {
      XrdCl::XRootDStatus status;
      // send an async read request
      mPrefetchHandler = proxy->ReadAsyncPrepare(0, mFile->file()->prefetch_size());
      status = proxy->PreReadAsync(0, mFile->file()->prefetch_size(), mPrefetchHandler, 0);
      if (!status.IsOK())
      {

        eos_err("pre-fetch failed error=%s", status.ToStr().c_str());
        mPrefetchHandler = 0;
      }
    }
  }
  if (lock)
    mLock.UnLock();
  
  return mPrefetchHandler ? true : false;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::datax::WaitPrefetch(fuse_req_t req, bool lock)
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  if (lock)
    mLock.Lock();
 
  if (mPrefetchHandler && mFile->file())
  {
    XrdCl::Proxy* proxy = mFile->xrdioro(req) ? mFile->xrdioro(req) : mFile->xrdiorw(req);

    if (mPrefetchHandler && proxy)
    {
      XrdCl::XRootDStatus status = proxy->WaitRead(mPrefetchHandler);
      if (status.IsOK())
      {
        eos_info("pre-read done with size=%lu md-size=%lu", mPrefetchHandler->vbuffer().size(), mMd->size());
        if ((mPrefetchHandler->vbuffer().size() == mMd->size()) && mFile->file())
        {
          ssize_t nwrite = mFile->file()->pwrite(mPrefetchHandler->buffer(), mPrefetchHandler->vbuffer().size(), 0);
          eos_debug("nwb=%lu to local cache", nwrite);
        }
      }
      else
      {

        eos_err("pre-read failed error=%s", status.ToStr().c_str());
      }
    }
  }
  if (lock)
    mLock.UnLock();
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
       fit != mFile->get_xrdiorw().end(); ++fit)
  {
    if (fit->second->IsOpening())
    {
      eos_info("status=pending url=%s", fit->second->url().c_str());
      fit->second->WaitOpen();
      eos_info("status=final url=%s", fit->second->url().c_str());
    }
    else
    {
      eos_info("status=final url=%s", fit->second->url().c_str());
    }
  }
}

/* -------------------------------------------------------------------------- */
int
data::datax::detach(fuse_req_t req, std::string& cookie, int flags)
/* -------------------------------------------------------------------------- */
{
  bool isRW = false;

  if (flags & (O_RDWR | O_WRONLY) )
    isRW = true;

  eos_info("cookie=%s flags=%o isrw=%d", cookie.c_str(), flags, isRW);

  int rflush = flush(req);

  XrdSysMutexHelper lLock(mLock);
  int bcache = mFile->file() ? mFile->file()->detach(cookie) : 0;
  int jcache = mFile->journal() ? mFile->journal()->detach(cookie) : 0;
  int xio = 0;

  if (isRW)
  {
    if (mFile->xrdiorw(req))
    {
      mFile->xrdiorw(req)->detach();
    }
  }
  else
  {
    if (mFile->xrdioro(req))
    {
      mFile->xrdioro(req)->detach();
    }
  }
  return rflush | bcache | jcache | xio;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::store_cookie(std::string & cookie)
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
  cachehandler::rm(mIno);
  int bcache = mFile->file() ? mFile->file()->unlink() : 0;
  int jcache = mFile->journal() ? mFile->journal()->unlink() : 0;
  return bcache | jcache;
}
// IO bridge interface

/* -------------------------------------------------------------------------- */
ssize_t
/* -------------------------------------------------------------------------- */
data::datax::pread(fuse_req_t req, void *buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  eos_info("offset=%llu count=%lu", offset, count);
  mLock.Lock();

  if (mFile->journal())
  {
    ssize_t jts = ((journalcache*) (mFile->journal()))->get_truncatesize();


    if (jts >= 0)
    {
      // reduce reads in case of truncation stored in the journal
      if ((ssize_t) offset > jts)
      {
        offset = 0;
        count = 0;
      }
      else
      {
        if ((ssize_t) (offset + count) > jts)
        {
          count = jts - offset;
        }
      }
    }
  }

  ssize_t br = 0;

  if (mFile->file())
  {
    // read from file start cache
    br = mFile->file()->pread(buf, count, offset);
  }

  if (br < 0)
  {
    mLock.UnLock();
    return br;
  }

  if (br == (ssize_t) count)
  {
    mLock.UnLock();
    return br;
  }


  if (mFile->file() && (offset < mFile->file()->prefetch_size()))
  {
    mLock.UnLock();
    if (prefetch(req))
    {
      WaitPrefetch(req);
      mLock.Lock();
      ssize_t br = mFile->file()->pread(buf, count, offset);

      if (br < 0)
        return br;

      if (br == (ssize_t) count)
        return br;
    }
    else
    {
      mLock.Lock();
    }
  }

  ssize_t jr = 0;

  if (mFile->journal())
  {
    // read from journal cache
    jr = mFile->journal() ? mFile->journal()->pread((char*) buf + br, count - br, offset + br) : 0;

  }

  if (jr < 0)
  {
    mLock.UnLock();
    return jr;
  }

  if ( (br + jr) == (ssize_t) count)
  {
    mLock.UnLock();
    return (br + jr);
  }

  // read the missing part remote
  XrdCl::Proxy* proxy = mFile->xrdioro(req) ? mFile->xrdioro(req) : mFile->xrdiorw(req);

  if (proxy)
  {
    if (!mFile->is_caching())
    {
      // if caching is disabled, we wait for outstanding writes
      XrdCl::XRootDStatus status = proxy->WaitWrite();
      // it is not obvious what we should do if there was a write error,
      // we just proceed
    }

    uint32_t bytesRead=0;
    if (proxy->Read( offset + br ,
                    count - br ,
                    (char*) buf + br ,
                    bytesRead).IsOK())
    {
      mLock.UnLock();

      std::vector<journalcache::chunk_t> chunks;
      if (mFile->journal())
      {
        jr = 0;

        // retrieve all journal chunks matching our range
        chunks = ((journalcache*) (mFile->journal()))->get_chunks( offset + br + jr, count - br - jr );

        for (auto it = chunks.begin(); it != chunks.end(); ++it)
        {
          eos_err("offset=%ld count=%lu overlay-chunk offset=%ld size=%lu\n", offset, count, it->offset, it->size);
          // overlay journal contents again over the remote contents
          ssize_t ljr = mFile->journal()->pread((char*) buf + br + jr + (it->offset - offset - br - jr) , it->size, it->offset );

          if (ljr >= 0)
          {
            // check if the journal contents extends the remote read
            ssize_t chunkread = it->offset + it->size - offset - br;
            if ( chunkread > bytesRead )
            {
              bytesRead = chunkread;
            }
          }
        }
      }
      return (br + jr + bytesRead);
    }
    else
    {

      mLock.UnLock();
      // IO error
      return -1;
    }
  }
  mLock.UnLock();
  return -1;
}

/* -------------------------------------------------------------------------- */
ssize_t
/* -------------------------------------------------------------------------- */
data::datax::pwrite(fuse_req_t req, const void *buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(mLock);

  eos_info("offset=%llu count=%lu", offset, count);
  ssize_t dw = 0 ;

  if (mFile->file())
  {
    dw = mFile->file()->pwrite(buf, count, offset);
  }

  if (dw < 0)
    return dw;
  else
  {
    if (mFile->journal())
    {
      if (!mFile->journal()->fits(count))
      {
        if (flush_nolock(req))
        {
          return -1;
        }

        if (!mWaitForOpen)
        {
          bool journal_recovery = false;
          // collect all writes in flight
          for (auto it = mFile->get_xrdiorw().begin();
               it != mFile->get_xrdiorw().end(); ++it)
          {
            XrdCl::XRootDStatus status = it->second->WaitWrite();
            if (!status.IsOK())
            {
              journal_recovery = true;
            }
          }

          if (journal_recovery)
          {
            eos_err("journal-flushing failed");
            errno = EREMOTEIO ;
            return -1;
          }
          else
          {
            // truncate the journal
            if (mFile->journal()->reset())
            {
              char msg[1024];
              snprintf(msg, sizeof (msg), "journal reset failed - ino=%08lx", id());
              throw std::runtime_error(msg);
            }
          }
        }
      }

      // now there is space to write for us
      ssize_t jw = mFile->journal()->pwrite(buf, count, offset);
      if (jw < 0)
      {
        return jw;
      }
      dw = jw;
    }

    if (!mFile->xrdiorw(req)->IsOpening())
    {
      // send an asynchronous upstream write
      XrdCl::Proxy::write_handler handler =
              mFile->xrdiorw(req)->WriteAsyncPrepare(count);

      XrdCl::XRootDStatus status =
              mFile->xrdiorw(req)->WriteAsync(offset, count, buf, handler, 0);

      if (!status.IsOK())
      {
        eos_err("async remote-io failed msg=\"%s\"", status.ToString().c_str());
        // TODO: we can recover this later
        errno = EREMOTEIO;
        return -1;
      }
      if (mFlags & O_SYNC)
      {
        XrdCl::XRootDStatus status = mFile->xrdiorw(req)->WaitWrite();
        if (!status.IsOK())
        {
          eos_err("pseudo-sync remote-io failed msg=\"%s\"", status.ToString().c_str());
          // TODO: we can recover this later
          errno = EREMOTEIO;
          return -1;
        }
      }
    }
    else
    {
      mWaitForOpen = true;
    }
  }

  if ( (off_t) (offset + count) > mSize)
  {

    mSize = count + offset;
  }
  eos_info("offset=%llu count=%lu result=%d", offset, count, dw);
  return dw;
}

/* -------------------------------------------------------------------------- */
ssize_t
/* -------------------------------------------------------------------------- */
data::datax::peek_pread(fuse_req_t req, char* &buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  eos_info("offset=%llu count=%lu", offset, count);

  mLock.Lock();

  if (mFile->journal())
  {
    ssize_t jts = ((journalcache*) (mFile->journal()))->get_truncatesize();

    if (jts >= 0)
    {
      // reduce reads in case of truncation stored in the journal
      if ((ssize_t) offset > jts)
      {
        offset = 0;
        count = 0;
      }
      else
      {
        if ((ssize_t) (offset + count) > jts)
        {
          count = jts - offset;
        }
      }
    }
  }

  buffer = sBufferManager.get_buffer();

  if (count > buffer->capacity())
    buffer->reserve(count);

  buf = buffer->ptr();

  ssize_t br = 0;

  if (mFile->file())
  {
    br = mFile->file()->pread(buf, count, offset);

    if (br < 0)
    {
      return br;
    }

    if ( (br == (ssize_t) count) || (br == (ssize_t) mMd->size()) )
    {
      return br;
    }
  }

  if (mFile->file() && (offset < mFile->file()->prefetch_size()))
  {
    if (prefetch(req, false))
    {
      WaitPrefetch(req, false);
      ssize_t br = mFile->file()->pread(buf, count, offset);

      if (br < 0)
        return br;

      if (br == (ssize_t) count)
        return br;
    }
  }

  ssize_t jr = 0;

  if (mFile->journal())
  {
    jr = mFile->journal() ? mFile->journal()->pread(buf + br, count - br, offset + br) : 0;

    if (jr < 0)
      return jr;

    if ( (br + jr) == (ssize_t) count)
      return (br + jr);
  }

  // read the missing part remote
  XrdCl::Proxy * proxy = mFile->xrdioro(req) ? mFile->xrdioro(req) : mFile->xrdiorw(req);

  XrdCl::XRootDStatus status;

  eos_info("offset=%llu count=%lu br=%lu jr=%lu", offset, count, br, jr);

  if (proxy)
  {
    uint32_t bytesRead=0;
    status = proxy->Read( offset + br + jr,
                         count - br - jr,
                         (char*) buf + br + jr,
                         bytesRead);

    if ( status. IsOK() )
    {
      std::vector<journalcache::chunk_t> chunks;
      if (mFile->journal())
      {
        jr = 0;

        // retrieve all journal chunks matching our range
        chunks = ((journalcache*) (mFile->journal()))->get_chunks( offset + br + jr, count - br - jr );

        for (auto it = chunks.begin(); it != chunks.end(); ++it)
        {
          eos_info("offset=%ld count=%lu overlay-chunk offset=%ld size=%lu", offset, count, it->offset, it->size);
          // overlay journal contents again over the remote contents
          ssize_t ljr = mFile->journal()->pread((char*) buf + br + jr + (it->offset - offset - br - jr) , it->size, it->offset );

          if (ljr >= 0)
          {
            // check if the journal contents extends the remote read
            ssize_t chunkread = it->offset + it->size - offset - br;
            if ( chunkread > bytesRead )
            {
              bytesRead = chunkread;
            }
          }
        }
      }
      return (br + jr + bytesRead);
    }
    else
    {

      eos_err("sync remote-io failed msg=\"%s\"", status.ToString().c_str());
      // IO error
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
  //  if (offset == mSize)
  //    return 0;

  int dt = 0;

  if (mFile->file())
  {
    dt = mFile->file()->truncate(offset);
  }

  // if we have a journal it copes with the truncation size
  int jt = 0;

  if (mFile->journal())
  {
    jt = mFile->journal() ? mFile->journal()->truncate(offset) : 0;
  }

  eos_info("dt=%d jt=%d", dt, jt);

  if (!mFile->journal())
  {
    if (mFile->xrdiorw(req))
    {
      if (mFile->xrdiorw(req)->IsOpening())
      {
        mFile->xrdiorw(req)->WaitOpen();
      }

      // the journal keeps track of truncation, otherwise we do it here
      XrdCl::XRootDStatus st = mFile->xrdiorw(req)->Truncate( offset );
      if ( !st.IsOK() )
      {
        return -1;
      }
    }
    else
    {

      return -1;
    }
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

  if (mFile->file())
  {
    ds = mFile->file()->sync();
  }

  int js = 0;

  if (mFile->journal())
  {
    js = mFile->journal() ? mFile->journal()->sync() : 0;
  }

  bool journal_recovery = false;

  for (auto it = mFile->get_xrdiorw().begin();
       it != mFile->get_xrdiorw().end(); ++it)
  {
    if (it->second->IsOpening())
    {
      it->second->WaitOpen();
    }
    XrdCl::XRootDStatus status = it->second->WaitWrite();
    if (!status.IsOK())
    {
      journal_recovery = true;
    }
    else
    {
      status = it->second->Sync();
      if (!status.IsOK())
      {
        journal_recovery = true;
      }
    }
  }

  if (journal_recovery)
  {

    eos_err("syncing failed");
    errno = EREMOTEIO ;
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
  off_t dsize = mFile->file() ? mFile->file()->size() : 0 ;

  if ( mSize > dsize )
    return mSize;
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
  int dt = mFile->file()->truncate(0);
  int jt = mFile->journal() ? mFile->journal()->truncate(0) : 0;
  mSize = 0;

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
  if (md_ino)
  {
    remoteurl += "ino:";
    char sino[128];
    snprintf(sino, sizeof (sino), "%lx", md_ino);
    remoteurl += sino;
  }
  else
  {
    remoteurl += "pino:";
    char pino[128];
    snprintf(pino, sizeof (pino), "%lx", md_pino);
    remoteurl += pino;
    remoteurl += "/";
    remoteurl += basename;
  }
  remoteurl += "&eos.app=fuse&mgm.mtime=0&mgm.fusex=1&eos.bookingsize=0";

  XrdCl::URL url(remoteurl);
  XrdCl::URL::ParamsMap query = url.GetParams();
  fusexrdlogin::loginurl(url, query, req, md_ino);
  url.SetParams(query);
  remoteurl = url.GetURL();

  if (isRW)
    mRemoteUrlRW = remoteurl;
  else
    mRemoteUrlRO = remoteurl;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::dmap::ioflush()
/* -------------------------------------------------------------------------- */
{
  while (1)
  {
    {
      eos_static_debug("");

      std::vector<shared_data> data;
      {
        // avoid mutex contention
        XrdSysMutexHelper mLock(this);

        for (auto it = this->begin(); it != this->end(); ++it)
        {
          data.push_back(it->second);
        }
      }

      for (auto it = this->begin(); it != this->end(); ++it)
      {
        eos_static_info("dbmap-in %08lx => %lx", it->first, &(*(it->second)));
      }

      for (auto it = data.begin(); it != data.end(); ++it)
      {
        {
          XrdSysMutexHelper lLock( (*it)->Locker());
          eos_static_info("dbmap-in => ino:%16lx %lx attached=%d", (*it)->id(), &(*it), (*it)->attached_nolock());

          if (!(*it)->attached_nolock())
          {
            // files which are detached might need an upstream sync
            bool repeat = true;
            while (repeat)
            {
              std::map<std::string, XrdCl::Proxy*>& map = (*it)->file()->get_xrdiorw();
              for (auto fit = map.begin();
                   fit != map.end(); ++fit)
              {
                if (fit->second->IsOpening() || fit->second->IsClosing())
                {
                  eos_static_info("skipping xrdclproxyrw state=%d %d", fit->second->state(), fit->second->IsClosed());
                  // skip files which are opening or closing
                  break;
                }
                if (fit->second->IsOpen())
                {
                  eos_static_info("flushing journal for req=%s id=%08lx", fit->first.c_str(), (*it)->id());
                  // flush the journal
                  (*it)->journalflush(fit->first);

                  fit->second->set_state(XrdCl::Proxy::WAITWRITE);
                  eos_static_info("changing to write wait state");
                }

                if (fit->second->IsWaitWrite())
                {
                  if (!fit->second->OutstandingWrites())
                  {
                    if (fit->second->state_age() > 1.0)
                    {
                      eos_static_info("changing to close async state");
                      fit->second->CloseAsync();
                    }
                    else
                    {
                      eos_static_info("waiting for right age before async close");
                    }
                  }
                }
                {
                  std::string msg;
                  if (fit->second->HadFailures(msg))
                  {
                    // ---------------------------------------------------------
                    // we really have to avoid this to happen, but
                    // we can put everything we have cached in a save place for
                    // manual recovery and tag the error message
                    // ---------------------------------------------------------
                    std::string file_rescue_location;
                    std::string journal_rescue_location;
                    int dt = (*it)->file()->file() ? (*it)->file()->file()->rescue(file_rescue_location) : 0 ;
                    int jt = (*it)->file()->journal() ? (*it)->file()->journal()->rescue(journal_rescue_location) : 0;
                    eos_static_crit("ino:%16lx msg=%s file-recovery=%s journal-recovery=%s",
                                    (*it)->id(),
                                    msg.c_str(),
                                    (!dt) ? file_rescue_location.c_str() : "<none>",
                                    (!jt) ? journal_rescue_location.c_str() : "<none>");

                  }

                  eos_static_info("deleting xrdclproxyrw state=%d %d", fit->second->state(), fit->second->IsClosed());
                  delete fit->second;
                  (*it)->file()->get_xrdiorw().erase(fit);
                  break;
                }
              }
              repeat = false;
            }
          }
        }

        XrdSysMutexHelper mLock(this);
        XrdSysMutexHelper lLock( (*it)->Locker());
        // re-check that nobody is attached
        if (!(*it)->attached_nolock() && !(*it)->file()->get_xrdiorw().size())
        {
          // here we make the data object unreachable for new clients
          (*it)->detach_nolock();
          cachehandler::rm( (*it)->id());
          this->erase( (*it)->id());
          this->erase( (*it)->id() + 0xffffffff);
        }
      }
      XrdSysTimer sleeper;
      sleeper.Wait(1000);
    }
  }
}
