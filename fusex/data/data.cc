//------------------------------------------------------------------------------
//! @file data.cc
//! @author Andreas-Joachim Peters CERN
//! @brief meta data handling class
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
    io->attach(); // object ref counting
    return io;
  }
  else
  {
    shared_data io = std::make_shared<datax>(md);
    io->set_id(ino, req);
    datamap[io->id()] = io;
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
    return;
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

  if ( (ssize_t)mFile->journal()->size() > (ssize_t)mFile->file()->prefetch_size())
  {
    if (mWaitForOpen)
    {
      if (journalflush(req))
      {
        return -1;
      }
      mWaitForOpen = false;
    }
  }
  else
  {
    // attache for the asynchronous thread
    std::string cookie("flusher");
    mFile->journal()->attach(req, cookie, true);
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
  eos_info("syncing cache");
  cachesyncer cachesync(*((XrdCl::File*)mFile->xrdiorw(cid)));
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
data::datax::attach(fuse_req_t freq, std::string& cookie, bool isRW)
/* -------------------------------------------------------------------------- */
{
  eos_info("cookie=%s isrw=%d md-size=%d %s", cookie.c_str(), isRW, mMd->size(),
           mRemoteUrl.c_str());

  int bcache = mFile->file() ? mFile->file()->attach(freq, cookie, isRW) : 0;
  int jcache = mFile->journal() ? mFile->journal()->attach(freq, cookie, isRW) : 0;

  if (bcache)
  {
    char msg[1024];
    snprintf(msg, sizeof (msg), "attach to cache failed - ino=%08lx", id());
    throw std::runtime_error(msg);
  }

  if (jcache)
  {
    char msg[1024];
    snprintf(msg, sizeof (msg), "attach to journal failed - ino=%08lx", id());
    throw std::runtime_error(msg);
  }

  if (isRW)
  {
    if (!mFile->xrdiorw(freq))
    {
      // attach an rw io object
      mFile->set_xrdiorw(freq, new XrdCl::Proxy());
      mFile->xrdiorw(freq)->attach();
      mFile->xrdiorw(freq)->set_id(id(), req());
      XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update;
      XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW | XrdCl::Access::UX;
      mFile->xrdiorw(freq)->OpenAsync(mRemoteUrl.c_str(), targetFlags, mode, 0);
    }
    else
    {
      mFile->xrdiorw(freq)->attach();
    }
  }
  else
  {
    if (!mFile->xrdioro(freq))
    {
      mFile->set_xrdioro(freq, new XrdCl::Proxy());
      mFile->xrdioro(freq)->attach();
      mFile->xrdioro(freq)->set_id(id(), req());
      XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Read;
      XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UX;
      mFile->xrdioro(freq)->OpenAsync(mRemoteUrl.c_str(), targetFlags, mode, 0);
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
data::datax::prefetch(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  eos_info("handler=%d file=%lx size=%lu md-size=%lu", mPrefetchHandler ? 1 : 0,
           mFile ? mFile->file() : 0,
           mFile ? mFile->file() ? mFile->file()->size() : 0 : 0,
           mMd->size());
  XrdSysMutexHelper lLock(mLock);

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
  return mPrefetchHandler ? true : false;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::datax::WaitPrefetch(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  XrdSysMutexHelper lLock(mLock);
  if (mPrefetchHandler)
  {
    XrdCl::Proxy* proxy = mFile->xrdioro(req) ? mFile->xrdioro(req) : mFile->xrdiorw(req);

    if (mPrefetchHandler && proxy)
    {
      XrdCl::XRootDStatus status = proxy->WaitRead(mPrefetchHandler);
      if (status.IsOK())
      {
        eos_err("pre-read done with size=%lu md-size=%lu", mPrefetchHandler->vbuffer().size(), mMd->size());
        if ((mPrefetchHandler->vbuffer().size() == mMd->size()) && mFile->file())
        {
          ssize_t nwrite = mFile->file()->pwrite(mPrefetchHandler->buffer(), mPrefetchHandler->vbuffer().size(), 0);
          eos_info("nwb=%lu to local cache", nwrite);
        }
      }
      else
      {
        eos_err("pre-read failed error=%s", status.ToStr().c_str());
      }
    }
  }
}

/* -------------------------------------------------------------------------- */
int
data::datax::detach(fuse_req_t req, std::string& cookie, bool isRW)
/* -------------------------------------------------------------------------- */
{
  eos_info("cookie=%s isrw=%d", cookie.c_str(), isRW);

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
  return bcache | jcache | xio;
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

  // read from file start cache
  ssize_t br = mFile->file()->pread(buf, count, offset);

  if (br < 0)
  {
    return br;
  }

  if (br == (ssize_t) count)
  {
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

  // read from journal cache
  ssize_t jr = mFile->journal() ? mFile->journal()->pread((char*) buf + br, count - br, offset + br) : 0;

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
    uint32_t bytesRead=0;
    if (proxy->Read( offset + br + jr,
                    count - br - jr,
                    (char*) buf + br + jr,
                    bytesRead).IsOK())
    {
      mLock.UnLock();
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
  ssize_t dw = mFile->file()->pwrite(buf, count, offset);
  if (dw < 0)
    return dw;
  else
  {
    if (!mFile->journal()->fits(count))
    {
      if (flush(req))
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

  buffer = sBufferManager.get_buffer();

  if (count > buffer->capacity())
    buffer->reserve(count);

  buf = buffer->ptr();

  ssize_t br = mFile->file()->pread(buf, count, offset);

  if (br < 0)
  {
    mLock.UnLock();
    return br;
  }

  if ( (br == (ssize_t) count) || (br == (ssize_t) mMd->size()) )
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

  ssize_t jr = mFile->journal() ? mFile->journal()->pread(buf, count, offset) : 0;

  if (jr < 0)
    return jr;

  if ( (br + jr) == (ssize_t) count)
    return (br + jr);

  // read the missing part remote
  XrdCl::Proxy * proxy = mFile->xrdioro(req) ? mFile->xrdioro(req) : mFile->xrdiorw(req);

  if (proxy)
  {
    uint32_t bytesRead=0;
    if (proxy->Read( offset + br + jr,
                    count - br - jr,
                    (char*) buf + br + jr,
                    bytesRead).IsOK())
    {
      return (br + jr + bytesRead);
    }
    else
    {
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
  eos_info("offset=%llu", offset);
  if (offset == mSize)
    return 0;

  int dt = mFile->file()->truncate(offset);
  int jt = mFile->journal() ? mFile->journal()->truncate(offset) : 0;

  if (offset > mSize)
  {

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
  int ds = mFile->file()->sync();
  int js = mFile->journal() ? mFile->journal()->sync() : 0;

  bool journal_recovery = false;

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
  return ds | js;
}

/* -------------------------------------------------------------------------- */
size_t
/* -------------------------------------------------------------------------- */
data::datax::size()
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  off_t dsize = mFile->file()->size();

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
                        const uint64_t md_pino)
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  mRemoteUrl = "root://";
  mRemoteUrl += hostport;
  mRemoteUrl += "//fusex-open";
  mRemoteUrl += "?eos.lfn=";
  if (md_ino)
  {
    mRemoteUrl += "ino:";
    char sino[128];
    snprintf(sino, sizeof (sino), "%lx", md_ino);
    mRemoteUrl += sino;
  }
  else
  {
    mRemoteUrl += "pino:";
    char pino[128];
    snprintf(pino, sizeof (pino), "%lx", md_pino);
    mRemoteUrl += pino;
    mRemoteUrl += "/";
    mRemoteUrl += basename;
  }
  mRemoteUrl += "&eos.app=fuse&mgm.mtime=0";
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

      for (auto it = data.begin(); it != data.end(); ++it)
      {
        if (!(*it)->attached())
        {
          // files which are detached might need an upstream sync
          bool repeat = true;
          while (repeat)
          {
            for (auto fit = (*it)->file()->get_xrdiorw().begin();
                 fit != (*it)->file()->get_xrdiorw().end(); ++fit)
            {
              if (fit->second->IsOpening() || fit->second->IsClosing())
              {
                eos_static_info("skipping xrdclproxyrw state=%d %d", fit->second->state(), fit->second->IsClosed());
                // skip files which are opening or closing
                continue;
              }
              if (fit->second->IsOpen())
              {
                eos_static_info("flushing journal for req=%s", fit->first.c_str());
                XrdSysMutexHelper lLock( (*it)->Locker());
                // flush the journal
                (*it)->journalflush(fit->first);
                // detach from the journal
                std::string cookie("flusher");
                (*it)->file()->journal()->detach(cookie);
                fit->second->CloseAsync();
              }

              if (fit->second->IsClosed())
              {
                eos_static_info("deleting xrdclproxyrw state=%d %d", fit->second->state(), fit->second->IsClosed());
                delete fit->second;
                (*it)->file()->get_xrdiorw().erase(fit);
                break;
              }
            }
            repeat = false;
          }
        }
        if ( !(*it)->file()->get_xrdiorw().size())
        {
          eos_static_info("deleting shared_data id=%08lx", (*it)->id());
          XrdSysMutexHelper mLock(this);
          this->erase((*it)->id());
        }
      }

      /*
      if (!mFile->xrdiorw(req)->attached())
      {
      XrdCl::XRootDStatus status = mFile->xrdiorw(req)->CloseAsync(0);
      if (!status.IsOK())
      {
      eos_err("async remote-io failed msg=\"%s\"", status.ToString().c_str());
      xio = -1;
      errno = EREMOTEIO;
      }
      }

      if (!mFile->xrdioro(req)->attached())
      {
      XrdCl::XRootDStatus status = mFile->xrdioro(req)->CloseAsync(0);
      if (!status.IsOK())
      {
      eos_err("async remote-io failed msg=\"%s\"", status.ToString().c_str());
      xio = -1;
      errno = EREMOTEIO;
      }
      }
       */

      XrdSysTimer sleeper;
      sleeper.Wait(1000);
    }
  }
}