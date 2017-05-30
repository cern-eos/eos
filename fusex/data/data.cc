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
#include "misc/MacOSXHelper.hh"
#include "common/Logging.hh"
#include <iostream>
#include <sstream>

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
          fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(datamap);
  if (datamap.count(ino))
  {
    shared_data io = datamap[ino];
    return io;
  }
  else
  {
    std::string mdstream;
    shared_data io = std::make_shared<datax>();
    io->set_id(ino, req);
    datamap[io->id()] = io;
    return io;
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
    datamap[ino]->unlink();
    datamap.erase(ino);
    eos_static_info("datacache::unlink size=%lu", datamap.size());
  }
  else
  {
    shared_data io = std::make_shared<datax>();
    io->set_id(ino, req);
    io->unlink();
  }
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::datax::flush()
/* -------------------------------------------------------------------------- */
{
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::attach(std::string& cookie)
/* -------------------------------------------------------------------------- */
{
  int bcache = mFile->file() ? mFile->file()->attach(cookie) : 0;
  int jcache = mFile->journal() ? mFile->journal()->attach(cookie) : 0;

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

  return bcache | jcache;
}

/* -------------------------------------------------------------------------- */
int
data::datax::detach(std::string& cookie)
/* -------------------------------------------------------------------------- */
{
  int bcache = mFile->file() ? mFile->file()->detach(cookie) : 0;
  int jcache = mFile->journal() ? mFile->journal()->detach(cookie) : 0;
  return bcache | jcache;
}

/* -------------------------------------------------------------------------- */
int 
/* -------------------------------------------------------------------------- */
data::datax::store_cookie(std::string& cookie)
/* -------------------------------------------------------------------------- */
{
  int bc = mFile->file() ? mFile->file()->set_cookie(cookie) : 0;
  int jc = mFile->journal() ? mFile->journal()->set_cookie(cookie) : 0;
  return bc | jc;
}


/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::unlink()
/* -------------------------------------------------------------------------- */
{
  cachehandler::rm(mIno);
  int bcache = mFile->file() ? mFile->file()->unlink() : 0;
  int jcache = mFile->journal() ? mFile->journal()->unlink() : 0;
  return bcache | jcache;
}

// IO bridge interface

/* -------------------------------------------------------------------------- */
ssize_t
/* -------------------------------------------------------------------------- */
data::datax::pread(void *buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  ssize_t br = mFile->file()->pread(buf, count, offset);
  if (br < 0)
    return br;
  ssize_t jr = mFile->journal() ? mFile->journal()->pread(buf, count, offset) : 0;
  if (jr < 0)
    return jr;

  return br + jr;
}

/* -------------------------------------------------------------------------- */
ssize_t
/* -------------------------------------------------------------------------- */
data::datax::pwrite(const void *buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  ssize_t dw = mFile->file()->pwrite(buf, count, offset);
  if (dw < 0)
    return dw;
  else
  {
    ssize_t jw = mFile->journal()->pwrite(buf, count, offset);
    if (jw < 0)
    {
      return jw;
    }
    dw = jw;
  }

  if ( (off_t) (offset + count) > mSize)
  {
    mSize = count + offset;
  }
  return dw;
}

/* -------------------------------------------------------------------------- */
ssize_t
/* -------------------------------------------------------------------------- */
data::datax::peek_pread(char* &buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  ssize_t br = mFile->file()->peek_read(buf, count, offset);

  ssize_t jr = mFile->journal() ? mFile->journal()->peek_read(buf, count, offset) : 0;

  eos_static_debug("br=%lld jr=%lld", br,jr);
  if (br < 0)
    return br;
  
  if (jr < 0)
    return jr;

  if (br == (ssize_t) count)
    return br;
  
  return br + jr;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::datax::release_pread()
/* -------------------------------------------------------------------------- */
{
  mFile->file()->release_read();
  if (mFile->journal())
  {
    mFile->journal()->release_read();
  }
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::truncate(off_t offset)
/* -------------------------------------------------------------------------- */
{
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
  int ds = mFile->file()->sync();
  int js = mFile->journal() ? mFile->journal()->sync() : 0;
  return ds | js;
}

/* -------------------------------------------------------------------------- */
size_t
/* -------------------------------------------------------------------------- */
data::datax::size()
/* -------------------------------------------------------------------------- */
{
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
  // truncate the block cache
  int dt = mFile->file()->truncate(0);
  int jt = mFile->journal() ? mFile->journal()->truncate(0) : 0;
  mSize = 0;
  
  return dt | jt;
}
  