//------------------------------------------------------------------------------
//! @file cache.cc
//! @author Andreas-Joachim Peters CERN
//! @brief data cache in-memory implementation
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

#include "cache.hh"
#include "common/Logging.hh"
#include "common/Path.hh"
#include <unistd.h>

/* -------------------------------------------------------------------------- */
memorycache::memorycache() : ino(0)
/* -------------------------------------------------------------------------- */
{
  return;
}

/* -------------------------------------------------------------------------- */
memorycache::memorycache(fuse_ino_t _ino) : ino(_ino)
/* -------------------------------------------------------------------------- */
{
  return;
}

/* -------------------------------------------------------------------------- */
memorycache::~memorycache()
/* -------------------------------------------------------------------------- */
{
  return;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
memorycache::attach()
/* -------------------------------------------------------------------------- */
{
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
memorycache::detach()
/* -------------------------------------------------------------------------- */
{
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
memorycache::unlink()
/* -------------------------------------------------------------------------- */
{
  return 0;
}

/* -------------------------------------------------------------------------- */
ssize_t
/* -------------------------------------------------------------------------- */
memorycache::pread(void *buf, size_t count, off_t offset)
{
  return (ssize_t) readData (buf, offset, count);
}

ssize_t
/* -------------------------------------------------------------------------- */
memorycache::pwrite(const void *buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  return (ssize_t) writeData (buf, offset, count);
}

ssize_t
/* -------------------------------------------------------------------------- */
memorycache::peek_read(char* &buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  return (ssize_t) peekData(buf, offset, count);
}

void
/* -------------------------------------------------------------------------- */
memorycache::release_read()
/* -------------------------------------------------------------------------- */
{
  return releasePeek();
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
memorycache::truncate(off_t offset)
/* -------------------------------------------------------------------------- */
{
  truncateData(offset);
  return 0;
}

int
/* -------------------------------------------------------------------------- */
memorycache::sync()
/* -------------------------------------------------------------------------- */
{
  return 0;
}

/* -------------------------------------------------------------------------- */
size_t
/* -------------------------------------------------------------------------- */
memorycache::size()
{
  return getSize();
}


std::string diskcache::sLocation;
bufferllmanager diskcache::sBufferManager;

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
diskcache::init()
/* -------------------------------------------------------------------------- */
{
  cachehandler::cacheconfig config = cachehandler::instance().get_config();

  if (::access(config.location.c_str(), W_OK))
  {
    return errno;
  }
  sLocation = config.location;
  return 0;
}

/* -------------------------------------------------------------------------- */
diskcache::diskcache() : ino(0), nattached(0)
/* -------------------------------------------------------------------------- */
{

  return;
}

/* -------------------------------------------------------------------------- */
diskcache::diskcache(fuse_ino_t _ino) : ino(_ino), nattached(0)
/* -------------------------------------------------------------------------- */
{

  return;
}

/* -------------------------------------------------------------------------- */
diskcache::~diskcache()
/* -------------------------------------------------------------------------- */
{

}

/* -------------------------------------------------------------------------- */
int
diskcache::location(std::string& path, bool mkpath)
/* -------------------------------------------------------------------------- */
{
  char cache_path[1024 + 20];
  snprintf(cache_path, sizeof (cache_path), "%s/%08lx/%08lu",
           sLocation.c_str(), ino / 10000, ino);

  if (mkpath)
  {
    eos::common::Path cPath(cache_path);
    if (!cPath.MakeParentPath(S_IRWXU))
    {
      return errno;
    }
  }
  path = cache_path;
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
diskcache::attach()
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(this);
  if (nattached == 0)
  {
    std::string path;
    //
    int rc = location(path);
    if (rc)
    { 
      return rc;
    }
    
    // need to open the file
    fd = open(path.c_str(), O_CREAT | O_RDWR, S_IRWXU);

    if (fd < 0)
    {
      return errno;
    }
  }
  nattached++;
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
diskcache::detach()
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(this);
  nattached--;
  if (!nattached)
  {
    int rc = close(fd);

    if (rc)
      return errno;
  }
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
diskcache::unlink()
/* -------------------------------------------------------------------------- */
{
  std::string path;
  int rc = location(path);
  if (!rc)
    rc = ::unlink(path.c_str());
  return rc;
}

/* -------------------------------------------------------------------------- */
ssize_t
/* -------------------------------------------------------------------------- */
diskcache::pread(void *buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  return ::pread(fd, buf, count, offset);
}

/* -------------------------------------------------------------------------- */
ssize_t
/* -------------------------------------------------------------------------- */
diskcache::peek_read(char* &buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  this->Lock();
  buffer = sBufferManager.get_buffer();
  if (count > buffer->capacity())
    buffer->reserve(count);
  buf = buffer->ptr();
  
  return ::pread(fd, buf,  count, offset);
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
diskcache::release_read()
/* -------------------------------------------------------------------------- */
{
  sBufferManager.put_buffer(buffer);
  buffer.reset();
  this->UnLock();
  return;
}

/* -------------------------------------------------------------------------- */
ssize_t
diskcache::pwrite(const void *buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  return ::pwrite(fd, buf, count, offset);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
diskcache::truncate(off_t offset)
/* -------------------------------------------------------------------------- */
{
  return ::ftruncate(fd, offset);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
diskcache::sync()
/* -------------------------------------------------------------------------- */
{
  return ::fdatasync(fd);
}

/* -------------------------------------------------------------------------- */
size_t
/* -------------------------------------------------------------------------- */
diskcache::size()
/* -------------------------------------------------------------------------- */
{
  struct stat buf;
  buf.st_size=0;
  if (::fstat(fd, &buf))
  {
    throw std::runtime_error("diskcache stat failure");
  }
  return buf.st_size;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
cachehandler::init(cachehandler::cacheconfig & _config)
/* -------------------------------------------------------------------------- */
{
  config = _config;

  if (config.type == cachehandler::cache_t::INVALID)
    return EINVAL;

  if (config.type == cachehandler::cache_t::DISK)
  {
    if (diskcache::init())
    {

      fprintf(stderr, "error: cache directory %s cannot be initiazlied - check existance/permissions!\n", config.location.c_str());
      return EPERM;
    }
  }
  return 0;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
cachehandler::logconfig()
{
  eos_static_warning("data-cache-type        := %s",
                     (config.type == cachehandler::cache_t::MEMORY) ? "memory" : "disk");

  if (config.type == cachehandler::cache_t::DISK)
  {
    eos_static_warning("data-cache-location  := %s",
                       config.location.c_str());

    if (config.mbsize == 0)
    {
      eos_static_warning("data-cache-size      := unlimited");
    }
    else
    {

      eos_static_warning("data-cache-size      := %d MB", config.mbsize);
    }
  }
}

/* -------------------------------------------------------------------------- */
cache::shared_file  
/* -------------------------------------------------------------------------- */
cachehandler::get(fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(instance());

  if (!instance().count(ino))
  {
    cache::shared_file entry;
    if (instance().inmemory())
    {
      entry = std::make_shared<memorycache>(ino);
    }
    else
    {
      entry = std::make_shared<diskcache>(ino);
    }

    (instance())[ino] =  entry;
    return entry;
  }
  else
  {

    return (instance())[ino];
  }
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
cachehandler::rm(fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(instance());

  if (instance().count(ino))
  {
    instance().erase(ino);
  }
  return 0;
}


