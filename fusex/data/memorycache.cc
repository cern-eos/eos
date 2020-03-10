//------------------------------------------------------------------------------
//! @file memorycache.cc
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

#include "memorycache.hh"
#include "common/Logging.hh"
#include "common/Path.hh"
#include <unistd.h>
#include <sys/types.h>
#include <errno.h>
#include "common/XattrCompat.hh"

/* -------------------------------------------------------------------------- */
memorycache::memorycache(fuse_ino_t _ino) : ino(_ino)
  /* -------------------------------------------------------------------------- */
{
  (void) ino;
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
memorycache::attach(fuse_req_t req, std::string& cookie, int flags)
/* -------------------------------------------------------------------------- */
{
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
memorycache::detach(std::string& cookie)
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
memorycache::pread(void* buf, size_t count, off_t offset)
{
  return (ssize_t) buffer.readData(buf, offset, count);
}

ssize_t
/* -------------------------------------------------------------------------- */
memorycache::pwrite(const void* buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  return (ssize_t) buffer.writeData(buf, offset, count);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
memorycache::truncate(off_t offset)
/* -------------------------------------------------------------------------- */
{
  buffer.truncateData(offset);
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
  return buffer.getSize();
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
memorycache::set_attr(const std::string& key, const std::string& value)
{
  XrdSysMutexHelper lLock(xattrmtx);
  xattr[key] = value;
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
memorycache::attr(const std::string& key, std::string& value)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(xattrmtx);

  if (xattr.count(key)) {
    value = xattr[key];
    return 0;
  }

  errno = ENOATTR;
  return -1;
}
