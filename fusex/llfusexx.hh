//------------------------------------------------------------------------------
//! @file llfusexx.hh
//! @author Justin Salmon, Andreas-Joachim Peters CERN
//! @brief C++ template class for low-level FUSE calls
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

#ifndef __LLFUSEXX_H__
#define __LLFUSEXX_H__

#include <sys/stat.h>
#include <sys/types.h>

#if ( FUSE_MOUNT_VERSION == 290 )
#define FUSE_SUPPORTS_FLOCK
#pragma message("FUSE_SUPPORTS_FLOCK")
#endif


#ifndef FUSE_USE_VERSION
#ifdef __APPLE__
#define FUSE_USE_VERSION 27
#pragma message("FUSE 27")
#define EL2NSYNC    45  /* Level 2 not synchronized */
#else

#ifdef _FUSE3
#define FUSE_USE_VERSION 30
#define FUSE_SUPPORTS_FLOCK
#pragma message("FUSE_SUPPORTS_FLOCK")
#pragma message("FUSE 30")
#else
#define FUSE_USE_VERSION 28
#pragma message("FUSE 28")
#endif


#endif
#endif

extern "C" {
#ifdef _FUSE3
#include <fuse3/fuse_lowlevel.h>
#else
#include <fuse/fuse_lowlevel.h>
#endif
}

#include <cstdlib>

#ifndef FUSE_SET_ATTR_ATIME_NOW
#define FUSE_SET_ATTR_ATIME_NOW (1 << 7)
#endif
#ifndef FUSE_SET_ATTR_MTIME_NOW
#define FUSE_SET_ATTR_MTIME_NOW (1 << 8)
#endif


namespace llfusexx
{
//----------------------------------------------------------------------------
//! Interface to the low-level FUSE API
//----------------------------------------------------------------------------

template <typename T>
class FuseBase
{
protected:
  //------------------------------------------------------------------------
  //! Structure holding function pointers to the low-level "operations"
  //! (function implementations)
  //------------------------------------------------------------------------
  struct fuse_lowlevel_ops operations;

  //------------------------------------------------------------------------
  //! @return const reference to the operations struct
  //------------------------------------------------------------------------

  const fuse_lowlevel_ops&
  get_operations()
  {
    return operations;
  }

  void disable_xattr()
  {
    operations.getxattr = 0;
    operations.setxattr = 0;
    operations.listxattr = 0;
    operations.removexattr = 0;
  }

  void disable_link()
  {
    operations.link = 0;
  }

  //------------------------------------------------------------------------
  //! Constructor
  //!
  //! Install pointers to operation functions as implemented by the user
  //! subclass
  //------------------------------------------------------------------------

  FuseBase()
  {
    operations.init = &T::init;
    operations.destroy = &T::destroy;
    operations.getattr = &T::getattr;
    operations.lookup = &T::lookup;
    operations.setattr = &T::setattr;
    operations.opendir = &T::opendir;
    operations.access = &T::access;
    operations.readdir = &T::readdir;
#ifdef _FUSE3
    operations.readdirplus = &T::readdirplus;
#endif
    operations.mkdir = &T::mkdir;
    operations.unlink = &T::unlink;
    operations.rmdir = &T::rmdir;
    operations.rename = &T::rename;
    operations.open = &T::open;
    operations.create = &T::create;
    operations.mknod = &T::mknod;
    operations.read = &T::read;
    operations.write = &T::write;
    operations.statfs = &T::statfs;
    operations.release = &T::release;
    operations.releasedir = &T::releasedir;
    operations.fsync = &T::fsync;
    operations.forget = &T::forget;
#ifdef _FUSE3
    operations.forget_multi = &T::forget_multi;
#endif
    operations.flush = &T::flush;
    operations.setxattr = &T::setxattr;
    operations.getxattr = &T::getxattr;
    operations.listxattr = &T::listxattr;
    operations.removexattr = &T::removexattr;
    operations.readlink = &T::readlink;
    operations.link = &T::link;
    operations.symlink = &T::symlink;
    operations.getlk = &T::getlk;
    operations.setlk = &T::setlk;
#ifdef FUSE_SUPPORTS_FLOCK
    operations.flock = &T::flock;
#endif
  }

public:

  int run(int argc, char* argv[], void* userdata);
};
}

#endif /* __LLFUSEXX_H__ */
