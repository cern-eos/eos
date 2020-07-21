//------------------------------------------------------------------------------
//! @file kernelcache.hh
//! @author Andreas-Joachim Peters CERN
//! @brief kernel cache interface
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

#ifndef FUSE_KERNELCACHE_HH_
#define FUSE_KERNELCACHE_HH_

#include "llfusexx.hh"
#include "eosfuse.hh"
#include "common/Logging.hh"

class kernelcache
{
public:

  static void inval_inode(fuse_ino_t inode, bool isfile = false)
  {
    eos_static_debug("begin: ino=%08llx", inode);
#ifdef _FUSE3
    int rc = fuse_lowlevel_notify_inval_inode(EosFuse::Instance().Session(),
#else
    int rc = fuse_lowlevel_notify_inval_inode(EosFuse::Instance().Channel(),
#endif
					      inode, isfile ? 0 : -1, 0);
    eos_static_debug("end: ino=%08llx rc=%d", inode, rc);
  }

  static void inval_entry(fuse_ino_t parent_inode, const std::string name)
  {
    eos_static_debug("begin: ino=%08llx name=%s", parent_inode, name.c_str());
#ifdef _FUSE3
    int rc = fuse_lowlevel_notify_inval_entry(EosFuse::Instance().Session(),
#else
    int rc = fuse_lowlevel_notify_inval_entry(EosFuse::Instance().Channel(),
#endif
					     parent_inode, name.c_str(), name.length());

    eos_static_debug("end: ino=%08llx name=%s rc=%d", parent_inode, name.c_str(),
                     rc);
  }
};
#endif /* FUSE_KERNCALCACHE_HH_ */
