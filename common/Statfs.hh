//------------------------------------------------------------------------------
// File: Statfs.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#ifndef __EOSCOMMON_STATFS_HH__
#define __EOSCOMMON_STATFS_HH__

#include "common/Namespace.hh"
#include "common/Logging.hh"
#include <XrdOuc/XrdOucHash.hh>
#include <XrdOuc/XrdOucString.hh>
#include <XrdSys/XrdSysPthread.hh>
#ifndef __APPLE__
#include <sys/vfs.h>
#else
#include <sys/param.h>
#include <sys/mount.h>
#endif

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class storing a statfs struct and providing some convenience functions to
//! convert into an env representation
//------------------------------------------------------------------------------
class Statfs: public LogId
{
public:

  //----------------------------------------------------------------------------
  //! Empty constructor, empty contents
  //----------------------------------------------------------------------------
  Statfs() {
    memset(&statFs, 0, sizeof(struct statfs));
  }

  //----------------------------------------------------------------------------
  //! Constructor absorbing the raw statfs struct
  //----------------------------------------------------------------------------
  Statfs(struct statfs raw) {
    resetContents(raw);
  }

  //----------------------------------------------------------------------------
  //! Return reference to the internal statfs struct
  //----------------------------------------------------------------------------
  struct statfs* GetStatfs()
  {
    return &statFs;
  }

  //----------------------------------------------------------------------------
  //! Return reference to the internal environment serialization
  //----------------------------------------------------------------------------
  const char* GetEnv()
  {
    return env.c_str();
  }

  //----------------------------------------------------------------------------
  //! Reset internal statfs contents with the given ones,
  //! recalculate environment
  //----------------------------------------------------------------------------
  void resetContents(struct statfs contents) {
    statFs = contents;
    recalculateEnv();
  }

  //----------------------------------------------------------------------------
  //! Recalculate "environment variable" based on current statfs
  //! struct contents
  //----------------------------------------------------------------------------
  void recalculateEnv() {
    char s[1024];
    sprintf(s,
            "statfs.type=%ld&statfs.bsize=%ld&statfs.blocks=%ld&"
            "statfs.bfree=%ld&statfs.bavail=%ld&statfs.files=%ld&statfs.ffree=%ld",
            (long) statFs.f_type, (long) statFs.f_bsize, (long) statFs.f_blocks,
            (long) statFs.f_bfree, (long) statFs.f_bavail, (long) statFs.f_files,
            (long) statFs.f_ffree);
    env = s;
  }

  //----------------------------------------------------------------------------
  //! Execute the statfs function on the given path and build the env
  //! representation.
  //----------------------------------------------------------------------------
  int perform(const std::string &path)
  {
    env = "";
    int retc = 0;

    retc = ::statfs(path.c_str(), (struct statfs*) &statFs);

    if (!retc) {
      recalculateEnv();
    } else {
      eos_err("failed statfs path=%s, errno=%i, strerrno=%s", path.c_str(),
              errno, strerror(errno));
    }

    return retc;
  }

  //----------------------------------------------------------------------------
  //! Static function do add a statfs struct for path to the global statfs hash
  //----------------------------------------------------------------------------
  static std::unique_ptr<Statfs> DoStatfs(const char* path) {
    std::unique_ptr<Statfs> sfs(new Statfs());
    if (!sfs->perform(path)) {
      return sfs;
    } else {
      return {};
    }
  }

private:
  struct statfs statFs; //< the stored statfs struct
  XrdOucString env; //< env representation of the contents
};

EOSCOMMONNAMESPACE_END

#endif
