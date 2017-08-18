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
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
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

  //------------------------------------------------------------------------------
  //! Subclass Callback
  //------------------------------------------------------------------------------
  class Callback
  {
  public:
    typedef struct callback_data {
      callback_data() : caller(0), path(0), statfs(0), retc(-1) {}
      void* caller;
      const char* path;
      struct statfs* statfs;
      int retc;
    } callback_data_t;

    typedef int (*callback_t)(callback_data_t*);

    Callback()
    {
      call = 0;
    }

    Callback(callback_t tocall, callback_data_t& info)
    {
      call = tocall;
      data = info;
      data.retc = 0;
    }

    virtual ~Callback() = default;

    Callback(const Callback& obj)
    {
      call = obj.call;
      data = obj.data;
    }

    callback_t call;
    callback_data_t data;
  };

  //----------------------------------------------------------------------------
  //! Constructor taking the path of the filesystem to stat as in parameter
  //----------------------------------------------------------------------------
  Statfs(const char* inpath):
    mPath(inpath)
  {
    memset(&statFs, 0, sizeof(struct statfs));
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~Statfs() = default;

  //----------------------------------------------------------------------------
  //! Return reference to the internal statfs struct
  //----------------------------------------------------------------------------
  struct statfs* GetStatfs()
  {
    return &statFs;
  }

  const char* GetEnv()
  {
    return env.c_str();
  }

  //----------------------------------------------------------------------------
  //! Execute the statfs function on the given path and build the env
  //! representation. Optional the 'statfs' method can be given as a callback
  //! function
  //----------------------------------------------------------------------------
  int DoStatfs(Callback::callback_t call = 0, Callback::callback_data_t* data = 0)
  {
    env = "";
    int retc = 0;
    std::string tmp_path;

    if (call) {
      tmp_path = data->path;
      retc = call(data);
    } else {
      tmp_path = mPath.c_str();
      retc = ::statfs(mPath.c_str(), (struct statfs*) &statFs);
    }

    if (!retc) {
      char s[1024];
      sprintf(s,
              "statfs.type=%ld&statfs.bsize=%ld&statfs.blocks=%ld&"
              "statfs.bfree=%ld&statfs.bavail=%ld&statfs.files=%ld&statfs.ffree=%ld",
              (long) statFs.f_type, (long) statFs.f_bsize, (long) statFs.f_blocks,
              (long) statFs.f_bfree, (long) statFs.f_bavail, (long) statFs.f_files,
              (long) statFs.f_ffree);
      env = s;
    } else {
      eos_err("failed statfs path=%s, errno=%i, strerrno=%s", tmp_path.c_str(),
              errno, strerror(errno));
    }

    return retc;
  }

  static XrdSysMutex gMutex; //< Static global mutex for the global statfs hash
  //< Static global hash containing statfs class objects for several file systems
  static XrdOucHash<Statfs>gStatfs;

  //----------------------------------------------------------------------------
  //! Return the statfs structure for a given path from the global statfs hash
  //----------------------------------------------------------------------------
  static Statfs* GetStatfs(const char* path)
  {
    gMutex.Lock();
    Statfs* sfs = gStatfs.Find(path);
    gMutex.UnLock();
    return sfs;
  }

  //----------------------------------------------------------------------------
  //! Static function do add a statfs struct for path to the global statfs hash
  //----------------------------------------------------------------------------
  static Statfs* DoStatfs(const char* path,
                          Callback::callback_t call = 0,
                          Callback::callback_data_t* data = 0)
  {
    Statfs* sfs = new Statfs(path);

    if (data) {
      data->statfs = &sfs->statFs;
    }

    if (!sfs->DoStatfs(call, data)) {
      gMutex.Lock();
      gStatfs.Rep(path, sfs);
      gMutex.UnLock();
      return sfs;
    } else {
      delete sfs;
      return 0;
    }
  }

private:
  struct statfs statFs; //< the stored statfs struct
  XrdOucString mPath; //< path to the filesystem stat'ed
  XrdOucString env; //< env representation of the contents
};

EOSCOMMONNAMESPACE_END

#endif
