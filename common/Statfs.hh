// ----------------------------------------------------------------------
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

/**
 * @file   Statfs.hh
 * 
 * @brief  Filesystem 'statfs' store,
 * 
 * 
 */

#ifndef __EOSCOMMON_STATFS_HH__
#define __EOSCOMMON_STATFS_HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#ifndef __APPLE__
#include <sys/vfs.h>
#else
#include <sys/param.h>
#include <sys/mount.h>
#endif
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Class storing a statfs struct and providing some convenience functions to convert into an env representation
/*----------------------------------------------------------------------------*/
class Statfs {
  struct statfs statFs; //< the stored statfs struct
  XrdOucString path;    //< path to the filesystem stat'ed
  XrdOucString env;     //< env representation of the contents
  
public:

  static XrdSysMutex gMutex;         //< static global mutex for the global statfs hash
  static XrdOucHash<Statfs> gStatfs; //< static global hash containing statfs class objects for several file systems

  // ---------------------------------------------------------------------------
  //! Return the statfs structure for a given path from the global statfs hash
  // ---------------------------------------------------------------------------
  static Statfs* GetStatfs(const char* path) {
    gMutex.Lock();
    Statfs* sfs = gStatfs.Find(path);
    gMutex.UnLock();
    return sfs;
  }

  // ---------------------------------------------------------------------------
  //! Return reference to the internal statfs struct
  // ---------------------------------------------------------------------------
  struct statfs* GetStatfs() {return &statFs;}
  const char* GetEnv() {return env.c_str();}


  // ---------------------------------------------------------------------------
  //! Execute the statfs function on the given path and build the env representation
  // ---------------------------------------------------------------------------
  int DoStatfs() {
    env ="";
    int retc=::statfs(path.c_str(), (struct statfs*) &statFs);
    if (!retc) {
      char s[1024];    
      sprintf(s,"statfs.type=%ld&statfs.bsize=%ld&statfs.blocks=%ld&statfs.bfree=%ld&statfs.bavail=%ld&statfs.files=%ld&statfs.ffree=%ld", (long)statFs.f_type,(long)statFs.f_bsize,(long)statFs.f_blocks,(long)statFs.f_bfree,(long)statFs.f_bavail,(long)statFs.f_files,(long)statFs.f_ffree);
      env = s;
    }
    return retc;
  }


  // ---------------------------------------------------------------------------
  //! Constructor taking the path of the filesystem to stat as in parameter
  // ---------------------------------------------------------------------------
  Statfs(const char* inpath) {
    path = inpath;
    memset(&statFs, 0, sizeof(struct statfs));
  }
  
  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~Statfs(){}
  
  // ---------------------------------------------------------------------------
  //! Static function do add a statfs struct for path to the global statfs hash
  // ---------------------------------------------------------------------------
  static Statfs* DoStatfs(const char* path) {
    Statfs* sfs = new Statfs(path);
    if (!sfs->DoStatfs()) {
      gMutex.Lock();
      gStatfs.Rep(path, sfs);
      gMutex.UnLock();
      return sfs;
    } else {
      delete sfs;
      return 0;
    }   
  }
};
/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif
