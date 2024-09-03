// ----------------------------------------------------------------------
// File: Glob.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/ASwitzerland                                  *
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

//-----------------------------------------------------------------------------
//! @brief  Class applying bash pattern amtching
//-----------------------------------------------------------------------------
#ifndef __EOSCOMMON__GLOB__HH
#define __EOSCOMMON__GLOBE__HH

#include <glob.h>
#include <mutex>
#include <string.h>
#include <vector>
#include <stdexcept>
#include <string>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include "common/Namespace.hh"
#include "common/Path.hh"

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Static Class running bash pattern matching
//! To use this static functions include this header file
//! Example (call this for each entry in a listing)
//!   using eos::common::Glob;
//!   Glob glob;
//!   glob.Match("asdf*.txt", "asdf1.txt"; (true in this case, otherwise false)
//------------------------------------------------------------------------------
class Glob
{
public:
  //----------------------------------------------------------------------------
  //! 
  //----------------------------------------------------------------------------

  Glob() : mIt(0) {}
  
  virtual ~Glob() {}
  static void *opendir(const char *name) {
    return (DIR*) (gGlob);
  }
  
  static struct dirent *readdir(void *dirp) {
    Glob* thisglob = (Glob*) dirp;
    return thisglob->getEntry();
  }

  static void closedir(void *dirp) {return;}
  static int stat(const char *pathname, struct stat *statbuf)  { statbuf->st_mode = S_IFREG; return 0;}
  static int lstat(const char *pathname, struct stat *statbuf) { statbuf->st_mode = S_IFREG; return 0;}

  struct dirent* getEntry() {
    if (mIt < mNames.size()) {
      mEntry.d_ino = mIt+1; // 0 leads to NOMATCH with glibc 2-17!!!
      mEntry.d_off = mIt;
      mEntry.d_reclen = 255;
      mEntry.d_type = DT_REG;;
      snprintf(mEntry.d_name, mEntry.d_reclen, "%s",mNames[mIt++].c_str());
      return &mEntry;
    } else {
      return nullptr;
    }
  }
  
  bool Match(const std::string& pattern, const std::string& path) {
    static std::mutex g_i_mutex;
    mIt=0;
    mNames.clear();
    std::lock_guard<std::mutex> lock(g_i_mutex);
    gGlob = this;
    bool result = false;
    eos::common::Path cPath(path.c_str());
    mNames.resize(1);
    mNames[0] = cPath.GetName();
    glob_t globbuf;
    memset(&globbuf,0, sizeof(globbuf));
    //    globbuf.gl_offs = 2;
    globbuf.gl_opendir=opendir;
    globbuf.gl_readdir=readdir;
    globbuf.gl_closedir=closedir;
    globbuf.gl_stat=stat;
    globbuf.gl_lstat=lstat;

    glob(pattern.c_str(), GLOB_ALTDIRFUNC, NULL, &globbuf);
    if (globbuf.gl_pathc && mIt){ //mIt = 1 if getEntry() was called
      result = true;
    } else {
      result = false;
    }
    globfree(&globbuf);
    return result;
  }

  static Glob* gGlob;
  
private:
  std::vector<std::string> mNames;
  uint64_t mIt;
  struct dirent mEntry;
};

EOSCOMMONNAMESPACE_END
#endif
