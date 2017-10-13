//------------------------------------------------------------------------------
//! @file dircleaner.hh
//! @author Andreas-Joachim Peters CERN
//! @brief class keeping dir contents at a given level
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

#ifndef FUSE_DIRCLEANER_HH_
#define FUSE_DIRCLEANER_HH_

#include <sys/stat.h>
#include <sys/types.h>
#include "common/Logging.hh"
#include <memory>
#include <map>
#include <set>
#include <atomic>
#include <exception>
#include <stdexcept>
#include <thread>
#include <mutex>

class dircleaner
{
public:

  typedef struct fileinfo {
    std::string path;
    time_t mtime;
    size_t size;
  } file_info_t;

  typedef std::multimap<time_t, file_info_t> tree_map_t;

  typedef struct tree_info {

    tree_info()
    {
      totalsize = 0;
      totalfiles = 0;
    }
    tree_map_t treemap;
    int64_t totalsize;
    int64_t totalfiles;
    std::string path;

    void Print(std::string& out);
    XrdSysMutex Locker;

    // thrad safe change size, files
    void change(int64_t size, int64_t files)
    {
      eos_static_info("size=%ld files=%ld", size, files);
      XrdSysMutexHelper mLock(Locker);
      totalsize += size;
      totalfiles += files;
    }

    // safe reset function
    void reset()
    {
      XrdSysMutexHelper mLock(Locker);
      totalsize = 0;
      totalfiles = 0;
    }

    // thread safe get size
    int64_t get_size()
    {
      XrdSysMutexHelper mLock(Locker);
      return totalsize;
    }

    // thread safe get files
    int64_t get_files()
    {
      XrdSysMutexHelper mLock(Locker);
      return totalfiles;
    }

  } tree_info_t;

  dircleaner(const std::string _path = "/tmp/", int64_t _maxsize = 0 ,
             int64_t _maxfiles = 0);
  virtual ~dircleaner();

  bool has_suffix(const std::string& str, const std::string& suffix);

  tree_info_t& get_external_tree()
  {
    return externaltreeinfo;
  }

  int cleanall(std::string filtersuffix = "");
  int scanall();
  int trim(bool force);
  void leveler();

private:
  std::recursive_mutex cleaningMutex;
  std::string path;

  int64_t max_files;
  int64_t max_size;

  tree_info_t treeinfo;
  tree_info_t externaltreeinfo;

  std::thread tLeveler;

} ;
#endif
