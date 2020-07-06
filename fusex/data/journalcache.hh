/*
 * journalcache.hh
 *
 *  Created on: Mar 15, 2017
 *      Author: Michal Simon
 *
 ************************************************************************
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

#ifndef FUSEX_JOURNALCACHE_HH_
#define FUSEX_JOURNALCACHE_HH_

#include "cache.hh"
#include "cachelock.hh"
#include "cachesyncer.hh"
#include "cacheconfig.hh"
#include "xrdclproxy.hh"
#include "interval_tree.hh"
#include "data/dircleaner.hh"

#include <stdint.h>

#include <string>

class journalcache
{

  struct header_t {
    uint64_t offset;
    uint64_t size;
  };

public:

  struct chunk_t {

    chunk_t() : offset(0), size(0), buff(0) { }

    /* constructor - no ownership of underlying buffer */
    chunk_t(off_t offset, size_t size, const void* buff) : offset(offset),
      size(size), buff(buff) { }

    /* constructor - with ownership of underlying buffer */
    chunk_t(off_t offset, size_t size, std::unique_ptr<char[]> buff) :
    offset(offset), size(size), buffOwnership(std::move(buff)),
    buff( (const void*) buffOwnership.get()) {}

    off_t offset;
    size_t size;
    std::unique_ptr<char[]> buffOwnership;
    const void* buff;

    bool operator<(const chunk_t& u) const
    {
      return offset < u.offset;
    }
  };

  // TODO Some dummy default
  static constexpr size_t sDefaultMaxSize = 128 * 1024 * 1024ll;

  journalcache(fuse_ino_t _ino);
  virtual ~journalcache();

  // base class interface
  int attach(fuse_req_t req, std::string& cookie, int flags);
  int detach(std::string& cookie);
  int unlink();

  ssize_t pread(void* buf, size_t count, off_t offset);
  ssize_t pwrite(const void* buf, size_t count, off_t offset);

  int truncate(off_t, bool invalidate = false);
  int sync();

  size_t size();

  off_t get_max_offset();

  ssize_t get_truncatesize()
  {
    XrdSysMutexHelper lck(mtx);
    return truncatesize;
  }

  int set_attr(const std::string& key, const std::string& value)
  {
    return 0;
  }

  int attr(const std::string& key, std::string& value)
  {
    return 0;
  }

  int remote_sync(cachesyncer& syncer);

  int remote_sync_async(XrdCl::Proxy* proxy);

  static int init(const cacheconfig& config);
  static int init_daemonized(const cacheconfig& config);

  bool fits(ssize_t count)
  {
    return (sMaxSize >= (cachesize + count));
  }

  int reset();

  int rescue(std::string& location);

  std::vector<chunk_t> get_chunks(off_t offset, size_t size);

  int set_cookie(const std::string& cookie)
  {
    return set_attr("user.eos.cache.cookie", cookie);
  }

  bool first_flush()
  {
    return (!nbFlushed) ? true : false;
  }

  void done_flush()
  {
    nbFlushed++;
  }

  std::string dump();
private:

  void process_intersection(interval_tree<uint64_t, const void*>& write,
                            interval_tree<uint64_t, uint64_t>::iterator acr, std::vector<chunk_t>& updates);

  int location(std::string& path, bool mkpath = true);

  static uint64_t offset_for_update(uint64_t offset, uint64_t shift)
  {
    return offset + sizeof(header_t) + shift;
  }

  int update_cache(std::vector<chunk_t>& updates);

  int read_journal();

  fuse_ino_t ino;
  size_t cachesize;
  ssize_t truncatesize;
  off_t max_offset;
  int fd;
  // the value is the offset in the cache file
  interval_tree<uint64_t, uint64_t> journal;
  size_t nbAttached;
  size_t nbFlushed;
  cachelock clck;
  XrdSysMutex mtx;
  int flags;
  bufferllmanager::shared_buffer buffer;
  static std::string sLocation;
  static size_t sMaxSize;

  struct stat attachstat;
  struct stat detachstat;
  
  static shared_ptr<dircleaner> jDirCleaner;
};

#endif /* FUSEX_JOURNALCACHE_HH_ */
