/*
 * journalcache.cc
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

#include "journalcache.hh"
#include "dircleaner.hh"
#include "io.hh"
#include "common/Path.hh"
#include "common/Logging.hh"
#ifdef __APPLE__
#include "XrdSys/XrdSysPlatform.hh"
#endif
#include <algorithm>
#include <iostream>

constexpr size_t journalcache::sDefaultMaxSize;

std::string journalcache::sLocation;
size_t journalcache::sMaxSize = journalcache::sDefaultMaxSize;

shared_ptr<dircleaner> journalcache::jDirCleaner;

journalcache::journalcache(fuse_ino_t ino) : ino(ino), cachesize(0),
					     truncatesize(-1), max_offset(0), fd(-1),nbAttached(0), nbFlushed(0)
{
  memset(&attachstat, 0, sizeof(attachstat));
  memset(&detachstat, 0, sizeof(detachstat));
}


journalcache::~journalcache()
{
  if (fd > 0) {
    eos_static_debug("closing fd=%d\n", fd);
    detachstat.st_size = 0 ;
    fstat(fd, &detachstat);
      
    int rc = close(fd);

    if (rc) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wterminate"
      throw std::logic_error("journalcache::~journalcache fd close failed");
#pragma GCC diagnostic pop

    }
    
    if (jDirCleaner) {
      jDirCleaner->get_external_tree().change(detachstat.st_size - attachstat.st_size,
					      0);
    }

    if (!(flags & O_CACHE)) {
      // only clean write caches
      journal.clear();
      unlink();
    }
    fd = -1;
  }
}

int journalcache::location(std::string& path, bool mkpath)
{
  char cache_path[1024 + 20];
  snprintf(cache_path, sizeof(cache_path), "%s/%03lX/%08lX.jc",
           sLocation.c_str(), 
	   (ino > 0x0fffffff)? (ino >> 28) % 4096 : ino%4096, ino);

  if (mkpath) {
    eos::common::Path cPath(cache_path);

    if (!cPath.MakeParentPath(S_IRWXU)) {
      return -errno;
    }
  }

  path = cache_path;
  return 0;
}

int journalcache::read_journal()
{
  journal.clear();
  const size_t bufsize = 1024;
  char buffer[bufsize];
  ssize_t bytesRead = 0, totalBytesRead = 0;
  int64_t pos = 0;
  ssize_t entrySize = 0;

  while (true) {
    bytesRead = ::pread(fd, buffer, bufsize, totalBytesRead);

    if (bytesRead <= 0) {
      break;
    }

    pos = 0;

    do {
      if (entrySize == 0) {
        header_t* header = reinterpret_cast<header_t*>(buffer + pos);
        journal.insert(header->offset, header->offset + header->size,
                       totalBytesRead + pos);
        entrySize = header->size;
        pos += sizeof(header_t);
      }

      size_t shift = entrySize > bytesRead - pos ? bytesRead - pos : entrySize;
      pos += shift;
      entrySize -= shift;
    } while (pos < bytesRead);

    totalBytesRead += bytesRead;
  }

  if (bytesRead < 0) {
    return errno;
  }

  return totalBytesRead;
}

int journalcache::attach(fuse_req_t req, std::string& cookie, int _flags)
{
  XrdSysMutexHelper lck(mtx);
  
  flags = _flags;
  if ((nbAttached == 0) && (fd == -1)) {
    std::string path;
    int rc = location(path);

    if (rc) {
      return rc;
    }

    if (stat(path.c_str(), &attachstat)) {
      // a new file
      if (jDirCleaner) {
	jDirCleaner->get_external_tree().change(0, 1);
      }
    }


    // need to open the file 
    size_t tries=0;
    do {
      fd = open(path.c_str(), O_CREAT | O_RDWR, S_IRWXU);

      if (fd < 0) {
        if (errno == ENOENT) {
          tries++;
          // re-create the directory structure                                                                                   
          rc = location(path);
          if (rc) {
            return rc;
          }
          if (tries < 10) {
            continue;
          } else {
            return -errno;
          }
        }
        return -errno;
      }
      break;
    } while(1);

    cachesize = read_journal();
  }

  nbAttached++;
  return 0;
}

int journalcache::detach(std::string& cookie)
{
  XrdSysMutexHelper lck(mtx);
  nbAttached--;
  return 0;
}

int journalcache::unlink()
{
  std::string path;
  int rc = location(path);

  if (!rc) {
    struct stat buf;
    rc = stat(path.c_str(), &buf);
    if (!rc) {
      rc = ::unlink(path.c_str());
      if (!rc) {
        // a deleted file
	if (jDirCleaner) {
	  jDirCleaner->get_external_tree().change(-buf.st_size, -1);
	}
      }
    }
  }

  return rc;
}

int journalcache::rescue(std::string& rescue_location)
{
  std::string path;
  int rc = location(path);

  if (!rescue_location.length()) {
    rescue_location = path;
    rescue_location += ".recover";
  }

  if (!rc) {
    return ::rename(path.c_str(), rescue_location.c_str());
  } else {
    return rc;
  }
}

ssize_t journalcache::pread(void* buf, size_t count, off_t offset)
{
  read_lock lck(clck);
  auto result = journal.query(offset, offset + count);

  // there is not a single interval that overlaps
  if (result.empty()) {
    return 0;
  }

  char* buffer = reinterpret_cast<char*>(buf);
  uint64_t off = offset;
  uint64_t bytesRead = 0;

  for (auto& itr : result) {
    if (itr->low <= off && off < itr->high) {
      // read from cache
      uint64_t cacheoff = itr->value + sizeof(header_t) + (off - itr->low);
      int64_t intervalsize = itr->high - off;
      int64_t bytesLeft = count - bytesRead;
      int64_t bufsize = intervalsize < bytesLeft ? intervalsize : bytesLeft;
      ssize_t ret = ::pread(fd, buffer, bufsize, cacheoff);

      if (ret < 0) {
        return -1;
      }

      bytesRead += ret;
      off += ret;
      buffer += ret;

      if (bytesRead >= count) {
        break;
      }
    }
  }

  if ((truncatesize != -1) && ((ssize_t) offset >= truncatesize)) {
    // offset after truncation mark
    return 0;
  }

  if ((truncatesize != -1) && ((ssize_t)(offset + bytesRead) > truncatesize)) {
    // read over truncation size
    return (truncatesize - offset);
  }

  return bytesRead;
}

void journalcache::process_intersection(interval_tree<uint64_t, const void*>&
                                        to_write, interval_tree<uint64_t, uint64_t>::iterator itr,
                                        std::vector<chunk_t>& updates)
{
  auto result = to_write.query(itr->low, itr->high);

  if (result.empty()) {
    return;
  }

  if (result.size() > 1) {
    throw std::logic_error("journalcache: overlapping journal entries");
  }

  const interval_tree<uint64_t, const void*>::iterator to_wrt = *result.begin();
  // the intersection
  uint64_t low = std::max(to_wrt->low, itr->low);
  uint64_t high = std::min(to_wrt->high, itr->high);
  // update
  chunk_t update;
  update.offset = offset_for_update(itr->value, low - itr->low);
  update.size = high - low;
  update.buff = static_cast<const char*>(to_wrt->value) + (low - to_wrt->low);
  updates.push_back(std::move(update));
  // update the 'to write' intervals
  uint64_t wrtlow = to_wrt->low;
  uint64_t wrthigh = to_wrt->high;
  const void* wrtbuff = to_wrt->value;
  to_write.erase(wrtlow, wrthigh);

  // the intersection overlaps with the given
  // interval so there is nothing more to do
  if (low == wrtlow && high == wrthigh) {
    return;
  }

  if (high < wrthigh) {
    // the remaining right-hand-side interval
    const char* buff = static_cast<const char*>(wrtbuff) + (high - wrtlow);
    to_write.insert(high, wrthigh, buff);
  }

  if (low > wrtlow) {
    // the remaining left-hand-side interval
    to_write.insert(wrtlow, low, wrtbuff);
  }
}

int journalcache::update_cache(std::vector<chunk_t>& updates)
{
  // make sure we are updating the cache in ascending order
  std::sort(updates.begin(), updates.end());
  int rc = 0;

  for (auto& u : updates) {
    rc = ::pwrite(fd, u.buff, u.size,
                  u.offset); // TODO is it safe to assume it will write it all

    if (rc <= 0) {
      return errno;
    }
  }

  return 0;
}

ssize_t journalcache::pwrite(const void* buf, size_t count, off_t offset)
{
  if (count <= 0) {
    return 0;
  }

  write_lock lck(clck);

  while (sMaxSize <= cachesize) {
    clck.write_wait();
  }

  interval_tree<uint64_t, const void*> to_write;
  std::vector<chunk_t> updates;
  to_write.insert(offset, offset + count, buf);
  auto res = journal.query(offset, offset + count);

  for (auto itr : res) {
    process_intersection(to_write, itr, updates);
  }

  int rc = update_cache(updates);

  if (rc) {
    return -1;
  }

  interval_tree<uint64_t, const void*>::iterator itr;

  // TODO this could be replaced with a single pwritev
  for (itr = to_write.begin(); itr != to_write.end(); ++itr) {
    uint64_t size = itr->high - itr->low;
    header_t header;
    header.offset = itr->low;
    header.size = size;
    iovec iov[2];
    iov[0].iov_base = &header;
    iov[0].iov_len = sizeof(header_t);
    iov[1].iov_base = const_cast<void*>(itr->value);
    iov[1].iov_len = size;
    // @todo: fix this properly for the mac if there is such support
    rc = ::pwrite(fd, iov[0].iov_base, iov[0].iov_len, cachesize);
    rc += ::pwrite(fd, iov[1].iov_base, iov[1].iov_len,
                   cachesize + iov[0].iov_len);

    // rc = ::pwritev( fd, iov, 2, cachesize ); // TODO is it safe to assume it will write it all
    if (rc <= 0) {
      return -1;
    }

    journal.insert(itr->low, itr->high, cachesize);
    cachesize += sizeof(header_t) + size;
  }

  if ((truncatesize != -1) && ((ssize_t)(offset + count) > truncatesize)) {
    // journal written after last truncation size
    truncatesize = offset + count;
  }

  if ( (ssize_t)(offset + count) >  max_offset) {
    max_offset = offset + count;
  }
  return count;
}

int journalcache::truncate(off_t offset, bool invalidate)
{
  int rc = 0;
  write_lock lck(clck);

  fstat(fd, &detachstat);

  if (offset) {
    truncatesize = offset;
    max_offset = offset;
  } else {
    // distinguish cache invalidation from 0 truncation
    if (invalidate) {
      truncatesize = -1;
    } else {
      truncatesize = 0;
    }

    max_offset = 0;
    journal.clear();
    cachesize = 0;
    if (!::ftruncate(fd, 0)) {
      if (jDirCleaner) {
	jDirCleaner->get_external_tree().change(detachstat.st_size - attachstat.st_size,
						0);
      }
      attachstat.st_size = offset;
    }
  }

  return rc;
}

int journalcache::sync()
{
  return ::fdatasync(fd);
}

size_t journalcache::size()
{
  return cachesize;
}

off_t journalcache::get_max_offset()
{
  read_lock lck(clck);
  return max_offset;
}


int journalcache::init(const cacheconfig& config)
{
  if (::access(config.location.c_str(), W_OK)) {
    return errno;
  }

  sLocation = config.journal;

  if (config.per_file_journal_max_size) {
    journalcache::sMaxSize = config.per_file_journal_max_size;
  }

  eos_static_info("journalcache location %s", sLocation.c_str());
  return 0;
}

int journalcache::init_daemonized(const cacheconfig& config)
{
  jDirCleaner = std::make_shared<dircleaner>(config.location,
					     "jc",
					     config.total_file_journal_size,
					     config.total_file_journal_inodes,
					     config.clean_threshold
					     );
  jDirCleaner->set_trim_suffix(".jc");

  if (config.clean_on_startup) {
    eos_static_info("cleaning journal path=%s",config.location.c_str());
    if (jDirCleaner->cleanall(".jc")) {
      eos_static_err("journal cleanup failed");
      return -1;
    }
  }

  return 0;
}

int journalcache::remote_sync(cachesyncer& syncer)
{
  write_lock lck(clck);
  int ret = syncer.sync(fd, journal, sizeof(header_t), truncatesize);

  if (!ret) {
    journal.clear();
    eos_static_debug("ret=%d truncatesize=%ld\n", ret, truncatesize);
    ret |= ::ftruncate(fd, 0);
    eos_static_debug("ret=%d errno=%d\n", ret, errno);
  }

  clck.broadcast();
  return ret;
}

int journalcache::remote_sync_async(XrdCl::Proxy* proxy)
{
  // sends all the journal content as asynchronous write requests
  int ret = 0;

  if (!proxy) {
    return -1;
  }

  off_t offshift = sizeof(header_t);
  write_lock lck(clck);

  for (auto itr = journal.begin(); itr != journal.end(); ++itr) {
    off_t cacheoff = itr->value + offshift;
    size_t size = itr->high - itr->low;
    // prepare async buffer
    XrdCl::Proxy::write_handler handler = proxy->WriteAsyncPrepare(size, itr->low,
                                          0);
    int bytesRead = ::pread(fd, (void*) handler->buffer(), size, cacheoff);

    if (bytesRead < 0) {
      // TODO handle error
      clck.broadcast();
      return -1;
    }

    if (bytesRead < (int) size) {
      // TODO handle error - still we continue
    }

    XrdCl::XRootDStatus st = proxy->ScheduleWriteAsync(0, handler);

    if (!st.IsOK()) {
      eos_static_err("failed to issue async-write");
      clck.broadcast();
      return -1;
    }
  }

  // there might be a truncate call after the writes to be applied
  if (truncatesize != -1) {
    XrdCl::XRootDStatus st = proxy->Truncate(truncatesize);

    if (!st.IsOK()) {
      eos_static_err("failed to truncate");
      clck.broadcast();
      return -1;
    }

    truncatesize = -1;
  }

  journal.clear();
  eos_static_debug("ret=%d truncatesize=%ld\n", ret, truncatesize);
  errno = 0;
  ret |= ::ftruncate(fd, 0);
  eos_static_debug("ret=%d errno=%d\n", ret, errno);
  clck.broadcast();
  return ret;
}

int journalcache::reset()
{
  write_lock lck(clck);
  journal.clear();
  int retc = ::ftruncate(fd, 0);
  cachesize = 0;
  max_offset = 0;
  truncatesize = -1;
  clck.broadcast();
  return retc;
}

std::vector<journalcache::chunk_t> journalcache::get_chunks(off_t offset,
    size_t size)
{
  read_lock lck(clck);
  auto result = journal.query(offset, offset + size);
  std::vector<chunk_t> ret;

  for (auto& itr : result) {
    uint64_t off = (off_t) itr->low < (off_t) offset ? offset : itr->low;
    uint64_t count = itr->high < offset + size ? itr->high - off : offset + size -
                     off;
    uint64_t cacheoff = itr->value + sizeof(header_t) + (off - itr->low);
    std::unique_ptr<char[] > buffer(new char[count]);
    ssize_t rc = ::pread(fd, buffer.get(), count, cacheoff);

    if (rc < 0) {
      return ret;
    }

    ret.push_back(chunk_t(off, count, std::move(buffer)));
  }

  return ret;
}
