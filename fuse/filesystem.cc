//------------------------------------------------------------------------------
//! @file filesystem.cc
//! @author Andreas-Joachim Peters, Geoffray Adde, Elvin Sindrilaru CERN
//! @brief remote IO filesystem implementation
//------------------------------------------------------------------------------

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

#include <climits>
#include <cstdlib>
#include <queue>
#include <sstream>
#include <string>
#include <stdint.h>
#include <iostream>
#include <libgen.h>
#include <pwd.h>
#include <string.h>
#include <pthread.h>
#include <algorithm>
#include <limits>
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTable.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include "MacOSXHelper.hh"
#include "FuseCache/CacheEntry.hh"
#include "ProcCache.hh"
#include "common/XrdErrorMap.hh"
#include "common/Timing.hh"
#include "filesystem.hh"
#include "GlobalInodeTranslator.hh"
#include "xrdutils.hh"

#ifndef __macos__
#define OSPAGESIZE 4096
#else
#define OSPAGESIZE 65536
#endif

fuse_filesystem::fuse_filesystem():
  pid_max(32767), uid_max(0), link_pidmap(false),
  lazy_open_ro(false), lazy_open_rw(false), async_open(false),
  lazy_open_disabled(false), inline_repair(false),
  max_inline_repair_size(268435456), do_rdahead(false),
  rm_level_protect(1), rm_watch_relpath(false), fuse_cache_write(false),
  encode_pathname(false), mode_overlay(0)
{
  hide_special_files = true;
  show_eos_attributes = false;
  rdahead_window = "131072";
  fuse_exec = false;
  fuse_shared = false;
  creator_cap_lifetime = 30;
  file_write_back_cache_size = 64 * 1024 * 1024;
  max_wb_in_memory_size = 512 * 1024 * 1024;
  base_fd = 1;
  XFC = 0;
  tCacheCleanup = 0;
  mutex_inode_path.SetBlockedStackTracing(false);
  mutex_dir2inodelist.SetBlockedStackTracing(false);
  mutex_fuse_cache.SetBlockedStackTracing(false);
  rwmutex_fd2fabst.SetBlockedStackTracing(false);
  rwmutex_inodeopenw.SetBlockedStackTracing(false);

  for (auto i = 0; i < N_OPEN_MUTEXES; ++i) {
    openmutexes[i].SetBlockedStackTracing(false);
  }

  mMapPidDenyRmMutex.SetBlockedStackTracing(false);
}

fuse_filesystem::~fuse_filesystem()
{
  FuseCacheEntry* dir = 0;
  std::map<unsigned long long, FuseCacheEntry*>::iterator iter;
  iter = inode2cache.begin();

  while ((iter != inode2cache.end())) {
    dir = (FuseCacheEntry*) iter->second;
    std::set<unsigned long long> lset = iter->second->GetEntryInodes();

    for (auto it = lset.begin(); it != lset.end(); ++it) {
      inode2parent.erase(*it);
    }

    inode2cache.erase(iter++);
    delete dir;
  }
}

void*
fuse_filesystem::CacheCleanup(void* p)
{
  fuse_filesystem* me = (fuse_filesystem*)p;
  XrdSysTimer sleeper;

  while (1) {
    XrdSysThread::SetCancelOn();
    sleeper.Snooze(10);
    size_t n_read_buffer = 0;
    uint64_t size_read_buffer = 0;
    XrdSysThread::SetCancelOff();
    // clean left-over thread buffers
    {
      XrdSysMutexHelper lock(me->IoBufferLock);

      for (auto it = me->IoBufferMap.begin(); it != me->IoBufferMap.end();) {
        bool alive = thread_alive(it->first);
        eos_static_debug("thread-id %lld buffer-size=%lld alive-%d", it->first,
                         it->second.GetSize(), alive);

        if (!alive) {
          auto del_it = it;
          eos_static_notice("releasing read-buffer thread=%lld", it->first);
          ++it;
          me->IoBufferMap.erase(del_it);
        } else {
          size_read_buffer += it->second.GetSize();
          n_read_buffer++;
          ++it;
        }
      }
    }
    time_t now = time(NULL);
    XrdSysMutexHelper cLock(LayoutWrapper::gCacheAuthorityMutex);
    uint64_t totalsize_before = 0;
    uint64_t totalsize_after = 0;
    uint64_t totalsize_clean = 0;

    // release according to owner authority time
    for (auto it = LayoutWrapper::gCacheAuthority.begin();
         it != LayoutWrapper::gCacheAuthority.end();) {
      totalsize_before += it->second.mSize;

      if ((it->second.mLifeTime) && (it->second.mLifeTime < now)) {
        auto d = it;
        it++;
        eos_static_notice("released cap owner-authority for file inode=%lu "
                          "expire-by-time", d->first);
        LayoutWrapper::gCacheAuthority.erase(d);
      } else {
        it++;

        if (it != LayoutWrapper::gCacheAuthority.end()) {
          totalsize_after += it->second.mSize;
        }
      }
    }

    // clean according to memory pressure and cache setting
    totalsize_clean = totalsize_after;

    if (totalsize_after > me->max_wb_in_memory_size) {
      for (auto it = LayoutWrapper::gCacheAuthority.begin();
           it != LayoutWrapper::gCacheAuthority.end();) {
        totalsize_clean -= it->second.mSize;
        auto d = it;
        it++;
        eos_static_notice("released cap owner-authority for file inode=%lu "
                          "expire-by-memory-pressure", d->first);
        LayoutWrapper::gCacheAuthority.erase(d);

        if (totalsize_clean < me->max_wb_in_memory_size) {
          break;
        }
      }
    }

    eos_static_notice("in-memory wb cache in-size=%.02f MB out-time-size=%.02f "
                      "MB out-max-size=%.02f MB nominal-max-size=%.02f MB",
                      totalsize_before / 1000000., totalsize_after / 1000000.0,
                      totalsize_clean / 1000000.0, me->max_wb_in_memory_size / 1000000.0);
  }

  return 0;
}

void
fuse_filesystem::log(const char* _level, const char* msg)
{
  std::string level = _level;

  if (level == "NOTICE") {
    eos_static_notice(msg);
  } else if (level == "INFO") {
    eos_static_info(msg);
  } else if (level == "WARNING") {
    eos_static_warning(msg);
  } else if (level == "ALERT") {
    eos_static_alert(msg);
  } else {
    eos_static_debug(msg);
  }
}

void
fuse_filesystem::log_settings()
{
  std::string s = "lazy-open-ro           := ";

  if (lazy_open_disabled) {
    s += "disabled";
  } else {
    s += lazy_open_ro ? "true" : "false";
  }

  log("WARNING", s.c_str());
  s = "lazy-open-rw           := ";

  if (lazy_open_disabled) {
    s += "disabled";
  } else {
    s += lazy_open_rw ? "true" : "false";
  }

  log("WARNING", s.c_str());
  s = "hide-special-files     := ";

  if (hide_special_files) {
    s += "true";
  } else {
    s += "false";
  }

  log("WARNING", s.c_str());
  s = "show-eos-attributes    := ";

  if (show_eos_attributes) {
    s += "true";
  } else {
    s += "false";
  }

  log("WARNING", s.c_str());

  if (mode_overlay) {
    s = "mode-overlay           := ";
    s += getenv("EOS_FUSE_MODE_OVERLAY");
  }

  s = "rm-level-protect       := ";
  XrdOucString rml;
  rml += rm_level_protect;
  s += rml.c_str();
  log("WARMNING", s.c_str());
  s = "local-mount-dir        := ";
  s += mount_dir.c_str();
  log("WARNING", s.c_str());
  s = "write-cache            := ";
  std::string efc = getenv("EOS_FUSE_CACHE") ? getenv("EOS_FUSE_CACHE") : "0";
  s += efc;
  log("WARNING", s.c_str());
  s = "write-cache-size       := ";
  std::string efcs = getenv("EOS_FUSE_CACHE_SIZE") ? getenv("EOS_FUSE_CACHE_SIZE")
                     : "0";
  s += efcs;
  log("WARNING", s.c_str());
  s = "write-cache-page-size  := ";
  std::string efpcs = getenv("EOS_FUSE_CACHE_PAGE_SIZE") ?
                      getenv("EOS_FUSE_CACHE_PAGE_SIZE") : "(default 262144)";
  s += efpcs;
  log("WARNING", s.c_str());
  s = "big-writes             := ";
  std::string bw = getenv("EOS_FUSE_BIGWRITES") ? getenv("EOS_FUSE_BIGWRITES") :
                   "0";
  s += bw;
  log("WARNING", s.c_str());
  s = "create-cap-lifetime    := ";
  XrdOucString cc;
  cc += (int) creator_cap_lifetime;
  s += cc.c_str();
  s += " seconds";
  log("WARNING", s.c_str());
  s = "file-wb-cache-size     := ";
  XrdOucString fbcs;
  fbcs += (int)(file_write_back_cache_size / 1024 * 1024);
  s += fbcs.c_str();
  s += " MB";
  log("WARNING", s.c_str());
  s = "file-wb-cache-max-size := ";
  XrdOucString mcs;
  mcs += (int)(max_wb_in_memory_size / 1024 * 1024);
  s += mcs.c_str();
  s += " MB";
  log("WARNING", s.c_str());
  eos_static_warning("proc filesystem path   := %s",
                     getenv("EOS_FUSE_PROCPATH") ? getenv("EOS_FUSE_PROCPATH") : "/proc/");
  eos_static_warning("krb5 authentication    := %s",
                     credConfig.use_user_krb5cc ? "true" : "false");
  eos_static_warning("krb5 unsafe inmem krb5 := %s",
                     credConfig.use_unsafe_krk5 ? "true" : "false");
  eos_static_warning("x509 authentication    := %s",
                     credConfig.use_user_gsiproxy ? "true" : "false");
  eos_static_warning("fallback to nobody     := %s",
                     credConfig.fallback2nobody ? "true" : "false");
  eos_static_warning("xrd null resp retry    := %d",
                     xrootd_nullresponsebug_retrycount);
  eos_static_warning("xrd null resp sleep    := %d",
                     xrootd_nullresponsebug_retrysleep);
}


char*
myrealpath(const char* __restrict path, char* __restrict resolved, pid_t pid);

//------------------------------------------------------------------------------
// Lock read
//------------------------------------------------------------------------------
void
fuse_filesystem::lock_r_p2i()
{
  mutex_inode_path.LockRead();
}

//------------------------------------------------------------------------------
// Unlock read
//------------------------------------------------------------------------------
void
fuse_filesystem::unlock_r_p2i()
{
  mutex_inode_path.UnLockRead();
}

//------------------------------------------------------------------------------
// Drop the basename and return only the last level path name
//------------------------------------------------------------------------------
std::string
fuse_filesystem::base_name(unsigned long long inode)
{
  eos::common::RWMutexReadLock vLock(mutex_inode_path);
  const char* fname = path(inode);

  if (fname) {
    std::string spath = fname;
    size_t len = spath.length();

    if (len) {
      if (spath[len - 1] == '/') {
        spath.erase(len - 1);
      }
    }

    size_t spos = spath.rfind("/");

    if (spos != std::string::npos) {
      spath.erase(0, spos + 1);
    }

    return spath;
  }

  return "";
}

//----------------------------------------------------------------------------
//! Return the CGI of an URL
//----------------------------------------------------------------------------
const char*
fuse_filesystem::get_cgi(const char* url)
{
  return url ? (strchr(url, '?')) : 0;
}

//----------------------------------------------------------------------------
//! Return the CGI of an URL
//----------------------------------------------------------------------------
XrdOucString
fuse_filesystem::get_url_nocgi(const char* url)
{
  XrdOucString surl = url;
  surl.erase(surl.find("?"));
  return surl;
}

//------------------------------------------------------------------------------
// Translate from inode to path
//------------------------------------------------------------------------------
const char*
fuse_filesystem::path(unsigned long long inode)
{
// Obs: use lock_r_p2i/unlock_r_p2i in the scope of the returned string
  if (inode2path.count(inode)) {
    return inode2path[inode].c_str();
  } else {
    return 0;
  }
}

//------------------------------------------------------------------------------
// Translate from path to inode
//------------------------------------------------------------------------------

unsigned long long
fuse_filesystem::inode(const char* path)
{
  eos::common::RWMutexReadLock rd_lock(mutex_inode_path);
  unsigned long long ret = 0;

  if (path2inode.count(path)) {
    ret = path2inode[path];
  }

  return ret;
}

//------------------------------------------------------------------------------
// Store an inode <-> path mapping
//------------------------------------------------------------------------------
void
fuse_filesystem::store_p2i(unsigned long long inode, const char* path)
{
  eos::common::RWMutexWriteLock wr_lock(mutex_inode_path);
  path2inode[path] = inode;
  inode2path[inode] = path;
}

//----------------------------------------------------------------------------
//! Store an inode/mtime pair
//----------------------------------------------------------------------------
void
fuse_filesystem::store_i2mtime(unsigned long long inode, timespec ts)
{
  eos::common::RWMutexWriteLock wr_lock(mutex_inode_path);
  inode2mtime[inode] = ts;
  eos_static_debug("%8lx %lu.%lu %lu.%lu\n", inode,
                   inode2mtime_open[inode].tv_sec,
                   inode2mtime_open[inode].tv_nsec,
                   inode2mtime[inode].tv_sec,
                   inode2mtime[inode].tv_nsec);
}

//----------------------------------------------------------------------------
//! Store and test inode/mtime pair - returns true if open can set keep_cache
//----------------------------------------------------------------------------
bool
fuse_filesystem::store_open_i2mtime(unsigned long long inode)
{
  bool retval = false;
  eos::common::RWMutexWriteLock wr_lock(mutex_inode_path);
  eos_static_debug("%8lx %lu.%lu %lu.%lu\n", inode,
                   inode2mtime_open[inode].tv_sec,
                   inode2mtime_open[inode].tv_nsec,
                   inode2mtime[inode].tv_sec,
                   inode2mtime[inode].tv_nsec);

  // This was never set !
  if (inode2mtime_open[inode].tv_sec == 0) {
    retval = true;
  } else if ((inode2mtime_open[inode].tv_sec == inode2mtime[inode].tv_sec) &&
             (inode2mtime_open[inode].tv_nsec == inode2mtime[inode].tv_nsec)) {
    retval = true;
  } else {
    retval = false;
  }

  inode2mtime_open[inode] = inode2mtime[inode];
  eos_static_debug("%lx %lu.%lu %lu.%lu out=%d\n", inode,
                   inode2mtime_open[inode].tv_sec,
                   inode2mtime_open[inode].tv_nsec,
                   inode2mtime[inode].tv_sec,
                   inode2mtime[inode].tv_nsec, retval);
  return retval;
}

//------------------------------------------------------------------------------
// Replace a prefix when directories are renamed
//------------------------------------------------------------------------------
void
fuse_filesystem::replace_prefix(const char* oldprefix, const char* newprefix)
{
  eos::common::RWMutexWriteLock wr_lock(mutex_inode_path);
  std::string sprefix = oldprefix;
  std::string nprefix = newprefix;
  std::vector< std::pair<std::string, unsigned long long> > to_insert;

  for (auto it = path2inode.begin(); it != path2inode.end();) {
    auto dit = it;

    if (it->first.substr(0, sprefix.length()) == sprefix) {
      std::string path = it->first;
      path.erase(0, sprefix.length());
      path.insert(0, nprefix);
      eos_static_info("prefix-replace %s %s %llu", it->first.c_str(), path.c_str(),
                      (unsigned long long)it->second);
      dit++;
      unsigned long long ino = it->second;
      inode2path[ino] = path;
      path2inode.erase(it);
      // we can't insert the new element here because it invalidates all the iterators
      to_insert.push_back(std::make_pair(path, ino));
      it = dit;
    } else {
      it++;
    }
  }

  for (auto it = to_insert.begin(); it != to_insert.end(); it++) {
    path2inode.insert(*it);
  }
}
//------------------------------------------------------------------------------
// Store an inode <-> path mapping given the parent inode
//------------------------------------------------------------------------------
void
fuse_filesystem::store_child_p2i(unsigned long long inode,
                                 unsigned long long childinode,
                                 const char* name)
{
  eos::common::RWMutexWriteLock wr_lock(mutex_inode_path);
  std::string fullpath = inode2path[inode];
  std::string sname = name;
  eos_static_debug("parent_inode=%llu, child_inode=%llu, name=%s, fullpath=%s",
                   inode, childinode, name, fullpath.c_str());

  if (sname != ".") {
    if (sname == "..") {
      if (inode == 1) {
        fullpath = "/";
      } else {
        size_t spos = fullpath.find("/");
        size_t bpos = fullpath.rfind("/");

        if ((spos != std::string::npos) && (spos != bpos)) {
          fullpath.erase(bpos);
        }
      }
    } else {
      if (*fullpath.rbegin() != '/') {
        fullpath += "/";
      }

      fullpath += name;
    }

    eos_static_debug("sname=%s fullpath=%s inode=%llu childinode=%llu ",
                     sname.c_str(), fullpath.c_str(), inode, childinode);
    path2inode[fullpath] = childinode;
    inode2path[childinode] = fullpath;
  }
}


//------------------------------------------------------------------------------
// Delete an inode <-> path mapping given the inode
//------------------------------------------------------------------------------
void
fuse_filesystem::forget_p2i(unsigned long long inode)
{
  eos::common::RWMutexWriteLock wr_lock(mutex_inode_path);

  if (inode2path.count(inode)) {
    std::string path = inode2path[inode];

    // only delete the reverse lookup if it points to the originating inode
    if (path2inode[path] == inode) {
      path2inode.erase(path);
    }

    inode2path.erase(inode);
  }

  inode2mtime.erase(inode);
  inode2mtime_open.erase(inode);
}
//------------------------------------------------------------------------------
// Redirect an inode to a new inode - repair actions change inodes, so we have
// two ino1,ino2=>path1 mappings
//------------------------------------------------------------------------------
void
fuse_filesystem::redirect_p2i(unsigned long long inode,
                              unsigned long long new_inode)
{
  eos::common::RWMutexWriteLock wr_lock(mutex_inode_path);

  if (inode2path.count(inode)) {
    std::string path = inode2path[inode];

    // only delete the reverse lookup if it points to the originating inode
    if (path2inode[path] == inode) {
      path2inode.erase(path);
      path2inode[path] = new_inode;
    }

    // since inodes are cache dupstream we leave for the rare case of a restore a blind entry
    //   inode2path.erase (inode);
    //   inode2path.erase (inode);
    //   inode2path.erase (inode);
    //   inode2path.erase (inode);
    inode2path[new_inode] = path;
  }
}

//------------------------------------------------------------------------------
// Redirect an inode to the latest valid inode version - due to repair actions
//------------------------------------------------------------------------------
unsigned long long
fuse_filesystem::redirect_i2i(unsigned long long inode)
{
  eos::common::RWMutexReadLock rd_lock(mutex_inode_path);

  if (inode2path.count(inode)) {
    std::string path = inode2path[inode];

    if (path2inode.count(path)) {
      return path2inode[path];
    }
  }

  return inode;
}

//------------------------------------------------------------------------------
//      ******* Implementation of the FUSE directory cache *******
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Get a cached directory
//------------------------------------------------------------------------------
int
fuse_filesystem::dir_cache_get(unsigned long long inode,
                               struct timespec mtime,
                               struct timespec ctime,
                               struct dirbuf** b)
{
  int retc = 0;
  FuseCacheEntry* dir = 0;
  eos::common::RWMutexReadLock rd_lock(mutex_fuse_cache);

  if (inode2cache.count(inode) && (dir = inode2cache[inode])) {
    struct timespec oldtime = dir->GetModifTime();

    if ((oldtime.tv_sec == (mtime.tv_sec + ctime.tv_sec)) &&
        (oldtime.tv_nsec == (mtime.tv_nsec + ctime.tv_nsec))) {
      // Dir in cache and valid
      *b = static_cast<struct dirbuf*>(calloc(1, sizeof(dirbuf)));
      dir->GetDirbuf(*b);
      retc = 1; // found
    } else {
      eos_static_debug("entry expired %llu %llu %llu %llu",
                       mtime.tv_sec + ctime.tv_sec, oldtime.tv_sec,
                       mtime.tv_nsec + ctime.tv_nsec, oldtime.tv_nsec);
    }
  } else {
    eos_static_debug("not in cache");
  }

  return retc;
}


//------------------------------------------------------------------------------
// Forget a cached directory
//------------------------------------------------------------------------------
int
fuse_filesystem::dir_cache_forget(unsigned long long inode)
{
  eos::common::RWMutexWriteLock wr_lock(mutex_fuse_cache);

  if (inode2cache.count(inode)) {
    std::set<unsigned long long> lset = inode2cache[inode]->GetEntryInodes();

    for (auto it = lset.begin(); it != lset.end(); ++it) {
      inode2parent.erase(*it);
    }

    delete inode2cache[inode];
    inode2cache.erase(inode);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Add or update a cache directory entry
//------------------------------------------------------------------------------
void
fuse_filesystem::dir_cache_sync(unsigned long long inode,
                                int nentries,
                                struct timespec mtime,
                                struct timespec ctime,
                                struct dirbuf* b,
                                long lifetimens)
{
  eos::common::RWMutexWriteLock wr_lock(mutex_fuse_cache);
  FuseCacheEntry* dir = 0;
  struct timespec modtime;
  modtime.tv_sec  = mtime.tv_sec + ctime.tv_sec;
  modtime.tv_nsec = mtime.tv_nsec + ctime.tv_nsec;

  if ((inode2cache.count(inode)) && (dir = inode2cache[inode])) {
    dir->Update(nentries, modtime, b);
  } else {
    // Add new entry
    if (inode2cache.size() >= GetMaxCacheSize()) {
      // Size control of the cache
      unsigned long long indx = 0;
      unsigned long long entries_del =
        static_cast<unsigned long long>(0.25 * GetMaxCacheSize());
      std::map<unsigned long long, FuseCacheEntry*>::iterator iter;
      iter = inode2cache.begin();

      while ((indx <= entries_del) && (iter != inode2cache.end())) {
        dir = (FuseCacheEntry*) iter->second;
        std::set<unsigned long long> lset = iter->second->GetEntryInodes();

        for (auto it = lset.begin(); it != lset.end(); ++it) {
          inode2parent.erase(*it);
        }

        inode2cache.erase(iter++);
        delete dir;
        indx++;
      }
    }

    dir = new FuseCacheEntry(nentries, modtime, b, lifetimens);
    inode2cache[inode] = dir;
  }
}


//------------------------------------------------------------------------------
// Get a subentry from a cached directory
//------------------------------------------------------------------------------
int
fuse_filesystem::dir_cache_get_entry(fuse_req_t req,
                                     unsigned long long inode,
                                     unsigned long long entry_inode,
                                     const char* efullpath,
                                     struct stat* overwrite_stat)
{
  int retc = 0;
  eos::common::RWMutexReadLock rd_lock(mutex_fuse_cache);
  FuseCacheEntry* dir;

  if ((inode2cache.count(inode)) && (dir = inode2cache[inode])) {
    if (dir->IsFilled()) {
      struct fuse_entry_param e;

      if (dir->GetEntry(entry_inode, e)) {
        // we eventually need to overwrite the cached information
        if (overwrite_stat) {
          e.attr.MTIMESPEC = overwrite_stat->MTIMESPEC;
          e.attr.st_mtime = overwrite_stat->MTIMESPEC.tv_sec;
          e.attr.st_size = overwrite_stat->st_size;
        }

        store_p2i(entry_inode, efullpath);
        fuse_reply_entry(req, &e);
        eos_static_debug("mode=%x timeout=%.02f\n", e.attr.st_mode, e.attr_timeout);
        retc = 1; // found
      }
    }
  }

  return retc;
}


//------------------------------------------------------------------------------
// Add new subentry to a cached directory
//------------------------------------------------------------------------------
void
fuse_filesystem::dir_cache_add_entry(unsigned long long inode,
                                     unsigned long long entry_inode,
                                     struct fuse_entry_param* e)
{
  eos::common::RWMutexWriteLock wr_lock(mutex_fuse_cache);
  FuseCacheEntry* dir = 0;

  if ((inode2cache.count(inode)) && (dir = inode2cache[inode])) {
    inode2parent[entry_inode] = inode;
    dir->AddEntry(entry_inode, e);
  }
}


bool
fuse_filesystem::dir_cache_update_entry(unsigned long long entry_inode,
                                        struct stat* buf)
{
  eos::common::RWMutexReadLock rd_lock(mutex_fuse_cache);
  FuseCacheEntry* dir = 0;
  unsigned long long parent;
  eos_static_debug("ino=%lld size=%llu\n", entry_inode, buf->st_size);

  if ((inode2parent.count(entry_inode))) {
    parent = inode2parent[entry_inode];

    if ((inode2cache.count(parent)) && (dir = inode2cache[parent])) {
      return dir->UpdateEntry(entry_inode, buf);
    }
  }

  return false;
}

//------------------------------------------------------------------------------
// Create artificial file descriptor
//------------------------------------------------------------------------------
int
fuse_filesystem::generate_fd()
{
  int retc = -1;

  if (!pool_fd.empty()) {
    retc = pool_fd.front();
    pool_fd.pop();
  } else if (base_fd < INT_MAX) {
    base_fd++;
    retc = base_fd;
  } else {
    eos_static_err("no more file descirptors available.");
    retc = -1;
  }

  return retc;
}

//------------------------------------------------------------------------------
// Add new mapping between fd and raw file object
//------------------------------------------------------------------------------
int
fuse_filesystem::force_rwopen(
  unsigned long long inode,
  uid_t uid, gid_t gid, pid_t pid
)
{
  std::ostringstream sstr;
  sstr << inode << ":" << get_login(uid, gid, pid);
  eos::common::RWMutexReadLock rd_lock(rwmutex_fd2fabst);
  auto iter_fd = inodexrdlogin2fds.find(sstr.str());

  if (iter_fd != inodexrdlogin2fds.end()) {
    for (auto fdit = iter_fd->second.begin(); fdit != iter_fd->second.end();
         fdit++) {
      if (fd2count[*fdit] > 0) {
        std::shared_ptr<FileAbstraction> fabst = get_file(*fdit, NULL);

        // If there is already an entry for the current user and the current inode
        if (!fabst.get()) {
          errno = ENOENT;
          return 0;
        }

        if (fabst->GetRawFileRO()) {
          fabst->DecNumRefRO();
          return 0;
        }

        if (!fabst->GetRawFileRW()) {
          return 0;
        }

        if (fabst->GetRawFileRW()->MakeOpen()) {
          fabst->DecNumRefRW();
          errno = EIO;
          eos_static_info("makeopen returned -1");
          return -1; // return -1 if failure
        } else {
          eos_static_info("forced read-open");
          fabst->DecNumRefRW();
        }

        return *fdit; // return the fd if succeed (>0)
      }
    }
  }

  return 0; // return 0 if nothing to do
}

//------------------------------------------------------------------------------
// Add new mapping between fd and raw file object
//------------------------------------------------------------------------------
int
fuse_filesystem::add_fd2file(LayoutWrapper* raw_file,
                             unsigned long long inode,
                             uid_t uid, gid_t gid, pid_t pid,
                             bool isROfd,
                             const char* path,
                             bool mknod)
{
  eos_static_debug("file raw ptr=%p, inode=%lu, uid=%lu",
                   raw_file, inode, (unsigned long) uid);
  int fd = -1;
  std::ostringstream sstr;
  sstr << inode << ":" << get_login(uid, gid, pid);
  eos::common::RWMutexWriteLock wr_lock(rwmutex_fd2fabst);
  auto iter_fd = inodexrdlogin2fds.find(sstr.str());
  shared_ptr<FileAbstraction> fabst;

  // If there is already an entry for the current user and the current inode
  // then we return the old fd
  if (raw_file == nullptr) {
    if (iter_fd != inodexrdlogin2fds.end()) {
      fd = *iter_fd->second.begin();
      auto iter_file = fd2fabst.find(fd);

      if ((iter_file != fd2fabst.end()) &&
          (iter_file->second != nullptr)) {
        fabst = iter_file->second;

        for (auto fdit = iter_fd->second.begin(); fdit != iter_fd->second.end();
             fdit++) {
          if (isROfd == (fd2count[*fdit] < 0)) {
            fd2count[*fdit] += isROfd ? -1 : 1;
            isROfd ? fabst->IncNumOpenRO() : fabst->IncNumOpenRW();
            eos_static_debug("existing fdesc exisiting fabst: fabst=%p path=%s "
                             "isRO=%d => fdesc=%d",
                             fabst.get(), path, (int) isROfd, (int) *fdit);
            fabst->CleanReadCache();
            return *fdit;
          }
        }
      } else {
        return -1;
      }
    }

    return -1;
  }

  fd = generate_fd();

  if (fd > 0) {
    if (iter_fd != inodexrdlogin2fds.end()) {
      fabst = fd2fabst[ *iter_fd->second.begin() ];
    }

    if (!fabst.get()) {
      fabst = std::make_shared<FileAbstraction> (path);
      eos_static_debug("new fdesc new fabst: fbast=%p path=%s isRO=%d => "
                       "fdesc=%d", fabst.get(), path, (int) isROfd, (int) fd);
    } else {
      eos_static_debug("new fdesc existing fabst: fbast=%p path=%s isRO=%d "
                       "=> fdesc=%d", fabst.get(), path, (int) isROfd, (int) fd);
    }

    if (isROfd) {
      fabst->SetRawFileRO(raw_file);  // sets numopenRO to 1
    } else {
      fabst->SetRawFileRW(raw_file);  // sets numopenRW to 1

      if (mknod) {
        // dec ref count, because they won't be a close referring to an mknod call
        fabst->DecNumOpenRW();
        fabst->DecNumRefRW();
      }

      fabst->SetFd(fd);
    }

    fabst->GrabMaxWriteOffset();
    fabst->GrabUtimes();
    fd2fabst[fd] = fabst;
    fd2count[fd] = isROfd ? -1 : 1;

    if (mknod) {
      fd2count[fd] = 0;
    }

    inodexrdlogin2fds[sstr.str()].insert(fd);
    eos_static_debug("inserting fd : fabst=%p  key=%s  =>  fdesc=%d file-size=%llu",
                     fabst.get(), sstr.str().c_str(), (int) fd, fabst->GetMaxWriteOffset());
  } else {
    eos_static_err("error while getting file descriptor");

    if (raw_file) {
      delete raw_file;
    }
  }

  return fd;
}


//------------------------------------------------------------------------------
// Get the file abstraction object corresponding to the fd
//------------------------------------------------------------------------------
std::shared_ptr<FileAbstraction>
fuse_filesystem::get_file(int fd, bool* isRW, bool forceRWtoo)
{
  std::shared_ptr<FileAbstraction> fabst;
  eos_static_debug("fd=%i", fd);
  eos::common::RWMutexReadLock rd_lock(rwmutex_fd2fabst);
  auto iter = fd2fabst.find(fd);

  if (iter == fd2fabst.end()) {
    eos_static_err("no file abst for fd=%i", fd);
    return fabst;
  }

  fabst = iter->second;

  if (isRW) {
    *isRW = fd2count[fd] > 0;
  }

  fd2count[fd] > 0 ? iter->second->IncNumRefRW() : iter->second->IncNumRefRO();

  if (forceRWtoo && fd2count[fd] < 0) {
    iter->second->IncNumRefRW();
  }

  return fabst;
}

//------------------------------------------------------------------------------
// Remove entry from mapping
//------------------------------------------------------------------------------
int
fuse_filesystem::remove_fd2file(int fd, unsigned long long inode, uid_t uid,
                                gid_t gid,
                                pid_t pid)
{
  int retc = -1;
  eos_static_debug("fd=%i, inode=%lu", fd, inode);
  rwmutex_fd2fabst.LockWrite();
  auto iter = fd2fabst.find(fd);
  auto iter1 = inodexrdlogin2fds.end();

  if (iter != fd2fabst.end()) {
    std::shared_ptr<FileAbstraction> fabst = iter->second;
    bool isRW = (fd2count[fd] > 0);
    fd2count[fd] -= (fd2count[fd] < 0 ? -1 : 1);

    if ((!isRW && !fabst->IsInUseRO()) || (isRW && !fabst->IsInUseRW())) {
      // there is no more reference to that fd
      if (!fd2count[fd]) {
        eos_static_debug("remove fd=%d", fd);
        fd2count.erase(fd);
        fd2fabst.erase(fd);
        std::ostringstream sstr;
        sstr << inode << ":" << get_login(uid, gid, pid);
        iter1 = inodexrdlogin2fds.find(sstr.str());

        // If a file is repaired during an RW open, the inode can change and
        // we find the fd in a different inode
        // search the map for the filedescriptor and remove it
        if (iter1 != inodexrdlogin2fds.end()) {
          iter1->second.erase(fd);
        } else {
          // search the map for the filedescriptor and remove it
          for (iter1 = inodexrdlogin2fds.begin(); iter1 != inodexrdlogin2fds.end();
               ++iter1) {
            if (iter1->second.count(fd)) {
              iter1->second.erase(fd);
              break;
            }
          }
        }

        if (iter1->second.empty()) {
          inodexrdlogin2fds.erase(iter1);
        }

        // Return fd to the pool
        pool_fd.push(fd);
        rwmutex_fd2fabst.UnLockWrite();
      } else {
        rwmutex_fd2fabst.UnLockWrite();
      }

      if (isRW) {
        eos_static_debug("fabst=%p, rwfile is not in use, close it", fabst.get());
        retc = 0;
      } else {
        eos_static_debug("fabst=%p, rofile is not in use, close it", fabst.get());
        retc = 0;
      }
    } else {
      rwmutex_fd2fabst.UnLockWrite();
    }

    if (!fabst->IsInUse()) {
      eos_static_debug("fabst=%p is not in use anynmore", fabst.get());
    } else {
      eos_static_debug("fabst=%p is still in use, cannot remove", fabst.get());

      // Decrement number of references - so that the last process can
      // properly close the file
      if (isRW) {
        fabst->DecNumRefRW();
        fabst->DecNumOpenRW();
      } else {
        fabst->DecNumRefRO();
        fabst->DecNumOpenRO();
      }
    }
  } else {
    rwmutex_fd2fabst.UnLockWrite();
    eos_static_warning("fd=%i no long in map, maybe already closed ...", fd);
  }

  return retc;
}

char*
fuse_filesystem::attach_rd_buff(pthread_t tid, size_t size)
{
  XrdSysMutexHelper lock(IoBufferLock);
  IoBufferMap[tid].Resize(size);
  return (char*) IoBufferMap[tid].GetBuffer();
}

//------------------------------------------------------------------------------
//             ******* XROOTD connection/authentication functions *******
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Get user name from the uid and change the effective user ID of the thread
//------------------------------------------------------------------------------
int
fuse_filesystem::update_proc_cache(uid_t uid, gid_t gid, pid_t pid)
{
  return authidmanager.updateProcCache(uid, gid, pid);
}

std::string
fuse_filesystem::get_login(uid_t uid, gid_t gid, pid_t pid)
{
  return authidmanager.getLogin(uid, gid, pid);
}

//------------------------------------------------------------------------------
//             ******* XROOTD interface functions *******
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Remove extended attribute
//------------------------------------------------------------------------------
int
fuse_filesystem::rmxattr(const char* path,
                         const char* xattr_name,
                         uid_t uid,
                         gid_t gid,
                         pid_t pid)
{
  eos_static_info("path=%s xattr_name=%s uid=%u pid=%u", path, xattr_name, uid,
                  pid);
  eos::common::Timing rmxattrtiming("rmxattr");
  COMMONTIMING("START", &rmxattrtiming);
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  XrdOucString xa = xattr_name;
  request = safePath(path);
  request += "?";
  request += "mgm.pcmd=xattr&eos.app=fuse&";
  request += "mgm.subcmd=rm&";

  if (encode_pathname) {
    request += "eos.encodepath=1&";
  }

  request += "mgm.xattrname=";
  request += xattr_name;
  arg.FromString(request);
  std::string surl = user_url(uid, gid, pid);
  std::string auth = strongauth_cgi(uid, gid, pid);

  if (!auth.empty()) {
    surl += "?" + auth;
  }

  XrdCl::URL Url(surl.c_str());
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = xrdreq_retryonnullbuf(fs, arg, response);
  COMMONTIMING("GETPLUGIN", &rmxattrtiming);
  errno = 0;

  if (status.IsOK()) {
    int retc = 0;
    int items = 0;
    char tag[1024];
    // Parse output
    items = sscanf(response->GetBuffer(), "%1023s retc=%i", tag, &retc);

    if ((items != 2) || (strcmp(tag, "rmxattr:"))) {
      errno = ENOENT;
    } else if (retc) {
      errno = ENODATA;  // = ENOATTR
    }
  } else {
    eos_static_err("status is NOT ok : %s", status.ToString().c_str());
    errno = ((status.code == XrdCl::errAuthFailed) ? EPERM : EFAULT);

    if (status.code == XrdCl::errErrorResponse) {
      eos::common::error_retc_map(status.errNo);
    }
  }

  COMMONTIMING("END", &rmxattrtiming);

  if (EOS_LOGS_DEBUG) {
    rmxattrtiming.Print();
  }

  delete response;
  return errno;
}


//------------------------------------------------------------------------------
// Set extended attribute
int
fuse_filesystem::setxattr(const char* path,
                          const char* xattr_name,
                          const char* xattr_value,
                          size_t size,
                          uid_t uid,
                          gid_t gid,
                          pid_t pid)
{
  eos_static_info("path=%s xattr_name=%s xattr_value=%s uid=%u pid=%u",
                  path, xattr_name, xattr_value, uid, pid);
  eos::common::Timing setxattrtiming("setxattr");
  COMMONTIMING("START", &setxattrtiming);
  XrdOucString xa = xattr_name;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = safePath(path);
  request += "?";
  request += "mgm.pcmd=xattr&eos.app=fuse&";
  request += "mgm.subcmd=set&";

  if (encode_pathname) {
    request += "eos.encodepath=1&";
  }

  request += "mgm.xattrname=";
  request += xattr_name;
  std::string s_xattr_name = xattr_name;

  if (s_xattr_name.find("&") != std::string::npos) {
    // & is a forbidden character in attribute names
    errno = EINVAL;
    return errno;
  }

  request += "&";
  request += "mgm.xattrvalue=";
  XrdOucString key(xattr_name);
  XrdOucString value;
  XrdOucString b64value;
  eos::common::SymKey::Base64Encode(xattr_value, size, b64value);
  value = "base64:";
  value += b64value;
  request += value.c_str();
  arg.FromString(request);
  std::string surl = user_url(uid, gid, pid);
  std::string auth = strongauth_cgi(uid, gid, pid);

  if (!auth.empty()) {
    surl += "?" + auth;
  }

  XrdCl::URL Url(surl.c_str());
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg,
                                        response);
  COMMONTIMING("GETPLUGIN", &setxattrtiming);
  errno = 0;

  if (status.IsOK()) {
    int retc = 0;
    int items = 0;
    char tag[1024];
    // Parse output
    items = sscanf(response->GetBuffer(), "%1023s retc=%i", tag, &retc);

    if ((items != 2) || (strcmp(tag, "setxattr:"))) {
      errno = ENOENT;
    } else {
      errno = retc;
    }
  } else {
    eos_static_err("status is NOT ok : %s", status.ToString().c_str());
    errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;

    if (status.code == XrdCl::errErrorResponse) {
      eos::common::error_retc_map(status.errNo);
    }
  }

  COMMONTIMING("END", &setxattrtiming);

  if (EOS_LOGS_DEBUG) {
    setxattrtiming.Print();
  }

  delete response;
  return errno;
}

//------------------------------------------------------------------------------
// Read an extended attribute
//------------------------------------------------------------------------------
int
fuse_filesystem::getxattr(const char* path,
                          const char* xattr_name,
                          char** xattr_value,
                          size_t* size,
                          uid_t uid,
                          gid_t gid,
                          pid_t pid)
{
  eos_static_info("path=%s xattr_name=%s uid=%u pid=%u", path, xattr_name, uid,
                  pid);
  eos::common::Timing getxattrtiming("getxattr");
  COMMONTIMING("START", &getxattrtiming);
  XrdOucString xa = xattr_name;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = safePath(path);
  request += "?";
  request += "mgm.pcmd=xattr&eos.app=fuse&";
  request += "mgm.subcmd=get&";

  if (encode_pathname) {
    request += "eos.encodepath=1&";
  }

  request += "mgm.xattrname=";
  std::string s_xattr_name = xattr_name;

  if (s_xattr_name.find("&") != std::string::npos) {
    // & is a forbidden character in attribute names
    errno = EINVAL;
    return errno;
  }

  request += xattr_name;
  arg.FromString(request);
  std::string surl = user_url(uid, gid, pid);
  std::string auth = strongauth_cgi(uid, gid, pid);

  if (!auth.empty()) {
    surl += "?" + auth;
  }

  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = xrdreq_retryonnullbuf(fs, arg, response);
  COMMONTIMING("GETPLUGIN", &getxattrtiming);
  errno = 0;

  if (status.IsOK()) {
    int retc = 0;
    int items = 0;
    char tag[1024];
    char rval[4096];
    // Parse output
    items = sscanf(response->GetBuffer(), "%1023s retc=%i value=%4095s",
                   tag, &retc, rval);

    if ((items != 3) || (strcmp(tag, "getxattr:"))) {
      errno = EFAULT;
    } else {
      if (strcmp(xattr_name, "user.eos.XS") == 0) {
        char* ptr = rval;

        for (unsigned int i = 0; i < strlen(rval); i++, ptr++) {
          if (*ptr == '_') {
            *ptr = ' ';
          }
        }
      }

      XrdOucString value64 = rval;

      if (value64.beginswith("base64:")) {
        value64.erase(0, 7);
        ssize_t ret_size;
        eos::common::SymKey::Base64Decode(value64, *xattr_value, ret_size);
        *size = ret_size;
        eos_static_info("xattr-name=%s xattr-value=%s", xattr_name, *xattr_value);
      } else {
        eos_static_info("xattr-name=%s xattr-value=%s", xattr_name, value64.c_str());
        *size = value64.length();
        *xattr_value = (char*) calloc((*size) + 1, sizeof(char));
        *xattr_value = strncpy(*xattr_value, value64.c_str(), *size);
      }

      errno = retc;
    }
  } else {
    eos_static_err("status is NOT ok : %s", status.ToString().c_str());
    errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;

    if (status.code == XrdCl::errErrorResponse) {
      eos::common::error_retc_map(status.errNo);
    }
  }

  COMMONTIMING("END", &getxattrtiming);

  if (EOS_LOGS_DEBUG) {
    getxattrtiming.Print();
  }

  delete response;
  return errno;
}

//------------------------------------------------------------------------------
// List extended attributes
//------------------------------------------------------------------------------
int
fuse_filesystem::listxattr(const char* path,
                           char** xattr_list,
                           size_t* size,
                           uid_t uid,
                           gid_t gid,
                           pid_t pid)
{
  eos_static_info("path=%s uid=%u pid=%u", path, uid, pid);
  eos::common::Timing listxattrtiming("listxattr");
  COMMONTIMING("START", &listxattrtiming);
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = safePath(path);
  request += "?";
  request += "mgm.pcmd=xattr&eos.app=fuse&";

  if (encode_pathname) {
    request += "eos.encodepath=1&";
  }

  request += "mgm.subcmd=ls";
  arg.FromString(request);
  std::string surl = user_url(uid, gid, pid);
  std::string auth = strongauth_cgi(uid, gid, pid);

  if (!auth.empty()) {
    surl += "?" + auth;
  }

  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = xrdreq_retryonnullbuf(fs, arg, response);
  COMMONTIMING("GETPLUGIN", &listxattrtiming);
  errno = 0;

  if (status.IsOK()) {
    int retc = 0;
    int items = 0;
    char tag[1024];
    char rval[65536];
    // Parse output
    items = sscanf(response->GetBuffer(), "%1023s retc=%i %65535s", tag, &retc,
                   rval);
    eos_static_info("retc=%d tag=%s response=%s", retc, tag, rval);

    if ((items != 3) || (strcmp(tag, "lsxattr:"))) {
      errno = ENOENT;
    } else {
      char* ptr = rval;
      *size = strlen(rval);
      std::vector<std::string> xattrkeys;
      char* sptr = ptr;
      char* eptr = ptr;
      size_t attr_size = 0;

      for (unsigned int i = 0; i < (*size); i++, ptr++) {
        if (*ptr == '&') {
          *ptr = '\0';
          eptr = ptr;
          std::string xkey;
          xkey.assign(sptr, eptr - sptr);
          XrdOucString sxkey = xkey.c_str();

          if (!show_eos_attributes &&
              (sxkey.beginswith("user.admin.")  ||
               sxkey.beginswith("user.eos."))) {
            sptr = eptr + 1;
            continue;
          }

          attr_size += xkey.length() + 1;
          xattrkeys.push_back(xkey);
          sptr = eptr + 1;
        }
      }

      *xattr_list = (char*) calloc(attr_size, sizeof(char));
      ptr = *xattr_list;

      for (size_t i = 0; i < xattrkeys.size(); i++) {
        memcpy(ptr, xattrkeys[i].c_str(), xattrkeys[i].length());
        ptr += xattrkeys[i].length();
        *ptr = '\0';
        ptr++;
      }

      *size = attr_size;
      errno = retc;
    }
  } else {
    eos_static_err("status is NOT ok : %s", status.ToString().c_str());
    errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;

    if (status.code == XrdCl::errErrorResponse) {
      eos::common::error_retc_map(status.errNo);
    }
  }

  COMMONTIMING("END", &listxattrtiming);

  if (EOS_LOGS_DEBUG) {
    listxattrtiming.Print();
  }

  delete response;
  return errno;
}


//------------------------------------------------------------------------------
// Return file attributes. If a field is meaningless or semi-meaningless
// (e.g., st_ino) then it should be set to 0 or given a "reasonable" value.
//------------------------------------------------------------------------------
int
fuse_filesystem::stat(const char* path, struct stat* buf, uid_t uid, gid_t gid,
                      pid_t pid, unsigned long long inode, bool onlysizemtime)
{
  eos_static_info("path=%s, uid=%i, gid=%i inode=%lu",
                  path, (int) uid, (int) gid, inode);
  eos::common::Timing stattiming("stat");
  off_t file_size = -1;
  struct timespec _tim[2];
  struct timespec atim, &mtim = _tim[0];
  atim.tv_sec = atim.tv_nsec = mtim.tv_sec = mtim.tv_nsec = 0;
  errno = 0;
  COMMONTIMING("START", &stattiming);

  if (onlysizemtime && !inode) {
    return -1;
  }

  if (inode) {
    // Try to stat via an open file - first find the file descriptor using the
    // inodeuser2fd map and then find the file object using the fd2fabst map.
    // Meanwhile keep the mutex locked for read so that no other thread can
    // delete the file object
    eos_static_debug("path=%s, uid=%lu, inode=%lu",
                     path, (unsigned long) uid, inode);
    rwmutex_fd2fabst.LockRead();
    std::ostringstream sstr;
    sstr << inode << ":" << get_login(uid, gid, pid);
    google::dense_hash_map<std::string, std::set<int> >::iterator
    iter_fd = inodexrdlogin2fds.find(sstr.str());

    if (iter_fd != inodexrdlogin2fds.end()) {
      google::dense_hash_map<int, std::shared_ptr<FileAbstraction> >::iterator
      iter_file = fd2fabst.find(*iter_fd->second.begin());
      int fd = *iter_fd->second.begin();

      if (iter_file != fd2fabst.end()) {
        std::shared_ptr<FileAbstraction> fabst = iter_file->second;

        if (fabst != nullptr) {
          off_t cache_size = 0;
          struct stat tmp;
          bool isrw = true;

          if (XFC && fuse_cache_write) {
            cache_size = fabst->GetMaxWriteOffset();
            eos_static_debug("path=%s ino=%llu cache size %lu fabst=%p\n",
                             path ? path : "-undef-", inode, cache_size, fabst.get());
          }

          // try to stat wih RO file if opened
          LayoutWrapper* file = fabst->GetRawFileRW();

          if (!file) {
            file = fabst->GetRawFileRO();
            isrw = false;
          }

          rwmutex_fd2fabst.UnLockRead();

          // if we do lazy open, the file should be open on the fst to stat
          // otherwise, the file will be opened on the fst, just for a stat
          if (isrw) {
            // only stat via open files if we don't have cache capabilities
            if (!file->CanCache()) {
              if ((!file->Stat(&tmp))) {
                file_size = tmp.st_size;
                mtim.tv_sec = tmp.st_mtime;
                atim.tv_sec = tmp.st_atime;

                if (tmp.st_dev & 0x80000000) {
                  // this server delivers ns resolution in st_dev
                  mtim.tv_nsec = tmp.st_dev & 0x7fffffff;
                }

                if (cache_size > file_size) {
                  file_size = cache_size;
                }

                fabst->GetUtimes(&mtim);
                eos_static_debug("fd=%i, size-fd=%lld, mtim=%llu/%llu raw_file=%p", fd,
                                 file_size, tmp.MTIMESPEC.tv_sec, tmp.ATIMESPEC.tv_sec, file);
              } else {
                eos_static_err("fd=%i stat failed on open file", fd);
              }
            } else {
              file_size = cache_size;
              fabst->GetUtimes(&mtim);
            }
          } else {
            if (file->CanCache()) {
              // we can use the cache value here
              file_size = cache_size;
            }
          }
        } else {
          rwmutex_fd2fabst.UnLockRead();
          eos_static_err("fd=%i pointing to a null file abst obj",
                         *iter_fd->second.begin());
        }
      } else {
        rwmutex_fd2fabst.UnLockRead();
        eos_static_err("fd=%i not found in file obj map", *iter_fd->second.begin());
      }
    } else {
      rwmutex_fd2fabst.UnLockRead();
      eos_static_debug("path=%s not open", path);
    }

    if (onlysizemtime) {
      if (file_size == -1) {
        eos_static_debug("onlysizetime couldn't get the size from an open file");
        return -1;
      }

      buf->st_size = file_size;
      buf->MTIMESPEC = mtim;
      buf->st_mtime = mtim.tv_sec;
      eos_static_debug("onlysizetime size from open file");
      return 0;
    }
  }

  // Do stat using the Fils System object
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = safePath(path);
  request += "?";
  request += "mgm.pcmd=stat&eos.app=fuse";

  if (encode_pathname) {
    request += "&eos.encodepath=1";
  }

  arg.FromString(request);
  std::string surl = user_url(uid, gid, pid);
  std::string auth = strongauth_cgi(uid, gid, pid);

  if (!auth.empty()) {
    surl += "?" + auth;
  }

  eos_static_debug("stat url is %s", surl.c_str());
  XrdCl::URL Url(surl.c_str());
  XrdCl::FileSystem fs(Url);
  eos_static_debug("arg = %s", arg.ToString().c_str());
  COMMONTIMING("GETPLUGIN", &stattiming);
  XrdCl::XRootDStatus status = xrdreq_retryonnullbuf(fs, arg, response);

  if (status.IsOK() && response) {
    unsigned long long sval[10];
    unsigned long long ival[6];
    char tag[1024];
    tag[0] = 0;
    // Parse output
    int items = sscanf(response->ToString().c_str(),
                       "%1023s %llu %llu %llu %llu %llu %llu %llu %llu "
                       "%llu %llu %llu %llu %llu %llu %llu %llu",
                       tag, (unsigned long long*) &sval[0],
                       (unsigned long long*) &sval[1],
                       (unsigned long long*) &sval[2],
                       (unsigned long long*) &sval[3],
                       (unsigned long long*) &sval[4],
                       (unsigned long long*) &sval[5],
                       (unsigned long long*) &sval[6],
                       (unsigned long long*) &sval[7],
                       (unsigned long long*) &sval[8],
                       (unsigned long long*) &sval[9],
                       (unsigned long long*) &ival[0],
                       (unsigned long long*) &ival[1],
                       (unsigned long long*) &ival[2],
                       (unsigned long long*) &ival[3],
                       (unsigned long long*) &ival[4],
                       (unsigned long long*) &ival[5]);

    if ((items != 17) || (strcmp(tag, "stat:"))) {
      int retc = 0;
      items = sscanf(response->ToString().c_str(), "%1023s retc=%i", tag, &retc);

      if ((!strcmp(tag, "stat:")) && (items == 2)) {
        errno = retc;
      } else {
        errno = EFAULT;
      }

      eos_static_info("path=%s errno=%i tag=%s", path, errno, tag);
      delete response;
      return errno;
    } else {
      buf->st_dev = (dev_t) sval[0];
      buf->st_ino = (ino_t) sval[1];
      buf->st_mode = (mode_t) sval[2];

      if (S_ISREG(buf->st_mode) || S_ISLNK(buf->st_mode)) {
        buf->st_nlink = 1;
      } else {
        buf->st_nlink = (nlink_t) sval[3];
      }

      buf->st_uid = (uid_t) sval[4];
      buf->st_gid = (gid_t) sval[5];
      buf->st_rdev = (dev_t) sval[6];
      buf->st_size = (off_t) sval[7];
      buf->st_blksize = (blksize_t) sval[8];
      buf->st_blocks = (blkcnt_t) sval[9];
      buf->st_atime = (time_t) ival[0];
      buf->st_mtime = (time_t) ival[1];
      buf->st_ctime = (time_t) ival[2];
      buf->ATIMESPEC.tv_sec = (time_t) ival[0];
      buf->MTIMESPEC.tv_sec = (time_t) ival[1];
      buf->CTIMESPEC.tv_sec = (time_t) ival[2];
      buf->ATIMESPEC.tv_nsec = (time_t) ival[3];
      buf->MTIMESPEC.tv_nsec = (time_t) ival[4];
      buf->CTIMESPEC.tv_nsec = (time_t) ival[5];

      if (S_ISREG(buf->st_mode) && fuse_exec) {
        buf->st_mode |= (S_IXUSR | S_IXGRP | S_IXOTH);
      }

      buf->st_mode &= (~S_ISVTX); // clear the vxt bit
      buf->st_mode &= (~S_ISUID); // clear suid
      buf->st_mode &= (~S_ISGID); // clear sgid
      errno = 0;
    }
  }

  if (file_size == (off_t) - 1) {
    eos_static_debug("querying the cache for inode=%x", inode);
    // retrieve size from our local auth cache
    long long csize = 0;

    if ((csize = LayoutWrapper::CacheAuthSize(inode)) > 0) {
      file_size = csize;
    }

    eos_static_debug("local cache size=%lld", csize);
  }

  // eventually configure an overlay mode to enable bits by default
  buf->st_mode |= mode_overlay;

  if (file_size != -1) {
    buf->st_size = file_size;

    // If got size using the opened file then return size and mtime from the opened file
    if (mtim.tv_sec) {
      buf->MTIMESPEC = mtim;
      buf->ATIMESPEC = mtim;
      buf->st_atime = buf->ATIMESPEC.tv_sec;
      buf->st_mtime = buf->ATIMESPEC.tv_sec;
    }
  }

  COMMONTIMING("END", &stattiming);

  if (EOS_LOGS_DEBUG) {
    stattiming.Print();
  }

  eos_static_info("path=%s st-ino =%llu st-size=%llu st-mtim.tv_sec=%llu "
                  "st-mtim.tv_nsec=%llu errno=%i", path, buf->st_ino,
                  buf->st_size, buf->MTIMESPEC.tv_sec, buf->MTIMESPEC.tv_nsec,
                  errno);
  delete response;
  return errno;
}


//------------------------------------------------------------------------------
// Return statistics about the filesystem
//------------------------------------------------------------------------------
int
fuse_filesystem::statfs(const char* path, struct statvfs* stbuf, uid_t uid,
                        gid_t gid, pid_t pid)
{
  eos_static_info("path=%s", path);
  static unsigned long long a1 = 0;
  static unsigned long long a2 = 0;
  static unsigned long long a3 = 0;
  static unsigned long long a4 = 0;
  static XrdSysMutex statmutex;
  static time_t laststat = 0;
  statmutex.Lock();
  errno = 0;

  if ((time(NULL) - laststat) < ((15 + (int) 5.0 * rand() / RAND_MAX))) {
    stbuf->f_bsize = 4096;
    stbuf->f_frsize = 4096;
    stbuf->f_blocks = a3 / 4096;
    stbuf->f_bfree = a1 / 4096;
    stbuf->f_bavail = a1 / 4096;
    stbuf->f_files = a4;
    stbuf->f_ffree = a2;
    stbuf->f_fsid = 0xcafe;
    stbuf->f_namemax = 1024;
    statmutex.UnLock();
    return errno;
  }

  eos::common::Timing statfstiming("statfs");
  COMMONTIMING("START", &statfstiming);
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = safePath(path);
  request += "?";
  request += "mgm.pcmd=statvfs&eos.app=fuse&";

  if (encode_pathname) {
    request += "eos.encodepath=1&";
  }

  request += "path=";
  request += safePath(path);
  arg.FromString(request);
  std::string surl = user_url(uid, gid, pid);
  std::string auth = strongauth_cgi(uid, gid, pid);

  if (!auth.empty()) {
    surl += "?" + auth;
  }

  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = xrdreq_retryonnullbuf(fs, arg, response);

  if (status.IsOK() && response && response->GetBuffer()) {
    int retc;
    char tag[1024];

    if (!response->GetBuffer()) {
      statmutex.UnLock();
      errno = EFAULT;
      delete response;
      return errno;
    }

    // Parse output
    int items = sscanf(response->GetBuffer(),
                       "%1023s retc=%d f_avail_bytes=%llu f_avail_files=%llu "
                       "f_max_bytes=%llu f_max_files=%llu",
                       tag, &retc, &a1, &a2, &a3, &a4);

    if ((items != 6) || (strcmp(tag, "statvfs:"))) {
      statmutex.UnLock();
      errno = EFAULT;
      delete response;
      return errno;
    }

    errno = retc;
    laststat = time(NULL);
    statmutex.UnLock();
    stbuf->f_bsize = 4096;
    stbuf->f_frsize = 4096;
    stbuf->f_blocks = a3 / 4096;
    stbuf->f_bfree = a1 / 4096;
    stbuf->f_bavail = a1 / 4096;
    stbuf->f_files = a4;
    stbuf->f_ffree = a2;
    stbuf->f_namemax = 1024;
  } else {
    statmutex.UnLock();
    errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;
  }

  COMMONTIMING("END", &statfstiming);

  if (EOS_LOGS_DEBUG) {
    statfstiming.Print();
  }

  delete response;
  return errno;
}

//------------------------------------------------------------------------------
// Change permissions for the file
//------------------------------------------------------------------------------
int
fuse_filesystem::chmod(const char* path,
                       mode_t mode,
                       uid_t uid,
                       gid_t gid,
                       pid_t pid)
{
  eos_static_info("path=%s mode=%x uid=%u pid=%u", path, mode, uid, pid);
  eos::common::Timing chmodtiming("chmod");
  COMMONTIMING("START", &chmodtiming);
  int retc = 0;
  XrdOucString smode;
  smode += (int) mode & 0xfff; // mask sticky, vertex and gid bit
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = safePath(path);
  request += "?";
  request += "mgm.pcmd=chmod&eos.app=fuse&mode=";
  request += smode.c_str();

  if (encode_pathname) {
    request += "&eos.encodepath=1";
  }

  arg.FromString(request);
  std::string surl = user_url(uid, gid, pid);
  std::string auth = strongauth_cgi(uid, gid, pid);

  if (!auth.empty()) {
    surl += "?" + auth;
  }

  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = xrdreq_retryonnullbuf(fs, arg, response);
  COMMONTIMING("END", &chmodtiming);
  errno = 0;

  if (EOS_LOGS_DEBUG) {
    chmodtiming.Print();
  }

  if (status.IsOK()) {
    char tag[1024];

    if (!response->GetBuffer()) {
      errno = EFAULT;
      delete response;
      return errno;
    }

    // Parse output
    int items = sscanf(response->GetBuffer(), "%1023s retc=%d", tag, &retc);

    if ((items != 2) || (strcmp(tag, "chmod:"))) {
      errno = EFAULT;
    } else {
      errno = retc;
    }
  } else {
    eos_static_err("status is NOT ok : %s", status.ToString().c_str());
    errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;

    if (status.code == XrdCl::errErrorResponse) {
      eos::common::error_retc_map(status.errNo);
    }
  }

  delete response;
  return errno;
}

//------------------------------------------------------------------------------
// Postpone utimes to a file close if still open
//------------------------------------------------------------------------------
int
fuse_filesystem::utimes_if_open(unsigned long long inode,
                                struct timespec* utimes,
                                uid_t uid, gid_t gid, pid_t pid)
{
  rwmutex_fd2fabst.LockRead();
  std::ostringstream sstr;
  sstr << inode << ":" << get_login(uid, gid, pid);
  google::dense_hash_map<std::string, std::set<int> >::iterator
  iter_fd = inodexrdlogin2fds.find(sstr.str());

  if (iter_fd != inodexrdlogin2fds.end()) {
    google::dense_hash_map<int, std::shared_ptr<FileAbstraction> >::iterator
    iter_file = fd2fabst.find(*iter_fd->second.begin());

    if (iter_file != fd2fabst.end()) {
      std::shared_ptr<FileAbstraction> fabst = iter_file->second;
      rwmutex_fd2fabst.UnLockRead();
      fabst->SetUtimes(utimes);
      eos_static_info("ino=%ld mtime=%ld mtime.nsec=%ld", inode, utimes[1].tv_sec,
                      utimes[1].tv_nsec);
      return 0;
    }
  }

  rwmutex_fd2fabst.UnLockRead();
  return -1;
}

//------------------------------------------------------------------------------
// Update the last access time and last modification time
//------------------------------------------------------------------------------
int
fuse_filesystem::utimes(const char* path,
                        struct timespec* tvp,
                        uid_t uid,
                        gid_t gid,
                        pid_t pid)
{
  eos_static_info("path=%s uid=%u pid=%u", path, uid, pid);
  eos::common::Timing utimestiming("utimes");
  COMMONTIMING("START", &utimestiming);
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = safePath(path);
  request += "?";
  request += "mgm.pcmd=utimes&eos.app=fuse&tv1_sec=";
  char lltime[1024];
  sprintf(lltime, "%llu", (unsigned long long) tvp[0].tv_sec);
  request += lltime;
  request += "&tv1_nsec=";
  sprintf(lltime, "%llu", (unsigned long long) tvp[0].tv_nsec);
  request += lltime;
  request += "&tv2_sec=";
  sprintf(lltime, "%llu", (unsigned long long) tvp[1].tv_sec);
  request += lltime;
  request += "&tv2_nsec=";
  sprintf(lltime, "%llu", (unsigned long long) tvp[1].tv_nsec);
  request += lltime;

  if (encode_pathname) {
    request += "&eos.encodepath=1";
  }

  eos_static_debug("request: %s", request.c_str());
  arg.FromString(request);
  std::string surl = user_url(uid, gid, pid);
  std::string auth = strongauth_cgi(uid, gid, pid);

  if (!auth.empty()) {
    surl += "?" + auth;
  }

  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = xrdreq_retryonnullbuf(fs, arg, response);
  COMMONTIMING("END", &utimestiming);
  errno = 0;

  if (EOS_LOGS_DEBUG) {
    utimestiming.Print();
  }

  if (status.IsOK()) {
    int retc = 0;
    char tag[1024];
    // Parse output
    int items = sscanf(response->GetBuffer(), "%1023s retc=%d", tag, &retc);

    if ((items != 2) || (strcmp(tag, "utimes:"))) {
      errno = EFAULT;
    } else {
      errno = retc;
    }
  } else {
    eos_static_err("status is NOT ok : %s", status.ToString().c_str());
    errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;

    if (status.code == XrdCl::errErrorResponse) {
      eos::common::error_retc_map(status.errNo);
    }
  }

  delete response;
  return errno;
}

//----------------------------------------------------------------------------
// Symlink
//----------------------------------------------------------------------------
int
fuse_filesystem::symlink(const char* path, const char* link, uid_t uid,
                         gid_t gid,
                         pid_t pid)
{
  eos_static_info("path=%s link=%s uid=%u pid=%u", path, link, uid, pid);
  eos::common::Timing symlinktiming("symlink");
  COMMONTIMING("START", &symlinktiming);
  int retc = 0;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = safePath(path);
  request += "?";
  request += "mgm.pcmd=symlink&eos.app=fuse&target=";
  XrdOucString savelink = link;

  if (encode_pathname) {
    savelink = safePath(savelink.c_str()).c_str();
  } else {
    while (savelink.replace("&", "#AND#")) {
    }
  }

  request += savelink.c_str();

  if (encode_pathname) {
    request += "&eos.encodepath=1";
  }

  arg.FromString(request);
  std::string surl = user_url(uid, gid, pid);
  std::string auth = strongauth_cgi(uid, gid, pid);

  if (!auth.empty()) {
    surl += "?" + auth;
  }

  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = xrdreq_retryonnullbuf(fs, arg, response);
  COMMONTIMING("STOP", &symlinktiming);
  errno = 0;

  if (EOS_LOGS_DEBUG) {
    symlinktiming.Print();
  }

  if (status.IsOK()) {
    char tag[1024];
    // Parse output
    int items = sscanf(response->GetBuffer(), "%1023s retc=%d", tag, &retc);

    if (EOS_LOGS_DEBUG) {
      fprintf(stderr, "symlink-retc=%d\n", retc);
    }

    if ((items != 2) || (strcmp(tag, "symlink:"))) {
      errno = EFAULT;
    } else {
      errno = retc;
    }
  } else {
    eos_static_err("error=status is NOT ok : %s", status.ToString().c_str());
    errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;

    if (status.code == XrdCl::errErrorResponse) {
      eos::common::error_retc_map(status.errNo);
    }
  }

  delete response;
  return errno;
}

//----------------------------------------------------------------------------
// Readlink
//----------------------------------------------------------------------------

int
fuse_filesystem::readlink(const char* path, char* buf, size_t bufsize,
                          uid_t uid,
                          gid_t gid, pid_t pid)
{
  eos_static_info("path=%s uid=%u pid=%u", path, uid, pid);
  eos::common::Timing readlinktiming("readlink");
  COMMONTIMING("START", &readlinktiming);
  int retc = 0;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = safePath(path);
  request += "?";
  request += "mgm.pcmd=readlink&eos.app=fuse";

  if (encode_pathname) {
    request += "&eos.encodepath=1";
  }

  arg.FromString(request);
  std::string surl = user_url(uid, gid, pid);
  std::string auth = strongauth_cgi(uid, gid, pid);

  if (!auth.empty()) {
    surl += "?" + auth;
  }

  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = xrdreq_retryonnullbuf(fs, arg, response);
  COMMONTIMING("END", &readlinktiming);
  errno = 0;

  if (EOS_LOGS_DEBUG) {
    readlinktiming.Print();
  }

  if (status.IsOK()) {
    char tag[1024];

    if (!response->GetBuffer()) {
      errno = EFAULT;
      delete response;
      return errno;
    }

    // Parse output
    int items = sscanf(response->GetBuffer(), "%1023s retc=%d %*s", tag, &retc);

    if (EOS_LOGS_DEBUG) {
      fprintf(stderr, "readlink-retc=%d\n", retc);
    }

    if ((items != 2) || (strcmp(tag, "readlink:"))) {
      errno = EFAULT;
    } else {
      errno = retc;
    }

    if (!errno) {
      const char* rs = strchr(response->GetBuffer(), '=');

      if (rs) {
        const char* ss = strchr(rs, ' ');

        if (ss) {
          snprintf(buf, bufsize, "%s", ss + 1);

          if (encode_pathname) {
            strncpy(buf, eos::common::StringConversion::curl_unescaped(buf).c_str(),
                    bufsize);
          }
        } else {
          errno = EBADE;
        }
      } else {
        errno = EBADE;
      }
    }
  } else {
    eos_static_err("status is NOT ok : %s", status.ToString().c_str());
    errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;

    if (status.code == XrdCl::errErrorResponse) {
      eos::common::error_retc_map(status.errNo);
    }
  }

  delete response;
  return errno;
}

//------------------------------------------------------------------------------
// It returns -ENOENT if the path doesn't exist, -EACCESS if the requested
// permission isn't available, or 0 for success. Note that it can be called
// on files, directories, or any other object that appears in the filesystem.
//------------------------------------------------------------------------------

int
fuse_filesystem::access(const char* path,
                        int mode,
                        uid_t uid,
                        gid_t gid,
                        pid_t pid
                       )
{
  eos_static_info("path=%s mode=%d uid=%u pid=%u", path, mode, uid, pid);
  eos::common::Timing accesstiming("access");
  COMMONTIMING("START", &accesstiming);
  int retc = 0;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  char smode[16];
  snprintf(smode, sizeof(smode) - 1, "%d", mode);
  request = safePath(path);
  request += "?";
  request += "mgm.pcmd=access&eos.app=fuse&mode=";
  request += smode;

  if (encode_pathname) {
    request += "&eos.encodepath=1";
  }

  arg.FromString(request);
  std::string surl = user_url(uid, gid, pid);
  std::string auth = strongauth_cgi(uid, gid, pid);

  if (!auth.empty()) {
    surl += "?" + auth;
  }

  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = xrdreq_retryonnullbuf(fs, arg, response);
  COMMONTIMING("STOP", &accesstiming);
  errno = 0;

  if (EOS_LOGS_DEBUG) {
    accesstiming.Print();
  }

  if (status.IsOK()) {
    char tag[1024];
    // Parse output
    int items = sscanf(response->GetBuffer(), "%1023s retc=%d", tag, &retc);

    if (EOS_LOGS_DEBUG) {
      fprintf(stderr, "access-retc=%d\n", retc);
    }

    if ((items != 2) || (strcmp(tag, "access:"))) {
      errno = EFAULT;
    } else {
      errno = retc;
    }
  } else {
    eos_static_err("status is NOT ok : %s", status.ToString().c_str());
    errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;

    if (status.code == XrdCl::errErrorResponse) {
      eos::common::error_retc_map(status.errNo);
    }
  }

  delete response;
  return errno;
}


//------------------------------------------------------------------------------
// Get list of entries in directory
//------------------------------------------------------------------------------

int
fuse_filesystem::inodirlist(unsigned long long dirinode,
                            const char* path,
                            uid_t uid,
                            gid_t gid,
                            pid_t pid,
                            dirlist& dlist,
                            struct fuse_entry_param** stats,
                            size_t* nstats)
{
  eos_static_info("inode=%llu path=%s", dirinode, path);
  eos::common::Timing inodirtiming("inodirlist");
  COMMONTIMING("START", &inodirtiming);
  int retc = 0;
  char* ptr = 0;
  char* value = 0;
  int doinodirlist = -1;
  std::string request = path;
  size_t a_pos = request.find("mgm.path=/");

  // we have to replace '&' in path names with '#AND#'
  while ((a_pos = request.find("&", a_pos + 1)) != std::string::npos) {
    request.erase(a_pos, 1);
    request.insert(a_pos, "#AND#");
    a_pos += 4;
  }

  // add the kerberos token
  std::string auth = strongauth_cgi(uid, gid, pid);

  if (!auth.empty()) {
    request += "&" + auth;
  }

  COMMONTIMING("GETSTSTREAM", &inodirtiming);
  request.insert(0, user_url(uid, gid, pid));
  std::unique_ptr<XrdCl::File> file {new XrdCl::File()};
  XrdCl::XRootDStatus status = file->Open(request.c_str(),
                                          XrdCl::OpenFlags::Flags::Read);
  errno = 0;

  if (!status.IsOK()) {
    eos_static_err("got an error to request.");
    eos_static_err("error=status is NOT ok : %s", status.ToString().c_str());
    errno = ((status.code == XrdCl::errAuthFailed) ? EPERM : EFAULT);
    return errno;
  }

  // Start to read
  int npages = 1;
  off_t offset = 0;
  unsigned int nbytes = 0;
  value = (char*) malloc(PAGESIZE + 1);
  COMMONTIMING("READSTSTREAM", &inodirtiming);
  status = file->Read(offset, PAGESIZE, value + offset, nbytes);

  while ((status.IsOK()) && (nbytes == PAGESIZE)) {
    npages++;
    char* new_value = (char*) realloc(value, npages * PAGESIZE + 1);

    if (new_value == nullptr) {
      free(value);
      errno = ENOMEM;
      return errno;
    }

    value = new_value;
    offset += PAGESIZE;
    status = file->Read(offset, PAGESIZE, value + offset, nbytes);
  }

  if (status.IsOK()) {
    offset += nbytes;
  }

  value[offset] = 0;
  //eos_static_info("request reply is %s",value);
  COMMONTIMING("PARSESTSTREAM", &inodirtiming);
  std::vector<struct stat> statvec;

  if (status.IsOK()) {
    char tag[128];
    // Parse output
    int items = sscanf(value, "%127s retc=%d", tag, &retc);
    bool encodepath = false;

    if (retc) {
      free(value);
      errno = EFAULT;
      return errno;
    }

    if ((items != 2) || ((strcmp(tag, "inodirlist:")) &&
                         (strcmp(tag, "inodirlist_pathencode:")))) {
      eos_static_err("got an error(1).");
      free(value);
      errno = EFAULT;
      return errno;
    }

    if (!strcmp(tag, "inodirlist_pathencode:")) {
      encodepath = true;
    }

    ptr = strchr(value, ' ');

    if (ptr) {
      ptr = strchr(ptr + 1, ' ');
    }

    char* endptr = value + strlen(value) - 1;
    COMMONTIMING("PARSESTSTREAM1", &inodirtiming);
    bool parseerror = true;

    while ((ptr) && (ptr < endptr)) {
      parseerror = true;
      bool hasstat = false;
      // parse the entry name
      char* dirpathptr = ptr;

      while (dirpathptr < endptr && *dirpathptr == ' ') {
        dirpathptr++;
      }

      ptr = dirpathptr;

      if (ptr >= endptr) {
        break;
      }

      // go next field and set null character
      ptr = strchr(ptr + 1, ' ');

      if (ptr == 0 || ptr >= endptr) {
        break;
      }

      *ptr = 0;
      char* inodeptr = ptr + 1;

      // parse the inode
      while (inodeptr < endptr && *inodeptr == ' ') {
        inodeptr++;
      }

      ptr = inodeptr;

      if (ptr >= endptr) {
        break;
      }

      // go next field and set null character
      ptr = strchr(ptr + 1, ' ');

      if (!(ptr == 0 || ptr >= endptr)) {
        hasstat = true;
        *ptr = 0;
      }

      parseerror = false;
      char* statptr = NULL;

      if (hasstat) {
        // parse the stat
        statptr = ptr + 1;

        while (statptr < endptr && *statptr == ' ') {
          statptr++;
        }

        ptr = statptr;
        hasstat = (ptr < endptr); // we have a third token

        // check if there is actually a stat
        if (hasstat) {
          hasstat = (*statptr == '{'); // check if then token is a stat information

          if (!hasstat) {
            ptr = statptr;
          } else {
            ptr = strchr(ptr + 1, ' ');

            if (ptr < endptr) {
              *ptr = 0;
            }
          }
        }

        if (hasstat) {
          ptr++;
        }
      }

      // process the entry
      XrdOucString whitespacedirpath = dirpathptr;

      if (encode_pathname && encodepath) {
        whitespacedirpath = eos::common::StringConversion::curl_unescaped(
                              whitespacedirpath.c_str()).c_str();
      } else {
        whitespacedirpath.replace("%20", " ");
        whitespacedirpath.replace("%0A", "\n");
      }

      ino_t inode = strtouq(inodeptr, 0, 10);
      struct stat buf;

      if (stats) {
        if (hasstat) {
          char* statptr2;
          statptr++; // skip '{'

          for (statptr2 = statptr; *statptr2 && *statptr2 != ',' &&
               *statptr2 != '}'; statptr2++);

          eos::common::StringConversion::FastAsciiHexToUnsigned(statptr,
              &buf.ATIMESPEC.tv_nsec, statptr2 - statptr);
          statptr = statptr2 + 1; // skip ','

          for (statptr2 = statptr; *statptr2 && *statptr2 != ',' &&
               *statptr2 != '}'; statptr2++);

          eos::common::StringConversion::FastAsciiHexToUnsigned(statptr,
              &buf.ATIMESPEC.tv_sec, statptr2 - statptr);
          statptr = statptr2 + 1; // skip ','

          for (statptr2 = statptr; *statptr2 && *statptr2 != ',' &&
               *statptr2 != '}'; statptr2++);

          eos::common::StringConversion::FastAsciiHexToUnsigned(statptr, &buf.st_blksize,
              statptr2 - statptr);
          statptr = statptr2 + 1; // skip ','

          for (statptr2 = statptr; *statptr2 && *statptr2 != ',' &&
               *statptr2 != '}'; statptr2++);

          eos::common::StringConversion::FastAsciiHexToUnsigned(statptr, &buf.st_blocks,
              statptr2 - statptr);
          statptr = statptr2 + 1; // skip ','

          for (statptr2 = statptr; *statptr2 && *statptr2 != ',' &&
               *statptr2 != '}'; statptr2++);

          eos::common::StringConversion::FastAsciiHexToUnsigned(statptr,
              &buf.CTIMESPEC.tv_nsec, statptr2 - statptr);
          statptr = statptr2 + 1; // skip ','

          for (statptr2 = statptr; *statptr2 && *statptr2 != ',' &&
               *statptr2 != '}'; statptr2++);

          eos::common::StringConversion::FastAsciiHexToUnsigned(statptr,
              &buf.CTIMESPEC.tv_sec, statptr2 - statptr);
          statptr = statptr2 + 1; // skip ','

          for (statptr2 = statptr; *statptr2 && *statptr2 != ',' &&
               *statptr2 != '}'; statptr2++);

          eos::common::StringConversion::FastAsciiHexToUnsigned(statptr, &buf.st_dev,
              statptr2 - statptr);
          statptr = statptr2 + 1; // skip ','

          for (statptr2 = statptr; *statptr2 && *statptr2 != ',' &&
               *statptr2 != '}'; statptr2++);

          eos::common::StringConversion::FastAsciiHexToUnsigned(statptr, &buf.st_gid,
              statptr2 - statptr);
          statptr = statptr2 + 1; // skip ','

          for (statptr2 = statptr; *statptr2 && *statptr2 != ',' &&
               *statptr2 != '}'; statptr2++);

          eos::common::StringConversion::FastAsciiHexToUnsigned(statptr, &buf.st_ino,
              statptr2 - statptr);
          statptr = statptr2 + 1; // skip ','

          for (statptr2 = statptr; *statptr2 && *statptr2 != ',' &&
               *statptr2 != '}'; statptr2++);

          eos::common::StringConversion::FastAsciiHexToUnsigned(statptr, &buf.st_mode,
              statptr2 - statptr);
          statptr = statptr2 + 1; // skip ','

          for (statptr2 = statptr; *statptr2 && *statptr2 != ',' &&
               *statptr2 != '}'; statptr2++);

          eos::common::StringConversion::FastAsciiHexToUnsigned(statptr,
              &buf.MTIMESPEC.tv_nsec, statptr2 - statptr);
          statptr = statptr2 + 1; // skip ','

          for (statptr2 = statptr; *statptr2 && *statptr2 != ',' &&
               *statptr2 != '}'; statptr2++);

          eos::common::StringConversion::FastAsciiHexToUnsigned(statptr,
              &buf.MTIMESPEC.tv_sec, statptr2 - statptr);
          statptr = statptr2 + 1; // skip ','

          for (statptr2 = statptr; *statptr2 && *statptr2 != ',' &&
               *statptr2 != '}'; statptr2++);

          eos::common::StringConversion::FastAsciiHexToUnsigned(statptr, &buf.st_nlink,
              statptr2 - statptr);
          statptr = statptr2 + 1; // skip ','

          for (statptr2 = statptr; *statptr2 && *statptr2 != ',' &&
               *statptr2 != '}'; statptr2++);

          eos::common::StringConversion::FastAsciiHexToUnsigned(statptr, &buf.st_rdev,
              statptr2 - statptr);
          statptr = statptr2 + 1; // skip ','

          for (statptr2 = statptr; *statptr2 && *statptr2 != ',' &&
               *statptr2 != '}'; statptr2++);

          eos::common::StringConversion::FastAsciiHexToUnsigned(statptr, &buf.st_size,
              statptr2 - statptr);
          statptr = statptr2 + 1; // skip ','

          for (statptr2 = statptr; *statptr2 && *statptr2 != ',' &&
               *statptr2 != '}'; statptr2++);

          eos::common::StringConversion::FastAsciiHexToUnsigned(statptr, &buf.st_uid,
              statptr2 - statptr);

          if (S_ISREG(buf.st_mode) && fuse_exec) {
            buf.st_mode |= (S_IXUSR | S_IXGRP | S_IXOTH);
          }

          if (S_ISREG(buf.st_mode) || S_ISLNK(buf.st_mode)) {
            buf.st_nlink = 1;
          }

          buf.st_mode &= (~S_ISVTX); // clear the vxt bit
          buf.st_mode &= (~S_ISUID); // clear suid
          buf.st_mode &= (~S_ISGID); // clear sgid
          buf.st_mode |= mode_overlay;
        } else {
          buf.st_ino = 0;
        }

        statvec.push_back(buf);
      }

      if (!encode_pathname && !checkpathname(whitespacedirpath.c_str())) {
        eos_static_err("unsupported name %s : not stored in the FsCache",
                       whitespacedirpath.c_str());
      } else {
        bool show_entry = true;

        if (hide_special_files &&
            (whitespacedirpath.beginswith(EOS_COMMON_PATH_VERSION_FILE_PREFIX) ||
             whitespacedirpath.beginswith(EOS_COMMON_PATH_ATOMIC_FILE_PREFIX) ||
             whitespacedirpath.beginswith(EOS_COMMON_PATH_BACKUP_FILE_PREFIX))) {
          show_entry = false;
        }

        if (show_entry) {
          store_child_p2i(dirinode, inode, whitespacedirpath.c_str());
          dlist.push_back(inode);
        }
      }
    }

    if (parseerror) {
      eos_static_err("got an error(2).");
      free(value);
      errno = EFAULT;
      return errno;
    }

    doinodirlist = 0;
  }

  COMMONTIMING("PARSESTSTREAM2", &inodirtiming);

  if (stats) {
    *stats = (struct fuse_entry_param*) malloc(sizeof(struct fuse_entry_param) *
             statvec.size());
    *nstats = statvec.size();

    for (auto i = 0; i < (int) statvec.size(); i++) {
      struct fuse_entry_param& e = (*stats)[i];
      memset(&e, 0, sizeof(struct fuse_entry_param));
      e.attr = statvec[i];
      e.attr_timeout = 0;
      e.entry_timeout = 0;
      e.ino = e.attr.st_ino;
    }
  }

  COMMONTIMING("END", &inodirtiming);
  free(value);
  return doinodirlist;
}

//------------------------------------------------------------------------------
// Get directory entries
//------------------------------------------------------------------------------
struct dirent*
fuse_filesystem::readdir(const char* path_dir, size_t* size,
                         uid_t uid,
                         gid_t gid,
                         pid_t pid)
{
  eos_static_info("path=%s", path_dir);
  struct dirent* dirs = NULL;
  XrdCl::DirectoryList* response = 0;
  XrdCl::DirListFlags::Flags flags = XrdCl::DirListFlags::None;
  string path_str = safePath(path_dir);

  if (encode_pathname) {
    path_str += "?eos.encodepath=1";
  }

  std::string surl = user_url(uid, gid, pid);
  std::string auth = strongauth_cgi(uid, gid, pid);

  if (!auth.empty()) {
    surl += "?" + auth;
  }

  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = fs.DirList(path_str, flags, response);

  if (status.IsOK()) {
    *size = response->GetSize();
    dirs = static_cast<struct dirent*>(calloc(*size, sizeof(struct dirent)));
    int i = 0;

    for (XrdCl::DirectoryList::ConstIterator iter = response->Begin();
         iter != response->End();
         ++iter) {
      XrdCl::DirectoryList::ListEntry* list_entry =
        static_cast<XrdCl::DirectoryList::ListEntry*>(*iter);
      size_t len = list_entry->GetName().length();
      const char* cp = list_entry->GetName().c_str();
      const int dirhdrln = dirs[i].d_name - (char*) &dirs[i];
#ifdef __APPLE__
      dirs[i].d_fileno = i;
      dirs[i].d_type = DT_UNKNOWN;
      dirs[i].d_namlen = len;
#else
      dirs[i].d_ino = i;
      dirs[i].d_off = i * NAME_MAX;
#endif
      dirs[i].d_reclen = len + dirhdrln;
      dirs[i].d_type = DT_UNKNOWN;
      strncpy(dirs[i].d_name, cp, len);
      dirs[i].d_name[len] = '\0';
      i++;
    }

    delete response;
    return dirs;
  }

  *size = 0;
  delete response;
  return NULL;
}


//------------------------------------------------------------------------------
// Create a directory with the given name
//------------------------------------------------------------------------------
int
fuse_filesystem::mkdir(const char* path,
                       mode_t mode,
                       uid_t uid,
                       gid_t gid,
                       pid_t pid,
                       struct stat* buf)
{
  eos_static_info("path=%s mode=%d uid=%u pid=%u", path, mode, uid, pid);
  eos::common::Timing mkdirtiming("mkdir");
  errno = 0;
  COMMONTIMING("START", &mkdirtiming);
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = safePath(path);
  request += '?';
  request += "mgm.pcmd=mkdir";
  request += "&eos.app=fuse&mode=";
  request += (int) mode;

  if (encode_pathname) {
    request += "&eos.encodepath=1";
  }

  arg.FromString(request);
  std::string surl = user_url(uid, gid, pid);
  std::string auth = strongauth_cgi(uid, gid, pid);

  if (!auth.empty()) {
    surl += "?" + auth;
  }

  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = xrdreq_retryonnullbuf(fs, arg, response);
  COMMONTIMING("GETPLUGIN", &mkdirtiming);

  if (status.IsOK()) {
    unsigned long long sval[10];
    unsigned long long ival[6];
    char tag[1024];
    // Parse output
    int items = sscanf(response->GetBuffer(),
                       "%1023s %llu %llu %llu %llu %llu %llu %llu %llu "
                       "%llu %llu %llu %llu %llu %llu %llu %llu",
                       tag, (unsigned long long*) &sval[0],
                       (unsigned long long*) &sval[1],
                       (unsigned long long*) &sval[2],
                       (unsigned long long*) &sval[3],
                       (unsigned long long*) &sval[4],
                       (unsigned long long*) &sval[5],
                       (unsigned long long*) &sval[6],
                       (unsigned long long*) &sval[7],
                       (unsigned long long*) &sval[8],
                       (unsigned long long*) &sval[9],
                       (unsigned long long*) &ival[0],
                       (unsigned long long*) &ival[1],
                       (unsigned long long*) &ival[2],
                       (unsigned long long*) &ival[3],
                       (unsigned long long*) &ival[4],
                       (unsigned long long*) &ival[5]);

    if ((items != 17) || (strcmp(tag, "mkdir:"))) {
      int retc = 0;
      char tag[1024];
      // Parse output
      int items = sscanf(response->GetBuffer(), "%1023s retc=%d", tag, &retc);

      if ((items != 2) || (strcmp(tag, "mkdir:"))) {
        errno = EFAULT;
      } else {
        errno = retc;
      }

      delete response;
      return errno;
    } else {
      buf->st_dev = (dev_t) sval[0];
      buf->st_ino = (ino_t) sval[1];
      buf->st_mode = (mode_t) sval[2];
      buf->st_nlink = (nlink_t) sval[3];
      buf->st_uid = (uid_t) sval[4];
      buf->st_gid = (gid_t) sval[5];
      buf->st_rdev = (dev_t) sval[6];
      buf->st_size = (off_t) sval[7];
      buf->st_blksize = (blksize_t) sval[8];
      buf->st_blocks = (blkcnt_t) sval[9];
      buf->st_atime = (time_t) ival[0];
      buf->st_mtime = (time_t) ival[1];
      buf->st_ctime = (time_t) ival[2];
      buf->ATIMESPEC.tv_sec = (time_t) ival[0];
      buf->MTIMESPEC.tv_sec = (time_t) ival[1];
      buf->CTIMESPEC.tv_sec = (time_t) ival[2];
      buf->ATIMESPEC.tv_nsec = (time_t) ival[3];
      buf->MTIMESPEC.tv_nsec = (time_t) ival[4];
      buf->CTIMESPEC.tv_nsec = (time_t) ival[5];

      if (S_ISREG(buf->st_mode) && fuse_exec) {
        buf->st_mode |= (S_IXUSR | S_IXGRP | S_IXOTH);
      }

      buf->st_mode &= (~S_ISVTX); // clear the vxt bit
      buf->st_mode &= (~S_ISUID); // clear suid
      buf->st_mode &= (~S_ISGID); // clear sgid
      buf->st_mode |= mode_overlay;
      errno = 0;
    }
  } else {
    eos_static_err("status is NOT ok");
    errno = EFAULT;
  }

  COMMONTIMING("END", &mkdirtiming);

  if (EOS_LOGS_DEBUG) {
    mkdirtiming.Print();
  }

  eos_static_debug("path=%s inode=%llu", path, buf->st_ino);
  delete response;
  return errno;
}

//------------------------------------------------------------------------------
// Remove the given directory
//------------------------------------------------------------------------------
int
fuse_filesystem::rmdir(const char* path, uid_t uid, gid_t gid, pid_t pid)
{
  eos::common::Timing rmdirtiming("rmdir");
  COMMONTIMING("START", &rmdirtiming);
  eos_static_info("path=%s uid=%u pid=%u", path, uid, pid);
  std::string surl = user_url(uid, gid, pid);
  std::string auth = strongauth_cgi(uid, gid, pid);

  if (!auth.empty()) {
    surl += "?" + auth;
  }

  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  std::string spath = safePath(path);

  if (encode_pathname) {
    spath += "?eos.encodepath=1";
  }

  XrdCl::XRootDStatus status = fs.RmDir(spath);

  if (eos::common::error_retc_map(status.errNo)) {
    if ((errno == EIO) ||
        (status.GetErrorMessage().find("Directory not empty") != std::string::npos)) {
      errno = ENOTEMPTY;
    }
  } else {
    errno = 0;
  }

  COMMONTIMING("END", &rmdirtiming);

  if (EOS_LOGS_DEBUG) {
    rmdirtiming.Print();
  }

  return errno;
}


//------------------------------------------------------------------------------
// Map open return codes to errno's
int
fuse_filesystem::get_open_idx(const unsigned long long& inode)
{
  unsigned long long idx = 0;

  for (auto i = 0; i < (int) sizeof(unsigned long long) * 8;
       i += N_OPEN_MUTEXES_NBITS) {
    idx ^= ((N_OPEN_MUTEXES - 1) & (inode >> i));
  }

//eos_static_debug("inode=%lu  inode|=%lu  >>28|=%lu  xor=%lu",inode,inode&(N_OPEN_MUTEXES-1),(inode>>28)&(N_OPEN_MUTEXES-1),idx);
  return (int) idx;
}

//------------------------------------------------------------------------------
// Open a file
//------------------------------------------------------------------------------
int
fuse_filesystem::open(const char* path,
                      int oflags,
                      mode_t mode,
                      uid_t uid,
                      gid_t gid,
                      pid_t pid,
                      unsigned long long* return_inode,
                      bool mknod)
{
  eos_static_info("path=%s flags=%08x mode=%d uid=%u pid=%u", path, oflags, mode,
                  uid, pid);
  XrdOucString spath = user_url(uid, gid, pid).c_str();
  XrdSfsFileOpenMode flags_sfs = eos::common::LayoutId::MapFlagsPosix2Sfs(oflags);
  eos_static_debug("flags=%x", flags_sfs);
  struct stat buf;
  bool exists = true;
  bool lazy_open = (flags_sfs == SFS_O_RDONLY) ? lazy_open_ro : lazy_open_rw;
  bool isRO = (flags_sfs == SFS_O_RDONLY);
  eos::common::Timing opentiming("open");
  COMMONTIMING("START", &opentiming);
  spath += safePath(path).c_str();
  errno = 0;
  int t0;
  int retc = add_fd2file(0, *return_inode, uid, gid, pid, isRO, path);

  if (retc != -1) {
    eos_static_debug("file already opened, return fd=%i path=%s", retc, path);
    return retc;
  }

  if ((t0 = spath.find("/proc/")) != STR_NPOS) {
    XrdOucString orig_path = spath;
    // Clean the path
    int t1 = spath.find("//");
    int t2 = spath.find("//", t1 + 2);
    spath.erase(t2 + 2, t0 - t2 - 2);

    while (spath.replace("///", "//")) {
    };

    // Force a reauthentication to the head node
    if (spath.endswith("/proc/reconnect")) {
      if (credConfig.use_user_gsiproxy || credConfig.use_user_krb5cc) {
        authidmanager.reconnectProcCache(uid, gid, pid);
      } else {
        authidmanager.IncConnectionId();
      }

      errno = ECONNABORTED;
      return -1;
    }

    // Return the 'whoami' information in that file
    if (spath.endswith("/proc/whoami")) {
      spath.replace("/proc/whoami", "/proc/user/");
      //spath += "?mgm.cmd=whoami&mgm.format=fuse&eos.app=fuse";
      spath += '?';
      std::string auth = strongauth_cgi(uid, gid, pid);

      if (!auth.empty()) {
        spath += auth.c_str();
        spath += '&';
      }

      spath += "mgm.cmd=whoami&mgm.format=fuse&eos.app=fuse";

      if (encode_pathname) {
        spath += "&eos.encodepath=1";
      }

      XrdOucString open_path = get_url_nocgi(spath.c_str());
      XrdOucString open_cgi = get_cgi(spath.c_str());
      LayoutWrapper* file = new LayoutWrapper(new eos::fst::PlainLayout(NULL, 0, NULL,
                                              NULL, open_path.c_str()));

      if (stat(open_path.c_str(), &buf, uid, gid, pid, 0)) {
        exists = false;
      }

      retc = file->Open(open_path.c_str(), flags_sfs, mode, open_cgi.c_str(),
                        exists ? &buf : NULL, false, true);

      if (retc) {
        eos_static_err("open failed for %s : error code is %d", spath.c_str(),
                       (int) errno);
        return eos::common::error_retc_map(errno);
      } else {
        retc = add_fd2file(file, *return_inode, uid, gid, pid, isRO);
        return retc;
      }
    }

    if (spath.endswith("/proc/who")) {
      spath.replace("/proc/who", "/proc/user/");
      //spath += "?mgm.cmd=who&mgm.format=fuse&eos.app=fuse";
      spath += '?';
      std::string auth = strongauth_cgi(uid, gid, pid);

      if (!auth.empty()) {
        spath += auth.c_str();
        spath += '&';
      }

      spath += "mgm.cmd=who&mgm.format=fuse&eos.app=fuse";

      if (encode_pathname) {
        spath += "&eos.encodepath=1";
      }

      XrdOucString open_path = get_url_nocgi(spath.c_str());
      XrdOucString open_cgi = get_cgi(spath.c_str());
      LayoutWrapper* file = new LayoutWrapper(new eos::fst::PlainLayout(NULL, 0, NULL,
                                              NULL, open_path.c_str()));

      if (stat(open_path.c_str(), &buf, uid, gid, pid, 0)) {
        exists = false;
      }

      retc = file->Open(open_path.c_str(), flags_sfs, mode, open_cgi.c_str(),
                        exists ? &buf : NULL, false,  true);

      if (retc) {
        eos_static_err("open failed for %s", spath.c_str());
        delete file;
        return eos::common::error_retc_map(errno);
      } else {
        retc = add_fd2file(file, *return_inode, uid, gid, pid, isRO);
        return retc;
      }
    }

    if (spath.endswith("/proc/quota")) {
      spath.replace("/proc/quota", "/proc/user/");
      //spath += "?mgm.cmd=quota&mgm.subcmd=lsuser&mgm.format=fuse&eos.app=fuse";
      spath += '?';
      std::string auth = strongauth_cgi(uid, gid, pid);

      if (!auth.empty()) {
        spath += auth.c_str();
        spath += '&';
      }

      spath += "mgm.cmd=quota&mgm.subcmd=lsuser&mgm.format=fuse&eos.app=fuse";

      if (encode_pathname) {
        spath += "&eos.encodepath=1";
      }

      XrdOucString open_path = get_url_nocgi(spath.c_str());
      XrdOucString open_cgi = get_cgi(spath.c_str());
      LayoutWrapper* file = new LayoutWrapper(new eos::fst::PlainLayout(NULL, 0, NULL,
                                              NULL, open_path.c_str()));

      if (stat(open_path.c_str(), &buf, uid, gid, pid, 0)) {
        exists = false;
      }

      retc = file->Open(open_path.c_str(), flags_sfs, mode, open_cgi.c_str(),
                        exists ? &buf : NULL, false, true);

      if (retc) {
        eos_static_err("open failed for %s", spath.c_str());
        delete file;
        return eos::common::error_retc_map(errno);
      } else {
        retc = add_fd2file(file, *return_inode, uid, gid, pid, isRO);
        return retc;
      }
    }

    spath = orig_path;
  }

  // Try to open file using PIO (parallel io) only in read mode
  if ((!getenv("EOS_FUSE_NOPIO")) && (flags_sfs == SFS_O_RDONLY)) {
    XrdCl::Buffer arg;
    XrdCl::Buffer* response = 0;
    std::string file_path = path;
    size_t spos = file_path.rfind("//");

    if (spos != std::string::npos) {
      file_path.erase(0, spos + 1);
    }

    std::string request = safePath(file_path.c_str());
    request += "?eos.app=fuse&mgm.pcmd=open";

    if (encode_pathname) {
      request += "&eos.encodepath=1";
    }

    arg.FromString(request);
    std::string surl = user_url(uid, gid, pid);
    std::string auth = strongauth_cgi(uid, gid, pid);

    if (!auth.empty()) {
      surl += "?" + auth;
    }

    XrdCl::URL Url(surl);
    XrdCl::FileSystem fs(Url);
    XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg,
                                          response);

    if (status.IsOK()) {
      // Parse output
      XrdOucString tag;
      XrdOucString stripePath;
      std::vector<std::string> stripeUrls;
      XrdOucString origResponse = response->GetBuffer();
      XrdOucString stringOpaque = response->GetBuffer();
      // Add the eos.app=fuse tag to all future PIO open requests
      origResponse += "&eos.app=fuse";

      while (stringOpaque.replace("?", "&")) {
      }

      while (stringOpaque.replace("&&", "&")) {
      }

      std::unique_ptr<XrdOucEnv> openOpaque(new XrdOucEnv(stringOpaque.c_str()));
      char* opaqueInfo = (char*) strstr(origResponse.c_str(), "&mgm.logid");

      if (opaqueInfo) {
        opaqueInfo += 1;
        LayoutId::layoutid_t layout = openOpaque->GetInt("mgm.lid");

        for (unsigned int i = 0; i <= eos::common::LayoutId::GetStripeNumber(layout);
             i++) {
          tag = "pio.";
          tag += static_cast<int>(i);
          stripePath = "root://";
          stripePath += openOpaque->Get(tag.c_str());
          stripePath += "/";
          stripePath += file_path.c_str();
          stripeUrls.push_back(stripePath.c_str());
        }

        eos::fst::RaidMetaLayout* file;

        if (LayoutId::GetLayoutType(layout) == LayoutId::kRaidDP) {
          file = new eos::fst::RaidDpLayout(NULL, layout, NULL, NULL, "root://dummy");
        } else if ((LayoutId::GetLayoutType(layout) == LayoutId::kRaid6) ||
                   (LayoutId::GetLayoutType(layout) == LayoutId::kArchive)) {
          file = new eos::fst::ReedSLayout(NULL, layout, NULL, NULL, "root://dummy");
        } else {
          eos_static_warning("warning=no such supported layout for PIO");
          file = 0;
        }

        if (file) {
          retc = file->OpenPio(stripeUrls, flags_sfs, mode, opaqueInfo);

          if (retc) {
            eos_static_err("failed open for pio red, path=%s", spath.c_str());
            delete response;
            delete file;
            return eos::common::error_retc_map(errno);
          } else {
            if (return_inode) {
              // Try to extract the inode from the opaque redirection
              XrdOucEnv RedEnv = file->GetLastUrl().c_str();
              const char* sino = RedEnv.Get("mgm.id");

              if (sino) {
                *return_inode = gInodeTranslator.FidToInode(eos::common::FileId::Hex2Fid(sino));
              } else {
                *return_inode = 0;
              }

              eos_static_debug("path=%s created inode=%lu", path,
                               (unsigned long long) *return_inode);
            }

            retc = add_fd2file(new LayoutWrapper(file), *return_inode, uid, gid, pid, isRO);
            delete response;
            return retc;
          }
        }
      } else {
        eos_static_debug("opaque info not what we expected");
      }
    } else {
      eos_static_err("failed get request for pio read. query was %s, response "
                     "was %s and error was %s", arg.ToString().c_str(),
                     (response ? response->ToString().c_str() : "no-response"),
                     status.ToStr().c_str());
    }

    delete response;
  }

  eos_static_debug("the spath is:%s", spath.c_str());
  XrdOucString open_cgi = "eos.app=fuse";

  if (encode_pathname) {
    open_cgi += "&eos.encodepath=1";
  }

  if (oflags & (O_RDWR | O_WRONLY)) {
    open_cgi += "&eos.bookingsize=0";
  } else {
    open_cgi += "&eos.checksum=ignore";
  }

  if (do_rdahead) {
    open_cgi += "&fst.readahead=true&fst.blocksize=";
    open_cgi += rdahead_window.c_str();
  }

  if ((credConfig.use_user_krb5cc || credConfig.use_user_gsiproxy) &&
      fuse_shared) {
    open_cgi += "&";
    open_cgi += strongauth_cgi(uid, gid, pid).c_str();
  }

  // Check if the file already exists in case this is a write
  if (stat(path, &buf, uid, gid, pid, 0)) {
    exists = false;
  }

  eos_static_debug("open_path=%s, open_cgi=%s, exists=%d, flags_sfs=%d",
                   spath.c_str(), open_cgi.c_str(), (int) exists, (int) flags_sfs);
  retc = 1;

  // upgrade the WRONLY open to RW
  if (flags_sfs & SFS_O_WRONLY) {
    flags_sfs &= ~SFS_O_WRONLY;
    flags_sfs |= SFS_O_RDWR;
  }

  bool do_inline_repair = getInlineRepair();

  // Figure out if this file can be repaired inline
  if (exists) {
    if (((uint64_t) buf.st_size > getMaxInlineRepairSize())) {
      eos_static_notice("disabled inline repair path=%s file-size=%llu repair-limit=%llu",
                        spath.c_str(), buf.st_size, getMaxInlineRepairSize());
      do_inline_repair = false;
    }
  }

  if (isRO && force_rwopen(*return_inode, uid, gid, pid) < 0) {
    eos_static_err("forcing rw open failed for inode %lu path %s",
                   (unsigned long long)*return_inode, path);
    return eos::common::error_retc_map(errno);
  }

  LayoutWrapper* file = new LayoutWrapper(
    new eos::fst::PlainLayout(NULL, 0, NULL, NULL, spath.c_str()));
  retc = file->Open(spath.c_str(), flags_sfs, mode, open_cgi.c_str(),
                    exists ? &buf : NULL, async_open, !lazy_open, creator_cap_lifetime,
                    do_inline_repair);

  if (retc) {
    eos_static_err("open failed for %s : error code is %d.", spath.c_str(),
                   (int) errno);
    delete file;
    return eos::common::error_retc_map(errno);
  } else {
    // TODO: return_inode already dereferenced before
    if (return_inode) {
      // Try to extract the inode from the opaque redirection
      std::string url = file->GetLastUrl().c_str();
      XrdOucEnv RedEnv = file->GetLastUrl().c_str();
      const char* sino = RedEnv.Get("mgm.id");
      ino_t old_ino = *return_inode;
      ino_t new_ino = sino ? (gInodeTranslator.FidToInode(
                                eos::common::FileId::Hex2Fid(sino))) : 0;

      if (old_ino && (old_ino != new_ino)) {
        if (new_ino) {
          // An inode of an existing file can be changed during the process
          // of an open due to an auto-repair
          std::ostringstream sstr_old;
          std::ostringstream sstr_new;
          sstr_old << old_ino << ":" << get_login(uid, gid, pid);
          sstr_new << new_ino << ":" << get_login(uid, gid, pid);
          {
            eos::common::RWMutexWriteLock wr_lock(rwmutex_fd2fabst);

            if (inodexrdlogin2fds.count(sstr_old.str())) {
              inodexrdlogin2fds[sstr_new.str()] = inodexrdlogin2fds[sstr_old.str()];
              inodexrdlogin2fds.erase(sstr_old.str());
            }
          }
          {
            eos::common::RWMutexWriteLock wr_lock(mutex_inode_path);

            if (inode2path.count(old_ino)) {
              std::string ipath = inode2path[old_ino];

              if (path2inode.count(ipath)) {
                if (path2inode[ipath] != new_ino) {
                  path2inode[ipath] = new_ino;
                  inode2path[new_ino] = ipath;
                  eos_static_info("msg=\"inode replaced remotely\" path=%s "
                                  "old-ino=%lu new-ino=%lu", path, old_ino, new_ino);
                }
              }
            }
          }
        } else {
          eos_static_crit("new inode is null: cannot move old inode to new inode!");
          errno = EBADR;
          delete file;
          return -1;
        }
      }

      *return_inode = new_ino;
      eos_static_debug("path=%s opened ino=%lu", path,
                       (unsigned long long) *return_inode);
    }

    retc = add_fd2file(file, *return_inode, uid, gid, pid, isRO, path, mknod);
    COMMONTIMING("END", &opentiming);

    if (EOS_LOGS_DEBUG) {
      opentiming.Print();
    }

    return retc;
  }
}


//------------------------------------------------------------------------------
// Set utimes
//------------------------------------------------------------------------------
int
fuse_filesystem::utimes_from_fabst(std::shared_ptr<FileAbstraction> fabst,
                                   unsigned long long inode, uid_t uid, gid_t gid, pid_t pid)
{
  LayoutWrapper* raw_file = fabst->GetRawFileRW();

  if (!raw_file) {
    return 0;
  }

  if (raw_file->IsOpen()) {
    struct timespec ut[2];
    const char* path = 0;

    if ((path = fabst->GetUtimes(ut))) {
      const char* nowpath = 0;
      std::string npath;
      {
        // a file might have been renamed in the meanwhile
        lock_r_p2i();
        nowpath = this->path((unsigned long long)inode);
        unlock_r_p2i();

        if (nowpath) {
          // get it prefixed again
          getPath(npath, mPrefix, nowpath);
          nowpath = npath.c_str();
        } else {
          nowpath = path;
        }
      }

      if (strcmp(path, nowpath)) {
        eos_static_info("file renamed before close old-name=%s new-name=%s", path,
                        nowpath);
        path = nowpath;
      }

      // run the utimes command now after the close
      eos_static_debug("CLOSEDEBUG closing file open-path=%s current-path=%s "
                       "open with flag %d and utiming",
                       raw_file->GetOpenPath().c_str(), path,
                       (int) raw_file->GetOpenFlags());

      // run the utimes command now after the close
      if (this->utimes(path, ut, uid, gid, pid)) {
        // a file might have been renamed in the meanwhile
        lock_r_p2i();
        nowpath = this->path((unsigned long long)inode);
        unlock_r_p2i();

        if (nowpath) {
          // get it prefixed again
          getPath(npath, mPrefix, nowpath);
          nowpath = npath.c_str();
        } else {
          nowpath = path;
        }

        if (strcmp(nowpath, path)) {
          eos_static_info("file renamed again before close old-name=%s new-name=%s", path,
                          nowpath);
          path = nowpath;

          if (this->utimes(path, ut, uid, gid, pid)) {
            eos_static_err("file utime setting failed permanently for %s", path);
          }
        }
      }
    } else {
      eos_static_debug("CLOSEDEBUG no utime");
    }
  } else {
    // the file might have just been touched with an utime set
    struct timespec ut[2];
    ut[0].tv_sec = ut[1].tv_sec = 0;
    const char* path = fabst->GetUtimes(ut);
    const char* nowpath = 0;
    std::string npath;
    {
      // a file might have been renamed in the meanwhile
      lock_r_p2i();
      nowpath = this->path((unsigned long long)inode);
      unlock_r_p2i();

      if (nowpath) {
        // get it prefixed again
        getPath(npath, mPrefix, nowpath);
        nowpath = npath.c_str();
      } else {
        nowpath = path;
      }
    }

    if (strcmp(path, nowpath)) {
      eos_static_info("file renamed before close old-name=%s new-name=%s", path,
                      nowpath);
      path = nowpath;
    }

    if (ut[0].tv_sec || ut[1].tv_sec) {
      // this still allows to jump in for a rename, but we neglect this possiblity for now
      eos_static_debug("CLOSEDEBUG closing touched file open-path=%s "
                       "current-path=%s open with flag %d and utiming",
                       raw_file->GetOpenPath().c_str(), path,
                       (int)raw_file->GetOpenFlags());

      // run the utimes command now after the close
      if (this->utimes(path, ut, uid, gid, pid)) {
        // a file might have been renamed in the meanwhile
        lock_r_p2i();
        nowpath = this->path((unsigned long long)inode);
        unlock_r_p2i();

        if (nowpath) {
          // get it prefixed again
          getPath(npath, mPrefix, nowpath);
          nowpath = npath.c_str();
        } else {
          nowpath = path;
        }

        if (strcmp(nowpath, path)) {
          eos_static_info("file renamed again before close old-name=%s new-name=%s", path,
                          nowpath);
          path = nowpath;

          if (this->utimes(path, ut, uid, gid, pid)) {
            eos_static_err("file utime setting failed permanently for %s", path);
          }
        }
      }
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// Release is called when FUSE is completely done with a file; at that point,
// you can free up any temporarily allocated data structures.
//------------------------------------------------------------------------------
int
fuse_filesystem::close(int fildes, unsigned long long inode, uid_t uid,
                       gid_t gid,
                       pid_t pid)
{
  int ret = -1;
  eos_static_info("fd=%d inode=%lu, uid=%i, gid=%i, pid=%i", fildes, inode,
                  uid, gid, pid);
  std::shared_ptr<FileAbstraction> fabst = get_file(fildes);

  if (!fabst.get()) {
    errno = ENOENT;
    return ret;
  }

  if (XFC) {
    LayoutWrapper* file = fabst->GetRawFileRW();
    error_type error;
    fabst->mMutexRW.WriteLock();
    XFC->ForceAllWrites(fabst.get());
    eos::common::ConcurrentQueue<error_type> err_queue = fabst->GetErrorQueue();

    if (file && (err_queue.try_pop(error))) {
      eos_static_warning("write error found in err queue for inode=%llu - enabling restore",
                         inode);
      file->SetRestore();
    }

    fabst->mMutexRW.UnLock();
  }

  {
    // update our local stat cache
    struct stat buf;
    buf.st_size = fabst->GetMaxWriteOffset();
    dir_cache_update_entry(inode, &buf);
  }

  {
    // Commit the utime first - we cannot handle errors here
    ret = utimes_from_fabst(fabst, inode, uid, gid, pid);
    // Close file and remove it from all mappings
    ret = remove_fd2file(fildes, inode, uid, gid, pid);
  }

  if (ret) {
    errno = EIO;
  }

  return ret;
}


//------------------------------------------------------------------------------
// Flush file data to disk
//------------------------------------------------------------------------------
int
fuse_filesystem::flush(int fd, uid_t uid, gid_t gid, pid_t pid)
{
  int retc = 0;
  eos_static_info("fd=%d ", fd);
  bool isRW = false;
  std::shared_ptr<FileAbstraction> fabst = get_file(fd, &isRW);

  if (!fabst.get()) {
    errno = ENOENT;
    return -1;
  }

  if (!isRW) {
    fabst->DecNumRefRO();
    return 0;
  }

  if (XFC && fuse_cache_write) {
    off_t cache_size = fabst->GetMaxWriteOffset();
    eos_static_notice("cache-size=%llu max-offset=%d force=%d", cache_size,
                      file_write_back_cache_size, (cache_size > file_write_back_cache_size));
    fabst->mMutexRW.WriteLock();
    bool wait_async = true;

    if (fabst->GetRawFileRW() && fabst->GetRawFileRW()->CanCache()) {
      if (cache_size < file_write_back_cache_size) {
        wait_async = false;
      }
    }

    XFC->ForceAllWrites(fabst.get(), wait_async);
    eos::common::ConcurrentQueue<error_type> err_queue = fabst->GetErrorQueue();
    error_type error;

    if (err_queue.try_pop(error)) {
      eos_static_info("Extract error from queue");
      retc = error.first;

      if (retc) {
        errno = retc;
        retc = -1;
      }
    } else {
      eos_static_info("No flush error");
    }

    fabst->mMutexRW.UnLock();
  }

  fabst->DecNumRefRW();
  return retc;
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
fuse_filesystem::truncate(int fildes, off_t offset)
{
  eos::common::Timing truncatetiming("truncate");
  COMMONTIMING("START", &truncatetiming);
  int ret = -1;
  eos_static_info("fd=%d offset=%llu", fildes, (unsigned long long) offset);
  bool isRW = false;
  std::shared_ptr<FileAbstraction> fabst = get_file(fildes, &isRW);
  errno = 0;

  if (!fabst.get()) {
    errno = ENOENT;
    return ret;
  }

  if (!isRW) {
    fabst->DecNumRefRO();
    errno = EPERM;
    return ret;
  }

  LayoutWrapper* file = fabst->GetRawFileRW();

  if (!file) {
    errno = ENOENT;
    return ret;
  }

// update modification time
  struct timespec ts[2];
  eos::common::Timing::GetTimeSpec(ts[1], true);
  ts[0] = ts[1];
  fabst->SetUtimes(ts);

  if (XFC && fuse_cache_write) {
    fabst->mMutexRW.WriteLock();
    XFC->ForceAllWrites(fabst.get());
    ret = file->Truncate(offset);
    fabst->SetMaxWriteOffset(offset);
    fabst->mMutexRW.UnLock();
  } else {
    ret = file->Truncate(offset);
  }

  fabst->DecNumRefRW();

  if (ret == -1) {
    errno = EIO;
  }

  COMMONTIMING("END", &truncatetiming);

  if (EOS_LOGS_DEBUG) {
    truncatetiming.Print();
  }

  return ret;
}

//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
fuse_filesystem::truncate2(const char* fullpath, unsigned long long inode,
                           unsigned long truncsize, uid_t uid, gid_t gid, pid_t pid)
{
  if (inode) {
    // Try to truncate via an open file - first find the file descriptor using the
    // inodeuser2fd map and then find the file object using the fd2fabst map.
    // Meanwhile keep the mutex locked for read so that no other thread can
    // delete the file object
    eos_static_debug("path=%s, uid=%lu, inode=%lu",
                     fullpath, (unsigned long) uid, inode);
    eos::common::RWMutexReadLock rd_lock(rwmutex_fd2fabst);
    std::ostringstream sstr;
    sstr << inode << ":" << get_login(uid, gid, pid);
    google::dense_hash_map<std::string, std::set<int>>::iterator
        iter_fd = inodexrdlogin2fds.find(sstr.str());

    if (iter_fd != inodexrdlogin2fds.end()) {
      for (auto fdit = iter_fd->second.begin(); fdit != iter_fd->second.end(); fdit++)
        if (fd2count[*fdit] > 0) {
          return truncate(*fdit, truncsize);
        }
    } else {
      eos_static_debug("path=%s not open in rw", fullpath);
    }
  }

  int fd, retc = -1;
  unsigned long long rinode = 0;

  if ((fd = open(fullpath, O_WRONLY,
                 S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH,
                 uid, gid, pid, &rinode)) > 0) {
    retc = truncate(fd, truncsize);
    close(fd, rinode, uid, gid, pid);
  } else {
    retc = errno;
  }

  return retc;
}

//------------------------------------------------------------------------------
// Read from file. Returns the number of bytes transferred, or 0 if offset
// was at or beyond the end of the file
//------------------------------------------------------------------------------
ssize_t
fuse_filesystem::pread(int fildes,
                       void* buf,
                       size_t nbyte,
                       off_t offset)
{
  eos::common::Timing xpr("pread");
  COMMONTIMING("start", &xpr);
  eos_static_debug("fd=%d nbytes=%lu offset=%llu",
                   fildes, (unsigned long) nbyte,
                   (unsigned long long) offset);
  ssize_t ret = -1;
  bool isRW = false;
  std::shared_ptr<FileAbstraction> fabst = get_file(fildes, &isRW);
  std::string origin = "remote-ro";

  if (isRW) {
    origin = "remote-rw";
  }

  if (!fabst.get()) {
    errno = ENOENT;
    return ret;
  }

  LayoutWrapper* file = isRW ? fabst->GetRawFileRW() : fabst->GetRawFileRO();

  if (XFC && fuse_cache_write) {
    ret = file->ReadCache(offset, static_cast<char*>(buf), nbyte,
                          file_write_back_cache_size);

    // Either the data is not in the cache, the cache is empty or the cache
    // request is not complete
    if (ret != (int) nbyte) {
      off_t cache_size = fabst->GetMaxWriteOffset();

      if ((ret == -1) || (!cache_size) || ((off_t)(offset + nbyte) < cache_size)) {
        if (isRW) {
          origin = "flush";
          // cache miss
          fabst->mMutexRW.WriteLock();
          XFC->ForceAllWrites(fabst.get());
          ret = file->Read(offset, static_cast<char*>(buf), nbyte,
                           false);
          fabst->mMutexRW.UnLock();
        } else {
          ret = file->Read(offset, static_cast<char*>(buf), nbyte,
                           do_rdahead);
        }
      } else {
        origin = "cache-short";
      }
    } else {
      origin = "cache";
    }
  } else {
    ret = file->Read(offset, static_cast<char*>(buf), nbyte,
                     isRW ? false : do_rdahead);
  }

  // Release file reference
  isRW ? fabst->DecNumRefRW() : fabst->DecNumRefRO();
  COMMONTIMING("END", &xpr);

  if (ret == -1) {
    eos_static_err("failed read off=%ld, len=%u", offset, nbyte);
    errno = EIO;
  } else if ((size_t) ret != nbyte) {
    eos_static_info("read size=%u, returned=%u origin=%s", nbyte, ret,
                    origin.c_str());
  }

  eos_static_info("read size=%u, returned=%u origin=%s", nbyte, ret,
                  origin.c_str());

  if (EOS_LOGS_DEBUG) {
    xpr.Print();
  }

  return ret;
}

//------------------------------------------------------------------------------
// Write to file
//------------------------------------------------------------------------------
ssize_t
fuse_filesystem::pwrite(int fildes, const void* buf, size_t nbyte, off_t offset)
{
  eos::common::Timing xpw("pwrite");
  COMMONTIMING("start", &xpw);
  eos_static_debug("fd=%d nbytes=%lu cache=%d cache-w=%d",
                   fildes, (unsigned long) nbyte, XFC ? 1 : 0,
                   fuse_cache_write);
  int64_t ret = -1;
  bool isRW = false;
  std::shared_ptr<FileAbstraction> fabst = get_file(fildes, &isRW);

  if (!fabst.get()) {
    errno = ENOENT;
    return ret;
  }

  if (!isRW) {
    errno = EPERM;
    fabst->DecNumRefRO();
    return ret;
  }

  if (XFC && fuse_cache_write) {
    // store in cache
    fabst->GetRawFileRW()->WriteCache(offset, static_cast<const char*>(buf), nbyte,
                                      file_write_back_cache_size);
    fabst->mMutexRW.ReadLock();
    fabst->TestMaxWriteOffset(offset + nbyte);
    FileAbstraction* fab = fabst.get();
    XFC->SubmitWrite(fab, const_cast<void*>(buf), offset, nbyte);
    ret = nbyte;
    eos::common::ConcurrentQueue<error_type> err_queue = fabst->GetErrorQueue();
    error_type error;

    if (err_queue.try_pop(error)) {
      eos_static_info("Extract error from queue");
      ret = error.first;
    }

    fabst->mMutexRW.UnLock();
  } else {
    LayoutWrapper* file = fabst->GetRawFileRW();
    fabst->TestMaxWriteOffset(offset + nbyte);
    ret = file->Write(offset, static_cast<const char*>(buf), nbyte);

    if (ret == -1) {
      errno = EIO;
    }
  }

// update modification time
  struct timespec ts[2];
  eos::common::Timing::GetTimeSpec(ts[1], true);
  ts[0] = ts[1];
  fabst->SetUtimes(ts);
  fabst->DecNumRefRW();
  COMMONTIMING("END", &xpw);

// Release file reference
  if (EOS_LOGS_DEBUG) {
    xpw.Print();
  }

  return ret;
}


//------------------------------------------------------------------------------
// Flush any dirty information about the file to disk
//------------------------------------------------------------------------------
int
fuse_filesystem::fsync(int fildes)
{
  eos::common::Timing xps("fsync");
  COMMONTIMING("start", &xps);
  eos_static_info("fd=%d", fildes);
  int ret = 0;
  bool isRW;
  std::shared_ptr<FileAbstraction> fabst = get_file(fildes, &isRW);

  if (!fabst.get()) {
    errno = ENOENT;
    return ret;
  }

  if (!isRW) {
    fabst->DecNumRefRO();
    return 0;
  }

  if (XFC && fuse_cache_write) {
    fabst->mMutexRW.WriteLock();
    XFC->ForceAllWrites(fabst.get());
    fabst->mMutexRW.UnLock();
  }

  LayoutWrapper* file = fabst->GetRawFileRW();

  if (file) {
    ret = file->Sync();
  }

  if (ret) {
    errno = EIO;
  }

// Release file reference
  fabst->DecNumRefRW();
  COMMONTIMING("END", &xps);

  if (EOS_LOGS_DEBUG) {
    xps.Print();
  }

  return ret;
}

//------------------------------------------------------------------------------
// Remove (delete) the given file, symbolic link, hard link, or special node
//------------------------------------------------------------------------------
int
fuse_filesystem::unlink(const char* path, uid_t uid, gid_t gid, pid_t pid,
                        unsigned long long inode)
{
  eos::common::Timing xpu("unlink");
  COMMONTIMING("start", &xpu);
  eos_static_info("path=%s uid=%u, pid=%u", path, uid, pid);
  std::string surl = user_url(uid, gid, pid);
  std::string auth = strongauth_cgi(uid, gid, pid);

  if (!auth.empty()) {
    surl += "?" + auth;
  }

  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  std::string spath = safePath(path);

  if (encode_pathname) {
    spath += "?eos.encodepath=1";
  }

  XrdCl::XRootDStatus status = fs.Rm(spath);
// drop evt. the in-memory cache
  LayoutWrapper::CacheRemove(inode);

  if (!eos::common::error_retc_map(status.errNo)) {
    errno = 0;
  }

  COMMONTIMING("END", &xpu);

  if (EOS_LOGS_DEBUG) {
    xpu.Print();
  }

  return errno;
}

//------------------------------------------------------------------------------
// Rename file/dir
//------------------------------------------------------------------------------
int
fuse_filesystem::rename(const char* oldpath, const char* newpath, uid_t uid,
                        gid_t gid, pid_t pid)
{
  eos::common::Timing xpr("rename");
  COMMONTIMING("start", &xpr);
  eos_static_info("oldpath=%s newpath=%s", oldpath, newpath, uid, pid);
  XrdOucString sOldPath = oldpath;
  XrdOucString sNewPath = newpath;

// XRootd move cannot deal with space in the path names
  if (encode_pathname) {
    sOldPath = safePath(sOldPath.c_str()).c_str();
    sOldPath += "?eos.encodepath=1";
    sNewPath = safePath(sNewPath.c_str()).c_str();
    sNewPath += "?eos.encodepath=1";
  } else {
    sOldPath.replace(" ", "#space#");
    sNewPath.replace(" ", "#space#");
  }

  std::string surl = user_url(uid, gid, pid);
  std::string auth = strongauth_cgi(uid, gid, pid);

  if (!auth.empty()) {
    surl += "?" + auth;
  }

  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = fs.Mv(sOldPath.c_str(), sNewPath.c_str());

  if (!eos::common::error_retc_map(status.errNo)) {
    errno = 0;
    return 0;
  }

  COMMONTIMING("END", &xpr);

  if (EOS_LOGS_DEBUG) {
    xpr.Print();
  }

  return errno;
}

static void addSecUidGid(uid_t uid, gid_t gid, XrdOucString& str)
{
  str += "&xrdcl.secuid=";
  str += std::to_string(uid).c_str();
  str += "&xrdcl.secgid=";
  str += std::to_string(gid).c_str();
}

//------------------------------------------------------------------------------
// Build strong authentication CGI url info
//------------------------------------------------------------------------------
std::string
fuse_filesystem::strongauth_cgi(uid_t uid, gid_t gid, pid_t pid)
{
  XrdOucString str = "";

  if (fuse_shared && (credConfig.use_user_krb5cc ||
                      credConfig.use_user_gsiproxy)) {
    std::string authmet;

    if (gProcCache(pid).HasEntry(pid)) {
      gProcCache(pid).GetAuthMethod(pid, authmet);
    }

    for (size_t i = 0; i < authmet.size(); i++) {
      if (authmet[i] == '&' || authmet[i] == '=') {
        eos_static_alert("rejecting credential filename for using forbidden characters: %s",
                         authmet.c_str());
        str += "xrd.wantprot=unix";
        goto bye;
      }
    }

    if (authmet.compare(0, 5, "krb5:") == 0) {
      str += "xrd.k5ccname=";
      str += (authmet.c_str() + 5);
      str += "&xrd.wantprot=krb5,unix";
      addSecUidGid(uid, gid, str);
    } else if (authmet.compare(0, 5, "krk5:") == 0) {
      str += "xrd.k5ccname=";
      str += (authmet.c_str() + 5);
      str += "&xrd.wantprot=krb5,unix";
      addSecUidGid(uid, gid, str);
    } else if (authmet.compare(0, 5, "x509:") == 0) {
      str += "xrd.gsiusrpxy=";
      str += authmet.c_str() + 5;
      str += "&xrd.wantprot=gsi,unix";
      addSecUidGid(uid, gid, str);
    } else if (authmet.compare(0, 5, "unix:") == 0) {
      str += "xrd.wantprot=unix";
    } else {
      eos_static_err("don't know what to do with qualifiedid [%s]", authmet.c_str());
      goto bye;
    }
  }

bye:
  eos_static_debug("pid=%lu sep=%s", (unsigned long) pid, str.c_str());
  return str.c_str();
}

//------------------------------------------------------------------------------
// Get a user private physical connection URL like root://<user>@<host>
// - if we are a user private mount we don't need to specify that
//------------------------------------------------------------------------------
std::string
fuse_filesystem::user_url(uid_t uid, gid_t gid, pid_t pid)
{
  std::string url = "root://";

  if (fuse_shared) {
    url += get_login(uid, gid, pid);
    url += "@";
  }

  url += gMgmHost.c_str();
  url += "//";
  eos_static_debug("uid=%lu gid=%lu pid=%lu url=%s",
                   (unsigned long) uid,
                   (unsigned long) gid,
                   (unsigned long) pid, url.c_str());
  return url;
}

//------------------------------------------------------------------------------
// Decide if this is an 'rm -rf' command issued on the toplevel directory or
// anywhere whithin the EOS_FUSE_RMLVL_PROTECT levels from the root directory
//------------------------------------------------------------------------------
int
fuse_filesystem::is_toplevel_rm(int pid, const char* local_dir)
{
  eos_static_debug("is_toplevel_rm for pid %d and mountpoint %s", pid, local_dir);

  if (rm_level_protect == 0) {
    return 0;
  }

  time_t psstime = 0;

  if (!gProcCache(pid).HasEntry(pid) ||
      !gProcCache(pid).GetStartupTime(pid, psstime)) {
    eos_static_err("could not get process start time");
  }

  // Check the cache
  {
    eos::common::RWMutexReadLock rlock(mMapPidDenyRmMutex);
    auto it_map = mMapPidDenyRm.find(pid);

    // if the cached denial is up to date, return it
    if (it_map != mMapPidDenyRm.end()) {
      eos_static_debug("found an entry in the cache");

      // if the cached denial is up to date, return it
      if (psstime <= it_map->second.first) {
        eos_static_debug("found in cache pid=%i, rm_deny=%i", it_map->first,
                         it_map->second.second);

        if (it_map->second.second) {
          std::string cmd = gProcCache(pid).GetArgsStr(pid);
          eos_static_notice("rejected toplevel recursive deletion command %s",
                            cmd.c_str());
        }

        return (it_map->second.second ? 1 : 0);
      }

      eos_static_debug("the entry is oudated in cache %d, current %d",
                       (int) it_map->second.first, (int) psstime);
    }
  }
  eos_static_debug("no entry found or outdated entry, creating entry with psstime %d",
                   (int) psstime);
  auto entry = std::make_pair(psstime, false);
  // Try to print the command triggering the unlink
  std::ostringstream oss;
  std::vector<std::string> cmdv = gProcCache(pid).GetArgsVec(pid);
  std::string cmd = gProcCache(pid).GetArgsStr(pid);
  std::set<std::string> rm_entries;
  std::set<std::string> rm_opt; // rm command options (long and short)
  char exe[PATH_MAX];
  oss.str("");
  oss.clear();
  oss << "/proc/" << pid << "/exe";
  ssize_t len = ::readlink(oss.str().c_str(), exe, sizeof(exe) - 1);

  if (len == -1) {
    eos_static_err("error while reading cwd for path=%s", oss.str().c_str());
    return 0;
  }

  exe[len] = '\0';
  std::string rm_cmd = exe;
  std::string token;

  for (auto it = cmdv.begin() + 1; it != cmdv.end(); it++) {
    token = *it;
    // Long option

    if (token.find("--") == 0) {
      token.erase(0, 2);
      rm_opt.insert(token);
    } else if (token.find('-') == 0) {
      token.erase(0, 1);
      // Short option
      size_t length = token.length();

      for (size_t i = 0; i != length; ++i) {
        rm_opt.insert(std::string(&token[i], 1));
      }
    } else {
      rm_entries.insert(token);
    }
  }

  for (std::set<std::string>::iterator it = rm_opt.begin();
       it != rm_opt.end(); ++it) {
    eos_static_debug("rm option:%s", it->c_str());
  }

  // Exit if this is not a recursive removal
  auto fname = rm_cmd.length() < 2 ? rm_cmd : rm_cmd.substr(rm_cmd.length() - 2,
               2);
  bool isrm = rm_cmd.length() <= 2 ? (fname == "rm") : (fname == "rm" &&
              rm_cmd[rm_cmd.length() - 3] == '/');

  if (!isrm ||
      (isrm &&
       rm_opt.find("r") == rm_opt.end() &&
       rm_opt.find("recursive") == rm_opt.end())) {
    eos_static_debug("%s is not an rm command", rm_cmd.c_str());
    mMapPidDenyRmMutex.LockWrite();
    mMapPidDenyRm[pid] = entry;
    mMapPidDenyRmMutex.UnLockWrite();
    return 0;
  }

// check that we dealing with the system rm command
  bool skip_relpath = !rm_watch_relpath;

  if ((!skip_relpath) && (rm_cmd != rm_command)) {
    eos_static_warning("using rm command %s different from the system rm "
                       "command %s : cannot watch recursive deletion on "
                       "relative paths", rm_cmd.c_str(), rm_command.c_str());
    skip_relpath = true;
  }

  // Get the current working directory
  oss.str("");
  oss.clear();
  oss << "/proc/" << pid << "/cwd";
  char cwd[PATH_MAX];
  len = ::readlink(oss.str().c_str(), cwd, sizeof(cwd) - 1);

  if (len == -1) {
    eos_static_err("error while reading cwd for path=%s", oss.str().c_str());
    return 0;
  }

  cwd[len] = '\0';
  std::string scwd(cwd);

  if (*scwd.rbegin() != '/') {
    scwd += '/';
  }

// we are dealing with an rm command
  {
    std::set<std::string> rm_entries2;

    for (auto it = rm_entries.begin(); it != rm_entries.end(); it++) {
      char resolved_path[PATH_MAX];
      auto path2resolve = *it;
      eos_static_debug("path2resolve %s", path2resolve.c_str());

      if (path2resolve[0] != '/') {
        if (skip_relpath) {
          eos_static_debug("skipping recusive deletion check on command %s on "
                           "relative path %s because rm command used is likely "
                           "to chdir", cmd.c_str(), path2resolve.c_str());
          continue;
        }

        path2resolve = scwd + path2resolve;
      }

      if (myrealpath(path2resolve.c_str(), resolved_path, pid)) {
        rm_entries2.insert(resolved_path);
        eos_static_debug("path %s resolves to realpath %s", path2resolve.c_str(),
                         resolved_path);
      } else {
        eos_static_warning("could not resolve path %s for top level recursive "
                           "deletion protection", path2resolve.c_str());
      }
    }

    std::swap(rm_entries, rm_entries2);
  }
  // Make sure both the cwd and local mount dir ends with '/'
  std::string mount_dir(local_dir);

  if (*mount_dir.rbegin() != '/') {
    mount_dir += '/';
  }

  // First check if the command was launched from a location inside the hierarchy
  // of the local mount point
  eos_static_debug("cwd=%s, mount_dir=%s, skip_relpath=%d", scwd.c_str(),
                   mount_dir.c_str(), skip_relpath ? 1 : 0);
  std::string rel_path;
  int level;

  // Detect remove from inside the mount point hierarchy
  if (!skip_relpath && scwd.find(mount_dir) == 0) {
    rel_path = scwd.substr(mount_dir.length());
    level = std::count(rel_path.begin(), rel_path.end(), '/') + 1;
    eos_static_debug("rm_int current_lvl=%i, protect_lvl=%i", level,
                     rm_level_protect);

    if (level <= rm_level_protect) {
      entry.second = true;
      mMapPidDenyRmMutex.LockWrite();
      mMapPidDenyRm[pid] = entry;
      mMapPidDenyRmMutex.UnLockWrite();
      eos_static_notice("rejected toplevel recursive deletion command %s",
                        cmd.c_str());
      return 1;
    }
  }

  // At this point, absolute path are used.
  // Get the deepness level it reaches inside the EOS
  // mount point so that we can take the right decision
  for (std::set<std::string>::iterator it = rm_entries.begin();
       it != rm_entries.end(); ++it) {
    token = *it;

    if (token.find(mount_dir) == 0) {
      rel_path = token.substr(mount_dir.length());
      level = std::count(rel_path.begin(), rel_path.end(), '/') + 1;
      eos_static_debug("rm_ext current_lvl=%i, protect_lvl=%i", level,
                       rm_level_protect);

      if (level <= rm_level_protect) {
        entry.second = true;
        mMapPidDenyRmMutex.LockWrite();
        mMapPidDenyRm[pid] = entry;
        mMapPidDenyRmMutex.UnLockWrite();
        eos_static_notice("rejected toplevel recursive deletion command %s",
                          cmd.c_str());
        return 1;
      }
    }

    // Another case is when the delete command is issued on a directory higher
    // up in the hierarchy where the mountpoint was done
    if (mount_dir.find(*it) == 0) {
      level = 1;

      if (level <= rm_level_protect) {
        entry.second = true;
        mMapPidDenyRmMutex.LockWrite();
        mMapPidDenyRm[pid] = entry;
        mMapPidDenyRmMutex.UnLockWrite();
        eos_static_notice("rejected toplevel recursive deletion command %s",
                          cmd.c_str());
        return 1;
      }
    }
  }

  mMapPidDenyRmMutex.LockWrite();
  mMapPidDenyRm[pid] = entry;
  mMapPidDenyRmMutex.UnLockWrite();
  return 0;
}

//------------------------------------------------------------------------------
// Get the list of the features available on the MGM
//------------------------------------------------------------------------------
bool fuse_filesystem::get_features(const std::string& url,
                                   std::map<std::string, std::string>* features)
{
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  std::string request = "/?mgm.pcmd=version&mgm.version.features=1&eos.app=fuse";
  arg.FromString(request.c_str());
  XrdCl::URL Url(url.c_str());
  Url.SetUserName("init");
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg,
                                        response);
  status = xrdreq_retryonnullbuf(fs, arg, response);

  if (!status.IsOK()) {
    eos_static_crit("cannot read eos version");

    if (response) {
      delete response;
      response = 0;
    }

    return false;
  }

  std::string line;
  std::stringstream ss;
  bool infeatures = false;
  ss.str(response->GetBuffer(0));

  do {
    line.clear();
    std::getline(ss, line);

    if (line.empty()) {
      break;
    }

    if (!infeatures) {
      if (line.find("EOS_SERVER_FEATURES") != std::string::npos) {
        infeatures = true;
      }
    } else {
      auto pos = line.find("  =>  ");

      if (pos == std::string::npos) {
        eos_static_crit("error parsing instance features");
        delete response;
        return false; // there is something wrong here
      }

      string key   = line.substr(0, pos);
      string value = line.substr(pos + 6, std::string::npos);

      if ((pos = value.rfind("&mgm.proc.stderr")) != std::string::npos) {
        value.resize(pos);
      }

      (*features)[key] = value;
    }
  } while (1);

  if (!infeatures) {
    eos_static_warning("retrieving features is not supported on this eos instance");
    delete response;
    response = 0;
    return false;
  }

  delete response;
  return true;
}

//------------------------------------------------------------------------------
// Extract the EOS MGM endpoint for connection and also check that the MGM
// daemon is available.
//------------------------------------------------------------------------------
bool
fuse_filesystem::check_mgm(std::map<std::string, std::string>* features)
{
  std::string address = getenv("EOS_RDRURL") ? getenv("EOS_RDRURL") : "";

  if (address == "") {
    fprintf(stderr, "error: EOS_RDRURL is not defined so we fall back to "
            "root://localhost:1094// \n");
    address = "root://localhost:1094//";
    return 0;
  }

  XrdCl::URL url(address);

  if (!url.IsValid()) {
    eos_static_err("URL is not valid: %s", address.c_str());
    return 0;
  }

  // Check MGM is available
  if (!features) {
    uint16_t timeout = 15;

    if (getenv("EOS_FUSE_PING_TIMEOUT")) {
      timeout = (uint16_t) strtol(getenv("EOS_FUSE_PING_TIMEOUT"), 0, 10);
    }

    url.SetUserName("init");
    XrdCl::FileSystem fs(url);
    XrdCl::XRootDStatus st = fs.Ping(timeout);

    if (!st.IsOK()) {
      eos_static_err("Unable to contact MGM at address=%s (timed out after 10 seconds)",
                     address.c_str());
      return false;
    }
  }

  if (features) {
    get_features(address, features);
  }

  // Make sure the host has not '/' in the end and no prefix anymore
  gMgmHost = address.c_str();
  gMgmHost.replace("root://", "");
  int pos;

  if ((pos = gMgmHost.find("//")) != STR_NPOS) {
    gMgmHost.erase(pos);
  }

  if (gMgmHost.endswith("/")) {
    gMgmHost.erase(gMgmHost.length() - 1);
  }

  return true;
}

//------------------------------------------------------------------------------
// Init function
//------------------------------------------------------------------------------
bool
fuse_filesystem::initlogging()
{
  FILE* fstderr;

  // Open log file
  if (getuid() || getenv("EOS_FUSE_PRIVATE_ROOT_MOUNT")) {
    fuse_shared = false; //eosfsd
    char logfile[1024];

    if (getenv("EOS_FUSE_LOGFILE")) {
      snprintf(logfile, sizeof(logfile) - 1, "%s", getenv("EOS_FUSE_LOGFILE"));
    } else {
      snprintf(logfile, sizeof(logfile) - 1, "/tmp/eos-fuse.%d.log", getuid());
    }

    // Running as a user ... we log into /tmp/eos-fuse.$UID.log
    if (!(fstderr = freopen(logfile, "a+", stderr))) {
      fprintf(stdout, "error: cannot open log file %s\n", logfile);
      return false;
    } else {
      (void) ::chmod(logfile, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    }
  } else {
    fuse_shared = true; //eosfsd
    std::string log_path = "/var/log/eos/fuse/fuse.";

    // Running as root ... we log into /var/log/eos/fuse
    if (getenv("EOS_FUSE_LOG_PREFIX")) {
      log_path += getenv("EOS_FUSE_LOG_PREFIX");
      log_path += ".log";
    } else {
      log_path += "log";
    }

    eos::common::Path cPath(log_path.c_str());
    cPath.MakeParentPath(S_IRWXU | S_IRGRP | S_IROTH);

    if (!(fstderr = freopen(cPath.GetPath(), "a+", stderr))) {
      fprintf(stderr, "error: cannot open log file %s\n", cPath.GetPath());
      return false;
    } else {
      (void) ::chmod(cPath.GetPath(), S_IRUSR | S_IWUSR);
    }
  }

  setvbuf(fstderr, (char*) NULL, _IONBF, 0);
  eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  g_logging.SetUnit("FUSE@localhost");
  g_logging.gShortFormat = true;
  g_logging.EnableRateLimiter();
  XrdOucString fusedebug = getenv("EOS_FUSE_DEBUG");

  if ((getenv("EOS_FUSE_DEBUG")) && (fusedebug != "0")) {
    g_logging.SetLogPriority(LOG_DEBUG);
  } else {
    if ((getenv("EOS_FUSE_LOGLEVEL"))) {
      g_logging.SetLogPriority(atoi(getenv("EOS_FUSE_LOGLEVEL")));
    } else {
      g_logging.SetLogPriority(LOG_INFO);
    }
  }

  return true;
}

static bool getenv_boolean_flag(const std::string& name, bool default_value)
{
  const char* value = getenv(name.c_str());

  if (!value) {
    return default_value;
  }

  return (atoi(value) == 1);
}

bool
fuse_filesystem::init(int argc, char* argv[], void* userdata,
                      std::map<std::string, std::string>* features)
{
  if (!initlogging()) {
    return false;
  }

  try {
    path2inode.set_empty_key("");
    path2inode.set_deleted_key("#__deleted__#");
    inodexrdlogin2fds.set_empty_key("");
    inodexrdlogin2fds.set_deleted_key("#__deleted__#");
    fd2fabst.set_empty_key(-1);
    fd2fabst.set_deleted_key(-2);
    fd2count.set_empty_key(-1);
    fd2count.set_deleted_key(-2);
  } catch (std::length_error& len_excp) {
    eos_static_err("error: failed to insert into google map");
    return false;
  }

  eos::common::StringConversion::InitLookupTables();
// Create the root entry
  path2inode["/"] = 1;
  inode2path[1] = "/";
#ifdef STOPONREDIRECT
// Set the redirect limit
  XrdCl::DefaultEnv::GetEnv()->PutInt("RedirectLimit", 1);
  setenv("XRD_REDIRECTLIMIT", "1", 1);
#endif
  // Get parameters about strong authentication
  credConfig.use_user_krb5cc = getenv_boolean_flag("EOS_FUSE_USER_KRB5CC", false);
  credConfig.use_user_gsiproxy = getenv_boolean_flag("EOS_FUSE_USER_GSIPROXY",
                                 false);
  credConfig.use_unsafe_krk5 = getenv_boolean_flag("EOS_FUSE_USER_UNSAFEKRB5",
                               false);
  credConfig.fallback2nobody = getenv_boolean_flag("EOS_FUSE_FALLBACKTONOBODY",
                               false);
  credConfig.tryKrb5First = getenv_boolean_flag("EOS_FUSE_USER_KRB5FIRST", false);

  if (!credConfig.use_user_krb5cc && !credConfig.use_user_gsiproxy) {
    if (getenv("EOS_FUSE_SSS_KEYTAB")) {
      setenv("XrdSecPROTOCOL", "sss,krb5,gsi,unix", 1);
    } else {
      setenv("XrdSecPROTOCOL", "krb5,gsi,unix", 1);
    }
  }

  // Extract MGM endpoint and check availability
  if (!check_mgm(features)) {
    return false;
  }

  // Seed inode translator based on the MGM's inode encoding scheme
  if (features && (*features)["eos.inodeencodingscheme"] == "0") {
    eos_static_notice("The MGM is advertising support for legacy (version 0) inode encoding scheme.");
    gInodeTranslator.InodeToFid(eos::common::FileId::LegacyFidToInode(
                                  1)); // seed translator
  } else if (features && (*features)["eos.inodeencodingscheme"] == "1") {
    eos_static_notice("The MGM is advertising support for new (version 1) inode encoding scheme.");
    gInodeTranslator.InodeToFid(eos::common::FileId::NewFidToInode(
                                  1)); // seed translator
  } else {
    eos_static_notice("Could not determine which inode encoding scheme the MGM is using based on advertised features. Assuming old one. (version 0)");
    gInodeTranslator.InodeToFid(eos::common::FileId::LegacyFidToInode(
                                  1)); // seed translator
  }

  // Get read-ahead configuration
  if (getenv("EOS_FUSE_RDAHEAD") && (!strcmp(getenv("EOS_FUSE_RDAHEAD"), "1"))) {
    do_rdahead = true;

    if (getenv("EOS_FUSE_RDAHEAD_WINDOW")) {
      rdahead_window = getenv("EOS_FUSE_RDAHEAD_WINDOW");

      try {
        (void) std::stol(rdahead_window);
      } catch (const std::exception& e) {
        rdahead_window = "131072"; // default 128
      }
    }
  }

  // Get inline-repair configuration
  if (getenv("EOS_FUSE_INLINE_REPAIR") &&
      (!strcmp(getenv("EOS_FUSE_INLINE_REPAIR"), "1"))) {
    inline_repair = true;

    if (getenv("EOS_FUSE_MAX_INLINE_REPAIR_SIZE")) {
      max_inline_repair_size = strtoul(getenv("EOS_FUSE_MAX_INLINE_REPAIR_SIZE"), 0,
                                       10);
    } else {
      max_inline_repair_size = 268435456; // 256 MB
    }
  }

  encode_pathname = (features && features->count("eos.encodepath"));

  if (getenv("EOS_FUSE_LAZYOPENRO") &&
      (!strcmp(getenv("EOS_FUSE_LAZYOPENRO"), "1"))) {
    lazy_open_ro = true;
  }

  if (getenv("EOS_FUSE_LAZYOPENRW") &&
      (!strcmp(getenv("EOS_FUSE_LAZYOPENRW"), "1"))) {
    lazy_open_rw = true;
  }

  if (getenv("EOS_FUSE_ASYNC_OPEN") &&
      (!strcmp(getenv("EOS_FUSE_ASYNC_OPEN"), "1"))) {
    async_open = true;
  }

  if (getenv("EOS_FUSE_SHOW_SPECIAL_FILES") &&
      (!strcmp(getenv("EOS_FUSE_SHOW_SPECIAL_FILES"), "1"))) {
    hide_special_files = false;
  } else {
    hide_special_files = true;
  }

  if (getenv("EOS_FUSE_SHOW_EOS_ATTRIBUTES") &&
      (!strcmp(getenv("EOS_FUSE_SHOW_EOS_ATTRIBUTES"), "1"))) {
    show_eos_attributes = true;
  } else {
    show_eos_attributes = false;
  }

  if (features && !features->count("eos.lazyopen")) {
    // disable lazy open, no server side support
    lazy_open_ro = false;
    lazy_open_rw = false;
    lazy_open_disabled = true;
  }

  if (getenv("EOS_FUSE_CREATOR_CAP_LIFETIME")) {
    creator_cap_lifetime = (int) strtol(getenv("EOS_FUSE_CREATOR_CAP_LIFETIME"), 0,
                                        10);
  }

  if (getenv("EOS_FUSE_FILE_WB_CACHE_SIZE")) {
    file_write_back_cache_size = (int) strtol(getenv("EOS_FUSE_FILE_WB_CACHE_SIZE"),
                                 0, 10);
  }

  // Check if we should set files executable
  if (getenv("EOS_FUSE_EXEC") && (!strcmp(getenv("EOS_FUSE_EXEC"), "1"))) {
    fuse_exec = true;
  }

  // Initialise the XrdFileCache
  fuse_cache_write = false;

  if ((!(getenv("EOS_FUSE_CACHE"))) ||
      (getenv("EOS_FUSE_CACHE") && (!strcmp(getenv("EOS_FUSE_CACHE"), "0")))) {
    XFC = NULL;
  } else {
    if (!getenv("EOS_FUSE_CACHE_SIZE")) {
      setenv("EOS_FUSE_CACHE_SIZE", "30000000", 1);  // ~300MB
    }

    XFC = FuseWriteCache::GetInstance(static_cast<size_t>(atol(
                                        getenv("EOS_FUSE_CACHE_SIZE"))));
    fuse_cache_write = true;
  }

  if ((getenv("EOS_FUSE_CACHE_PAGE_SIZE"))) {
    CacheEntry::SetMaxSize((size_t)strtoul(getenv("EOS_FUSE_CACHE_PAGE_SIZE"), 0,
                                           10));
  }

// set the path of the proc fs (default is "/proc/"
  gProcCacheShardSize = AuthIdManager::proccachenbins;
  gProcCacheV.resize(gProcCacheShardSize);

  if (getenv("EOS_FUSE_PROCPATH")) {
    std::string pp(getenv("EOS_FUSE_PROCPATH"));

    if (pp[pp.size()] != '/') {
      pp.append("/");
    }

    for (auto it = gProcCacheV.begin(); it != gProcCacheV.end(); ++it) {
      it->SetProcPath(pp.c_str());
    }
  }

  if (authidmanager.StartCleanupThread()) {
    eos_static_notice("started proccache cleanup thread");
  } else {
    eos_static_err("filed to start proccache cleanup thread");
  }

  if (getenv("EOS_FUSE_XRDBUGNULLRESPONSE_RETRYCOUNT")) {
    xrootd_nullresponsebug_retrycount =
      std::max(0, (int)strtoul(getenv("EOS_FUSE_XRDBUGNULLRESPONSE_RETRYCOUNT"), 0,
                               10));
  } else {
    xrootd_nullresponsebug_retrycount = 3; // 256 MB
  }

  if (getenv("EOS_FUSE_XRDBUGNULLRESPONSE_RETRYSLEEPMS")) {
    xrootd_nullresponsebug_retrysleep =
      std::max(0, (int)strtoul(getenv("EOS_FUSE_XRDBUGNULLRESPONSE_RETRYSLEEPMS"), 0,
                               10));
  } else {
    xrootd_nullresponsebug_retrysleep = 1; // 256 MB
  }

  // Get the number of levels in the top hierarchy protected agains deletions
  if (!getenv("EOS_FUSE_RMLVL_PROTECT")) {
    rm_level_protect = 1;
  } else {
    rm_level_protect = atoi(getenv("EOS_FUSE_RMLVL_PROTECT"));
  }

  if (rm_level_protect) {
    rm_watch_relpath = false;
    char rm_cmd[PATH_MAX];
    (void) memset(rm_cmd, '\0', sizeof(rm_cmd));
    FILE* f = popen("exec bash -c 'type -P rm'", "r");
    char format[32];
    snprintf(format, sizeof(format), "%%%ds", PATH_MAX - 1);

    if (!f) {
      eos_static_err("could not run the system wide rm command procedure");
    } else if (fscanf(f, format, rm_cmd) != 1) {
      pclose(f);
      eos_static_err("cannot get rm command to watch");
    } else {
      pclose(f);

      if (strlen(rm_cmd) >= PATH_MAX) {
        eos_static_err("buffer overflow while reading rm command");
      } else {
        eos_static_notice("rm command to watch is %s", rm_cmd);
        rm_command = rm_cmd;
        char cmd[PATH_MAX + 16];
        sprintf(cmd, "%s --version", (const char*)rm_cmd);
        f = popen(cmd, "r");

        if (!f) {
          eos_static_err("could not run the rm command to watch");
        } else {
          char* line = NULL;
          size_t len = 0;

          if (getline(&line, &len, f) == -1) {
            pclose(f);
            eos_static_err("could not read rm command version to watch");
          } else if (line) {
            pclose(f);
            char* lasttoken = strrchr(line, ' ');

            if (lasttoken) {
              float rmver;

              if (!sscanf(lasttoken, "%f", &rmver)) {
                eos_static_err("could not interpret rm command version to watch %s",
                               lasttoken);
              } else {
                int rmmajv = floor(rmver);
                eos_static_notice("top level recursive deletion command to watch "
                                  "is %s, version is %f, major version is %d",
                                  rm_cmd, rmver, rmmajv);

                if (rmmajv >= 8) {
                  rm_watch_relpath = true;
                  eos_static_notice("top level recursive deletion CAN watch "
                                    "relative path removals");
                } else {
                  eos_static_warning("top level recursive deletion CANNOT watch "
                                     "relative path removals");
                }
              }
            }
          }

          free(line);
        }
      }
    }
  }

  authidmanager.setAuth(credConfig);

  if (getenv("EOS_FUSE_MODE_OVERLAY")) {
    mode_overlay = (mode_t)strtol(getenv("EOS_FUSE_MODE_OVERLAY"), 0, 8);
  } else {
    mode_overlay = 0;
  }

#ifndef __APPLE__
  // Get uid and pid specificities of the system
  {
    FILE* f = fopen("/proc/sys/kernel/pid_max", "r");

    if (f && fscanf(f, "%llu", (unsigned long long*)&pid_max)) {
      eos_static_notice("pid_max is %llu", pid_max);
    } else {
      eos_static_err("could not read pid_max in /proc/sys/kernel/pid_max. "
                     "defaulting to 32767");
      pid_max = 32767;
    }

    if (f) {
      fclose(f);
    }

    f = fopen("/etc/login.defs", "r");
    char line[4096];
    line[0] = '\0';
    uid_max = 0;

    while (f && fgets(line, sizeof(line), f)) {
      if (line[0] == '#') {
        continue;  //commented line on the first character
      }

      auto keyword = strstr(line, "UID_MAX");

      if (!keyword) {
        continue;
      }

      auto comment_tag = strstr(line, "#");

      if (comment_tag && comment_tag < keyword) {
        continue;  // commented line with the keyword
      }

      char buffer[4096];

      if (sscanf(line, "%4095s %llu", buffer, (unsigned long long*)&uid_max) != 2) {
        eos_static_err("could not parse line %s in /etc/login.defs", line);
        uid_max = 0;
        continue;
      } else {
        break;
      }
    }

    if (uid_max) {
      eos_static_notice("uid_max is %llu", uid_max);
    } else {
      eos_static_err("could not read uid_max value in /etc/login.defs. defaulting to 65535");
      uid_max = 65535;
    }

    if (f) {
      fclose(f);
    }
  }
#endif
  // Get parameters about strong authentication
  link_pidmap = getenv_boolean_flag("EOS_FUSE_PIDMAP", false);
  eos_static_notice("krb5=%d", credConfig.use_user_krb5cc ? 1 : 0);
  eos_static_notice("starting filesystem");

  if ((XrdSysThread::Run(&tCacheCleanup, fuse_filesystem::CacheCleanup,
                         static_cast<void*>(this),
                         0, "Cache Cleanup Thread"))) {
    eos_static_crit("failed to start cache clean-up thread");
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// this function is just to make it possible to use BSD realpath implementation
//------------------------------------------------------------------------------
size_t
strlcat(char* dst, const char* src, size_t siz)
{
  char* d = dst;
  const char* s = src;
  size_t n = siz;
  size_t dlen;

  /* Find the end of dst and adjust bytes left but don't go past end */
  while (n-- != 0 && *d != '\0') {
    d++;
  }

  dlen = d - dst;
  n = siz - dlen;

  if (n == 0) {
    return (dlen + strlen(s));
  }

  while (*s != '\0') {
    if (n != 1) {
      *d++ = *s;
      n--;
    }

    s++;
  }

  *d = '\0';
  return (dlen + (s - src)); /* count does not include NUL */
}

//------------------------------------------------------------------------------
// this function is a workaround to avoid that
// fuse calls itself while using realpath
// that would require to trace back the user process which made the first call
// to fuse. That would involve keeping track of fuse self call to stat
// especially in is_toplevel_rm
//------------------------------------------------------------------------------
int
fuse_filesystem::mylstat(const char* __restrict name,
                         struct stat* __restrict __buf,
                         pid_t pid)
{
  std::string path(name);

  if ((path.length() >= mount_dir.length()) &&
      (path.find(mount_dir) == 0)) {
    eos_static_debug("name=%s\n", name);
    uid_t uid = 0;
    gid_t gid = 0 ;

    if (!gProcCache(pid).HasEntry(pid) ||
        !gProcCache(pid).GetFsUidGid(pid, uid, gid)) {
      return ESRCH;
    }

    mutex_inode_path.LockRead();
    unsigned long long ino = path2inode.count(name) ? path2inode[name] : 0;
    mutex_inode_path.UnLockRead();
    return this->stat(name, __buf, uid, gid, pid, ino);
  } else {
    return ::lstat(name, __buf);
  }
}

//------------------------------------------------------------------------------
// This is code from taken from BSD implementation. It was just made ok for
// C++ compatibility and regular lstat was replaced with the above mylstat.
//------------------------------------------------------------------------------
char*
fuse_filesystem::myrealpath(const char* __restrict path,
                            char* __restrict resolved,
                            pid_t pid)
{
  struct stat sb;
  char* p, *q, *s;
  size_t left_len, resolved_len;
  unsigned symlinks;
  int m, serrno, slen;
  char left[PATH_MAX], next_token[PATH_MAX], symlink[PATH_MAX];

  if (path == NULL) {
    errno = EINVAL;
    return (NULL);
  }

  if (path[0] == '\0') {
    errno = ENOENT;
    return (NULL);
  }

  serrno = errno;

  if (resolved == NULL) {
    resolved = (char*) malloc(PATH_MAX);

    if (resolved == NULL) {
      return (NULL);
    }

    m = 1;
  } else {
    m = 0;
  }

  symlinks = 0;

  if (path[0] == '/') {
    resolved[0] = '/';
    resolved[1] = '\0';

    if (path[1] == '\0') {
      return (resolved);
    }

    resolved_len = 1;
    left_len = strlcpy(left, path + 1, sizeof(left));
  } else {
    if (getcwd(resolved, PATH_MAX) == NULL) {
      if (m) {
        free(resolved);
      } else {
        resolved[0] = '.';
        resolved[1] = '\0';
      }

      return (NULL);
    }

    resolved_len = strlen(resolved);
    left_len = strlcpy(left, path, sizeof(left));
  }

  if (left_len >= sizeof(left) || resolved_len >= PATH_MAX) {
    if (m) {
      free(resolved);
    }

    errno = ENAMETOOLONG;
    return (NULL);
  }

  /*
   * Iterate over path components in `left'.
   */
  while (left_len != 0) {
    /*
     * Extract the next path component and adjust `left'
     * and its length.
     */
    p = strchr(left, '/');
    s = p ? p : left + left_len;

    if (s - left >= (int) sizeof(next_token)) {
      if (m) {
        free(resolved);
      }

      errno = ENAMETOOLONG;
      return (NULL);
    }

    memcpy(next_token, left, s - left);
    next_token[s - left] = '\0';
    left_len -= s - left;

    if (p != NULL) {
      memmove(left, s + 1, left_len + 1);
    }

    if (resolved[resolved_len - 1] != '/') {
      if (resolved_len + 1 >= PATH_MAX) {
        if (m) {
          free(resolved);
        }

        errno = ENAMETOOLONG;
        return (NULL);
      }

      resolved[resolved_len++] = '/';
      resolved[resolved_len] = '\0';
    }

    if (next_token[0] == '\0') {
      continue;
    } else if (strcmp(next_token, ".") == 0) {
      continue;
    } else if (strcmp(next_token, "..") == 0) {
      /*
       * Strip the last path component except when we have
       * single "/"
       */
      if (resolved_len > 1) {
        resolved[resolved_len - 1] = '\0';
        q = strrchr(resolved, '/') + 1;
        *q = '\0';
        resolved_len = q - resolved;
      }

      continue;
    }

    /*
     * Append the next path component and lstat() it. If
     * lstat() fails we still can return successfully if
     * there are no more path components left.
     */
    resolved_len = strlcat(resolved, next_token, PATH_MAX);

    if (resolved_len >= PATH_MAX) {
      if (m) {
        free(resolved);
      }

      errno = ENAMETOOLONG;
      return (NULL);
    }

    if (mylstat(resolved, &sb, pid) != 0) {
      if (errno == ENOENT && p == NULL) {
        errno = serrno;
        return (resolved);
      }

      if (m) {
        free(resolved);
      }

      return (NULL);
    }

    if (S_ISLNK(sb.st_mode)) {
      if (symlinks++ > MAXSYMLINKS) {
        if (m) {
          free(resolved);
        }

        errno = ELOOP;
        return (NULL);
      }

      slen = ::readlink(resolved, symlink, sizeof(symlink) - 1);

      if (slen < 0) {
        if (m) {
          free(resolved);
        }

        return (NULL);
      }

      symlink[slen] = '\0';

      if (symlink[0] == '/') {
        resolved[1] = 0;
        resolved_len = 1;
      } else if (resolved_len > 1) {
        /* Strip the last path component. */
        resolved[resolved_len - 1] = '\0';
        q = strrchr(resolved, '/') + 1;
        *q = '\0';
        resolved_len = q - resolved;
      }

      /*
       * If there are any path components left, then
       * append them to symlink. The result is placed
       * in `left'.
       */
      if (p != NULL) {
        if (symlink[slen - 1] != '/') {
          if (slen + 1 >= (int) sizeof(symlink)) {
            if (m) {
              free(resolved);
            }

            errno = ENAMETOOLONG;
            return (NULL);
          }

          symlink[slen] = '/';
          symlink[slen + 1] = 0;
        }

        left_len = strlcat(symlink, left, sizeof(left));

        if (left_len >= sizeof(left)) {
          if (m) {
            free(resolved);
          }

          errno = ENAMETOOLONG;
          return (NULL);
        }
      }

      left_len = strlcpy(left, symlink, sizeof(left));
    }
  }

  /*
   * Remove trailing slash except when the resolved pathname
   * is a single "/".
   */
  if (resolved_len > 1 && resolved[resolved_len - 1] == '/') {
    resolved[resolved_len - 1] = '\0';
  }

  return (resolved);
}
