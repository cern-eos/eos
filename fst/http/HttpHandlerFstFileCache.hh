// ----------------------------------------------------------------------
// File: HttpHandlerFstFileCache.hh
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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

#pragma once

#include "common/AssistedThread.hh"
#include "fst/XrdFstOfsFile.hh"
#include <XrdSfs/XrdSfsInterface.hh>
#include <XrdSys/XrdSysPthread.hh>
#include <sys/time.h>
#include <list>
#include <map>
#include <memory>
#include <string>

EOSFSTNAMESPACE_BEGIN

/**
 * Class responsible for caching XrdFstOfsFile* of open files.
 * The HttpHandler uses this when handling read requests for byte
 * ranges. It is assumed that another request will shortly
 * arrive for the same file for a different byte range and it is less
 * costly to keep the file open than to re-open it for each request.
 * HttpHandler uses one instance of this class.
 *
 * This cache may contain multiple open XrdFstOfsFile* for the same file
 * because there could be concurrent requests for a given file and
 * it is not safe to issue read on the same object concurrently.
 * For this reason entries are removed from the cache while being used.
 * After use each of these XrdFstOfsFile* may be added or re-added
 * (still open) to the cache.
 *
 * Both the filename and the query portion of the url are a
 * part of the cache Entries' Keys, so that they Key includes the
 * cap.msg (i.e. the key will be specific to an MGM redirection).
 * For this caching to be useful it is needed that the client uses
 * something like Davix's Redirect Cache, to repeatly issue requests
 * for a file without going back to the MGM each time. We may keep
 * the cached file open longer than the cap validity time, as the cap
 * only needs to be valid at the time the file was opened.
 *
 * XrdFstOfsFile* are inserted in the cache while not being used, and removed
 * from the cache to be used. For Keys with multiple Entries the
 * most recetntly inserted is removed and returned.
 *
 * This cache is implemented using two containers. A std::list of
 * Entry keeps entries ordered by insert time (oldest at the front).
 * A std::multimap maps Key to an iterator pointing to an element
 * in the std::list. The Entry in the list also contains a backpointer
 * to the multimap. Within the multimap Entries with identical Keys
 * will be kept in the order inserted, i.e. oldest at the front of an
 * equal_range. The map is used to find and remove an entry given a Key.
 * The list is used to remove old entries because too full or the
 * entry is classed as unused.
 *
 * Removal of entries which are unused (too old) is done by a watcher
 * thread.
 */
class HttpHandlerFstFileCache {
public:

  struct EntryGuard;

  /**
   * Object to represent Entry's Key.
   */
  struct Key
  {
    Key() : omode_(0) { }

    Key(const std::string &name, const std::string &url,
        const std::string &query, XrdSfsFileOpenMode omode) :
      name_(name), url_(url), query_(query), omode_(omode) { }

    void clear()
    {
      name_.clear();
      url_.clear();
      query_.clear();
      omode_ = 0;
    }

    explicit operator bool() const
    {
      return !url_.empty();
    }

    bool operator==(const Key& other) const
    {
      return omode_ == other.omode_ &&
             url_   == other.url_   &&
             query_ == other.query_ &&
             name_  == other.name_;
    }

    bool operator<(const Key &rhs) const
    {
      if (omode_ < rhs.omode_) return true;
      if (omode_ > rhs.omode_) return false;
      int c = url_.compare(rhs.url_);
      if (c<0) return true;
      if (c>0) return false;
      c = query_.compare(rhs.query_);
      if (c<0) return true;
      if (c>0) return false;
      c = name_.compare(rhs.name_);
      if (c<0) return true;
      return false;
    }

    std::string name_;
    std::string url_;
    std::string query_;
    XrdSfsFileOpenMode omode_;
  };

  /**
   * Object to represent cache Entry data.
   */
  struct Entry {
    Entry() : itime_(0), fp_(0) { }

    void set(const Key &k, XrdFstOfsFile* const v) {
      key_ = k;
      fp_ = v;
      itime_ = 0;
    }

    void clear() {
      key_.clear();
      itime_ = 0;
      fp_ = 0;
    }

    XrdFstOfsFile *getfp() {
      return fp_;
    }

    explicit operator bool() const
    {
      return key_ && fp_;
    }

    Key key_;
    uint64_t itime_;
    XrdFstOfsFile *fp_;
    std::multimap<Key, std::list<EntryGuard>::iterator>::iterator mapitr_;
  };

  /**
   * Object that is inserted into the cache. This contains an Entry and acts as
   * an lifetime guared for the XrdFstOfsFile* part of the entry while it is cached.
   */
  struct EntryGuard {
    EntryGuard(const Entry &e) : entry_(e), own_(true) { }
    ~EntryGuard() {
      if (own_ && entry_.fp_) {
        entry_.fp_->close();
        delete entry_.fp_;
      }
    }

    // no copying
    EntryGuard(const EntryGuard &) = delete;
    EntryGuard& operator=(const EntryGuard &) = delete;

    Entry release() {
      own_ = false;
      return entry_;
    }

    Entry &get() {
      return entry_;
    }

    const Entry *operator->() const {
      return &entry_;
    }

    bool own_;
    Entry entry_;
  };

  HttpHandlerFstFileCache();
  ~HttpHandlerFstFileCache();

  /**
   * Run the file cache watcher thread
   */
  void Run(ThreadAssistant& assistant) noexcept;

  /**
   * Insert and entry into the cache.
   */
  bool           insert(const Entry &e);

  /**
   * Remove an entry from the cache.
   */
  Entry          remove(const Key &k);

  typedef std::list<EntryGuard> GuardList;
  typedef std::multimap<Key, std::list<EntryGuard>::iterator> KeyMap;

private:
  XrdSysMutex    mCacheLock;
  AssistedThread mThreadId;
  bool           mThreadActive;
  size_t         mMaxEntries;
  uint64_t       mMaxIdletimeMs;
  uint64_t       mIdletimeResMs;
  GuardList      mQueue;
  KeyMap         mQmap;
};

EOSFSTNAMESPACE_END
