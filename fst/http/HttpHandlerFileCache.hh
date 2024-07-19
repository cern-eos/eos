// ----------------------------------------------------------------------
// File: HttpHandlerFileCache.hh
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
#include "common/SymKeys.hh"
#include "fst/XrdFstOfsFile.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <list>
#include <map>
#include <memory>
#include <string>
#include <cstring>

EOSFSTNAMESPACE_BEGIN

class HttpHandlerFileCache {
public:

  struct Key
  {
    Key() : omode_(0) { }

    Key(const std::string &name, const std::string &url,
        const std::string &query, XrdSfsFileOpenMode omode) :
      name_(name), url_(url), query_(query), omode_(omode) { }

    set(const std::string &name, const std::string &url,
        const std::string &query, XrdSfsFileOpenMode omode)
    {
      name_  = name;
      url_   = url;
      query_ = query;
      omode_ = omode;
    }

    void clear()
    {
      name_.clear();
      url_.clear();
      query_.clear();
      omode_ = 0;
    }

    explicit operator bool() const
    {
      return !url_.empty() && !query_.empty();
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
      int c = ::strcmp(url_.c_str(), rhs.url_.c_str());
      if (c<0) return true;
      if (c>0) return false;
      c = ::strcmp(query_.c_str(), rhs.query_.c_str());
      if (c<0) return true;
      if (c>0) return false;
      c = ::strcmp(name_.c_str(), rhs.name_.c_str());
      if (c<0) return true;
      return false;
    }

    std::string name_;
    std::string url_;
    std::string query_;
    XrdSfsFileOpenMode omode_;
  };

  struct Entry {

    Entry() : cvalid(0), fp(0) { }

    bool set(const Key &k, XrdFstOfsFile* const v) {
      if (getCapValid(k.query_, cvalid)) return false;
      key = k;
      fp = v;
      return true;
    }

    void clear() {
      key.clear();
      fp = 0;
      cvalid = 0;
    }

    explicit operator bool() const
    {
      return key && fp;
    }

    Key key;
    time_t cvalid;
    XrdFstOfsFile *fp;
  };

  HttpHandlerFileCache();
  ~HttpHandlerFileCache();

  /**
   * Run the file cache watcher thread
   */
  void Run(ThreadAssistant& assistant) noexcept;

  bool           insert(Entry &e);
  Entry          remove(const Key &k);

private:
  static int getCapValid(const std::string &q, time_t &valid) {

    XrdOucEnv openOpaque(q.c_str());
    XrdOucEnv *dp{nullptr};
    const int caprc = eos::common::SymKey::ExtractCapability(&openOpaque, dp);
    std::unique_ptr<XrdOucEnv> decCap(dp);

    if (caprc) return EINVAL;

    // Check time validity
    if (!decCap->Get("cap.valid")) {
      // validity missing
      return EINVAL;
    } else {
      valid = atoi(decCap->Get("cap.valid"));
      return 0;
    }
  }

  XrdSysMutex    mCacheLock;
  AssistedThread mThreadId;
  bool           mThreadActive;
  size_t         mMaxEntries;
  time_t         mMaxLifetime;
  std::list<Entry> mQueue;
  std::map<Key, std::list<Entry>::iterator> mQmap;
};

EOSFSTNAMESPACE_END
