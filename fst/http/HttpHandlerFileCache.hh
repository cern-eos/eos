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
#include <deque>
#include <memory>
#include <string>

EOSFSTNAMESPACE_BEGIN

class HttpHandlerFileCache {
public:

  struct Key
  {
    Key() { }

    Key(const std::string &name, const std::string &url,
        const std::string &query, XrdSfsFileOpenMode &omode) :
      name_(name), url_(url), query_(query), omode_(omode) { }

    set(const std::string &name, const std::string &url,
        const std::string &query, XrdSfsFileOpenMode &omode)
    {
      name_  = name;
      url_   = url;
      query_ = query;
      omode_ = omode;
    }

    explicit operator bool() const
    {
      return !url_.empty();
    }

    bool operator==(const Key& other) const
    {
      return name_  == other.name_ &&
             url_   == other.url_  &&
             query_ == other.query_ &&
             omode_ == other.omode_;
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

  bool           insert(const Entry &e);
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
  std::deque<Entry> mQueue;
};

EOSFSTNAMESPACE_END
