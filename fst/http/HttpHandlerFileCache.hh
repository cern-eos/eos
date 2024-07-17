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
#include "fst/XrdFstOfsFile.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include <string>

EOSFSTNAMESPACE_BEGIN

class HttpHandlerFileCache {
public:

  struct Key {
    Key() { }
    Key(const std::string &url, const std::string &query, XrdSfsFileOpenMode &omode) :
      url_(url), query_(query), omode_(omode) { }
    set(const std::string &url, const std::string &query, XrdSfsFileOpenMode &omode) {
      url_ = url;
      query_ = query;
      omode_ = omode;
    }
    explicit operator bool() const {
      return !url_.empty();
    }

    std::string url_;
    std::string query_;
    XrdSfsFileOpenMode omode_;
  };

  HttpHandlerFileCache();
  ~HttpHandlerFileCache();

  /**
   * Run the file cache watcher thread
   */
  void Run(ThreadAssistant& assistant) noexcept;

  bool insert(const Key &k, XrdFstOfsFile* const v);
  XrdFstOfsFile* remove(const Key &k);

private:
  AssistedThread     mThreadId;
};

EOSFSTNAMESPACE_END
