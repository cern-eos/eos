// ----------------------------------------------------------------------
// File: ReplicationTracker.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "common/VirtualIdentity.hh"
#include "common/AssistedThread.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include "mgm/Namespace.hh"
#include "namespace/interface/IFileMD.hh"
#include "XrdOuc/XrdOucString.hh"
#include <atomic>
#include <memory>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class tracking the sanity of created files
//------------------------------------------------------------------------------

class ReplicationTracker {
public:

  struct Options {
    bool enabled;                  //< Is CreationTracking even enabled?
    uint64_t atomic_cleanup_age;   //< Age when atomic files will be auto-cleaned
    std::chrono::seconds interval; //< Run CreationTracking cleanup every this many seconds
  };

  ReplicationTracker(const char* path);
  virtual ~ReplicationTracker();

  void Create(std::shared_ptr<eos::IFileMD> fmd);
  std::string ConversionPolicy(bool injection, int fsid);
  void Commit(std::shared_ptr<eos::IFileMD> fmd);
  void Validate(std::shared_ptr<eos::IFileMD> fmd);

  void Scan(uint64_t atomic_age, bool cleanup, std::string* out=0);

  std::string Prefix(std::shared_ptr<eos::IFileMD> fmd);

  static ReplicationTracker* Create(const char* path) {
    return new ReplicationTracker(path);
  }

  bool enabled() { return (mEnabled.load())?true:false; }
  bool disable() { if (!enabled()) {return false;} else {mEnabled.store(0, std::memory_order_seq_cst); return true;}}
  bool enable()  { if  (enabled()) {return false;} else {mEnabled.store(1, std::memory_order_seq_cst); return true;}}

  bool conversion_enabled() { return (mConversionEnabled.load())?true:false; }
  bool conversion_disable() { if (!conversion_enabled()) {return false;} else {mConversionEnabled.store(0, std::memory_order_seq_cst); return true;}}
  bool conversion_enable()  { if  (conversion_enabled()) {return false;} else {mConversionEnabled.store(1, std::memory_order_seq_cst); return true;}}

  Options getOptions();

private:
  AssistedThread mThread; ///< thread id of the creation background tracker

  void backgroundThread(ThreadAssistant& assistant) noexcept;

  std::atomic<int> mEnabled;
  std::atomic<int> mConversionEnabled;
  XrdOucErrInfo mError;
  eos::common::VirtualIdentity mVid;
  std::string mPath;
};


EOSMGMNAMESPACE_END
