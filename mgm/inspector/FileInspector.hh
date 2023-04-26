// ----------------------------------------------------------------------
// File: FileInspector.hh
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
#include <mutex>

namespace qclient
{
class QClient;
}


EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class tracking the sanity of created files
//------------------------------------------------------------------------------

class FileInspector
{
public:
  struct Options {
    bool enabled; //< Is FileInspector even enabled?
    std::chrono::seconds
    interval; //< Run FileInsepctor cleanup every this many seconds
  };

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param space_name corresponding space name
  //----------------------------------------------------------------------------
  FileInspector(std::string_view space_name);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  virtual ~FileInspector();

  //----------------------------------------------------------------------------
  // Perform a single inspector cycle, QDB namespace
  //----------------------------------------------------------------------------
  void performCycleQDB(ThreadAssistant& assistant) noexcept;

  void Dump(std::string& out, std::string_view options);

  Options getOptions();

  inline bool enabled()
  {
    return mEnabled.load();
  }

  bool disable()
  {
    if (!enabled()) {
      return false;
    } else {
      mEnabled.store(0, std::memory_order_seq_cst);
      return true;
    }
  }

  bool enable()
  {
    if (enabled()) {
      return false;
    } else {
      mEnabled.store(1, std::memory_order_seq_cst);
      return true;
    }
  }

private:
  void backgroundThread(ThreadAssistant& assistant) noexcept;
  void Process(std::shared_ptr<eos::IFileMD> fmd);

  AssistedThread mThread; ///< thread id of the creation background tracker
  std::atomic<int> mEnabled;
  XrdOucErrInfo mError;
  eos::common::VirtualIdentity mVid;
  std::unique_ptr<qclient::QClient> mQcl;

  // Counters for the last and current scan by layout id
  std::map<uint64_t, std::map<std::string, uint64_t>> lastScanStats;
  std::map<uint64_t, std::map<std::string, uint64_t>> currentScanStats;
  //! Map from types of failures to pairs of fid and layoutid
  std::map<std::string, std::set<std::pair<uint64_t, uint64_t>>> lastFaultyFiles;
  //! Map from types of failures to pairs of fid and layoutid
  std::map<std::string, std::set<std::pair<uint64_t, uint64_t>>>
  currentFaultyFiles;
  //! Access Time Bins
  std::map<time_t, uint64_t> lastAccessTimeFiles;
  std::map<time_t, uint64_t> lastAccessTimeVolume;
  std::map<time_t, uint64_t> currentAccessTimeFiles;
  std::map<time_t, uint64_t> currentAccessTimeVolume;

  //! Birth Time Bins
  std::map<time_t, uint64_t> lastBirthTimeFiles;
  std::map<time_t, uint64_t> lastBirthTimeVolume;
  std::map<time_t, uint64_t> currentBirthTimeFiles;
  std::map<time_t, uint64_t> currentBirthTimeVolume;

  time_t timeCurrentScan;
  time_t timeLastScan;

  std::atomic<double> scanned_percent;
  uint64_t nfiles;
  uint64_t ndirs;

  std::mutex mutexScanStats;
  std::string mSpaceName; ///< Corresponding space name
};


EOSMGMNAMESPACE_END
