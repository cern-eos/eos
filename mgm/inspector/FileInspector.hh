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
#include <XrdOuc/XrdOucErrInfo.hh>
#include "mgm/Namespace.hh"
#include "namespace/interface/IFileMD.hh"
#include <XrdOuc/XrdOucString.hh>
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
    interval; //< Run FileInspector cleanup every this many seconds
  };

  enum LockFsView : bool {
    Off = false,
    On  = true
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

  void Dump(std::string& out, std::string_view options,
            const LockFsView lockfsview);

  Options getOptions(const LockFsView lockfsview);

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

  const std::string currencies[6] = { "EOS", "CHF", "EUR", "USD", "AUD", "YEN" };

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

  //! BirthVsAccess Time Bins
  std::map<time_t, std::map<time_t,uint64_t>> lastBirthVsAccessTimeFiles;
  std::map<time_t, std::map<time_t,uint64_t>> lastBirthVsAccessTimeVolume;
  std::map<time_t, std::map<time_t,uint64_t>> currentBirthVsAccessTimeFiles;
  std::map<time_t, std::map<time_t,uint64_t>> currentBirthVsAccessTimeVolume;

  //! User Cost Bins
  std::map<uid_t, uint64_t> lastUserCosts[2];
  std::map<uid_t, uint64_t> currentUserCosts[2];
  std::multimap<uint64_t,uid_t> lastCostsUsers[2];
  
  //! Group Cost Bins
  std::map<gid_t, uint64_t> lastGroupCosts[2];
  std::map<gid_t, uint64_t> currentGroupCosts[2];
  std::multimap<uint64_t,gid_t> lastCostsGroups[2];
  
  double lastUserTotalCosts[2] = {0};
  double lastGroupTotalCosts[2] = {0};

  //! User Bytes Bins
  std::map<uid_t, uint64_t> lastUserBytes[2];
  std::map<uid_t, uint64_t> currentUserBytes[2];
  std::multimap<uint64_t,uid_t> lastBytesUsers[2];

  //! Group Bytes Bins
  std::map<gid_t, uint64_t> lastGroupBytes[2];
  std::map<gid_t, uint64_t> currentGroupBytes[2];
  std::multimap<uint64_t,gid_t> lastBytesGroups[2];
  
  double lastUserTotalBytes[2] = {0};
  double lastGroupTotalBytes[2] = {0};

  //! Running count of number of time files have been classed faulty
  uint64_t currentNumFaultyFiles = 0;
  
  std::atomic<double> PriceTbPerYearDisk;
  std::atomic<double> PriceTbPerYearTape;

  std::string currency;
  
  std::atomic<time_t> timeCurrentScan;
  std::atomic<time_t> timeLastScan;

  std::atomic<double> scanned_percent;
  std::atomic<uint64_t> nfiles;
  std::atomic<uint64_t> ndirs;

  std::mutex mutexScanStats;
  std::string mSpaceName; ///< Corresponding space name

  //! Maximum number of classifications of faulty files to record
  static constexpr uint64_t maxfaulty = 1'000'000;
};


EOSMGMNAMESPACE_END
