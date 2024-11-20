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
#include "mgm/inspector/FileInspectorStats.hh"
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

  inline bool disable()
  {
    bool expected = true;
    return mEnabled.compare_exchange_strong(expected, false);
  }

  inline bool enable()
  {
    bool expected = false;
    return mEnabled.compare_exchange_strong(expected, true);
  }

  const std::string currencies[6] = { "EOS", "CHF", "EUR", "USD", "AUD", "YEN" };

private:
  void backgroundThread(ThreadAssistant& assistant) noexcept;
  void Process(std::shared_ptr<eos::IFileMD> fmd);

  AssistedThread mThread; ///< thread id of the creation background tracker
  std::atomic<bool> mEnabled;
  XrdOucErrInfo mError;
  eos::common::VirtualIdentity mVid;
  std::unique_ptr<qclient::QClient> mQcl;

  FileInspectorStats mCurrentStats;
  FileInspectorStats mLastStats;

  std::atomic<double> PriceTbPerYearDisk;
  std::atomic<double> PriceTbPerYearTape;

  std::string currency;

  std::atomic<double> scanned_percent;
  std::atomic<uint64_t> nfiles;
  std::atomic<uint64_t> ndirs;

  std::mutex mutexScanStats;
  std::string mSpaceName; ///< Corresponding space name

  //! Maximum number of classifications of faulty files to record
  static constexpr uint64_t maxfaulty = 1'000'000;
};


EOSMGMNAMESPACE_END
