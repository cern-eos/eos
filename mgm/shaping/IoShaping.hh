//  File: IoShaping.hh
//  Author: Ilkay Yanar - 42Lausanne / CERN
//  ----------------------------------------------------------------------

/*************************************************************************
 *  EOS - the CERN Disk Storage System                                   *
 *  Copyright (C) 2025 CERN/Switzerland                                  *
 *                                                                       *
 *  This program is free software: you can redistribute it and/or modify *
 *  it under the terms of the GNU General Public License as published by *
 *  the Free Software Foundation, either version 3 of the License, or    *
 *  (at your option) any later version.                                  *
 *                                                                       *
 *  This program is distributed in the hope that it will be useful,      *
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 *  GNU General Public License for more details.                         *
 *                                                                       *
 *  You should have received a copy of the GNU General Public License    *
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 *************************************************************************/

#pragma once

#include "common/AssistedThread.hh"
#include "common/Logging.hh"
#include "ioMonitor/include/IoMonitor.hh"
#include "mgm/Namespace.hh"
#include "proto/IoBuffer.pb.h"
#include "proto/Shaping.pb.h"
#include <google/protobuf/util/json_util.h>

EOSMGMNAMESPACE_BEGIN

//----------------------------------------------------------------------------
/// structure that represents a limit for a single app/uid/gid
//----------------------------------------------------------------------------
struct Limiter {
  struct limitsData {
    limitsData() : limit(0), isEnable(false), isTrivial(false) {};

    limitsData(size_t limit, bool enable = false, bool trivial = false)
        : limit(limit)
        , isEnable(enable)
        , isTrivial(trivial) {};

    size_t limit;
    bool isEnable;
    bool isTrivial;
  };

  //----------------------------------------------------------------------------
  /// All limited apps in read/write
  //----------------------------------------------------------------------------
  std::map<std::string, limitsData> rApps;
  std::map<std::string, limitsData> wApps;

  //----------------------------------------------------------------------------
  /// All limited uids in read/write
  //----------------------------------------------------------------------------
  std::map<uid_t, limitsData> rUids;
  std::map<uid_t, limitsData> wUids;

  //----------------------------------------------------------------------------
  /// All limited gids in read/write
  //----------------------------------------------------------------------------
  std::map<gid_t, limitsData> rGids;
  std::map<gid_t, limitsData> wGids;
};

class IoShaping : public eos::common::LogId {
private:
  //--------------------------------------------
  /// Thread that takes data from all FSTs
  /// and aggregates them all into one
  //--------------------------------------------
  AssistedThread _mReceivingThread;

  //--------------------------------------------
  /// Thread that publishes the IoMonitor
  /// configuration to each FST
  //--------------------------------------------
  AssistedThread _mPublishingThread;

  //--------------------------------------------
  /// Thread that calculates, using the _scaler
  /// and limiter variables, the limits that must
  /// be returned to the FSTs
  //--------------------------------------------
  AssistedThread _mShapingThread;

  //--------------------------------------------
  /// Variables to manage multithreading
  //--------------------------------------------
  mutable std::mutex _mSyncThread;
  std::atomic<bool> _mReceiving;
  std::atomic<bool> _mPublishing;
  std::atomic<bool> _mShaping;

  //--------------------------------------------
  /// The aggregated data of the FSTs
  //--------------------------------------------
  IoBuffer::Summaries _shapings;

  //--------------------------------------------
  /// The limits calculated by _mShapingThread
  /// and send back to the FSTs
  //--------------------------------------------
  Shaping::Scaler _scaler;

  //--------------------------------------------
  /// Where the limits are stored
  //--------------------------------------------
  Limiter _limiter;

  //--------------------------------------------
  /// The time the Receiving thread has to wait
  //--------------------------------------------
  std::atomic<size_t> _receivingTime;

  //----------------------------------------------------------------------------
  /// Extracts the data from each node
  /// every "_receivingTime" (aka std::atomic<size_t>) second.
  ///
  /// @param assistant reference to thread object
  //----------------------------------------------------------------------------
  void receive(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  /// Publish trafic data to each node
  /// every "_receivingTime" (aka std::atomic<size_t>) second.
  ///
  /// @param assistant reference to thread object
  //----------------------------------------------------------------------------
  void publishing(ThreadAssistant& assistant);

  //----------------------------------------------------------------------------
  /// Calculates, using the _scaler
  /// and limiter variables, the limits that must
  /// be returned to the FSTs
  /// every "_receivingTime" (aka std::atomic<size_t>) second.
  ///
  /// @param assistant reference to thread object
  //----------------------------------------------------------------------------
  void shaping(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  /// Aggregates data from IoBuffer (aka the protobuf version of ioStatSummary)
  ///
  /// @param data reference to vector of IoBuffer
  /// @return The final aggregate variable
  //----------------------------------------------------------------------------
  IoBuffer::Summaries aggregateSummaries(std::vector<IoBuffer::Summaries>& data);

  //----------------------------------------------------------------------------
  /// Function that calculates the limits for each app/uid/gid
  //----------------------------------------------------------------------------
  bool calculateScalerNodes();

public:
  //--------------------------------------------
  /// Orthodoxe canonical form
  //--------------------------------------------

  //--------------------------------------------
  /// Main constructor
  //--------------------------------------------
  IoShaping(size_t = 5);

  //--------------------------------------------
  /// Destructor
  //--------------------------------------------
  ~IoShaping();

  //--------------------------------------------
  /// Constructor by copy constructor
  //--------------------------------------------
  IoShaping(const IoShaping& other);

  //--------------------------------------------
  /// Overload the operator =
  //--------------------------------------------
  IoShaping& operator=(const IoShaping& other);

  //----------------------------------------------------------------------------
  /// Start receiving thread
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool startReceiving();

  //----------------------------------------------------------------------------
  /// Stop receiving thread
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool stopReceiving();

  //----------------------------------------------------------------------------
  /// Start publising thread
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool startPublishing();

  //----------------------------------------------------------------------------
  /// Stop publising thread
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool stopPublishing();

  //----------------------------------------------------------------------------
  /// Start shaping thread
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool startShaping();

  //----------------------------------------------------------------------------
  /// Stop shaping thread
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool stopShaping();

  //----------------------------------------------------------------------------
  /// Set new receiving time
  //----------------------------------------------------------------------------
  void setReceivingTime(size_t);

  //----------------------------------------------------------------------------
  /// Get _shaping variable
  //----------------------------------------------------------------------------
  IoBuffer::Summaries getShaping() const;

  //----------------------------------------------------------------------------
  /// Get _scaler variable
  //----------------------------------------------------------------------------
  Shaping::Scaler getScaler() const;

  //----------------------------------------------------------------------------
  /// Get _limiter variable
  //----------------------------------------------------------------------------
  Limiter getLimiter() const;

  //----------------------------------------------------------------------------
  /// Add a new window to the MGM ioMonitor configuration
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool addWindow(size_t);

  //----------------------------------------------------------------------------
  /// Remove a new window from the MGM ioMonitor configuration
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool rm(size_t winTime);

  //----------------------------------------------------------------------------
  /// Remove all the apps limit from the MGM ioMonitor configuration
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool rmAppsLimit();

  //----------------------------------------------------------------------------
  /// Remove all the uids limit from the MGM ioMonitor configuration
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool rmUidsLimit();

  //----------------------------------------------------------------------------
  /// Remove all the gids limit from the MGM ioMonitor configuration
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool rmGidsLimit();

  //----------------------------------------------------------------------------
  /// Set the new "on"/"off" status to a app limit
  ///
  /// @param app the target app limit
  /// @param rw context of "read" or "write"
  /// @param status the new status to set
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  template <typename T>
  bool setStatus(T app, const std::string rw, bool status) noexcept {
    std::lock_guard<std::mutex> lock(_mSyncThread);

    if (rw == "read" && _limiter.rApps.find(app) != _limiter.rApps.end()) {
      _limiter.rApps[app].isEnable = status;
    } else if (rw == "write" && _limiter.wApps.find(app) != _limiter.wApps.end()) {
      _limiter.wApps[app].isEnable = status;
    } else {
      return false;
    }
    return true;
  }

  //----------------------------------------------------------------------------
  /// Set the new "on"/"off" status to a uid/gid limit
  ///
  /// @param type keep the context of UID/GID
  /// @param id the target uid/gid limit
  /// @param rw context of "read" or "write"
  /// @param status the new status to set
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  template <typename T>
  bool setStatus(const io::TYPE type, T id, const std::string rw, bool status) noexcept {
    std::lock_guard<std::mutex> lock(_mSyncThread);

    if (type == io::TYPE::UID) {
      if (rw == "read" && _limiter.rUids.find(id) != _limiter.rUids.end()) {
        _limiter.rUids[id].isEnable = status;
      } else if (rw == "write" && _limiter.wUids.find(id) != _limiter.wUids.end()) {
        _limiter.wUids[id].isEnable = status;
      } else {
        return false;
      }
    } else if (type == io::TYPE::GID) {
      if (rw == "read" && _limiter.rGids.find(id) != _limiter.rGids.end()) {
        _limiter.rGids[id].isEnable = status;
      } else if (rw == "write" && _limiter.wGids.find(id) != _limiter.wGids.end()) {
        _limiter.wGids[id].isEnable = status;
      } else {
        return false;
      }
    } else {
      return false;
    }
    return true;
  }

  //----------------------------------------------------------------------------
  /// Set the new "isTrivial" bool variable to a uid/gid limit
  ///
  /// @param type keep the context of UID/GID
  /// @param id the target uid/gid limit
  /// @param rw context of "read" or "write"
  /// @param isTrivial the new trivial to set
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  template <typename T>
  bool setTrivial(const io::TYPE type, T id, const std::string rw, bool isTrivial) noexcept {
    std::lock_guard<std::mutex> lock(_mSyncThread);

    if (type == io::TYPE::UID) {
      if (rw == "read" && _limiter.rUids.find(id) != _limiter.rUids.end()) {
        _limiter.rUids[id].isTrivial = isTrivial;
      } else if (rw == "write" && _limiter.wUids.find(id) != _limiter.wUids.end()) {
        _limiter.wUids[id].isTrivial = isTrivial;
      } else {
        return false;
      }
    } else if (type == io::TYPE::GID) {
      if (rw == "read" && _limiter.rGids.find(id) != _limiter.rGids.end()) {
        _limiter.rGids[id].isTrivial = isTrivial;
      } else if (rw == "write" && _limiter.wGids.find(id) != _limiter.wGids.end()) {
        _limiter.wGids[id].isTrivial = isTrivial;
      } else {
        return false;
      }
    } else {
      return false;
    }
    return true;
  }

  //----------------------------------------------------------------------------
  /// Set the new "isTrivial" bool variable to a app limit
  ///
  /// @param id the target app limit
  /// @param rw context of "read" or "write"
  /// @param isTrivial the new trivial to set
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  template <typename T>
  bool setTrivial(T id, const std::string rw, bool isTrivial) noexcept {
    std::lock_guard<std::mutex> lock(_mSyncThread);

    if (rw == "read" && _limiter.rApps.find(id) != _limiter.rApps.end()) {
      _limiter.rApps[id].isTrivial = isTrivial;
    } else if (rw == "write" && _limiter.wApps.find(id) != _limiter.wApps.end()) {
      _limiter.wApps[id].isTrivial = isTrivial;
    } else {
      return false;
    }
    return true;
  }

  //----------------------------------------------------------------------------
  /// Set a limit for a uid/gid
  ///
  /// @param type keep the context of UID/GID
  /// @param id the target uid/gid
  /// @param limits the new limit to set
  /// @param rw context of "read" or "write"
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  template <typename T>
  bool setLimiter(const io::TYPE type, T id, size_t limits, const std::string rw) noexcept {
    std::lock_guard<std::mutex> lock(_mSyncThread);

    if (type == io::TYPE::UID) {
      if (rw == "read") {
        _limiter.rUids[id].limit = limits * 1000000;
      } else if (rw == "write") {
        _limiter.wUids[id].limit = limits * 1000000;
      } else {
        return false;
      }
    } else if (type == io::TYPE::GID) {
      if (rw == "read") {
        _limiter.rGids[id].limit = limits * 1000000;
      } else if (rw == "write") {
        _limiter.wGids[id].limit = limits * 1000000;
      } else {
        return false;
      }
    } else {
      return false;
    }
    return true;
  }

  //----------------------------------------------------------------------------
  /// Set a limit for a app
  ///
  /// @param app the target app
  /// @param limits the new limit to set
  /// @param rw context of "read" or "write"
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  template <typename T>
  bool setLimiter(T app, size_t limits, const std::string rw) noexcept {
    std::lock_guard<std::mutex> lock(_mSyncThread);
    if (rw == "read") {
      _limiter.rApps[app].limit = limits * 1000000;
    } else if (rw == "write") {
      _limiter.wApps[app].limit = limits * 1000000;
    } else {
      return false;
    }
    return true;
  }

  //----------------------------------------------------------------------------
  /// Remove a uid/gid limit
  ///
  /// @param type keep the context of UID/GID
  /// @param id the target uid/gid
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  template <typename T>
  bool rmLimit(io::TYPE type, T id) {
    std::lock_guard<std::mutex> lock(_mSyncThread);

    if (type == io::TYPE::UID) {
      if (_limiter.rUids.find(id) == _limiter.rUids.end() && _limiter.wUids.find(id) == _limiter.wUids.end()) {
        return false;
      }
      if (_limiter.rUids.find(id) != _limiter.rUids.end()) { _limiter.rUids.erase(_limiter.rUids.find(id)); }
      if (_limiter.wUids.find(id) != _limiter.wUids.end()) { _limiter.wUids.erase(_limiter.wUids.find(id)); }
    } else if (type == io::TYPE::GID) {
      if (_limiter.rGids.find(id) == _limiter.rGids.end() && _limiter.wGids.find(id) == _limiter.wGids.end()) {
        return false;
      }
      if (_limiter.rGids.find(id) != _limiter.rGids.end()) { _limiter.rGids.erase(_limiter.rGids.find(id)); }
      if (_limiter.wGids.find(id) != _limiter.wGids.end()) { _limiter.wGids.erase(_limiter.wGids.find(id)); }
    } else {
      return false;
    }
    return true;
  }

  //----------------------------------------------------------------------------
  /// Remove a app limit
  ///
  /// @param appName the target app
  ///
  /// @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  template <typename T>
  bool rmLimit(T appName) {
    std::lock_guard<std::mutex> lock(_mSyncThread);

    if (_limiter.rApps.find(appName) == _limiter.rApps.end() && _limiter.wApps.find(appName) == _limiter.wApps.end()) {
      return false;
    }

    if (_limiter.rApps.find(appName) != _limiter.rApps.end()) { _limiter.rApps.erase(_limiter.rApps.find(appName)); }
    if (_limiter.wApps.find(appName) != _limiter.wApps.end()) { _limiter.wApps.erase(_limiter.wApps.find(appName)); }
    return true;
  }
};

EOSMGMNAMESPACE_END
