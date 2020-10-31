// ----------------------------------------------------------------------
// File: TapeGc.hh
// Author: Steven Murray - CERN
// ----------------------------------------------------------------------

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

#ifndef __EOSMGM_TAPEGC_HH__
#define __EOSMGM_TAPEGC_HH__

#include "common/Logging.hh"
#include "mgm/Namespace.hh"
#include "mgm/tgc/BlockingFlag.hh"
#include "mgm/tgc/CachedValue.hh"
#include "mgm/tgc/Constants.hh"
#include "mgm/tgc/ITapeGcMgm.hh"
#include "mgm/tgc/Lru.hh"
#include "mgm/tgc/SmartSpaceStats.hh"
#include "mgm/tgc/SpaceConfig.hh"
#include "mgm/tgc/TapeGcStats.hh"
#include "namespace/interface/IFileMD.hh"
#include "proto/ConsoleReply.pb.h"
#include "proto/ConsoleRequest.pb.h"

#include <atomic>
#include <cstdint>
#include <ctime>
#include <mutex>
#include <stdexcept>
#include <thread>

/*----------------------------------------------------------------------------*/
/**
 * @file TapeGc.hh
 *
 * @brief Class implementing a tape aware garbage collector
 *
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! A tape aware garbage collector
//------------------------------------------------------------------------------
class TapeGc
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param mgm interface to the EOS MGM
  //! @param spaceName name of the EOS space that this garbage collector will
  //! manage
  //! @param maxConfigCacheAgeSecs maximum age in seconds of a tape-ware garbage
  //! collector's cached configuration
  //----------------------------------------------------------------------------
  TapeGc(
    ITapeGcMgm &mgm,
    const std::string &spaceName,
    std::time_t maxConfigCacheAgeSecs = TGC_DEFAULT_MAX_CONFIG_CACHE_AGE_SECS
  );

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~TapeGc();

  //----------------------------------------------------------------------------
  //! Delete copy constructor
  //----------------------------------------------------------------------------
  TapeGc(const TapeGc &) = delete;

  //----------------------------------------------------------------------------
  //! Delete move constructor
  //----------------------------------------------------------------------------
  TapeGc(const TapeGc &&) = delete;

  //----------------------------------------------------------------------------
  //! Delete assignment operator
  //----------------------------------------------------------------------------
  TapeGc &operator=(const TapeGc &) = delete;

  //----------------------------------------------------------------------------
  //! Idempotent method to start the worker thread of the tape-aware GC
  //----------------------------------------------------------------------------
  void startWorkerThread();

  //----------------------------------------------------------------------------
  //! Notify GC the specified file has been accessed
  //!
  //! @param fid file identifier
  //----------------------------------------------------------------------------
  void fileAccessed(IFileMD::id_t fid) noexcept;

  //----------------------------------------------------------------------------
  //! @return statistics
  //----------------------------------------------------------------------------
  TapeGcStats getStats() noexcept;

  //----------------------------------------------------------------------------
  //! Writes the JSON representation of this object to the specified stream.
  //!
  //! @param os Input/Output parameter specifying the stream to write to.
  //! @param maxLen The maximum length the stream should be.  A value of 0 means
  //! unlimited.  This method can go over the maxLen limit but it MUST throw
  //! a MaxLenExceeded exception if it does.
  //!
  //! @throw MaxLenExceeded if the length of the JSON string has exceeded maxLen
  //----------------------------------------------------------------------------
  void toJson(std::ostringstream &os, std::uint64_t maxLen = 0) const;

protected:

  /// The interface to the EOS MGM
  ITapeGcMgm &m_mgm;

  /// The name of the EOS space managed by this garbage collector
  std::string m_spaceName;

  /// Ensures startWorkerThread() only starts the worker thread once
  std::atomic_flag m_startWorkerThreadMethodCalled = ATOMIC_FLAG_INIT;

  /// True if the worker thread should stop
  BlockingFlag m_stop;

  /// Mutex dedicated to protecting the m_worker member variable
  std::mutex m_workerMutex;

  /// The one and only GC worker thread
  std::unique_ptr<std::thread> m_worker;

  /// Mutex protecting mLruQueue
  mutable std::mutex m_lruQueueMutex;

  /// Queue of Least Recently Used (LRU) files
  Lru m_lruQueue;

  //----------------------------------------------------------------------------
  //! Entry point for the GC worker thread
  //----------------------------------------------------------------------------
  void workerThreadEntryPoint() noexcept;

  //----------------------------------------------------------------------------
  //! @return the size of the LRU queue.  Zero is returned in the case of error.
  //----------------------------------------------------------------------------
  Lru::FidQueue::size_type getLruQueueSize() const noexcept;

  //----------------------------------------------------------------------------
  //! Try to garbage collect a single file if necessary and possible.
  //!
  //! Please note that a file is considered successfully garbage collected if
  //! it does not exists in the EOS namespace when it is popped from the LRU
  //! data structure.
  //!
  //! @return True if a file was garbage collected
  //----------------------------------------------------------------------------
  bool tryToGarbageCollectASingleFile() noexcept;

  //----------------------------------------------------------------------------
  //! Configuration
  //----------------------------------------------------------------------------
  CachedValue<SpaceConfig> m_config;

  //----------------------------------------------------------------------------
  //! Statistics about the EOS space being managed
  //----------------------------------------------------------------------------
  SmartSpaceStats m_spaceStats;

  //----------------------------------------------------------------------------
  //! Counter that is incremented each time a file is successfully stagerrm'ed
  //----------------------------------------------------------------------------
  std::atomic<std::uint64_t> m_nbStagerrms;

  //----------------------------------------------------------------------------
  //! Take note of a disk replica queued for deletion so that the amount of free
  //! space can be updated without having to wait for the next query to the EOS
  //! MGM
  //!
  //! @param fileSizeBytes File size in bytes
  //----------------------------------------------------------------------------
  void diskReplicaQueuedForDeletion(size_t fileSizeBytes);
};

EOSTGCNAMESPACE_END

#endif
