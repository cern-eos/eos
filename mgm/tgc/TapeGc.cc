// ----------------------------------------------------------------------
// File: TapeGc.cc
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

#include "mgm/tgc/Constants.hh"
#include "mgm/tgc/MaxLenExceeded.hh"
#include "mgm/tgc/TapeGc.hh"
#include "mgm/tgc/SpaceNotFound.hh"
#include "mgm/tgc/Utils.hh"

#include <cstring>
#include <functional>
#include <iomanip>
#include <ios>
#include <sstream>

EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
TapeGc::TapeGc(ITapeGcMgm &mgm, const std::string &spaceName, const std::time_t maxConfigCacheAgeSecs):
  m_mgm(mgm),
  m_spaceName(spaceName),
  m_config(std::bind(&ITapeGcMgm::getTapeGcSpaceConfig, &mgm, spaceName), maxConfigCacheAgeSecs),
  m_spaceStats(spaceName, mgm, m_config),
  m_nbStagerrms(0)
{
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
TapeGc::~TapeGc()
{
  try {
    std::lock_guard<std::mutex> workerLock(m_workerMutex);
    if(m_worker) {
      m_stop.setToTrue();
      m_worker->join();
    }
  } catch(std::exception &ex) {
    eos_static_err("msg=\"%s\"", ex.what());
  } catch(...) {
    eos_static_err("msg=\"Caught an unknown exception\"");
  }
}

//------------------------------------------------------------------------------
// Idempotent method to start the worker thread of the tape-aware GC
//------------------------------------------------------------------------------
void
TapeGc::startWorkerThread()
{
  try {
    // Do nothing if calling thread is not the first to call startWorkerThread()
    if (m_startWorkerThreadMethodCalled.test_and_set()) return;

    std::function<void()> entryPoint = std::bind(&TapeGc::workerThreadEntryPoint, this);

    {
      std::lock_guard<std::mutex> workerLock(m_workerMutex);
      m_worker = std::make_unique<std::thread>(entryPoint);
    }
  } catch(std::exception &ex) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " failed: " << ex.what();
    throw std::runtime_error(msg.str());
  } catch(...) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " failed: Caught an unknown exception";
    throw std::runtime_error(msg.str());
  }
}

//------------------------------------------------------------------------------
// Entry point for the GC worker thread
//------------------------------------------------------------------------------
void
TapeGc::workerThreadEntryPoint() noexcept
{
  do {
    while(!m_stop && tryToGarbageCollectASingleFile()) {
    }
  } while(!m_stop.waitForTrue(std::chrono::seconds(1)));
}

//------------------------------------------------------------------------------
// Notify GC the specified file has been opened
//------------------------------------------------------------------------------
void
TapeGc::fileOpened(const IFileMD::id_t fid) noexcept
{
  try {
    std::lock_guard<std::mutex> lruQueueLock(m_lruQueueMutex);
    const bool exceededBefore = m_lruQueue.maxQueueSizeExceeded();
    m_lruQueue.fileAccessed(fid);

    // Only log crossing the max queue size threshold - don't log each access
    if(!exceededBefore && m_lruQueue.maxQueueSizeExceeded()) {
      std::ostringstream msg;
      msg <<"space=\"" << m_spaceName << "\" fxid=" << std::hex << fid <<
        " msg=\"Max queue size of tape-aware GC has been passed - new files will be ignored\"";
      eos_static_warning(msg.str().c_str());
    }
  } catch(std::exception &ex) {
    eos_static_err("msg=\"%s\"", ex.what());
  } catch(...) {
    eos_static_err("msg=\"Caught an unknown exception\"");
  }
}

//------------------------------------------------------------------------------
// Try to garage collect a single file if necessary and possible
//------------------------------------------------------------------------------
bool
TapeGc::tryToGarbageCollectASingleFile() noexcept
{
  try {
    const auto config = m_config.get();

    try {
      const auto spaceStats = m_spaceStats.get();

      // Return no file was garbage collected if there is still enough available
      // space or if the total amount of space is not enough (not all disk
      // systems are on-line)
      if(spaceStats.availBytes >= config.availBytes || spaceStats.totalBytes < config.totalBytes) {
        return false;
      }
    } catch(SpaceNotFound &) {
      // Return no file was garbage collected if the space was not found
      return false;
    }

    IFileMD::id_t fid = 0;
    {
      std::lock_guard<std::mutex> lruQueueLock(m_lruQueueMutex);
      if (m_lruQueue.empty()) {
        return false; // No file was garbage collected
      }
      fid = m_lruQueue.getAndPopFidOfLeastUsedFile();
    }

    std::uint64_t fileToBeDeletedSizeBytes = 0;
    try {
      fileToBeDeletedSizeBytes = m_mgm.getFileSizeBytes(fid);
    } catch(std::exception &ex) {
      std::ostringstream msg;
      msg << "fxid=" << std::hex << fid << " msg=\"Unable to garbage collect disk replica: "
        << ex.what() << "\"";
      eos_static_info(msg.str().c_str());

      // Please note that a file is considered successfully garbage collected
      // if its size cannot be determined
      return true;
    } catch(...) {
      std::ostringstream msg;
      msg << "fxid=" << std::hex << fid << " msg=\"Unable to garbage collect disk replica: Unknown exception";
      eos_static_info(msg.str().c_str());

      // Please note that a file is considered successfully garbage collected
      // if its size cannot be determined
      return true;
    }

    // The garbage collector should explicitly ignore zero length files by
    // returning success
    if (0 == fileToBeDeletedSizeBytes) {
      std::ostringstream msg;
      msg << "fxid=" << std::hex << fid << " msg=\"Garbage collector ignoring zero length file\"";
      eos_static_info(msg.str().c_str());

      return true;
    }

    try {
      m_mgm.stagerrmAsRoot(fid);
    } catch(std::exception &ex) {
      std::ostringstream msg;
      msg << "fxid=" << std::hex << fid <<
        " msg=\"Putting file back in GC queue after failing to garbage collect its disk replica: " << ex.what();
      eos_static_info(msg.str().c_str());

      std::lock_guard<std::mutex> lruQueueLock(m_lruQueueMutex);
      m_lruQueue.fileAccessed(fid);
      return false; // No disk replica was garbage collected
    } catch(...) {
      std::ostringstream msg;
      msg << "fxid=" << std::hex << fid <<
        " msg=\"Putting file back in GC queue after failing to garbage collect its disk replica: Unknown exception";
      eos_static_info(msg.str().c_str());

      std::lock_guard<std::mutex> lruQueueLock(m_lruQueueMutex);
      m_lruQueue.fileAccessed(fid);
      return false; // No disk replica was garbage collected
    }

    m_nbStagerrms++;
    fileQueuedForDeletion(fileToBeDeletedSizeBytes);
    std::ostringstream msg;
    msg << "fxid=" << std::hex << fid << " msg=\"Garbage collected disk replica using stagerrm\"";
    eos_static_info(msg.str().c_str());

    return true; // A disk replica was garbage collected
  } catch(std::exception &ex) {
    eos_static_err("msg=\"%s\"", ex.what());
  } catch(...) {
    eos_static_err("msg=\"Caught an unknown exception\"");
  }

  return false; // No disk replica was garbage collected
}

//----------------------------------------------------------------------------
// Return statistics
//----------------------------------------------------------------------------
TapeGcStats
TapeGc::getStats() noexcept
{
  try {
    TapeGcStats tgcStats;

    tgcStats.nbStagerrms = m_nbStagerrms;
    tgcStats.lruQueueSize = getLruQueueSize();
    tgcStats.spaceStats = m_spaceStats.get();
    tgcStats.queryTimestamp = m_spaceStats.getQueryTimestamp();

    return tgcStats;
  } catch(...) {
    return TapeGcStats();
  }
}

//----------------------------------------------------------------------------
// Return the size of the LRU queue
//----------------------------------------------------------------------------
Lru::FidQueue::size_type
TapeGc::getLruQueueSize() const noexcept
{
  const char *const msgFormat =
    "TapeGc::getLruQueueSize() failed space=%s: %s";
  try {
    std::lock_guard<std::mutex> lruQueueLock(m_lruQueueMutex);
    return m_lruQueue.size();
  } catch(std::exception &ex) {
    eos_static_err(msgFormat, m_spaceName.c_str(), ex.what());
  } catch(...) {
    eos_static_err(msgFormat, m_spaceName.c_str(), "Caught an unknown exception");
  }

  return 0;
}

//----------------------------------------------------------------------------
// Return A JSON string representation of the GC
//----------------------------------------------------------------------------
void
TapeGc::toJson(std::ostringstream &os, const std::uint64_t maxLen) const {
  {
    std::lock_guard<std::mutex> lruQueueLock(m_lruQueueMutex);
    os <<
      "{"
      "\"spaceName\":\"" << m_spaceName << "\","
      "\"lruQueue\":";
    m_lruQueue.toJson(os, maxLen);
    os << "}";
  }

  {
    const auto osSize = os.tellp();
    if (0 > osSize) throw std::runtime_error(std::string(__FUNCTION__) + ": os.tellp() returned a negative number");
    if (maxLen && maxLen < (std::string::size_type)osSize) {
      std::ostringstream msg;
      msg << __FUNCTION__ << ": maxLen exceeded: maxLen=" << maxLen;
      throw MaxLenExceeded(msg.str());
    }
  }
}

//------------------------------------------------------------------------------
// Notify this object that a file has been queued for deletion
//------------------------------------------------------------------------------
void
TapeGc::fileQueuedForDeletion(const size_t deletedFileSize)
{
  m_spaceStats.fileQueuedForDeletion(deletedFileSize);
}

EOSTGCNAMESPACE_END
