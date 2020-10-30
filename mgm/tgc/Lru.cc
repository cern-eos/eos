// ----------------------------------------------------------------------
// File: Lru.cc
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

#include "mgm/tgc/Lru.hh"
#include "mgm/tgc/MaxLenExceeded.hh"

#include <iomanip>
#include <stdexcept>

EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Constructor
//------------------------------------------------------------------------------
Lru::Lru(const FidQueue::size_type maxQueueSize):
  mMaxQueueSize(maxQueueSize), mMaxQueueSizeExceeded(false)
{
  if(0 == maxQueueSize) {
    throw MaxQueueSizeIsZero(std::string(__FUNCTION__) +
      " failed: maxQueueSize must be greater than 0");
  }
}

//------------------------------------------------------------------------------
//! Notify the queue a file has been accessed
//------------------------------------------------------------------------------
void Lru::fileAccessed(const IFileMD::id_t fid)
{
  const auto mapEntry = mFidToQueueEntry.find(fid);

  // If a new file has been accessed
  if(mFidToQueueEntry.end() == mapEntry) {
    newFileHasBeenAccessed(fid);
  } else {
    queuedFileHasBeenAccessed(fid, mapEntry.value());
  }
}

//------------------------------------------------------------------------------
// Handle the fact a new file has been accessed
//------------------------------------------------------------------------------
void
Lru::newFileHasBeenAccessed(const IFileMD::id_t fid)
{
  // Ignore the new file if the maximum queue size has been reached
  // IMPORTANT: This should be a rare situation
  if(mFidToQueueEntry.size() == mMaxQueueSize) {
    mMaxQueueSizeExceeded = true;
  } else {
    // Add file to the front of the LRU queue
    mQueue.push_front(fid);
    mFidToQueueEntry[fid] = mQueue.begin();
  }
}

//------------------------------------------------------------------------------
// Handle the fact that a file already in the queue has been accessed
//------------------------------------------------------------------------------
void
Lru::queuedFileHasBeenAccessed(const IFileMD::id_t fid,
  FidQueue::iterator &queueItor)
{
  // Erase the existing file from the LRU queue
  mQueue.erase(queueItor);

  // Push the identifier of the file onto the front of the LRU queue
  mQueue.push_front(fid);
  mFidToQueueEntry[fid] = mQueue.begin();
}

//------------------------------------------------------------------------------
//! Notify the queue a file has been deleted from the EOS namespace
//------------------------------------------------------------------------------
void
Lru::fileDeletedFromNamespace(const IFileMD::id_t fid)
{
  const auto mapEntry = mFidToQueueEntry.find(fid);

  if(mFidToQueueEntry.end() != mapEntry) {
    mQueue.erase(mapEntry.value());
    mFidToQueueEntry.erase(mapEntry);
  }
}

//------------------------------------------------------------------------------
// Return true if the queue is empty
//------------------------------------------------------------------------------
bool
Lru::empty() const
{
  return mQueue.empty();
}

//------------------------------------------------------------------------------
// Return queue size
//------------------------------------------------------------------------------
Lru::FidQueue::size_type
Lru::size() const
{
  return mFidToQueueEntry.size();
}

//------------------------------------------------------------------------------
// Pop and return the identifier of the least used file
//------------------------------------------------------------------------------
IFileMD::id_t
Lru::getAndPopFidOfLeastUsedFile()
{
  if(mQueue.empty()) {
    throw QueueIsEmpty(std::string(__FUNCTION__) +
      " failed: The queue is empty");
  } else {
    mMaxQueueSizeExceeded = false;

    const auto lruFid = mQueue.back();
    mQueue.pop_back();
    mFidToQueueEntry.erase(lruFid);
    return lruFid;
  }
}

//------------------------------------------------------------------------------
// Return true if the maximum queue size has been exceeded
//------------------------------------------------------------------------------
bool
Lru::maxQueueSizeExceeded() const noexcept {
  return mMaxQueueSizeExceeded;
}

//----------------------------------------------------------------------------
// Return A JSON string representation of the LRU queue
//----------------------------------------------------------------------------
void
Lru::toJson(std::ostringstream &os, const std::uint64_t maxLen) const {
  os << "{\"size\":\"" << size() << "\",\"fids_from_MRU_to_LRU\":";
  os << std::setfill('0') << std::hex << "[";
  bool isFirstFid = true;
  for (const auto fid: mQueue) {
    if (isFirstFid) {
      isFirstFid = false;
    } else {
      os << ",";
    }
    os << "\"0x" << std::setw(16) << fid << "\"";

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
  os << "]}";

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

EOSTGCNAMESPACE_END
