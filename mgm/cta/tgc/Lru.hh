// ----------------------------------------------------------------------
// File: Lru.hh
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

#ifndef __EOSMGMTGCLRU_HH__
#define __EOSMGMTGCLRU_HH__

#include "common/Murmur3.hh"
#include "common/hopscotch_map.hh"
#include "mgm/Namespace.hh"
#include "namespace/interface/IFileMD.hh"

#include <list>
#include <stdexcept>
#include <unordered_map>

/*----------------------------------------------------------------------------*/
/**
 * @file Lru.hh
 *
 * @brief Class implementing a Least Recenting Used (LRU) queue
 *
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class implementing a Least Recenting Used (LRU) queue
//------------------------------------------------------------------------------
class Lru {
public:

  //----------------------------------------------------------------------------
  //! Data type for storing a queue of file identifiers
  //----------------------------------------------------------------------------
  typedef std::list<IFileMD::id_t> FidQueue;

  //----------------------------------------------------------------------------
  //! Exception thrown when maxQueueSize has been incorrectly set to zero.
  //----------------------------------------------------------------------------
  struct MaxQueueSizeIsZero: public std::runtime_error {
    MaxQueueSizeIsZero(const std::string &msg): std::runtime_error(msg) {}
  };

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param maxQueueSize The maximum number of entries permitted in the LRU
  //!                     queue.  This value must be greater than 0.
  //!
  //! @throw MaxQueueSizeIsZero If maxQueueSize is equal to 0.
  //----------------------------------------------------------------------------
  Lru(const FidQueue::size_type maxQueueSize = 10000000);

  //----------------------------------------------------------------------------
  //! Notify the queue a file has been accessed
  //!
  //! @param fid The file identifier
  //----------------------------------------------------------------------------
  void fileAccessed(const IFileMD::id_t fid);

  //----------------------------------------------------------------------------
  //! Notify the queue a file has been deleted from the EOS namespace
  //!
  //! @param fid The file identifier
  //----------------------------------------------------------------------------
  void fileDeletedFromNamespace(const IFileMD::id_t fid);

  //----------------------------------------------------------------------------
  //! @return true if the queue is empty
  //----------------------------------------------------------------------------
  bool empty() const;

  //----------------------------------------------------------------------------
  //! @return queue size
  //----------------------------------------------------------------------------
  FidQueue::size_type size() const;

  //----------------------------------------------------------------------------
  //! Exception thrown when the queue is empty
  //----------------------------------------------------------------------------
  struct QueueIsEmpty: public std::runtime_error {
    QueueIsEmpty(const std::string &msg): std::runtime_error(msg) {}
  };

  //----------------------------------------------------------------------------
  //! Pop and return the identifier of the least used file
  //!
  //! @return the file identifier
  //!
  //! @throw QueueIsEmpty if the queue is empty
  //----------------------------------------------------------------------------
  IFileMD::id_t getAndPopFidOfLeastUsedFile();

  //----------------------------------------------------------------------------
  //! @return True if the maximum queue size has been exceeded
  //----------------------------------------------------------------------------
  bool maxQueueSizeExceeded() const noexcept;

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

private:

  //----------------------------------------------------------------------------
  //! The maximum number of entries permitted in the LRU queue
  //----------------------------------------------------------------------------
  FidQueue::size_type mMaxQueueSize;

  //----------------------------------------------------------------------------
  //! True if the maximum size of the LRU queue has been exceeded.  This member
  //! variable is used to reduce the number of warning messages sent to the
  //! logger.
  //----------------------------------------------------------------------------
  bool mMaxQueueSizeExceeded;

  //----------------------------------------------------------------------------
  //! The queue of files from the most used at the front to the least used at
  //! the back
  //----------------------------------------------------------------------------
  FidQueue mQueue;

  //----------------------------------------------------------------------------
  //! Data type for a map from file ID to entry within the LRU queue
  //----------------------------------------------------------------------------
  typedef tsl::hopscotch_map < IFileMD::id_t, FidQueue::iterator,
    Murmur3::MurmurHasher<IFileMD::id_t> > FidToQueueEntryMap;

  //----------------------------------------------------------------------------
  //! Map from file ID to entry within the LRU queue
  //----------------------------------------------------------------------------
  FidToQueueEntryMap mFidToQueueEntry;

  //----------------------------------------------------------------------------
  //! Handle the fact a new file has been accessed
  //!
  //! @param fid The file identifier
  //----------------------------------------------------------------------------
  void newFileHasBeenAccessed(const IFileMD::id_t fid);

  //----------------------------------------------------------------------------
  //! Handle the fact that a file already in the queue has been accessed
  //!
  //! @param fid The file identifier
  //! @param queueItor The queue entry
  //----------------------------------------------------------------------------
  void queuedFileHasBeenAccessed(const IFileMD::id_t fid, FidQueue::iterator &queueItor);
};

EOSTGCNAMESPACE_END

#endif
