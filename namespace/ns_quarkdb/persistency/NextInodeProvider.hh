/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Retrieve the next free container / file inode.
//------------------------------------------------------------------------------

#pragma once
#include "namespace/Namespace.hh"
#include <mutex>

namespace qclient {
  class QHash;
}

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class InodeBlock
//------------------------------------------------------------------------------
class InodeBlock
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  InodeBlock(int64_t start, int64_t len);

  //----------------------------------------------------------------------------
  //! Reserve, only if there's enough space
  //----------------------------------------------------------------------------
  bool reserve(int64_t &out);

  //----------------------------------------------------------------------------
  //! Get first free ID - what reserve _would_ have returned, without actually
  //! allocating the inode.
  //----------------------------------------------------------------------------
  bool getFirstFreeID(int64_t &out) const;

  //----------------------------------------------------------------------------
  //! Check if block has more inodes to give
  //----------------------------------------------------------------------------
  bool empty() const;

  //----------------------------------------------------------------------------
  //! Blacklist all IDs below the given number, including the threshold itself.
  //----------------------------------------------------------------------------
  void blacklistBelow(int64_t threshold);

private:
  int64_t mStart;
  int64_t mLen;

  int64_t mNextId;
};

//------------------------------------------------------------------------------
//! Class NextInodeProvider
//------------------------------------------------------------------------------
class NextInodeProvider
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  NextInodeProvider();

  //----------------------------------------------------------------------------
  //! Configuration method
  //!
  //! @param hash hash object to be used
  //! @param filed filed that we use to get the first free id
  //----------------------------------------------------------------------------
  void configure(qclient::QHash& hash, const std::string& field);

  //----------------------------------------------------------------------------
  //! Get first free id
  //----------------------------------------------------------------------------
  int64_t getFirstFreeId();

  //----------------------------------------------------------------------------
  //! Blacklist all IDs below the given number - from that point on, no IDs
  //! less or equal to what is specified will be given out.
  //----------------------------------------------------------------------------
  void blacklistBelow(int64_t threshold);

  //----------------------------------------------------------------------------
  //! Method used for reseving a batch of ids and return the first free one
  //----------------------------------------------------------------------------
  int64_t reserve();

private:
  //----------------------------------------------------------------------------
  //! Get counter value stored in DB, no caching
  //----------------------------------------------------------------------------
  int64_t getDBValue();

  //----------------------------------------------------------------------------
  //! Allocate new inode block
  //----------------------------------------------------------------------------
  void allocateInodeBlock();

  //----------------------------------------------------------------------------
  //! Blacklist DB threshold
  //----------------------------------------------------------------------------
  void blacklistDBThreshold(int64_t threshold);

  std::mutex mMtx;
  qclient::QHash* pHash; ///< qclient hash - no ownership
  std::string pField;

  InodeBlock mInodeBlock;
  int64_t mStepIncrease;
};

EOSNSNAMESPACE_END
