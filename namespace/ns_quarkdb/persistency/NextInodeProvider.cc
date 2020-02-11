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

#include "common/Logging.hh"
#include "common/Assert.hh"
#include "namespace/ns_quarkdb/persistency/NextInodeProvider.hh"
#include "namespace/interface/IFileMD.hh"
#include "qclient/structures/QHash.hh"
#include <memory>
#include <numeric>

#define __PRI64_PREFIX "l"
#define PRId64         __PRI64_PREFIX "d"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Inode block constructor
//------------------------------------------------------------------------------
InodeBlock::InodeBlock(int64_t start, int64_t len)
: mStart(start), mLen(len) {

  mNextId = mStart;
}

//------------------------------------------------------------------------------
// Check if block has more inodes to give
//------------------------------------------------------------------------------
bool InodeBlock::empty() const {
  return mStart + mLen <= mNextId;
}

//------------------------------------------------------------------------------
// Reserve, only if there's enough space
//------------------------------------------------------------------------------
bool InodeBlock::reserve(int64_t &out) {
  if(!empty()) {
    out = mNextId;
    mNextId++;
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Get first free ID - what reserve _would_ have returned, without actually
// allocating the inode.
//------------------------------------------------------------------------------
bool InodeBlock::getFirstFreeID(int64_t &out) const {
  if(empty()) {
    return false;
  }

  out = mNextId;
  return true;
}

//------------------------------------------------------------------------------
// Blacklist all IDs below the given number, including the threshold itself.
//------------------------------------------------------------------------------
void InodeBlock::blacklistBelow(int64_t threshold) {
  if(mNextId <= threshold) {
    mNextId = threshold+1;
  }
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
NextInodeProvider::NextInodeProvider()
  : pHash(nullptr), pField(""), mInodeBlock(0, 0), mStepIncrease(1)
{
}

//------------------------------------------------------------------------------
// Get first free id
//------------------------------------------------------------------------------
int64_t NextInodeProvider::getFirstFreeId()
{
  std::lock_guard<std::mutex> lock(mMtx);

  int64_t out;
  if(mInodeBlock.getFirstFreeID(out)) {
    return out;
  }

  return getDBValue() + 1;
}

//------------------------------------------------------------------------------
// The hash contains the current largest *reserved* inode we've seen so far.
// To obtain the next free one, we increment that counter and return its value.
// We reserve inodes by blocks to avoid roundtrips to the db, increasing the
// block-size slowly up to 5000 so as to avoid wasting lots of inodes if the MGM
// is unstable and restarts often.
//------------------------------------------------------------------------------
int64_t NextInodeProvider::reserve()
{
  std::lock_guard<std::mutex> lock(mMtx);

  int64_t out;
  if(mInodeBlock.reserve(out)) {
    return out;
  }

  // We're out if inodes, allocate next inode block
  allocateInodeBlock();
  eos_assert(mInodeBlock.reserve(out));
  return out;
}

//------------------------------------------------------------------------------
// Blacklist all IDs below the given number - from that point on, no IDs
// less or equal to what is specified will be given out.
//------------------------------------------------------------------------------
void NextInodeProvider::blacklistBelow(int64_t threshold)
{
  std::lock_guard<std::mutex> lock(mMtx);

  mInodeBlock.blacklistBelow(threshold);
  if(mInodeBlock.empty()) {
    // Our cached inode block has ran out of inodes - suspicious.
    // We might need to touch the DB.
    blacklistDBThreshold(threshold);
  }
}

//------------------------------------------------------------------------------
// Blacklist DB threshold
//------------------------------------------------------------------------------
void NextInodeProvider::blacklistDBThreshold(int64_t threshold) {
  int64_t currentValue = getDBValue();

  if(currentValue < threshold) {
    // Major event coming up, blacklisting inodes operation hitting the DB.
    eos_static_notice("Inode blacklisting operation hitting QDB: " PRId64 " -> " PRId64, currentValue, threshold);

    // We need to set currentValue to "threshold". We use HINCRBY due to paranoia,
    // to ensure we would **never** decrease the value in the DB.

    int64_t diff = threshold - currentValue;
    eos_assert(diff > 0);
    eos_assert(pHash->hincrby(pField, diff) == threshold);
    eos_assert(getDBValue() == threshold);
  }
}

//------------------------------------------------------------------------------
// Configure hash and field
//------------------------------------------------------------------------------
void NextInodeProvider::configure(qclient::QHash& hash,
                                  const std::string& field)
{
  std::lock_guard<std::mutex> lock(mMtx);
  pHash = &hash;
  pField = field;
}

//------------------------------------------------------------------------------
// Get counter value stored in DB, no caching
//------------------------------------------------------------------------------
int64_t NextInodeProvider::getDBValue() {
  int64_t id = 0;

  std::string sval = pHash->hget(pField);
  if (!sval.empty()) {
    id = std::stoull(sval);
  }

  return id;
}

//------------------------------------------------------------------------------
// Allocate new inode block
//------------------------------------------------------------------------------
void NextInodeProvider::allocateInodeBlock() {
  int64_t blockEnd = pHash->hincrby(pField, mStepIncrease);
  mInodeBlock = InodeBlock(blockEnd - mStepIncrease + 1, mStepIncrease);

  // Increase step for next round
  if (mStepIncrease <= 5000) {
    mStepIncrease++;
  }
}

EOSNSNAMESPACE_END
