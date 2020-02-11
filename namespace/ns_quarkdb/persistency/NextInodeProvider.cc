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
#include "namespace/ns_quarkdb/persistency/NextInodeProvider.hh"
#include "namespace/interface/IFileMD.hh"
#include "qclient/structures/QHash.hh"
#include <memory>
#include <numeric>

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
bool InodeBlock::empty() {
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
  : pHash(nullptr), pField(""), mNextId(0), mBlockEnd(-1), mStepIncrease(1)
{
}

//------------------------------------------------------------------------------
// Get first free id
//------------------------------------------------------------------------------
int64_t NextInodeProvider::getFirstFreeId()
{
  std::lock_guard<std::mutex> lock(mMtx);

  if (mBlockEnd < mNextId) {
    IFileMD::id_t id = 0;
    std::string sval = pHash->hget(pField);

    if (!sval.empty()) {
      id = std::stoull(sval);
    }

    return id + 1;
  }

  return mNextId;
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

  if (mBlockEnd < mNextId) {
    mBlockEnd = pHash->hincrby(pField, mStepIncrease);
    mNextId = mBlockEnd - mStepIncrease + 1;

    // Increase step for next round
    if (mStepIncrease <= 5000) {
      mStepIncrease++;
    }
  }

  return mNextId++;
}

//------------------------------------------------------------------------------
// Blacklist all IDs below the given number - from that point on, no IDs
// less or equal to what is specified will be given out.
//------------------------------------------------------------------------------
void NextInodeProvider::blacklistBelow(int64_t threshold)
{
  std::lock_guard<std::mutex> lock(mMtx);

  if(mBlockEnd <= threshold) {
    mBlockEnd = threshold + mStepIncrease;
    pHash->hset(pField, std::to_string(mBlockEnd));
    mNextId = threshold+1;
  }
  else {
    mNextId = std::max(mNextId, threshold+1);
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

EOSNSNAMESPACE_END
