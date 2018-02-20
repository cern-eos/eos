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
#include "qclient/QHash.hh"
#include <memory>
#include <numeric>

EOSNSNAMESPACE_BEGIN

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
    id_t id = 0;
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
// block-size slowly up to 200 so as to avoid wasting lots of inodes if the MGM
// is unstable and restarts often.
//------------------------------------------------------------------------------
int64_t NextInodeProvider::reserve()
{
  std::lock_guard<std::mutex> lock(mMtx);

  if (mBlockEnd < mNextId) {
    mBlockEnd = pHash->hincrby(pField, mStepIncrease);
    mNextId = mBlockEnd - mStepIncrease + 1;

    // Increase step for next round
    if (mStepIncrease <= 2000) {
      mStepIncrease++;
    }
  }

  return mNextId++;
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
