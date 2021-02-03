//------------------------------------------------------------------------------
//! @file RainBlock.cc
//! @author Elvin-Alin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#include "fst/layout/RainBlock.hh"
#include "common/BufferManager.hh"

namespace
{
// Max 1GB of memory with blocks of at most 64MB each
eos::common::BufferManager gRainBuffMgr(1 * eos::common::GB, 6);
}

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RainBlock::RainBlock(uint32_t capacity):
  mCapacity(capacity), mLastOffset(0ul)
{
  mBuffer = gRainBuffMgr.GetBuffer(mCapacity);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RainBlock::~RainBlock()
{
  gRainBuffMgr.Recycle(mBuffer);
}

//----------------------------------------------------------------------------
// Save data in the current block
//----------------------------------------------------------------------------
char*
RainBlock::Write(const char* buffer, uint64_t offset, uint32_t length)
{
  if ((offset >= mCapacity) ||
      (offset + length > mCapacity)) {
    eos_static_err("msg=\"block can not hold so much data\" capcity=%llu "
                   "data_off=%llu data_len=%llu", mCapacity, offset, length);
    return nullptr;
  }

  if (offset > mLastOffset) {
    mHasHoles = true;
  }

  if (offset + length > mLastOffset) {
    mLastOffset = offset + length;
  }

  char* ptr = mBuffer->GetDataPtr();
  ptr += offset;
  (void) memcpy(ptr, buffer, length);
  return ptr;
}

//----------------------------------------------------------------------------
// Fill the remaining part of the buffer with zeros and mark it as complete
//----------------------------------------------------------------------------
bool
RainBlock::FillWithZeros(bool force)
{
  if (mHasHoles) {
    return false;
  }

  uint64_t len = mCapacity;
  char* ptr = mBuffer->GetDataPtr();

  if (force) {
    (void) memset(ptr, '\0', len);
  } else {
    if (mLastOffset < mCapacity) {
      len = mCapacity - mLastOffset;
      ptr += mLastOffset;
      (void) memset(ptr, '\0', len);
    }
  }

  mLastOffset = mCapacity;
  return true;
}

EOSFSTNAMESPACE_END
