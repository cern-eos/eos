//------------------------------------------------------------------------------
//! @file RainGroup.hh
//! @author Elvin-Alin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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

#include "fst/layout/RainGroup.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RainGroup::RainGroup(uint64_t grp_offset, int size, uint32_t block_sz):
  mOffset(grp_offset)
{
  for (int i = 0; i < size; ++i) {
    mBlocks.emplace_back(block_sz);
  }
}

//------------------------------------------------------------------------------
// Override [] operator
//------------------------------------------------------------------------------
eos::fst::RainBlock&
RainGroup::operator [](unsigned int i)
{
  return mBlocks[i];
}

//------------------------------------------------------------------------------
// Fill the blocks with zeros if they are not fully written
//------------------------------------------------------------------------------
bool
RainGroup::FillWithZeros()
{
  bool ret = true;

  for (auto& block : mBlocks) {
    ret = ret && block.FillWithZeros();
  }

  return ret;
}

//------------------------------------------------------------------------------
// Save future of async requests
//------------------------------------------------------------------------------
void
RainGroup::StoreFuture(std::future<XrdCl::XRootDStatus>&& future)
{
  mFutures.push_back(std::move(future));
}

//----------------------------------------------------------------------------
// Wait for completion of all registered futures and check if they were all
// successful.
//----------------------------------------------------------------------------
bool
RainGroup::WaitAsyncOK()
{
  bool all_ok = true;

  for (auto& fut : mFutures) {
    XrdCl::XRootDStatus status = fut.get();

    if (!status.IsOK()) {
      all_ok = false;
    }
  }

  mFutures.clear();
  return all_ok;
}

EOSFSTNAMESPACE_END
