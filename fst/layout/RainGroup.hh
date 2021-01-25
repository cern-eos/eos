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

#pragma once
#include "fst/layout/RainBlock.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class RainGroup
//------------------------------------------------------------------------------
class RainGroup: public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  RainGroup(uint64_t grp_offset, int size, uint32_t block_sz);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RainGroup() = default;

  //----------------------------------------------------------------------------
  //! Override [] operator
  //----------------------------------------------------------------------------
  eos::fst::RainBlock& operator [](unsigned int i);

  //----------------------------------------------------------------------------
  //! Get group offset of the current object
  //----------------------------------------------------------------------------
  inline uint64_t GetGroupOffset() const
  {
    return mOffset;
  }

  //----------------------------------------------------------------------------
  //! Lock group
  //----------------------------------------------------------------------------
  inline void Lock() const
  {
    mMutex.lock();
  }

  //----------------------------------------------------------------------------
  //! Lock group
  //----------------------------------------------------------------------------
  inline void Unlock() const
  {
    mMutex.unlock();
  }

  //----------------------------------------------------------------------------
  //! Fill the blocks with zeros if they are not fully written
  //----------------------------------------------------------------------------
  bool FillWithZeros();

private:
  uint64_t mOffset; ///< Group offset of the current object
  std::vector<eos::fst::RainBlock> mBlocks;
  mutable std::mutex mMutex;
};

EOSFSTNAMESPACE_END
