//------------------------------------------------------------------------------
//! @file FusexCastBatch.hh
//! @author Elvin Sindrilaru - CERN
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
#include "mgm/Namespace.hh"
#include <functional>
#include <list>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class FusexCastBatch
//------------------------------------------------------------------------------
class FusexCastBatch
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FusexCastBatch() = default;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~FusexCastBatch()
  {
    if (!mBatch.empty()) {
      Execute();
    }
  }

  //----------------------------------------------------------------------------
  //! Don't allow copy or move of these objects
  //----------------------------------------------------------------------------
  FusexCastBatch(const FusexCastBatch&) = delete;
  FusexCastBatch& operator =(const FusexCastBatch&) = delete;
  FusexCastBatch(FusexCastBatch&&) = delete;
  FusexCastBatch& operator =(FusexCastBatch&&) = delete;

  //----------------------------------------------------------------------------
  //! Add update to batch
  //!
  //! @param f callable to be executed at a later stage
  //----------------------------------------------------------------------------
  void Register(std::function<void()>&& f)
  {
    mBatch.emplace_back(std::move(f));
  }

  //----------------------------------------------------------------------------
  //! Perform all the callbacks registered in the list
  //----------------------------------------------------------------------------
  void Execute()
  {
    for (auto& f : mBatch) {
      f();
    }

    mBatch.clear();
  }

  //----------------------------------------------------------------------------
  //! Get size of the batch to be executed
  //----------------------------------------------------------------------------
  unsigned long GetSize() const
  {
    return mBatch.size();
  }

private:
  std::list<std::function<void()>> mBatch;
};

EOSMGMNAMESPACE_END
