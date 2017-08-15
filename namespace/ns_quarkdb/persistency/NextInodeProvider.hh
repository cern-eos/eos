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
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/ns_quarkdb/LRU.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include <mutex>

EOSNSNAMESPACE_BEGIN

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
  //! Method used for reseving a batch of ids and return the first free one
  //----------------------------------------------------------------------------
  int64_t reserve();

private:
  std::mutex mMtx;
  qclient::QHash* pHash; ///< qclient hash - no ownership
  std::string pField;
  int64_t mNextId;
  int64_t mBlockEnd;
  int64_t mStepIncrease;
};

EOSNSNAMESPACE_END
