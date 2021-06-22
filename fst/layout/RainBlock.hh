//------------------------------------------------------------------------------
//! @file RainBlock.hh
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
#include "fst/Namespace.hh"
#include "common/Logging.hh"
#include "common/BufferManager.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class RainBlock
//------------------------------------------------------------------------------
class RainBlock: public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param capacity maximum size of the current block
  //----------------------------------------------------------------------------
  RainBlock(uint32_t capacity);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RainBlock();

  //----------------------------------------------------------------------------
  //! Move assignment operator
  //----------------------------------------------------------------------------
  RainBlock& operator =(RainBlock&& other) noexcept
  {
    if (this != &other) {
      mCapacity = other.mCapacity;
      mLastOffset = other.mLastOffset;
      mHasHoles = other.mHasHoles;
      mBuffer = other.mBuffer;
      other.mBuffer = nullptr;
    }

    return *this;
  }

  //----------------------------------------------------------------------------
  //! Move constructor
  //----------------------------------------------------------------------------
  RainBlock(RainBlock&& other) noexcept
  {
    *this = std::move(other);
  }

  //----------------------------------------------------------------------------
  //! Override operator ()
  //----------------------------------------------------------------------------
  char* operator()()
  {
    return mBuffer->GetDataPtr();
  }

  //----------------------------------------------------------------------------
  //! Write data in the current block
  //!
  //! @param buffer buffer containing the data
  //! @param offset offset withing the current block
  //! @param length lenght of the data
  //!
  //! @return pointer to the internal buffer where current price was written
  //!         or nullptr otherwise
  //----------------------------------------------------------------------------
  char* Write(const char* buffer, uint64_t offset, uint32_t lenght);

  //----------------------------------------------------------------------------
  //! Fill the remaining (unused) part of the buffer with zeros and mark it
  //! as complete
  //!
  //! @param force if true when fill the entire block with \0
  //!
  //! @retrun true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool FillWithZeros(bool force = false);

  //----------------------------------------------------------------------------
  //! Get pointer to the undelying data
  //----------------------------------------------------------------------------
  inline char* GetDataPtr()
  {
    return mBuffer->GetDataPtr();
  }

private:
#ifdef IN_TEST_HARNESS
public:
#endif
  uint32_t mCapacity; ///< Max size of the current block
  uint32_t mLastOffset; ///< Last written offset
  uint32_t mLength {0ull}; ///< Length of useful data, relevant if no holes
  bool mHasHoles {false}; ///< Mark if block contains holes
  std::shared_ptr<eos::common::Buffer> mBuffer; ///< Actual data buffer
};

EOSFSTNAMESPACE_END
