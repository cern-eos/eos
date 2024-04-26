//------------------------------------------------------------------------------
//! @file HeaderCRC.hh
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//! @brief Header information present at the start of each stripe file
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
#include <XrdCl/XrdClFile.hh>
#include "common/Logging.hh"
#include "fst/Namespace.hh"

EOSFSTNAMESPACE_BEGIN

class FileIo;

//------------------------------------------------------------------------------
//! Header information present at the start of each stripe file
//------------------------------------------------------------------------------
class HeaderCRC : public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @sizeHeader maximum size the header will have in the beginning of the file
  //! @sizeBlock the size of the stripe block
  //!
  //----------------------------------------------------------------------------
  HeaderCRC(int sizeHeader, int sizeBlock);

  //----------------------------------------------------------------------------
  //! Constructor with parameter
  //!
  //! @sizeHeader maximum size the header will have in the beginning of the file
  //! @numBlocks number of data blocks in the current file
  //! @sizeBlock the size of the stripe block
  //----------------------------------------------------------------------------
  HeaderCRC(int sizeHeader, long long int numBlocks, int sizeBlock);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~HeaderCRC() = default;

  //----------------------------------------------------------------------------
  //! Write header to file
  //!
  //! @param pFile file to which to header will be written
  //! @param timeout timeout value
  //!
  //! @return status of the operation
  //----------------------------------------------------------------------------
  bool WriteToFile(FileIo* pFile, uint16_t timeout);


  //----------------------------------------------------------------------------
  //! Read header from file
  //!
  //! @param pFile file from which the header will be read
  //! @param timeout timeout value
  //!
  //! @return status of the operation
  //----------------------------------------------------------------------------
  bool ReadFromFile(FileIo* pFile, uint16_t timeout);

  //----------------------------------------------------------------------------
  //! Get tag of the header
  //----------------------------------------------------------------------------
  inline const char* GetTag() const
  {
    return mTag;
  }

  //----------------------------------------------------------------------------
  //! Get size of header
  //----------------------------------------------------------------------------
  inline int GetSize() const
  {
    return mSizeHeader;
  }

  //----------------------------------------------------------------------------
  //! Get block size the file contains
  //----------------------------------------------------------------------------
  inline size_t GetSizeBlock() const
  {
    return mSizeBlock;
  }

  //----------------------------------------------------------------------------
  //! Get size of last block in file
  //----------------------------------------------------------------------------
  inline size_t GetSizeLastBlock() const
  {
    return mSizeLastBlock;
  }

  //----------------------------------------------------------------------------
  //! Get number of blocks in file
  //----------------------------------------------------------------------------
  inline long int GetNoBlocks() const
  {
    return mNumBlocks;
  }

  //----------------------------------------------------------------------------
  //! Get id of the stripe the header belongs to
  //----------------------------------------------------------------------------
  inline unsigned int GetIdStripe() const
  {
    return mIdStripe;
  }

  //----------------------------------------------------------------------------
  //! Set number of blocks in the file
  //----------------------------------------------------------------------------
  inline void SetNoBlocks(long long int numBlocks)
  {
    mNumBlocks = numBlocks;
  }

  //----------------------------------------------------------------------------
  //! Set size of last block in the file
  //----------------------------------------------------------------------------
  inline void SetSizeLastBlock(size_t sizeLastBlock)
  {
    mSizeLastBlock = sizeLastBlock;
  }

  //----------------------------------------------------------------------------
  //! Set id of the stripe the header belongs to
  //----------------------------------------------------------------------------
  inline void SetIdStripe(unsigned int stripe)
  {
    mIdStripe = stripe;
  }

  //----------------------------------------------------------------------------
  //! Test if header is valid
  //----------------------------------------------------------------------------
  inline const bool IsValid() const
  {
    return mValid;
  }

  //----------------------------------------------------------------------------
  //! Set the header state (valid/corrupted)
  //----------------------------------------------------------------------------
  inline void SetState(bool state)
  {
    mValid = state;
  }

  //----------------------------------------------------------------------------
  //! Get size of the file based on the info in the header
  //----------------------------------------------------------------------------
  off_t GetSizeFile() const
  {
    if (mNumBlocks) {
      return static_cast<off_t>((mNumBlocks - 1) * mSizeBlock +
                                mSizeLastBlock);
    }

    return 0;
  }

  //----------------------------------------------------------------------------
  //! Dump header info in readable format
  //----------------------------------------------------------------------------
  std::string DumpInfo() const;

private:
  char mTag[16]; ///< layout tag
  bool mValid; ///< status of the file
  long long int mNumBlocks; ///< total number of blocks
  unsigned int mIdStripe; ///< index of the stripe the header belongs to
  size_t mSizeLastBlock; ///< size of the last block of data
  size_t mSizeBlock; ///< size of a block of data
  int mSizeHeader; ///< size of the header
  static char msTagName[]; ///< default tag name
};

EOSFSTNAMESPACE_END
