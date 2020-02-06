//------------------------------------------------------------------------------
// File: HeaderCRC.cc
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
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

#include "fst/layout/HeaderCRC.hh"
#include "fst/io/FileIo.hh"
#include <stdint.h>

EOSFSTNAMESPACE_BEGIN

char HeaderCRC::msTagName[] = "_HEADER__RAIDIO_";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
HeaderCRC::HeaderCRC(int sizeHeader, int sizeBlock) :
  mValid(false),
  mNumBlocks(-1),
  mIdStripe(-1),
  mSizeLastBlock(-1),
  mSizeBlock(sizeBlock),
  mSizeHeader(sizeHeader)
{
  if (mSizeHeader == 0) {
    mSizeHeader = eos::common::LayoutId::OssXsBlockSize;
  }
}

//------------------------------------------------------------------------------
// Constructor with parameter
//------------------------------------------------------------------------------
HeaderCRC::HeaderCRC(int sizeHeader, long long numBlocks, int sizeBlock) :
  mValid(false),
  mNumBlocks(numBlocks),
  mIdStripe(-1),
  mSizeLastBlock(-1),
  mSizeBlock(sizeBlock),
  mSizeHeader(sizeHeader)
{
  (void) memcpy(mTag, msTagName, strlen(msTagName));

  if (mSizeHeader == 0) {
    mSizeHeader = eos::common::LayoutId::OssXsBlockSize;
  }
}

//------------------------------------------------------------------------------
// Read header from generic file
//------------------------------------------------------------------------------
bool
HeaderCRC::ReadFromFile(FileIo* pFile, uint16_t timeout)
{
  long int offset = 0;
  size_t read_sizeblock = 0;
  char* buff = new char[mSizeHeader];

  if (pFile->fileRead(offset, buff, mSizeHeader, timeout) !=
      static_cast<uint32_t>(mSizeHeader)) {
    delete[] buff;
    mValid = false;
    return mValid;
  }

  memcpy(mTag, buff, sizeof mTag);
  std::string tag = mTag;

  if (strncmp(mTag, msTagName, strlen(msTagName))) {
    delete[] buff;
    mValid = false;
    return mValid;
  }

  offset += sizeof mTag;
  memcpy(&mIdStripe, buff + offset, sizeof mIdStripe);
  offset += sizeof mIdStripe;
  memcpy(&mNumBlocks, buff + offset, sizeof mNumBlocks);
  offset += sizeof mNumBlocks;
  memcpy(&mSizeLastBlock, buff + offset, sizeof mSizeLastBlock);
  offset += sizeof mSizeLastBlock;
  memcpy(&read_sizeblock, buff + offset, sizeof read_sizeblock);

  if (mSizeBlock == 0) {
    mSizeBlock = read_sizeblock;
  } else if (mSizeBlock != read_sizeblock) {
    eos_err("error=block size read from file does not match block size expected");
    mValid = false;
  }

  delete[] buff;
  mValid = true;
  return mValid;
}

//------------------------------------------------------------------------------
// Write header to generic file
//------------------------------------------------------------------------------
bool
HeaderCRC::WriteToFile(FileIo* pFile, uint16_t timeout)
{
  int offset = 0;
  char* buff = new char[mSizeHeader];
  memcpy(buff + offset, msTagName, sizeof msTagName);
  offset += sizeof mTag;
  memcpy(buff + offset, &mIdStripe, sizeof mIdStripe);
  offset += sizeof mIdStripe;
  memcpy(buff + offset, &mNumBlocks, sizeof mNumBlocks);
  offset += sizeof mNumBlocks;
  memcpy(buff + offset, &mSizeLastBlock, sizeof mSizeLastBlock);
  offset += sizeof mSizeLastBlock;
  memcpy(buff + offset, &mSizeBlock, sizeof mSizeBlock);
  offset += sizeof mSizeBlock;
  memset(buff + offset, 0, mSizeHeader - offset);

  if (pFile->fileWrite(0, buff, mSizeHeader, timeout) < 0) {
    mValid = false;
  } else {
    mValid = true;
  }

  delete[] buff;
  return mValid;
}

//------------------------------------------------------------------------------
// Dump header info in readable format
//------------------------------------------------------------------------------
std::string
HeaderCRC::DumpInfo() const
{
  std::ostringstream oss;

  if (!mValid) {
    oss << "ERROR: RAIN header not valid!";
    return oss.str();
  }

  oss << "Stripe index    : " << mIdStripe << std::endl
      << "Num. blocks     : " << mNumBlocks << std::endl
      << "Block size      : " << mSizeBlock << std::endl
      << "Size last block : " << mSizeLastBlock << std::endl;
  return oss.str();
}

EOSFSTNAMESPACE_END
