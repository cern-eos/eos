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
#include "common/BufferManager.hh"
#include <stdint.h>
#include <stdlib.h>

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
  mSizeHeader(sizeHeader),
  mBlockChecksum(nullptr),
  mBlockChecksumSize(0)
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
  mSizeHeader(sizeHeader),
  mBlockChecksum(nullptr),
  mBlockChecksumSize(0)
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
  mValid = false;
  long int offset = 0;
  size_t read_sizeblock = 0;
  size_t block_xs_size = 0;
  auto buff = eos::common::GetAlignedBuffer(mSizeHeader);

  if (buff == nullptr) {
    eos_static_err("msg=\"failed to allocate buffer\" size=%lu", mSizeHeader);
    return mValid;
  }

  if (pFile->fileRead(offset, buff.get(), mSizeHeader, timeout) !=
      static_cast<uint32_t>(mSizeHeader)) {
    return mValid;
  }

  memcpy(mTag, buff.get(), sizeof mTag);

  if (strncmp(mTag, msTagName, strlen(msTagName))) {
    return mValid;
  }

  offset += sizeof mTag;
  memcpy(&mIdStripe, buff.get() + offset, sizeof mIdStripe);
  offset += sizeof mIdStripe;
  memcpy(&mNumBlocks, buff.get() + offset, sizeof mNumBlocks);
  offset += sizeof mNumBlocks;
  memcpy(&mSizeLastBlock, buff.get() + offset, sizeof mSizeLastBlock);
  offset += sizeof mSizeLastBlock;
  memcpy(&read_sizeblock, buff.get() + offset, sizeof read_sizeblock);
  offset += sizeof read_sizeblock;
  memcpy(&mChecksumType, buff.get() + offset, sizeof mChecksumType);
  offset += sizeof mChecksumType;
  memcpy(&block_xs_size, buff.get() + offset, sizeof block_xs_size);
  offset += sizeof block_xs_size;
  char* block_xs = new char[block_xs_size];
  memcpy(block_xs, buff.get() + offset, block_xs_size);
  SetBlockChecksum(block_xs, block_xs_size, mChecksumType);
  delete[] block_xs;

  if (mSizeBlock == 0) {
    mSizeBlock = read_sizeblock;
  } else if (mSizeBlock != read_sizeblock) {
    eos_static_err("msg=\"read block size does not match expected size\" "
                   "got=%lu expected=%lu", read_sizeblock, mSizeBlock);
    return mValid;
  }

  mValid = true;
  return mValid;
}

//------------------------------------------------------------------------------
// Write header to generic file
//------------------------------------------------------------------------------
bool
HeaderCRC::WriteToFile(FileIo* pFile, uint16_t timeout)
{
  mValid = false;
  int offset = 0;
  auto buff = eos::common::GetAlignedBuffer(mSizeHeader);

  if (buff == nullptr) {
    eos_static_err("msg=\"failed to allocate buffer\" size=%lu", mSizeHeader);
    return mValid;
  }

  memcpy(buff.get() + offset, msTagName, strlen(msTagName));
  offset += strlen(msTagName);
  memcpy(buff.get() + offset, &mIdStripe, sizeof mIdStripe);
  offset += sizeof mIdStripe;
  memcpy(buff.get() + offset, &mNumBlocks, sizeof mNumBlocks);
  offset += sizeof mNumBlocks;
  memcpy(buff.get() + offset, &mSizeLastBlock, sizeof mSizeLastBlock);
  offset += sizeof mSizeLastBlock;
  memcpy(buff.get() + offset, &mSizeBlock, sizeof mSizeBlock);
  offset += sizeof mSizeBlock;
  memcpy(buff.get() + offset, &mChecksumType, sizeof mChecksumType);
  offset += sizeof mChecksumType;
  memcpy(buff.get() + offset, &mBlockChecksumSize, sizeof mBlockChecksumSize);
  offset += sizeof mBlockChecksumSize;
  memcpy(buff.get() + offset, mBlockChecksum.get(), mBlockChecksumSize);
  offset += mBlockChecksumSize;
  // TODO: check that offset is not bigger then header size
  memset(buff.get() + offset, 0, mSizeHeader - offset);

  if (pFile->fileWrite(0, buff.get(), mSizeHeader, timeout) > 0) {
    mValid = true;
  }

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

  auto xs = GetBlockChecksum();
  oss << "Stripe index    : " << mIdStripe << std::endl
      << "Num. blocks     : " << mNumBlocks << std::endl
      << "Block size      : " << mSizeBlock << std::endl
      << "Size last block : " << mSizeLastBlock << std::endl
      << "Checksum type   : " << eos::common::LayoutId::GetChecksumString(
        mChecksumType) << std::endl
      << "Checksum block  : " << xs->GetHexChecksum() << std::endl;
  return oss.str();
}

EOSFSTNAMESPACE_END
