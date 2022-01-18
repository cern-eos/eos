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
#include <stdlib.h>

EOSFSTNAMESPACE_BEGIN

char HeaderCRC::msTagName[] = "_HEADER__RAIDIO_";


//------------------------------------------------------------------------------
// Get OS page size aligned buffer
//------------------------------------------------------------------------------
std::unique_ptr<char, void(*)(void*)>
GetAlignedBuffer(const size_t size)
{
  static long os_pg_size = sysconf(_SC_PAGESIZE);
  char* raw_buffer = nullptr;
  std::unique_ptr<char, void(*)(void*)> buffer
  ((char*) raw_buffer, [](void* ptr) {
    if (ptr) {
      free(ptr);
    }
  });

  if (os_pg_size < 0) {
    return buffer;
  }

  if (posix_memalign((void**) &raw_buffer, os_pg_size, size)) {
    return buffer;
  }

  buffer.reset(raw_buffer);
  return buffer;
}

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
  mValid = false;
  long int offset = 0;
  size_t read_sizeblock = 0;
  auto buff = GetAlignedBuffer(mSizeHeader);

  if (buff == nullptr) {
    eos_static_err("msg=\"failed to allocate buffer\" size=%lu", mSizeHeader);
    return mValid;
  }

  if (pFile->fileRead(offset, buff.get(), mSizeHeader, timeout) !=
      static_cast<uint32_t>(mSizeHeader)) {
    return mValid;
  }

  memcpy(mTag, buff.get(), sizeof mTag);
  std::string tag = mTag;

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
  auto buff = GetAlignedBuffer(mSizeHeader);

  if (buff == nullptr) {
    eos_static_err("msg=\"failed to allocate buffer\" size=%lu", mSizeHeader);
    return mValid;
  }

  memcpy(buff.get() + offset, msTagName, sizeof msTagName);
  offset += sizeof mTag;
  memcpy(buff.get() + offset, &mIdStripe, sizeof mIdStripe);
  offset += sizeof mIdStripe;
  memcpy(buff.get() + offset, &mNumBlocks, sizeof mNumBlocks);
  offset += sizeof mNumBlocks;
  memcpy(buff.get() + offset, &mSizeLastBlock, sizeof mSizeLastBlock);
  offset += sizeof mSizeLastBlock;
  memcpy(buff.get() + offset, &mSizeBlock, sizeof mSizeBlock);
  offset += sizeof mSizeBlock;
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

  oss << "Stripe index    : " << mIdStripe << std::endl
      << "Num. blocks     : " << mNumBlocks << std::endl
      << "Block size      : " << mSizeBlock << std::endl
      << "Size last block : " << mSizeLastBlock << std::endl;
  return oss.str();
}

EOSFSTNAMESPACE_END
