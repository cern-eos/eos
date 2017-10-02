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

//------------------------------------------------------------------------------
// author: Branko Blagojević <branko.blagojevic@comtrade.com>
// desc:   Class for ZStandard compression and decompression
//------------------------------------------------------------------------------

#ifndef __EOS_COMMON__ZSTANDARD__HH__
#define __EOS_COMMON__ZSTANDARD__HH__

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#include <sys/stat.h>
#include <iostream>
#include <fstream>
#include <string>

#include "namespace/MDException.hh"
#include "namespace/utils/Buffer.hh"
#include "namespace/utils/DataHelper.hh"
#include "common/Namespace.hh"
#include "common/compression/Compression.hh"
#include "common/RWMutex.hh"
#include "common/ConcurrentQueue.hh"

EOSCOMMONNAMESPACE_BEGIN

//! @brief Class providing efficient, thread-safe zstd compression and decompression
class ZStandard : public Compression
{
private:
  char*       pDictBuffer;
  size_t      pDictSize;
  ZSTD_CDict* pCDict;
  ZSTD_DDict* pDDict;
  ConcurrentQueue<ZSTD_CCtx*> mCompressCtxPool; //! Pool of compression context objects for efficient concurrent usage
  ConcurrentQueue<ZSTD_DCtx*> mDecompressCtxPool; //! Pool of decompression context objects for efficient concurrent usage

  //! @brief Load the dictionary from file to memory
  //! @param dictionaryPath path of the dictioanry file
  void LoadDict(const std::string& dictionaryPath);

  //! @brief Creates the compression dictionary object
  void CreateCDict();

  //! @brief Creates the decompression dictionary object
  void CreateDDict();

public:
  ZStandard():
    pDictBuffer(nullptr), pDictSize(0),
    pCDict(nullptr), pDDict(nullptr)
  {};

  ~ZStandard();

  //! @brief Load and use both the dictionary for both compression and decompression
  //! @param dictionaryPath path of the dictionary file
  void SetDicts(const std::string& dictionaryPath);

  //! @brief Load and use the compression dictionary
  //! @param dictionaryPath path of the dictionary file
  void SetCDict(const std::string& dictionaryPath);

  //! @brief Load and use the decompression dictionary
  //! @param dictionaryPath path of the dictionary file
  void SetDDict(const std::string& dictionaryPath);

  //! @brief Compress binary data
  //! @param record binary data
  virtual void Compress(Buffer& record) override;

  //! @brief Decompress binary data
  //! @param record binary data
  virtual void Decompress(Buffer& record) override;
};

EOSCOMMONNAMESPACE_END

#endif // __EOS_COMMON__ZSTANDARD__HH__

