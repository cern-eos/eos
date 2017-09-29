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

EOSCOMMONNAMESPACE_BEGIN

class ZStandard : public Compression
{
private:
  unsigned    pCompressionLevel;  /*compression level (1-19, default:5)*/
  char*       pDictBuffer;
  size_t      pDictSize;
  ZSTD_CDict* pCDict;
  ZSTD_DDict* pDDict;
  ZSTD_CCtx*  pCCtx;
  ZSTD_DCtx*  pDCtx;
  RWMutex mCompressLock;

  void loadDict(const std::string& dictionaryPath);

  void createCDict();

  void createDDict();

  uint32_t decompression(Buffer& record, uint32_t& crcHead);

public:
  ZStandard():
    pCompressionLevel(5), pDictBuffer(nullptr), pDictSize(0),
    pCDict(nullptr), pDDict(nullptr),
    pCCtx(nullptr), pDCtx(nullptr)
  {};

  ~ZStandard();

  void setDicts(const std::string& dictionaryPath);

  void setCDict(const std::string& dictionaryPath);

  void setDDict(const std::string& dictionaryPath);

  void setCompressionLevel(unsigned compressionLevel);

  virtual void compress(Buffer& record) override;

  virtual void decompress(Buffer& record) override;

  uint32_t updateCRC32(Buffer& record, uint32_t& crcHead);
};

EOSCOMMONNAMESPACE_END

#endif // __EOS_COMMON__ZSTANDARD__HH__

