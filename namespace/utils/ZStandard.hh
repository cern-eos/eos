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

#ifndef __EOS_NS__ZSTANDARD__HH__
#define __EOS_NS__ZSTANDARD__HH__

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#include <sys/stat.h>
#include <iostream>
#include <fstream>
#include <string>

#include "namespace/MDException.hh"
#include "namespace/utils/Buffer.hh"
#include "namespace/utils/DataHelper.hh"

namespace eos
{
class ZStandard
{
private:
  unsigned    pCompressionLevel;  /*compression level (1-19, default:5)*/
  char*       pDictBuffer;
  size_t      pDictSize;
  ZSTD_CDict* pCDict;
  ZSTD_DDict* pDDict;
  ZSTD_CCtx*  pCCtx;
  ZSTD_DCtx*  pDCtx;

  void loadDict(const std::string& dictionaryPath)
  {
    struct stat statbuf;

    if (stat(dictionaryPath.c_str(), &statbuf) != 0) {
      MDException e(errno);
      e.getMessage() << "Unable to stat source: " << dictionaryPath;
      throw e;
    }

    pDictSize = (size_t)statbuf.st_size;
    pDictBuffer = new char[pDictSize];

    if (pDictBuffer == nullptr) {
      MDException ex(errno);
      ex.getMessage() << "Dictionary read failed: ";
      ex.getMessage() << "memory allocation failed";
      throw ex;
    }

    //-------------------------------------------------------------------------
    // Dictionary load
    //-------------------------------------------------------------------------
    std::ifstream file(dictionaryPath);

    if (file.is_open()) {
      file.read(pDictBuffer, pDictSize);
    } else {
      MDException ex(EFAULT);
      ex.getMessage() << "Can't open ZSTD dictionary file: " << dictionaryPath;
      throw ex;
    }
  };

  void createCDict()
  {
    pCDict = (ZSTD_CDict*)pDictBuffer;

    if (pCDict == nullptr) {
      MDException ex(errno);
      ex.getMessage() << "Creation of compression dictionary failed";
      throw ex;
    }

    pCCtx = ZSTD_createCCtx();

    if (pCCtx == nullptr) {
      MDException ex(EFAULT);
      ex.getMessage() << "ZSTD_createCCtx() error";
      throw ex;
    }
  };

  void createDDict()
  {
    pDDict = ZSTD_createDDict(pDictBuffer, pDictSize);

    if (pDDict == nullptr) {
      MDException ex(errno);
      ex.getMessage() << "Creation of decompression dictionary failed";
      throw ex;
    }

    pDCtx = ZSTD_createDCtx();

    if (pDCtx == nullptr) {
      MDException ex(EFAULT);
      ex.getMessage() << "ZSTD_createDCtx() error";
      throw ex;
    }
  };

  void compression(Buffer& record)
  {
    size_t const cBuffSize = ZSTD_compressBound(record.size());
    void* const cBuff = malloc(cBuffSize);

    if (cBuff == nullptr) {
      MDException ex(errno);
      ex.getMessage() << "Compression failed: ";
      ex.getMessage() << "memory allocation failed";
      throw ex;
    }

    ZSTD_parameters params = ZSTD_getParams(pCompressionLevel, record.size(),
                                            pDictSize);
    size_t const cSize = ZSTD_compress_advanced(pCCtx, cBuff, cBuffSize,
                         record.getDataPtr(),
                         record.getSize(),
                         pCDict, pDictSize, params);

    if (ZSTD_isError(cSize)) {
      MDException ex(errno);
      ex.getMessage() << "Compression failed: ";
      ex.getMessage() << ZSTD_getErrorName(cSize);
      throw ex;
    }

    record.clear();
    record.putData(cBuff, cSize);
    free(cBuff);
  };

  void decompression(Buffer& record)
  {
    size_t const dBuffSize = ZSTD_DStreamOutSize();
    void* const dBuff = malloc(dBuffSize);

    if (dBuff == nullptr) {
      MDException ex(errno);
      ex.getMessage() << "Decompression failed: ";
      ex.getMessage() << "memory allocation failed";
      throw ex;
    }

    if (pDDict == nullptr) {
      MDException ex(errno);
      ex.getMessage() << "Decompression failed: ";
      ex.getMessage() << "dictionary was not set";
      throw ex;
    }

    size_t const dSize = ZSTD_decompress_usingDDict(pDCtx, dBuff, dBuffSize,
                         record.getDataPtr(),
                         record.getSize(), pDDict);

    if (ZSTD_isError(dSize)) {
      MDException ex(errno);
      ex.getMessage() << "Decompression failed: ";
      ex.getMessage() << ZSTD_getErrorName(dSize);
      throw ex;
    }

    record.clear();
    record.putData(dBuff, dSize);
    free(dBuff);
  };

  uint32_t decompression(Buffer& record, uint32_t& crcHead)
  {
    size_t const dBuffSize = ZSTD_DStreamOutSize();
    void* const dBuff = malloc(dBuffSize);

    if (dBuff == nullptr) {
      MDException ex(errno);
      ex.getMessage() << "Decompression failed: ";
      ex.getMessage() << "memory allocation failed";
      throw ex;
    }

    if (pDDict == nullptr) {
      MDException ex(errno);
      ex.getMessage() << "Decompression failed: ";
      ex.getMessage() << "dictionary was not set";
      throw ex;
    }

    size_t const dSize = ZSTD_decompress_usingDDict(pDCtx, dBuff, dBuffSize,
                         record.getDataPtr(),
                         record.getSize(), pDDict);

    if (ZSTD_isError(dSize)) {
      MDException ex(errno);
      ex.getMessage() << "Decompression failed: ";
      ex.getMessage() << ZSTD_getErrorName(dSize);
      throw ex;
    }

    uint32_t crc = DataHelper::updateCRC32(crcHead, dBuff, dSize);
    free(dBuff);
    return crc;
  };

public:
  ZStandard():
    pCompressionLevel(5), pDictBuffer(nullptr), pDictSize(0),
    pCDict(nullptr), pDDict(nullptr),
    pCCtx(nullptr), pDCtx(nullptr)
  {};

  ~ZStandard()
  {
    delete[] pDictBuffer;
    ZSTD_freeDDict(pDDict);
    ZSTD_freeCCtx(pCCtx);
    ZSTD_freeDCtx(pDCtx);
  };

  void setDicts(const std::string& dictionaryPath)
  {
    loadDict(dictionaryPath);
    createCDict();
    createDDict();
  };

  void setCDict(const std::string& dictionaryPath)
  {
    loadDict(dictionaryPath);
    createCDict();
  };

  void setDDict(const std::string& dictionaryPath)
  {
    loadDict(dictionaryPath);
    createDDict();
  };

  void setCompressionLevel(unsigned compressionLevel)
  {
    pCompressionLevel = compressionLevel;
  };

  void compress(Buffer& record)
  {
    compression(record);
  };

  void decompress(Buffer& record)
  {
    decompression(record);
  };

  uint32_t updateCRC32(Buffer& record, uint32_t& crcHead)
  {
    return decompression(record, crcHead);
  };
};
}

#endif // __EOS_NS__ZSTANDARD__HH__
