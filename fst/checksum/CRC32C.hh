// ----------------------------------------------------------------------
// File: CRC32C.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#ifndef __EOSFST_CRC32C_HH__
#define __EOSFST_CRC32C_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/checksum/CheckSum.hh"
#include "common/crc32c/crc32c.h"
/*----------------------------------------------------------------------------*/
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucString.hh>
#include <XrdSys/XrdSysPthread.hh>
/*----------------------------------------------------------------------------*/
#include <zlib.h>

#ifdef ISAL_FOUND
#include <isa-l.h>
#endif

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class CRC32C : public CheckSum
{
private:
  off_t crc32coffset;
  mutable uint32_t crcsum;

public:

  CRC32C() : CheckSum("crc32c")
  {
    Reset();
  }

  off_t
  GetLastOffset() const override
  {
    return crc32coffset;
  }

  bool
  Add(const char* buffer, size_t length, off_t offset)
  {
    std::lock_guard<std::mutex> lock(mMutex);

    if (offset < 0) {
      offset = crc32coffset;
    }

    if (offset != crc32coffset) {
      needsRecalculation = true;
      return false;
    }

    if (finalized) {                /* handle read + append case, a little hackish. Alternative: set needsRecalculation */
      crcsum = ~crcsum;           /* undo what Finalize did under the hood */
      finalized = false;
    }

#ifdef ISAL_FOUND
    crcsum = crc32_iscsi((unsigned char*) buffer, length, crcsum);
#else
    crcsum = checksum::crc32c(crcsum, (const Bytef*) buffer, length);
#endif
    crc32coffset += length;
    return true;
  }

  const char*
  GetHexChecksum() const override
  {
    if (!finalized) {
      Finalize();
    }

    char scrc32[1024];
    sprintf(scrc32, "%08x", crcsum);
    Checksum = scrc32;
    return Checksum.c_str();
  }

  const char*
  GetBinChecksum(int& len) const override
  {
    if (!finalized) {
      Finalize();
    }

    len = sizeof(unsigned int);
    return (char*) &crcsum;
  }

  int
  GetCheckSumLen() const override
  {
    return sizeof(unsigned int);
  }

  void
  Reset()
  {
    crcsum = checksum::crc32cInit();
    crc32coffset = 0;
    needsRecalculation = 0;
    finalized = false;
  }

  void
  Finalize() const override
  {
    if (!finalized) {
      crcsum = checksum::crc32cFinish(crcsum);
      finalized = true;
    }
  }

  virtual
  ~CRC32C() { };

};

EOSFSTNAMESPACE_END

#endif
