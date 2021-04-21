// ----------------------------------------------------------------------
// File: BLAKE3.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#ifndef __EOSFST_BLAKE3_HH__
#define __EOSFST_BLAKE3_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/checksum/CheckSum.hh"
#include "common/blake3/blake3.h"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <zlib.h>

#ifdef ISAL_FOUND
#include <isa-l.h>
#endif

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class BLAKE3 : public CheckSum
{
private:
  blake3_hasher hasher;
  uint8_t blake3checksum[BLAKE3_OUT_LEN];
  off_t blake3offset;
  bool finalized;

public:

  BLAKE3() : CheckSum("blake3")
  {
    Reset();
  }

  off_t
  GetLastOffset()
  {
    return blake3offset;
  }

  bool
  Add(const char* buffer, size_t length, off_t offset)
  {
    if (offset != blake3offset) {
      needsRecalculation = true;
      return false;
    }

    if (finalized) {
	return false;
    }

    blake3_hasher_update(&hasher, buffer, length);

    blake3offset += length;
    return true;
  }

  const char*
  GetHexChecksum()
  {
    if (!finalized) {
      Finalize();
    }

    Checksum="";
    for (size_t i=0; i<BLAKE3_OUT_LEN;++i) {
      char b3[3];
      sprintf(b3, "%02x", blake3checksum[i]);
      Checksum += b3;
    }
    return Checksum.c_str();
  }

  const char*
  GetBinChecksum(int& len)
  {
    if (!finalized) {
      Finalize();
    }

    len = BLAKE3_OUT_LEN;
    return (char*) &blake3checksum;
  }

  int
  GetCheckSumLen()
  {
    return BLAKE3_OUT_LEN;
  }

  void
  Reset()
  {
    blake3_hasher_init(&hasher);
    memset(&blake3checksum,0,BLAKE3_OUT_LEN);
    blake3offset = 0;
    needsRecalculation = 0;
    finalized = false;
  }

  void
  Finalize()
  {
    if (!finalized) {
      blake3_hasher_finalize(&hasher, blake3checksum, BLAKE3_OUT_LEN);
      finalized = true;
    }
  }

  virtual
  ~BLAKE3() { };

};

EOSFSTNAMESPACE_END

#endif
