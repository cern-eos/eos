// ----------------------------------------------------------------------
// File: CRC32.hh
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

#ifndef __EOSFST_CRC32_HH__
#define __EOSFST_CRC32_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/checksum/CheckSum.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <zlib.h>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class CRC32 : public CheckSum {
private:
  off_t crc32offset;
  unsigned int crcsum;
  
public:
  CRC32() : CheckSum("crc32") {Reset();}

  off_t GetLastOffset() {return crc32offset;}

  bool Add(const char* buffer, size_t length, off_t offset) {
    if (offset != crc32offset) {
      needsRecalculation = true;
      return false;
    }
    crcsum = crc32(crcsum, (const Bytef*) buffer, length);
    crc32offset += length;
    return true;
  }

  const char* GetHexChecksum() {
    char scrc32[1024];
    sprintf(scrc32,"%08x",crcsum);
    Checksum = scrc32;
    return Checksum.c_str();
  }

  const char* GetBinChecksum(int &len) {
    len = sizeof(unsigned int);
    return (char*) &crcsum;
  }

  int GetCheckSumLen() { return sizeof(unsigned int);}

  void Reset() {
    crc32offset = 0; crcsum = crc32(0L, Z_NULL,0);needsRecalculation=0;
  }

  virtual ~CRC32(){};

};

EOSFSTNAMESPACE_END

#endif
