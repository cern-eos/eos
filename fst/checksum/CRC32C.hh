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
#include "fst/checksum/crc32c.h"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <zlib.h>

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class CRC32C : public CheckSum
{
private:
  off_t crc32coffset;
  uint32_t crcsum;
  bool finalized;

public:

  CRC32C () : CheckSum ("crc32c")
  {
    Reset();
  }

  off_t
  GetLastOffset ()
  {
    return crc32coffset;
  }

  bool
  Add (const char* buffer, size_t length, off_t offset)
  {
    if (offset != crc32coffset)
    {
        needsRecalculation = true;
        return false;
    }
    crcsum = checksum::crc32c(crcsum, (const Bytef*) buffer, length);
    crc32coffset += length;
    return true;
  }

  const char*
  GetHexChecksum ()
  {
    if (!finalized)
        Finalize();

    char scrc32[1024];
    sprintf(scrc32, "%08x", crcsum);
    Checksum = scrc32;
    return Checksum.c_str();
  }

  const char*
  GetBinChecksum (int &len)
  {
    if (!finalized)
        Finalize();

    len = sizeof (unsigned int);
    return (char*) &crcsum;
  }

  int
  GetCheckSumLen ()
  {
    return sizeof (unsigned int);
  }

  void
  Reset ()
  {
    crcsum = checksum::crc32cInit();
    crc32coffset = 0;
    needsRecalculation = 0;
    finalized = false;
  }

  void
  Finalize ()
  {
    if (!finalized)
    {
        crcsum = checksum::crc32cFinish(crcsum);
        finalized = true;
    }
  }

  virtual
  ~CRC32C () { };

};

EOSFSTNAMESPACE_END

#endif
