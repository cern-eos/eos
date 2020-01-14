// ----------------------------------------------------------------------
// File: XXHASH64.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#ifndef __EOSFST_XXHASH64_HH__
#define __EOSFST_XXHASH64_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/checksum/CheckSum.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <xxhash.h>

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class XXHASH64 : public CheckSum
{
private:
  off_t xxhash64offset;
  uint64_t crcsum;
  XXH64_state_t* state;

public:

  XXHASH64 () : CheckSum ("xxhash64"), state(0)
  {
    Reset();
  }

  off_t
  GetLastOffset ()
  {
    return xxhash64offset;
  }

  bool
  Add (const char* buffer, size_t length, off_t offset)
  {
    if (offset != xxhash64offset)
    {
      needsRecalculation = true;
      return false;
    }
    crcsum = XXH64_update(state, (const Bytef*) buffer, length);
    xxhash64offset += length;
    return true;
  }

  const char*
  GetHexChecksum ()
  {
    char sxxhash64[1024];
    sprintf(sxxhash64, "%16lx", crcsum);
    Checksum = sxxhash64;
    return Checksum.c_str();
  }

  const char*
  GetBinChecksum (int &len)
  {
    len = sizeof (unsigned int);
    return (char*) &crcsum;
  }

  int
  GetCheckSumLen ()
  {
    return sizeof (unsigned int);
  }

  void 
  Finalize () 
  {
    if (!finalized) {
      crcsum = XXH64_digest(state);
    }
  }
  
  void
  Reset ()
  {
    if (state) {
      XXH64_freeState(state);
    }
    state = XXH64_createState();
    XXH64_reset(state, 0);
    xxhash64offset = 0;
    crcsum = 0;

    needsRecalculation = 0;
    finalized = false;
  }

  virtual
  ~XXHASH64 () { };

};

EOSFSTNAMESPACE_END

#endif
