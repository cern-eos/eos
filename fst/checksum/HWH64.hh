// ----------------------------------------------------------------------
// File: HWH64.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#ifndef __EOSFST_HWH64_HH__
#define __EOSFST_HWH64_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/checksum/CheckSum.hh"
/*----------------------------------------------------------------------------*/
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucString.hh>
/*----------------------------------------------------------------------------*/
#include "common/highwayhash/highwayhash.h"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

#define HWH64_DIGEST_LENGTH 8

using namespace highwayhash;

class HWH64 : public CheckSum
{

private:
  const HHKey key HH_ALIGNAS(32) = {1, 2, 3, 4};
  mutable HighwayHashCatT<HH_TARGET_PREFERRED> ctx;
  mutable HHResult64 result;

  off_t hwhoffset;
public:
  HWH64() : CheckSum("hwh"), ctx(key)
  {
    Reset();
  }

  off_t GetLastOffset() const override
  {
    return hwhoffset;
  }

  bool Add(const char* buffer, size_t length, off_t offset)
  {
    if (offset < 0) {
      offset = hwhoffset;
    }

    if (offset != hwhoffset || finalized) {
      needsRecalculation = true;
      return false;
    }

    ctx.Append((const char*) buffer, (unsigned long) length);
    hwhoffset += length;
    return true;
  }

  const char* GetHexChecksum() const override
  {
    Checksum = "";
    char hexs[17];
    sprintf(hexs, "%016" PRIx64, result);
    Checksum += hexs;
    return Checksum.c_str();
  }

  const char* GetBinChecksum(int& len) const override
  {
    len = HWH64_DIGEST_LENGTH;
    return (char*) &result;
  }

  int GetCheckSumLen() const override
  {
    return HWH64_DIGEST_LENGTH;
  }

  void Finalize() const override
  {
    if (!finalized) {
      ctx.Finalize(&result);
      finalized = true;
    }
  }

  void Reset()
  {
    ctx.Reset(key);
    hwhoffset = 0;
    needsRecalculation = 0;
    finalized = false;
  }

  virtual ~HWH64() {};

};

EOSFSTNAMESPACE_END

#endif
