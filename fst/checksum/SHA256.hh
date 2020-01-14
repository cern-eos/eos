// ----------------------------------------------------------------------
// File: SHA256.hh
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

#ifndef __EOSFST_SHA256_HH__
#define __EOSFST_SHA256_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/checksum/CheckSum.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <openssl/sha.h>

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class SHA256 : public CheckSum
{
private:
  SHA256_CTX ctx;

  off_t sha256offset;
  unsigned char sha256[SHA256_DIGEST_LENGTH + 1];
public:

  SHA256 () : CheckSum ("sha256")
  {
    Reset();
  }

  off_t
  GetLastOffset ()
  {
    return sha256offset;
  }

  bool
  Add (const char* buffer, size_t length, off_t offset)
  {
    if (offset != sha256offset)
    {
        needsRecalculation = true;
        return false;
    }
    SHA256_Update(&ctx, (const void*) buffer, (unsigned long) length);
    sha256offset += length;
    return true;
  }

  const char*
  GetHexChecksum ()
  {
    Checksum = "";
    char hexs[16];
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++)
    {
        sprintf(hexs, "%02x", sha256[i]);
        Checksum += hexs;
    }
    return Checksum.c_str();
  }

  const char*
  GetBinChecksum (int &len)
  {
    len = SHA256_DIGEST_LENGTH;
    return (char*) &sha256;
  }

  int
  GetCheckSumLen ()
  {
    return SHA256_DIGEST_LENGTH;
  }

  void
  Finalize ()
  {
    if (!finalized) 
    {
      SHA256_Final(sha256, &ctx);
      sha256[SHA256_DIGEST_LENGTH] = 0;
      finalized = true;
    }
  }

  void
  Reset ()
  {
    sha256offset = 0;
    SHA256_Init(&ctx);
    memset(sha256, 0, SHA256_DIGEST_LENGTH + 1);
    needsRecalculation = 0;
    sha256[0] = 0;
    finalized = false;
  }

  virtual
  ~SHA256 () { };

};

EOSFSTNAMESPACE_END

#endif
