// ----------------------------------------------------------------------
// File: SHA1.hh
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

#ifndef __EOSFST_SHA1_HH__
#define __EOSFST_SHA1_HH__

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

class SHA1 : public CheckSum
{
private:
  SHA_CTX ctx;

  off_t sha1offset;
  unsigned char sha1[SHA_DIGEST_LENGTH + 1];
  unsigned char sha1hex[(SHA_DIGEST_LENGTH * 2) + 1];
public:

  SHA1 () : CheckSum ("sha1")
  {
    Reset();
  }

  off_t
  GetLastOffset ()
  {
    return sha1offset;
  }

  bool
  Add (const char* buffer, size_t length, off_t offset)
  {
    if (offset != sha1offset)
    {
        needsRecalculation = true;
        return false;
    }
    SHA1_Update(&ctx, (const void*) buffer, (unsigned long) length);
    sha1offset += length;
    return true;
  }

  const char*
  GetHexChecksum ()
  {
    Checksum = "";
    char hexs[16];
    for (int i = 0; i < SHA_DIGEST_LENGTH; i++)
    {
        sprintf(hexs, "%02x", sha1[i]);
        Checksum += hexs;
    }
    return Checksum.c_str();
  }

  const char*
  GetBinChecksum (int &len)
  {
    len = SHA_DIGEST_LENGTH;
    return (char*) &sha1;
  }

  int
  GetCheckSumLen ()
  {
    return SHA_DIGEST_LENGTH;
  }

  void
  Finalize ()
  {
    SHA1_Final(sha1, &ctx);
    sha1[SHA_DIGEST_LENGTH] = 0;
  }

  void
  Reset ()
  {
    sha1offset = 0;
    SHA1_Init(&ctx);
    memset(sha1, 0, SHA_DIGEST_LENGTH + 1);
    memset(sha1, 0, (SHA_DIGEST_LENGTH * 2) + 1);
    needsRecalculation = 0;
    sha1[0] = 0;
  }

  virtual
  ~SHA1 () { };

};

EOSFSTNAMESPACE_END

#endif
