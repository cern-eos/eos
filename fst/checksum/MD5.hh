// ----------------------------------------------------------------------
// File: MD5.hh
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

#ifndef __EOSFST_MD5_HH__
#define __EOSFST_MD5_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/checksum/CheckSum.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <openssl/md5.h>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class MD5 : public CheckSum {
private:
  MD5_CTX ctx;
  off_t md5offset;
  unsigned char md5[MD5_DIGEST_LENGTH+1];
  unsigned char md5hex[(MD5_DIGEST_LENGTH*2) +1];
public:
  MD5() : CheckSum("md5") {Reset();}

  off_t GetLastOffset() {return md5offset;}

  bool Add(const char* buffer, size_t length, off_t offset) {
    if (offset != md5offset) {
      needsRecalculation = true;
      return false;
    }
    MD5_Update(&ctx, (const void*) buffer, (unsigned long) length);
    md5offset += length;
    return true;
  }

  const char* GetHexChecksum() {
    Checksum="";
    char hexs[16];
    for (int i=0; i< MD5_DIGEST_LENGTH; i++) {
      sprintf(hexs,"%02x",md5[i]);
      Checksum += hexs;
    }
    return Checksum.c_str();
  }

  const char* GetBinChecksum(int &len) {
    len = MD5_DIGEST_LENGTH;
    return (char*) &md5;
  }

  int GetCheckSumLen() { return MD5_DIGEST_LENGTH;}

  void Finalize() {
    if (!finalized) 
    {
      MD5_Final(md5, &ctx);
      md5[MD5_DIGEST_LENGTH] = 0;
      finalized=true;
    }
  }

  void Reset () {
    md5offset = 0; MD5_Init(&ctx); memset(md5,0,MD5_DIGEST_LENGTH+1);needsRecalculation=0;md5hex[0]=0; finalized=false;
  }

  virtual ~MD5(){};

};

EOSFSTNAMESPACE_END

#endif
