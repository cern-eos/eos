// ----------------------------------------------------------------------
// File: Adler.hh
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

#ifndef __EOSFST_ADLER_HH__
#define __EOSFST_ADLER_HH__

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

class Adler : public CheckSum {
private:
  off_t adleroffset;
  unsigned int adler;
  
public:
  Adler() : CheckSum("adler") {Reset();}

  off_t GetLastOffset() {return adleroffset;}

  bool Add(const char* buffer, size_t length, off_t offset) {
    if (offset != adleroffset) {
      needsRecalculation = true;
      return false;
    }
    adler = adler32(adler, (const Bytef*) buffer, length);
    adleroffset += length;
    return true;
  }

  const char* GetHexChecksum() {
    char sadler[1024];
    sprintf(sadler,"%08x",adler);
    Checksum = sadler;
    return Checksum.c_str();
  }

  const char* GetBinChecksum(int &len) {
    len = sizeof(unsigned int);
    return (char*) &adler;
  }

  int GetCheckSumLen() { return sizeof(unsigned int);}

  void Reset() {
    adleroffset = 0; adler = adler32(0L, Z_NULL,0); needsRecalculation=0;
  }

  virtual ~Adler(){};

};

EOSFSTNAMESPACE_END

#endif
