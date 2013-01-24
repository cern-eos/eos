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
#include <map>

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

struct ltoff_t
{

  bool operator()(off_t s1, off_t s2) const
  {
    return (s1 < s2);
  }
};

typedef struct
{
  off_t offset;
  size_t length;
  unsigned int adler;
} Chunk;

typedef std::map<off_t, Chunk, ltoff_t> MapChunks;
typedef std::map<off_t, Chunk, ltoff_t>::iterator IterMap;

class Adler : public CheckSum
{
private:
  off_t adleroffset;
  off_t maxoffset;
  unsigned int adler;
  MapChunks map;

public:

  Adler () : CheckSum ("adler")
  {
    Reset();
  }

  bool Add (const char* buffer, size_t length, off_t offset);
  MapChunks& AddElementToMap (MapChunks& map, Chunk& chunk);

  off_t
  GetLastOffset ()
  {
    return adleroffset;
  }

  off_t
  GetMaxOffset ()
  {
    return maxoffset;
  }

  int
  GetCheckSumLen ()
  {
    return sizeof (unsigned int);
  }
  void ValidateAdlerMap ();

  const char* GetHexChecksum ();
  const char* GetBinChecksum (int &len);

  void Finalize ();

  void
  Reset ()
  {
    map.clear();
    adleroffset = 0;
    adler = adler32(0L, Z_NULL, 0);
    needsRecalculation = false;
    maxoffset = 0;
  }

  void
  ResetInit (off_t offsetInit, size_t lengthInit, const char* checksumInitHex)
  {
    Chunk currChunk;
    maxoffset = 0;
    adleroffset = offsetInit + lengthInit;

    // check if this is actually a valid pointer
    if (checksumInitHex)
      return;

    int checksumInitBin = strtol(checksumInitHex, 0, 16);

    // if a file is truncated we get 0,0,<some checksum => reset to 0
    if (lengthInit != 0)
    {
      adler = checksumInitBin;
    }
    else
    {
      adler = adler32(0L, Z_NULL, 0);
    }

    fprintf(stderr, "adler is %x\n", adler);
    currChunk.offset = offsetInit;
    currChunk.length = lengthInit;
    currChunk.adler = adler;

    map.clear();
    map = AddElementToMap(map, currChunk);
    maxoffset = (offsetInit + lengthInit);
    needsRecalculation = false;
  }

  virtual
  ~Adler () { };

};

EOSFSTNAMESPACE_END

#endif
