// ----------------------------------------------------------------------
// File: Adler.cc
// Author: Andreas-Joachim Peters/Elvin Sindrilaru - CERN
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

/*----------------------------------------------------------------------------*/
#include "fst/checksum/Adler.hh"

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
bool
Adler::Add (const char* buffer, size_t length, off_t offset)
{
  if (offset != adleroffset)
    needsRecalculation = true;

  adler = adler32(0L, Z_NULL, 0);
  Chunk currChunk;
  adler = adler32(adler, (const Bytef*) buffer, length);
  adleroffset = offset + length;
  if (adleroffset > maxoffset)
  {
    maxoffset = adleroffset;
  }
  currChunk.offset = offset;
  currChunk.length = length;
  currChunk.adler = adler;

  map = AddElementToMap(map, currChunk);

  return true;
}

/*----------------------------------------------------------------------------*/
MapChunks&
Adler::AddElementToMap (MapChunks& map, Chunk& chunk)
{
  off_t offEndChunk = chunk.offset + chunk.length;
  IterMap iter = map.find(offEndChunk);

  if (iter != map.end())
  {
    map.erase(iter);
    map.insert(std::pair<off_t, Chunk > (offEndChunk, chunk));
  }
  else
  {
    map.insert(std::pair<off_t, Chunk > (offEndChunk, chunk));
  }

  return map;
}

/*----------------------------------------------------------------------------*/
const char*
Adler::GetHexChecksum ()
{
  char sadler[1024];
  sprintf(sadler, "%08x", adler);
  Checksum = sadler;
  return Checksum.c_str();
}

/*----------------------------------------------------------------------------*/
const char*
Adler::GetBinChecksum (int &len)
{
  len = sizeof (unsigned int);
  return (char*) &adler;
}

/*----------------------------------------------------------------------------*/

/* compute the adler value of the map if we have the full map
 * (starts from 0 and there are no holes)
 */
void
Adler::ValidateAdlerMap ()
{
  unsigned int value;
  adler = adler32(0L, Z_NULL, 0);

  if (map.begin() == map.end())
  {
    adler = adler32(0L, Z_NULL, 0);
    return;
  }

  IterMap iter1 = map.begin();
  IterMap iter2 = iter1;

  value = iter1->second.adler;

  IterMap iter3;
  //  for (iter3= map.begin(); iter3!= map.end(); iter3++) {
  //    fprintf(stderr,"ADLER VALIDATE %llu %llu %llu %x\n", iter3->first,iter3->second.offset, iter3->second.length, iter3->second.adler);
  //  } 

  if (iter1->second.offset != 0)
  {
    // the first chunk is not at the beginning
    needsRecalculation = true;
    adler = adler32(0L, Z_NULL, 0);
    return;
  }

  if (map.begin() == map.end())
  {
    //we have no chunk
    adler = adler32(0L, Z_NULL, 0);
    return;
  }

  needsRecalculation = false;

  iter2++;

  if (iter2 == map.end())
  {
    if ((iter1->first) != maxoffset)
    {
      // there was probably some overwrite
      needsRecalculation = true;
    }
    else
    {
      if (iter1->second.offset != 0)
      {
        needsRecalculation = true;
      }
    }
    //we have one chunk
    adler = value;
    return;
  }

  off_t appliedoffset = 0;

  for (; iter2 != map.end(); iter1++, iter2++)
  {
    appliedoffset = iter2->first;
    value = adler32_combine(value, iter2->second.adler, iter2->second.length);
    if (iter1->first != iter2->second.offset)
    {
      needsRecalculation = true;
      break;
    }
  }

  if (appliedoffset != maxoffset)
  {
    // there was probably some overwrite
    needsRecalculation = true;
  }

  if (!needsRecalculation)
  {
    adler = value;
  }
  else
  {
    adler = adler32(0L, Z_NULL, 0);
  }

  fflush(stdout);
  return;
}

/*----------------------------------------------------------------------------*/
void
Adler::Finalize ()
{
  ValidateAdlerMap();
}

/*----------------------------------------------------------------------------*/
EOSFSTNAMESPACE_END
