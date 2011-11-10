// ----------------------------------------------------------------------
// File: Adler.cc
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

/*----------------------------------------------------------------------------*/
#include "fst/checksum/Adler.hh"

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
bool
Adler::Add(const char* buffer, size_t length, off_t offset)
{
  Chunk currChunk;
  adler = adler32(adler, (const Bytef*) buffer, length);
  adleroffset += length;

  currChunk.offset = offset;
  currChunk.length = length;
  currChunk.adler = adler;

  map = AddElementToMap(map, currChunk);
   
  return true;
}

/*----------------------------------------------------------------------------*/
MapChunks&
Adler::AddElementToMap(MapChunks& map, Chunk& chunk)
{
  IterMap iter = map.find(chunk.offset);
  off_t offEndChunk = chunk.offset + chunk.length;

  if (iter != map.end()) {
    chunk.adler = adler32_combine(iter->second.adler, chunk.adler, chunk.length);
    chunk.offset = iter->second.offset;
    chunk.length = iter->second.length + chunk.length;
    map.erase(iter);
    map.insert(std::pair<off_t, Chunk>(offEndChunk, chunk));
  }
  else {
    map.insert(std::pair<off_t, Chunk>(offEndChunk, chunk));
  }
  
  return map;
}

/*----------------------------------------------------------------------------*/
const char*
Adler::GetHexChecksum()
{
  char sadler[1024];
  sprintf(sadler,"%08x",adler);
  Checksum = sadler;
  return Checksum.c_str();
}

/*----------------------------------------------------------------------------*/
const char*
Adler::GetBinChecksum(int &len)
{
  len = sizeof(unsigned int);
  return (char*) &adler;
}

/*----------------------------------------------------------------------------*/
/* compute the adler value of the map if we have the full map
 * (starts from 0 and there are no holes)
 */
void
Adler::ValidateAdlerMap()
{
  unsigned int value;
  IterMap iter1 = map.begin();
  IterMap iter2 = iter1;
  iter2++;
  value = iter1->second.adler;
  needsRecalculation = false;
    
  if (iter2 == map.end()) {
    //we have one chunk
    adler = value;
    return;
  }

  value = adler32_combine(value, iter2->second.adler, iter2->second.length);
    
  if ((iter1->second.offset != 0) ||
      (iter1->first != iter2->second.offset)) {
    needsRecalculation = true;
    return;
  }

  for ( ; iter2 != map.end(); iter1++, iter2++) {
    value = adler32_combine(value, iter2->second.adler, iter2->second.length);
    if (iter1->first != iter2->second.offset) {
      needsRecalculation = true;
      break;
    }
  }
  
  if (!needsRecalculation) {
      adler = value;
  }
  
  return;
}

/*----------------------------------------------------------------------------*/
void
Adler::Finalize()
{
  ValidateAdlerMap();
}

/*----------------------------------------------------------------------------*/
EOSFSTNAMESPACE_END
