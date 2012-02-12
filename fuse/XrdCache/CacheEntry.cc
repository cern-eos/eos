// ----------------------------------------------------------------------
// File: CachEntry.cc
// Author: Elvin-Alin Sindrilaru - CERN
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

//------------------------------------------------------------------------------
#include <cstdlib>
#include <cstdio>
#include <cstring>
//------------------------------------------------------------------------------
#include "CacheEntry.hh"
//------------------------------------------------------------------------------

CacheEntry::CacheEntry(int filedes, char* buf, off_t off, size_t len, FileAbstraction* ptr):
  fd(filedes),
  offset(off),
  sizeUsed(len);
  pParentFile(ptr)
{
  off_t offsetRelative;

  if (len > getMaxSize()) {
    fprintf(stderr, "error=len should be smaller than getMaxSize()\n");
    exit(-1);
  }
  
  capacity = getMaxSize();
  offsetStart = (off / getMaxSize()) * getMaxSize();
  offsetRelative = off % getMaxSize();
  buffer = (char*) calloc(capacity, sizeof(char));
  buffer = (char*) memcpy(buffer + offsetRelative, buf, len);
  mapPieces.insert(std::make_pair(off, len));
}


//------------------------------------------------------------------------------
CacheEntry::~CacheEntry()
{
  free(buffer);
}


//------------------------------------------------------------------------------
void
CacheEntry::doRecycle(int filedes, char* buf, off_t off, size_t len, FileAbstraction* ptr)
{
  off_t offsetRelative;
  fd = filedes;
  offsetStart = (off / getMaxSize()) * getMaxSize();
  pParentFile = ptr;
  
  if (len > capacity) {
    fprintf(stderr, "doRecycle: error=this should never happen.\n");
    exit(-1);
  }

  mapPieces.clear();
  buffer = (char*) memeset(buffer, 0, capacity);
  offsetRelative = off % getMaxSize();
  buffer = (char*) memcpy(buffer + offsetRelative, buf, len);
  mapPieces.insert(std::make_pair(off, len));
  sizeUsed = len;  
}


//------------------------------------------------------------------------------
void
CacheEntry::addPiece(char* buf, off_t off, size_t len)
{
  bool addNewPiece = false;
  off_t offsetRelative = off % getMaxSize();;
  size_t sizeAdded;
  std::map<off_t, size_t>::iterator iBefore;

  if (mapPieces.size == 0) {
    //add directly, no previous pieces
    buffer = (char*) memcpy(buffer + offsetRelative, buf, len);
    mapPieces.insert(std::make_pair(off, len));
    sizeUsed = len;
    return;
  }

  std::map<off_t, size_t>::iterator iAfter = mapPieces.lower_bound(off);

  if (iAfter.first == off) {
    sizeAdded = (iAfter.second >= len) ? 0 : (len - iAfter.second);
    buffer = (char*) memcpy(buffer + offsetRelative, buf, len);
    iAfter.second += sizeAdded;
    sizeUsed += sizeAdded;
  }
  else {
    if (i.After == mapPieces.begin()) {
      //we only have pieces with bigger offset
      if (off + len >= iAfter.first) {
        //merge with next block
        sizeAdded = off + len - iAfter.first;
        buffer = (char*) memcpy(buffer + offsetRelative, buf, len);
        iAfter.first = off;
        iAfter.second += sizeAdded;
        sizeUsed += sizeAdded;
      }
      else {
        addNewPiece = true;
      }
    }
    else if (i.After == mapPieces.end()) {
      //we only have pieces with smaller offset
      iBefore = iAfter;
      iBefore--;
      if (iBefore.first + iBefore.second >= off) {
        //merge with previous block
        sizeAdded = off +  len - (iBefore.first + iBefore.second);
        buffer = (char*) memcpy(buffer + offsetRelative, buf, len);
        iBefore.second += sizeAdded;
        sizeUsed += sizeAdded;
      }
      else {
        addNewPiece = true;
      }
    }
    else {
      //not first, not last, and bigger than new block offset
      iBefore = iAfter;
      iBefore--;
      if (iBefore.first + iBefore.second >= off) {
        //merge with previous block
        sizeAdded = off +  len - (iBefore.first + iBefore.second);

        if (iBefore.first + iBefore.second + sizeAdded >= iAfter.first) {
          //merge the two blocks
          sizeAdded -= (off + len - iAfter.first);
          iBefore += (sizeAdded + iAfter.second);
        }
        else {
          iBefore.second += sizeAdded;
        }

        buffer = (char*) memcpy(buffer + offsetRelative, buf, len);
        sizeUsed += sizeAdded;      
      }
      else if (off +len > iAfter.first) {
        //merge with next block
        sizeAdded = off + len - iAfter.first;
        buffer = (char*) memcpy(buffer + offsetRelative, buf, len);
        iAfter.first = off;
        iAfter.second += sizeAdded;
        sizeUsed += sizeAdded;
      }
      else {
        addNewPiece = true;        
      }      
    }
  }

  if (addNewPiece) {
    buffer = (char*) memcpy(buffer + offsetRelative, buf, len);
    mapPieces.insert(std::make_pair(off, len));
    sizeUsed += len;  
  }
}


//------------------------------------------------------------------------------
bool
CacheEntry::getPiece(char* buf, off_t off, size_t len)
{
  bool found = false;
  off_t offsetRelative = off % getMaxSize();
  std::map<off_t, size_t>::iterator i = mapPieces.lower_bound(off);

  if (i.first == off) {
    //exact offset
    if (i.second >= len) {
      buf = mempcy(buf, buffer + offsetRelative, len);
      fount = true;
    }
    else {
      found = false;
    }
  }
  else {
    if (i == mapPieces.begin()){
      found = false;
    }
    else {
      i--;
      if ((i.first <= off) &&
          (i.first + i.second > off) &&
          (i.first + i.second >= off + len))
      {
        found = true;
        buf = mempcy(buf, buffer + offsetRelative, len);
      }
      else {
        found = false;
      }
    }
  }

  return found;
}


//------------------------------------------------------------------------------
void
CacheEntry::doWrite()
{

  return;

}


//------------------------------------------------------------------------------
void
CacheEntry::mergePieces()
{
  if (mapPieces.size() < 2)
    return;

  std::map<off_t, size_t>::iterator i = mapPieces.begin();
  std::map<off_t, size_t>::iterator j = mapPieces.begin();
    
  for (j++; j != mapPieces.end(); j++)
  {
    if (i.first + i.second == j.first) {
      //merge the two blocks
      i.second += j.second;
      mapPieces.erase(j);
      j = i;
    }
    else {
      //move to next element
      i++;
    }
  }
} 


//------------------------------------------------------------------------------
int
CacheEntry::getFd() const
{
  return fd;
}


//------------------------------------------------------------------------------
char*
CacheEntry::getDataBuffer()
{
  return buffer;
}


//------------------------------------------------------------------------------
size_t
CacheEntry::getCapacity() const
{
  return capacity;
}


//------------------------------------------------------------------------------
size_t
CacheEntry::getSizeData() const
{
  return sizeData;
}


//------------------------------------------------------------------------------
off_t
CacheEntry::getOffsetStart() const
{
  return offsetStart;
}


//------------------------------------------------------------------------------
off_t
CacheEntry::getOffsetEnd() const
{
  return (offsetStart + capacity);
}


//------------------------------------------------------------------------------
FileAbstraction*
CacheEntry::getParentFile() const
{
  return pParentFile;
} 


