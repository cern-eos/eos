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
  sizeData(len),
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
  pParentFile->incrementNoWriteBlocks();
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
  buffer = (char*) memset(buffer, 0, capacity);
  offsetRelative = off % getMaxSize();
  buffer = (char*) memcpy(buffer + offsetRelative, buf, len);
  mapPieces.insert(std::make_pair(off, len));
  sizeData = len;  
}


//------------------------------------------------------------------------------
void
CacheEntry::addPiece(char* buf, off_t off, size_t len)
{

  size_t sizeNew;
  size_t sizeAdded;
  off_t offNew;
  off_t offsetRelative = off % getMaxSize();;
  bool addNewPiece = false;
  std::map<off_t, size_t>::iterator iBefore;
  std::map<off_t, size_t>::reverse_iterator iReverse;
  char* pBuffer = buffer + offsetRelative;
  
  if (mapPieces.size() == 0) {
    //add directly, no previous pieces in map
    pBuffer = (char*) memcpy(pBuffer, buf, len);
    mapPieces.insert(std::make_pair(off, len));
    sizeData = len;
    sizeAdded = len;
    pParentFile->incrementWrites(sizeAdded);
    return;
  }

  std::map<off_t, size_t>::iterator iAfter = mapPieces.lower_bound(off);

  if (iAfter->first == off) {
    sizeAdded = (iAfter->second >= len) ? 0 : (len - iAfter->second);
    pBuffer = (char*) memcpy(pBuffer, buf, len);
    iAfter->second += sizeAdded;
    sizeData += sizeAdded;
  }
  else {
    if (iAfter == mapPieces.begin()) {
      //we only have pieces with bigger offset
      if ((off_t)(off + len) >= iAfter->first) {
        //merge with next block
        sizeAdded = off + len - iAfter->first;
        pBuffer = (char*) memcpy(pBuffer, buf, len);
        offNew = off;
        sizeNew = iAfter->second + sizeAdded;
        mapPieces.erase(iAfter);
        mapPieces.insert(std::make_pair(offNew, sizeNew));
        sizeData += sizeAdded;
      }
      else {
        addNewPiece = true;
      }
    }
    else if (iAfter == mapPieces.end()) { 
      //we only have pieces with smaller offset
      //fprintf(stderr,"[%s] Only pieces with smaller offset.\n", __FUNCTION__);
      iReverse = mapPieces.rbegin();
      if ((off_t)(iReverse->first + iReverse->second) >= off) {
        //merge with previous block
        //fprintf(stderr, "[%s] Merge with previous piece offsetRelative=%zu, len=%zu.\n",
        //        __FUNCTION__, offsetRelative, len);
        sizeAdded = off +  len - (iReverse->first + iReverse->second);
        pBuffer = (char*) memcpy(pBuffer, buf, len);
        //fprintf(stderr, "[%s] Finish merging(1).\n", __FUNCTION__);
        iReverse->second += sizeAdded;
        sizeData += sizeAdded;
        //fprintf(stderr, "[%s] Finish merging(2).\n", __FUNCTION__);
      }
      else {
        //fprintf(stderr, "[%s] Add new piece.\n", __FUNCTION__);
        addNewPiece = true;
      }
    }
    else {
      //not first, not last, and bigger than new block offset
      iBefore = iAfter;
      iBefore--;
      if ((off_t)(iBefore->first + iBefore->second) >= off) {
        //merge with previous block
        sizeAdded = off +  len - (iBefore->first + iBefore->second);

        if ((off_t)(iBefore->first + iBefore->second + sizeAdded) >= iAfter->first) {
          //merge the two blocks
          sizeAdded -= (off + len - iAfter->first);
          iBefore->second += (sizeAdded + iAfter->second);
          //remove the iAfter block
          mapPieces.erase(iAfter);
        }
        else {
          iBefore->second += sizeAdded;
        }

        buffer = (char*) memcpy(buffer + offsetRelative, buf, len);
        sizeData += sizeAdded;      
      }
      else if ((off_t)(off +len) > iAfter->first) {
        //merge with next block
        sizeAdded = off + len - iAfter->first;
        pBuffer = (char*) memcpy(pBuffer, buf, len);
        offNew = off;
        sizeNew = iAfter->second + sizeAdded;
        mapPieces.erase(iAfter);
        mapPieces.insert(std::make_pair(offNew, sizeNew));
        sizeData += sizeAdded;
      }
      else {
        addNewPiece = true;        
      }      
    }
  }

  if (addNewPiece) {
    buffer = (char*) memcpy(buffer + offsetRelative, buf, len);
    mapPieces.insert(std::make_pair(off, len));
    sizeData += len;
    sizeAdded = len;
  }

  pParentFile->incrementWrites(sizeAdded);
  return;
}


//------------------------------------------------------------------------------
bool
CacheEntry::getPiece(char* buf, off_t off, size_t len)
{
  bool found = false;
  off_t offsetRelative = off % getMaxSize();
  std::map<off_t, size_t>::reverse_iterator iReverse;
  std::map<off_t, size_t>::iterator i = mapPieces.lower_bound(off);
  

  if (i->first == off) {
    //exact match
    if (i->second >= len) {
      buf = (char*)memcpy(buf, buffer + offsetRelative, len);
      found = true;
    }
    else {
      found = false;
    }
  }
  else {
    if (i == mapPieces.begin()){
      found = false;
    }
    else if (i == mapPieces.end()) {
      iReverse = mapPieces.rbegin();
      if ((iReverse->first <= off) &&
          ((off_t)(iReverse->first + iReverse->second) > off) &&
          (iReverse->first + iReverse->second >= off + len))
      {
        found = true;
        buf = (char*)memcpy(buf, buffer + offsetRelative, len);
      }
      else {
        found = false;
      }
    }
    else {
      i--;
      if ((i->first <= off) &&
          ((off_t)(i->first + i->second) > off) &&
          (i->first + i->second >= off + len))
      {
        found = true;
        buf = (char*)memcpy(buf, buffer + offsetRelative, len);
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
  pParentFile->decrementWrites(sizeData);
  pParentFile->decrementNoWriteBlocks();

  //TODO:: write the pieces
  
  return;
};


//------------------------------------------------------------------------------
bool
CacheEntry::isFull()
{
  return (capacity == sizeData);  
};


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
    if ((off_t)(i->first + i->second) == j->first) {
      //merge the two blocks
      i->second += j->second;
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


