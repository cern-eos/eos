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
#include "CacheEntry.hh"
//------------------------------------------------------------------------------

CacheEntry::CacheEntry(int filedes, char* buf, off_t off, size_t len, FileAbstraction* ptr):
  fd(filedes),
  sizeData(len),
  inQueue(false),
  pParentFile(ptr) 
{
  char* pBuffer; 
  off_t offsetRelative;

  if (len > getMaxSize()) {
    fprintf(stderr, "error=len should be smaller than getMaxSize()\n");
    exit(-1);
  }

  capacity = getMaxSize();
  offsetStart = (off / getMaxSize()) * getMaxSize();
  offsetRelative = off % getMaxSize();
  buffer = (char*) calloc(capacity, sizeof(char));
  pBuffer = buffer + offsetRelative;
  pBuffer = (char*) memcpy(pBuffer, buf, len);
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
  char* pBuffer; 
  off_t offsetRelative;
    
  fd = filedes;
  offsetStart = (off / getMaxSize()) * getMaxSize();
  pParentFile = ptr;
  inQueue = false;

  if (len > capacity) {
    fprintf(stderr, "error=len should never be bigger than capacity.\n");
    exit(-1);
  }

  mapPieces.clear();
  offsetRelative = off % getMaxSize();
  pBuffer = buffer + offsetRelative;
  pBuffer = (char*) memcpy(pBuffer, buf, len);
  mapPieces.insert(std::make_pair(off, len));
  sizeData = len;
}


//------------------------------------------------------------------------------
size_t
CacheEntry::addPiece(char* buf, off_t off, size_t len)
{
  off_t offNew;
  size_t sizeAdded;
  size_t sizeNew;
  size_t sizeErased = 0;
  bool addNewPiece = false;
  off_t offsetOldEnd;
  off_t offsetRelative = off % getMaxSize();
  off_t offsetPieceEnd = off + len;
  char* pBuffer = buffer + offsetRelative;

  std::map<off_t, size_t>::iterator iBefore;
  std::map<off_t, size_t>::reverse_iterator iReverse;
  std::map<off_t, size_t>::iterator iAfter = mapPieces.lower_bound(off);

  if (iAfter->first == off) {
    sizeAdded = (iAfter->second >= len) ? 0 : (len - iAfter->second);
    std::map<off_t, size_t>::iterator iTmp;
    iTmp = iAfter;
    iTmp++;
    while ((iTmp != mapPieces.end()) && (offsetPieceEnd >= iTmp->first)) {
      offsetOldEnd = iTmp->first + iTmp->second;
      if (offsetPieceEnd > offsetOldEnd) {
        sizeAdded -= iTmp->second;
        sizeErased += iTmp->second;
        mapPieces.erase(iTmp++);
      }
      else {
        sizeAdded -= (offsetPieceEnd - iTmp->first);
        sizeErased += iTmp->second;
        mapPieces.erase(iTmp++);
        break;
      }
    }
    pBuffer = (char*) memcpy(pBuffer, buf, len);
    iAfter->second += (sizeAdded + sizeErased);
    sizeData += sizeAdded;
  } else {
    if (iAfter == mapPieces.begin()) {
      //we only have pieces with bigger offset
      if (offsetPieceEnd >= iAfter->first) {
        //merge with next block
        offsetOldEnd = iAfter->first + iAfter->second;
        if (offsetPieceEnd > offsetOldEnd) {
          //new block also longer then old block
          sizeAdded = (iAfter->first - off) + (offsetPieceEnd - offsetOldEnd) ;
          sizeErased += iAfter->second;
          mapPieces.erase(iAfter++);
          while ((iAfter != mapPieces.end()) && (offsetPieceEnd >= iAfter->first)) {
            offsetOldEnd = iAfter->first + iAfter->second;
            if (offsetPieceEnd > offsetOldEnd) {
              sizeAdded -= iAfter->second;
              sizeErased += iAfter->second;
              mapPieces.erase(iAfter++);
            }
            else {
              sizeAdded -= (offsetPieceEnd - iAfter->first);
              sizeErased += iAfter->second;
              mapPieces.erase(iAfter++);
              break;
            }
          }
          offNew = off;
          sizeNew = sizeAdded + sizeErased;
        }
        else {
          //new block shorter than old block
          sizeAdded = iAfter->first - off;
          offNew = off;
          sizeNew = iAfter->second + sizeAdded;
          mapPieces.erase(iAfter);
        }
        pBuffer = (char*) memcpy(pBuffer, buf, len);
        mapPieces.insert(std::make_pair(offNew, sizeNew));
        sizeData += sizeAdded;
      } else {
        addNewPiece = true;
      }
    } else if (iAfter == mapPieces.end()) {
      //we only have pieces with smaller offset
      iReverse = mapPieces.rbegin();
      offsetOldEnd = iReverse->first + iReverse->second;
      if (offsetOldEnd >= off) {
        //merge with previous block
        if (offsetOldEnd >= offsetPieceEnd) {
          //just update the data, no off or len modification
          sizeAdded = 0;
        }
        else {
          //extend the current block at the end
          sizeAdded = offsetPieceEnd - offsetOldEnd;
          iReverse->second += sizeAdded;
        }
        pBuffer = (char*) memcpy(pBuffer, buf, len);
        sizeData += sizeAdded;
      } else {
        addNewPiece = true;
      }
    } else {
      //not first, not last, and bigger than new block offset
      iBefore = iAfter;
      iBefore--;
      offsetOldEnd = iBefore->first + iBefore->second;
      if (offsetOldEnd >= off) {
        //merge with previous block
        if (offsetOldEnd >= offsetPieceEnd) {
          //just update the data, no off or len modification
          pBuffer = (char*) memcpy(pBuffer, buf, len);
          sizeAdded = 0;
        }
        else {
          sizeAdded = offsetPieceEnd - offsetOldEnd;
          if (offsetPieceEnd >= iAfter->first) {
            //new block overlaps with iAfter block
            if (offsetPieceEnd > (off_t)(iAfter->first + iAfter->second)) {
              //new block spanns both old blocks and more
              sizeAdded -= iAfter->second;
              sizeErased = iAfter->second;
              mapPieces.erase(iAfter++);
              while ((iAfter != mapPieces.end()) && (offsetPieceEnd >= iAfter->first)) {
                offsetOldEnd = iAfter->first + iAfter->second;
                if (offsetPieceEnd > offsetOldEnd) {
                  sizeAdded -= iAfter->second;
                  sizeErased += iAfter->second;
                  mapPieces.erase(iAfter++);
                }
                else {
                  sizeAdded -= (offsetPieceEnd - iAfter->first);
                  sizeErased += iAfter->second;
                  mapPieces.erase(iAfter++);
                  break;
                }
              }
              iBefore->second += (sizeAdded + sizeErased);
            }
            else {
              //new block spanns both old blocks but not more
              sizeAdded -= (offsetPieceEnd - iAfter->first);
              iBefore->second += (sizeAdded + iAfter->second);
              mapPieces.erase(iAfter);
            }
          } else {
            //new block does no overlap with iAfter block
            iBefore->second += sizeAdded;
          }
          pBuffer = (char*) memcpy(pBuffer, buf, len);
          sizeData += sizeAdded;
        }
      } else if (offsetPieceEnd >= iAfter->first) {
        //merge with next block
        offsetOldEnd = iAfter->first + iAfter->second;
        if (offsetPieceEnd > offsetOldEnd) {
          //new block bigger than iAfter block
          sizeAdded = len - iAfter->second;
          sizeErased = iAfter->second;
          mapPieces.erase(iAfter++);
          while ((iAfter != mapPieces.end()) && (offsetPieceEnd >= iAfter->first)) {
            offsetOldEnd = iAfter->first + iAfter->second;
            if (offsetPieceEnd > offsetOldEnd) {
              sizeAdded -= iAfter->second;
              sizeErased += iAfter->second;
              mapPieces.erase(iAfter++);
            }
            else {
              sizeAdded -= (offsetPieceEnd - iAfter->first);
              sizeErased += iAfter->second;
              mapPieces.erase(iAfter++);
              break;
            }
          }
          offNew = off;
          sizeNew = sizeErased + sizeAdded;
        }
        else {
          //new block shorter than iAfter block
          sizeAdded = len - (offsetPieceEnd - iAfter->first);
          offNew = off;
          sizeNew = iAfter->second + sizeAdded;        
          mapPieces.erase(iAfter);
        }
        pBuffer = (char*) memcpy(pBuffer, buf, len);
        mapPieces.insert(std::make_pair(offNew, sizeNew));
        sizeData += sizeAdded;
      }
      else {
          addNewPiece = true;
      } 
    }
  }

  if (addNewPiece) {
    pBuffer = (char*) memcpy(pBuffer, buf, len);
    mapPieces.insert(std::make_pair(off, len));
    sizeAdded = len;
    sizeData += sizeAdded;
  }

  return sizeAdded;
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
    } else {
      found = false;
    }
  } else {
    if (i == mapPieces.begin()) {
      found = false;
    } else if (i == mapPieces.end()) {
      iReverse = mapPieces.rbegin();

      if ((iReverse->first <= off) &&
          ((off_t)(iReverse->first + iReverse->second) > off) &&
          (iReverse->first + iReverse->second >= off + len)) {
        found = true;
        buf = (char*)memcpy(buf, buffer + offsetRelative, len);
      } else {
        found = false;
      }
    } else {
      i--;

      if ((i->first <= off) &&
          ((off_t)(i->first + i->second) > off) &&
          (i->first + i->second >= off + len)) {
        found = true;
        buf = (char*)memcpy(buf, buffer + offsetRelative, len);
      } else {
        found = false;
      }
    }
  }

  return found;
}


//------------------------------------------------------------------------------
int
CacheEntry::doWrite()
{
  int retc;
  off_t offsetRelative;
  std::map<off_t, size_t>::iterator iCurrent = mapPieces.begin();
  const std::map<off_t, size_t>::iterator iEnd = mapPieces.end();

  for( ; iCurrent != iEnd; iCurrent++) {
    offsetRelative = iCurrent->first % getMaxSize();
    eos_static_debug("size=%lu offset=%ll", iCurrent->second, iCurrent->first);
    retc = XrdPosixXrootd::Pwrite(fd, buffer + offsetRelative, iCurrent->second, iCurrent->first);
    if (retc != (int)iCurrent->second) {
      fprintf(stderr, "error=error while writing using XrdPosixXrootd\n");
      return retc;
    }
  }

  return 0;
}


//------------------------------------------------------------------------------
void
CacheEntry::setInQueue(bool status)
{
  inQueue = status;
}


//------------------------------------------------------------------------------
bool
CacheEntry::isInQueue() const
{
  return inQueue;
}


//------------------------------------------------------------------------------
bool
CacheEntry::isFull()
{
  return (capacity == sizeData);
};


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


