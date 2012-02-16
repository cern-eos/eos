// ----------------------------------------------------------------------
// File: CacheEntry.hh
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
#ifndef __EOS_CACHEENTRY_HH__
#define __EOS_CACHEENTRY_HH__
//------------------------------------------------------------------------------
#include <map>
//------------------------------------------------------------------------------
#include <sys/time.h>
#include <sys/types.h>
//------------------------------------------------------------------------------
#include <XrdPosix/XrdPosixXrootd.hh>
//------------------------------------------------------------------------------
#include "FileAbstraction.hh"
//------------------------------------------------------------------------------

class CacheEntry
{
public:

  CacheEntry(int filedes, char* buf, off_t off, size_t len, FileAbstraction* ptr);
  ~CacheEntry();
  static const size_t getMaxSize() {
    return 1048576;
  };    //1MB=1048576 512KB=524288

  int    getFd() const;
  char*  getDataBuffer();
  size_t getSizeData() const;
  size_t getCapacity() const;
  off_t  getOffsetStart() const;
  off_t  getOffsetEnd() const;
  bool   getPiece(char* buf, off_t off, size_t len);
  FileAbstraction*  getParentFile() const;

  bool   isFull();
  int    doWrite();
  size_t addPiece(char* buf, off_t off, size_t len);
  void   doRecycle(int filedes, char* buf, off_t offset, size_t lenBuf,
                   FileAbstraction* ptr);

private:

  int fd;                            //file descriptor
  char*  buffer;                     //buffer of the object
  size_t capacity;                   //total capcity 512 KB ~ 1MB
  size_t sizeData;                   //size of useful data
  off_t  offsetStart;                //offset relative to the file

  std::map<off_t, size_t> mapPieces; //pieces read/to be written
  FileAbstraction* pParentFile;      //pointer to parent file
};

#endif

