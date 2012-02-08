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
#include "FileAbstraction.hh"
//------------------------------------------------------------------------------

class CacheEntry
{
 public:
  CacheEntry(int filedes, char* buf, off_t off, size_t len, FileAbstraction *ptr);
  ~CacheEntry();

  int    GetFd() const { return fd; };
  char*  GetDataBuffer() { return buffer; }; 
  size_t GetLength() const { return length; };
  off_t  GetOffset() const { return offset; };
  off_t  GetOffsetEnd() const { return (offset + length); };

  FileAbstraction*  GetParentFile() const { return pParentFile; }; 
  void   Recycle(int filedes, char* buf, off_t offset, size_t lenBuf,
                  FileAbstraction* ptr);

 private:
  int fd;                       //file descriptor
  size_t capacity;
  size_t length;
  off_t  offset;
  char*  buffer;                //buffer of the object
  FileAbstraction* pParentFile; //pointer to parent file
};

#endif

