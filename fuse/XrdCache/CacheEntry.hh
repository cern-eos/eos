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

/**
 * @file   CacheEntry.hh
 *
 * @brief  Cached objects represenation
 *
 *
 */

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


//------------------------------------------------------------------------------
//! Class representing a block saved in cache
//------------------------------------------------------------------------------
class CacheEntry
{
public:

  //------------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------------
  CacheEntry(int filedes, char *buf, off_t off, size_t len, FileAbstraction &ptr, bool iswr);

  //------------------------------------------------------------------------------
  //! Destructor
  //------------------------------------------------------------------------------
  ~CacheEntry();

  //------------------------------------------------------------------------------
  //! Maximum size of the cache
  //------------------------------------------------------------------------------
  static const size_t getMaxSize() {
    return maxSize;
  }

  //------------------------------------------------------------------------------
  //! Get file descriptor
  //------------------------------------------------------------------------------
  int    getFd() const;

  //------------------------------------------------------------------------------
  //! Get handler to the data buffer
  //------------------------------------------------------------------------------
  char*  getDataBuffer();

  //------------------------------------------------------------------------------
  //! Get the size of meaningful data
  //------------------------------------------------------------------------------
  size_t getSizeData() const;

  //------------------------------------------------------------------------------
  //! Get total capacity of the object
  //------------------------------------------------------------------------------
  size_t getCapacity() const;

  //------------------------------------------------------------------------------
  //! Get start offset value
  //------------------------------------------------------------------------------
  off_t  getOffsetStart() const;
  
  //------------------------------------------------------------------------------
  //! Get end offset value
  //------------------------------------------------------------------------------
  off_t  getOffsetEnd() const;

  //------------------------------------------------------------------------------
  //! Try to get a piece from the current block
  //------------------------------------------------------------------------------
  bool getPiece(char* buf, off_t off, size_t len);

  //------------------------------------------------------------------------------
  //! Get handler to the parent file object
  //------------------------------------------------------------------------------
  FileAbstraction*  getParentFile() const;

  //------------------------------------------------------------------------------
  //! Test if block is for writing
  //------------------------------------------------------------------------------
  bool   isWr();

  //------------------------------------------------------------------------------
  //! Test if block full with meaningfull data
  //------------------------------------------------------------------------------
  bool   isFull();

  //------------------------------------------------------------------------------
  //! Method that does the actual writing
  //------------------------------------------------------------------------------
  int    doWrite();

  //------------------------------------------------------------------------------
  //! Add a new piece to the block
  //------------------------------------------------------------------------------
  size_t addPiece(char* buf, off_t off, size_t len);

  //------------------------------------------------------------------------------
  //! Method to recycle a previously used block
  //------------------------------------------------------------------------------
  void   doRecycle(int filedes, char* buf, off_t offset, size_t lenBuf,
                   FileAbstraction &ptr, bool iswr);

 private:

  int fd;                            //< file descriptor
  bool isWrType;                     //< is write block type
  char*  buffer;                     //< buffer of the object
  size_t capacity;                   //< total capcity 512 KB ~ 4MB
  size_t sizeData;                   //< size of useful data
  off_t  offsetStart;                //< offset relative to the file
  static size_t maxSize;       //< max size of entry

  std::map<off_t, size_t> mapPieces; //< pieces read/to be written
  FileAbstraction* pParentFile;      //< pointer to parent file
};

#endif

