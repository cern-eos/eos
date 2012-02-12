// ----------------------------------------------------------------------
// File: FileAbstraction.hh
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
#ifndef __EOS_FILEABSTRACTION_HH__
#define __EOS_FILEABSTRACTION_HH__
//------------------------------------------------------------------------------
#include <pthread.h>
#include <sys/types.h>
//------------------------------------------------------------------------------
#include <utility>
//------------------------------------------------------------------------------
#include "ConcurrentQueue.hh"
//------------------------------------------------------------------------------

typedef std::pair<int, std::pair<off_t, size_t> > error_type;

class FileAbstraction
{
 public:

  ConcurrentQueue<error_type>* errorsQueue;
  
  FileAbstraction(int key, unsigned long ino);
  ~FileAbstraction();

  int GetId() const;
  int GetNoReferences() const;
  void GetWritesInfo(unsigned long& no, size_t& size);
  void GetReadsInfo(unsigned long& no, size_t& size);
  ConcurrentQueue<error_type>& GetErrorQueue() const;

  unsigned long GetInode() const;
  unsigned long GetNoBlocks() const;
  unsigned long GetNoReadBlocks() const;
  unsigned long GetNoWriteBlocks() const;

  void IncrementWrites(size_t sWrite);
  void IncrementReads(size_t sRead);
  
  void DecrementWrites(size_t sWrite);
  void DecrementReads(size_t sRead);

  void IncrementNoReferences();
  void DecrementNoReferences();

  long long int GenerateBlockKey(off_t offsetEnd);
  void WaitFinishWrites();
  
 private:
  int idFile;                   //internally assigned key
  int noReferences;             //number of held referencess to this file
  unsigned long inode;          //inode of current file  
  unsigned long noReads;        //no of read blocks
  unsigned long noWrites;       //no. of write blocks in cache
  size_t sizeWrites;            //the size of write blocks in cache
  size_t sizeReads;             //the size of read blocks in cache

  long long lastPossibleKey;    //last possible offset in file
  long long firstPossibleKey;   //first possibleoffset in file

  pthread_cond_t writesCond;    //condition to notify when there are no
  pthread_mutex_t updMutex;     //mutex for modifying the no. of blocks or ref.
};

#endif
