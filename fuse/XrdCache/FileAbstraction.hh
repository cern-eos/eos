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

  int GetId() { return idFile; };
  ConcurrentQueue<error_type>& GetErrorQueue() const { return *errorsQueue; };
  unsigned long GetInode() const { return inode; };
   
  unsigned long GetNoWrites();
  void GetWrites(unsigned long& no, size_t& size);
  void IncrementWrites(size_t sWrite);
  void DecrementWrites(size_t sWrite);

  int GetNoReferences();
  void IncrementNoReferences();
  void DecrementNoReferences();

  void IncrementNoBlocks();
  unsigned long GetNoBlocks();
  unsigned long GetDecrementNoBlocks();

  long long int GenerateBlockKey(off_t offsetEnd);
  void WaitFinishWrites();
  
 private:
  int idFile;                    //internally assigned key
  int noReferences;              //number of held referencess
  unsigned long inode;           //inode of current file  
  unsigned long noBlocks;        //no of blocks in cache which belong to this file
  unsigned long noWrites;        //no. of write blocks in cache
  size_t sizeWrites;             //the size of write blocks in cache

  pthread_cond_t writesCond;     //condition to notify when there are no
  pthread_mutex_t writesMutex;   //more writes pending on the current file
  pthread_mutex_t updMutex;      //mutex for modifying the no. of blocks or ref.
};

#endif
