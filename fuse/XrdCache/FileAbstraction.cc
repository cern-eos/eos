// ----------------------------------------------------------------------
// File: FileAbstraction.cc
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
#include "FileAbstraction.hh"
//------------------------------------------------------------------------------

FileAbstraction::FileAbstraction(int id, unsigned long ino):
    idFile(id),
    noReferences(0),
    inode(ino),
    noBlocks(0),
    noWrites(0),
    sizeWrites(0)
{
  //max file size we can deal with is 90TB 
  firstPossibleKey = static_cast<long long>(1e14 * idFile);          
  lastPossibleKey = static_cast<long long>((1e14 * (idFile + 1)) - 1);

  fprintf(stdout, "idFile=%i, firstPossibleKey=%llu, lastPossibleKey=%llu \n",
          idFile, firstPossibleKey, lastPossibleKey);
  
  errorsQueue = new ConcurrentQueue<error_type>();
  pthread_mutex_init(&updMutex, NULL);
  pthread_cond_init(&writesCond, NULL);
}

//------------------------------------------------------------------------------
FileAbstraction::~FileAbstraction()
{
  pthread_cond_destroy(&writesCond);  
  pthread_mutex_destroy(&updMutex);
  delete errorsQueue;
}


//------------------------------------------------------------------------------
void
FileAbstraction::GetReadsInfo(unsigned long& no, size_t &size)
{
  pthread_mutex_lock(&updMutex);
  no = noReadBlocks;
  size = sizeReads;
  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
void
FileAbstraction::GetWritesInfo(unsigned long& no, size_t &size)
{
  pthread_mutex_lock(&updMutex);
  no = noWriteBlocks;
  size = sizeWrites;
  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
unsigned long
FileAbstraction::GetNoWriteBlocks()
{
  unsigned long no;
  pthread_mutex_lock(&updMutex);
  no = noWriteBlocks;
  pthread_mutex_unlock(&updMutex);
  return no;
}


//------------------------------------------------------------------------------
unsigned long
FileAbstraction::GetNoReadBlocks()
{
  unsigned long no;
  pthread_mutex_lock(&updMutex);
  no = noReadBlocks;
  pthread_mutex_unlock(&updMutex);
  return no;
}


//------------------------------------------------------------------------------
unsigned long
FileAbstraction::GetNoBlocks()
{
  unsigned long no;
  pthread_mutex_lock(&updMutex);
  no = noReadBlocks + noWriteBlocks;
  pthread_mutex_unlock(&updMutex);
  return no;
}


//------------------------------------------------------------------------------
void
FileAbstraction::IncrementWrites(size_t size)
{
  pthread_mutex_lock(&updMutex);
  noWrites++;
  sizeWrites += size;
  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
void
FileAbstraction::IncrementReads(size_t size)
{
  pthread_mutex_lock(&updMutex);
  noReads++;
  sizeReads += size;
  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
void
FileAbstraction::DecrementWrites(size_t size)
{
  pthread_mutex_lock(&updMutex);
  noWrites--;
  sizeWrites -= size;
  if (noWrites == 0)
  {
    //notify pending reading processes
    pthread_cond_signal(&writesCond);
  }
  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
void
FileAbstraction::DecrementReads(size_t size)
{
  pthread_mutex_lock(&updMutex);
  noWrites--;
  sizeWrites -= size;
  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
int
FileAbstraction::GetNoReferences()
{
  int no;
  pthread_mutex_lock(&updMutex);
  no = noReferences;
  pthread_mutex_unlock(&updMutex);
  return no;
}


//------------------------------------------------------------------------------
void
FileAbstraction::IncrementNoReferences()
{
  pthread_mutex_lock(&updMutex);
  noReferences++;
  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
void
FileAbstraction::DecrementNoReferences()
{
  pthread_mutex_lock(&updMutex);
  noReferences--;
  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
void
FileAbstraction::WaitFinishWrites()
{
  pthread_mutex_lock(&updMutex);
  if (noWrites != 0)
    pthread_cond_wait(&writesCond, &updMutex);
  pthread_mutex_unlock(&updMutex);  
}  


//------------------------------------------------------------------------------
long long int
FileAbstraction::GenerateBlockKey(off_t offsetEnd)
{
  offsetEnd %= CacheEntry::getMaxSize();
  return static_cast<long long int>((1e14 * idFile) + offsetEnd);
}


//------------------------------------------------------------------------------
int
FileAbstraction::GetId() const
{
  return idFile;
}


//------------------------------------------------------------------------------
ConcurrentQueue<error_type>&
FileAbstraction::GetErrorQueue() const
{
  return *errorsQueue;
}

//------------------------------------------------------------------------------
unsigned long
FileAbstraction::GetInode() const
{
  return inode;
}

