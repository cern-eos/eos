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
#include "CacheEntry.hh"
#include "common/Logging.hh"
//------------------------------------------------------------------------------

FileAbstraction::FileAbstraction(int id, unsigned long ino):
  idFile(id),
  nReferences(0),
  inode(ino),
  nWriteBlocks(0),
  sizeWrites(0),
  sizeReads(0)
{
  //max file size we can deal with is ~ 90TB
  firstPossibleKey = static_cast<long long>(1e14 * idFile);
  lastPossibleKey = static_cast<long long>((1e14 * (idFile + 1)));

  eos_static_debug("idFile=%i, firstPossibleKey=%llu, lastPossibleKey=%llu",
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
size_t
FileAbstraction::getSizeRdWr()
{
  size_t size;
  pthread_mutex_lock(&updMutex);
  size = sizeWrites + sizeReads;
  pthread_mutex_unlock(&updMutex);
  return size;
}


//------------------------------------------------------------------------------
size_t
FileAbstraction::getSizeWrites()
{
  size_t size;
  pthread_mutex_lock(&updMutex);
  size = sizeWrites;
  pthread_mutex_unlock(&updMutex);
  return size;
}


//------------------------------------------------------------------------------
size_t
FileAbstraction::getSizeReads()
{
  size_t size;
  pthread_mutex_lock(&updMutex);
  size = sizeReads;
  pthread_mutex_unlock(&updMutex);
  return size;
}


//------------------------------------------------------------------------------
long long int
FileAbstraction::getNoWriteBlocks()
{
  long long int n;
  pthread_mutex_lock(&updMutex);
  n = nWriteBlocks;
  pthread_mutex_unlock(&updMutex);
  return n;
}


//------------------------------------------------------------------------------
long long
FileAbstraction::getFirstPossibleKey() const
{
  return firstPossibleKey;
}


//------------------------------------------------------------------------------
long long
FileAbstraction::getLastPossibleKey() const
{
  return lastPossibleKey;
}


//------------------------------------------------------------------------------
void
FileAbstraction::incrementNoWriteBlocks()
{
  pthread_mutex_lock(&updMutex);
  nWriteBlocks++;
  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
void
FileAbstraction::incrementWrites(size_t size)
{
  pthread_mutex_lock(&updMutex);
  sizeWrites += size;
  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
void
FileAbstraction::incrementReads(size_t size)
{
  pthread_mutex_lock(&updMutex);
  sizeReads += size;
  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
void
FileAbstraction::decrementNoWriteBlocks()
{
  pthread_mutex_lock(&updMutex);
  nWriteBlocks--;
  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
void
FileAbstraction::decrementWrites(size_t size)
{
  pthread_mutex_lock(&updMutex);
  eos_static_debug("old size=%zu", sizeWrites);
  sizeWrites -= size;
  eos_static_debug("new size=%zu", sizeWrites);

  if (sizeWrites == 0) {
    //notify pending reading processes
    pthread_cond_signal(&writesCond);
  }

  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
void
FileAbstraction::decrementReads(size_t size)
{
  pthread_mutex_lock(&updMutex);
  sizeReads -= size;
  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
int
FileAbstraction::getNoReferences()
{
  int no;
  pthread_mutex_lock(&updMutex);
  no = nReferences;
  pthread_mutex_unlock(&updMutex);
  return no;
}


//------------------------------------------------------------------------------
void
FileAbstraction::incrementNoReferences()
{
  pthread_mutex_lock(&updMutex);
  nReferences++;
  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
void
FileAbstraction::decrementNoReferences()
{
  pthread_mutex_lock(&updMutex);
  nReferences--;
  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
void
FileAbstraction::waitFinishWrites()
{
  pthread_mutex_lock(&updMutex);
  eos_static_debug("sizeWrites=%zu", sizeWrites);

  if (sizeWrites != 0) {
    pthread_cond_wait(&writesCond, &updMutex);
  }

  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
long long int
FileAbstraction::generateBlockKey(off_t offset)
{
  offset = (offset / CacheEntry::getMaxSize()) * CacheEntry::getMaxSize();
  return static_cast<long long int>((1e14 * idFile) + offset);
}


//------------------------------------------------------------------------------
bool
FileAbstraction::isInUse()
{
  bool retVal = false;
  pthread_mutex_lock(&updMutex);
  if ((sizeReads + sizeWrites != 0) || (nReferences > 1)) {
    retVal =  true;
  }
  pthread_mutex_unlock(&updMutex);
  return retVal;
}

//------------------------------------------------------------------------------
int
FileAbstraction::getId() const
{
  return idFile;
}


//------------------------------------------------------------------------------
ConcurrentQueue<error_type>&
FileAbstraction::getErrorQueue() const
{
  return *errorsQueue;
}

//------------------------------------------------------------------------------
unsigned long
FileAbstraction::getInode() const
{
  return inode;
}

