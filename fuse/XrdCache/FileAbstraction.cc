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
  errorsQueue = new ConcurrentQueue<error_type>();
  pthread_mutex_init(&updMutex, NULL);
  pthread_mutex_init(&writesMutex, NULL);
  pthread_cond_init(&writesCond, NULL);
}

//------------------------------------------------------------------------------
FileAbstraction::~FileAbstraction()
{
  pthread_cond_destroy(&writesCond);  
  pthread_mutex_destroy(&writesMutex);
  pthread_mutex_destroy(&updMutex);
  delete errorsQueue;
}


//------------------------------------------------------------------------------
void
FileAbstraction::GetWrites(unsigned long& no, size_t &size)
{
  pthread_mutex_lock(&writesMutex);
  no = noWrites;
  size = sizeWrites;
  pthread_mutex_unlock(&writesMutex);
}

//------------------------------------------------------------------------------
unsigned long
FileAbstraction::GetNoWrites()
{
  unsigned long no;
  pthread_mutex_lock(&writesMutex);
  no = noWrites;
  pthread_mutex_unlock(&writesMutex);
  return no;
}

//------------------------------------------------------------------------------
unsigned long
FileAbstraction::GetNoBlocks()
{
  unsigned long no;
  pthread_mutex_lock(&updMutex);
  no = noBlocks;
  pthread_mutex_unlock(&updMutex);
  return no;
}


//------------------------------------------------------------------------------
void
FileAbstraction::IncrementWrites(size_t size)
{
  pthread_mutex_lock(&writesMutex);
  noWrites++;
  sizeWrites += size;
  pthread_mutex_unlock(&writesMutex);
}


//------------------------------------------------------------------------------
void
FileAbstraction::DecrementWrites(size_t size)
{
  pthread_mutex_lock(&writesMutex);
  noWrites--;
  sizeWrites -= size;
  if (noWrites == 0)
  {
    //notify pending reading processes
    pthread_cond_signal(&writesCond);
  }
  pthread_mutex_unlock(&writesMutex);

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
FileAbstraction::IncrementNoBlocks()
{
  pthread_mutex_lock(&updMutex);
  noBlocks++;
  pthread_mutex_unlock(&updMutex);
}


//------------------------------------------------------------------------------
unsigned long
FileAbstraction::GetDecrementNoBlocks()
{
  unsigned long no;
  pthread_mutex_lock(&updMutex);
  noBlocks--;
  no = noBlocks;
  pthread_mutex_unlock(&updMutex);
  return no;
}


//------------------------------------------------------------------------------
void
FileAbstraction::WaitFinishWrites()
{
  pthread_mutex_lock(&writesMutex);
  eos_static_debug("nowrites=%d", noWrites);
  if (noWrites != 0) 
    pthread_cond_wait(&writesCond, &writesMutex);
  pthread_mutex_unlock(&writesMutex);  
}  


//------------------------------------------------------------------------------
long long int
FileAbstraction::GenerateBlockKey(off_t offsetEnd)
{
  return static_cast<long long int>((1e16 * idFile) + offsetEnd);
}
  
