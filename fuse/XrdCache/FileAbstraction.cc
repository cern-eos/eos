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


/*----------------------------------------------------------------------------*/
/** 
 * Construct a file abstraction object
 * 
 * @param id generated id value
 * @param ino inode value
 *
 */
/*----------------------------------------------------------------------------*/
FileAbstraction::FileAbstraction(int id, unsigned long ino):
  idFile(id),
  nReferences(0),
  inode(ino),
  sizeWrites(0),
  sizeReads(0),
  nWriteBlocks(0)
{
  //max file size we can deal with is ~ 90TB
  firstPossibleKey = static_cast<long long>(1e14 * idFile);
  lastPossibleKey = static_cast<long long>((1e14 * (idFile + 1)));

  eos_static_debug("idFile=%i, firstPossibleKey=%llu, lastPossibleKey=%llu",
                   idFile, firstPossibleKey, lastPossibleKey);

  errorsQueue = new ConcurrentQueue<error_type>();
  cUpdate = XrdSysCondVar(0);
}

/*----------------------------------------------------------------------------*/
/** 
 * Destructor
 * 
 */
/*----------------------------------------------------------------------------*/
FileAbstraction::~FileAbstraction()
{
  delete errorsQueue;
}


/*----------------------------------------------------------------------------*/
/** 
 *
 * @return sum of the write and read blocks size in cache
 *
 */
/*----------------------------------------------------------------------------*/
size_t
FileAbstraction::getSizeRdWr()
{
  size_t size;
  XrdSysCondVarHelper cHepler(cUpdate);
  size = sizeWrites + sizeReads;
  return size;
}


/*----------------------------------------------------------------------------*/
/** 
 *
 * @return size of write blocks in cache
 *
 */
/*----------------------------------------------------------------------------*/
size_t
FileAbstraction::getSizeWrites()
{
  size_t size;
  XrdSysCondVarHelper cHepler(cUpdate);
  size = sizeWrites;
  return size;
}


/*----------------------------------------------------------------------------*/
/** 
 *
 * @return size of read blocks in cache
 *
 */
/*----------------------------------------------------------------------------*/
size_t
FileAbstraction::getSizeReads()
{
  size_t size;
  XrdSysCondVarHelper cHepler(cUpdate);
  size = sizeReads;
  return size;
}


/*----------------------------------------------------------------------------*/
/** 
 *
 * @return number of write blocks in cache
 *
 */
/*----------------------------------------------------------------------------*/
long long int
FileAbstraction::getNoWriteBlocks()
{
  long long int n;
  XrdSysCondVarHelper cHepler(cUpdate);
  n = nWriteBlocks;
  return n;
}


/*----------------------------------------------------------------------------*/
/** 
 *
 * @return value of the first possible key
 *
 */
/*----------------------------------------------------------------------------*/
long long
FileAbstraction::getFirstPossibleKey() const
{
  return firstPossibleKey;
}


/*----------------------------------------------------------------------------*/
/** 
 *
 * @return value of the last possible key
 *
 */
/*----------------------------------------------------------------------------*/
long long
FileAbstraction::getLastPossibleKey() const
{
  return lastPossibleKey;
}


/*----------------------------------------------------------------------------*/
/** 
 *
 * @param size added write size
 * @param newBlock true if a new write block is added, false otherwise
 *
 */
/*----------------------------------------------------------------------------*/
void
FileAbstraction::incrementWrites(size_t size, bool newBlock)
{
  XrdSysCondVarHelper cHepler(cUpdate);
  sizeWrites += size;
  if (newBlock) {
    nWriteBlocks++;
  }
}


/*----------------------------------------------------------------------------*/
/** 
 *
 * @param size added read size
 *
 */
/*----------------------------------------------------------------------------*/
void
FileAbstraction::incrementReads(size_t size)
{
  XrdSysCondVarHelper cHepler(cUpdate);
  sizeReads += size;
}


/*----------------------------------------------------------------------------*/
/** 
 *
 * @param size freed write size
 * @param fullBlock true if a whole write block is removed, false otherwise
 *
 */
/*----------------------------------------------------------------------------*/
void
FileAbstraction::decrementWrites(size_t size, bool fullBlock)
{
  cUpdate.Lock();
  eos_static_debug("writes old size=%zu", sizeWrites);
  sizeWrites -= size;
  if (fullBlock) {
    nWriteBlocks--;
  }
  eos_static_debug("writes new size=%zu", sizeWrites);

  if (sizeWrites == 0) {
    //notify pending reading processes
    cUpdate.Signal();
  }

  cUpdate.UnLock();
}


/*----------------------------------------------------------------------------*/
/** 
 *
 * @param size freed read size
 *
 */
/*----------------------------------------------------------------------------*/
void
FileAbstraction::decrementReads(size_t size)
{
  XrdSysCondVarHelper cHepler(cUpdate);
  sizeReads -= size;
}


/*----------------------------------------------------------------------------*/
/** 
 *
 * @return number of references held to the current file object
 *
 */
/*----------------------------------------------------------------------------*/
int
FileAbstraction::getNoReferences()
{
  int no;
  XrdSysCondVarHelper cHepler(cUpdate);
  no = nReferences;
  return no;
}


//------------------------------------------------------------------------------
void
FileAbstraction::incrementNoReferences()
{
  XrdSysCondVarHelper cHepler(cUpdate);
  nReferences++;
}


//------------------------------------------------------------------------------
void
FileAbstraction::decrementNoReferences()
{
  XrdSysCondVarHelper cHepler(cUpdate);
  nReferences--;
}


//------------------------------------------------------------------------------
void
FileAbstraction::waitFinishWrites()
{
  cUpdate.Lock();
  eos_static_debug("sizeWrites=%zu", sizeWrites);

  if (sizeWrites != 0) {
    cUpdate.Wait();
  }

  cUpdate.UnLock();
}


/*----------------------------------------------------------------------------*/
/** 
 *
 * @param offset offset
 *
 * @return newly generated key 
 *
 */
/*----------------------------------------------------------------------------*/
long long int
FileAbstraction::generateBlockKey(off_t offset)
{
  offset = (offset / CacheEntry::getMaxSize()) * CacheEntry::getMaxSize();
  return static_cast<long long int>((1e14 * idFile) + offset);
}


/*----------------------------------------------------------------------------*/
/** 
 *
 * @param strongConstraint 
 *
 * @return true if file object is still in use (there are blocks in cache belonging
 * to it
 *
 */
/*----------------------------------------------------------------------------*/
bool
FileAbstraction::isInUse(bool strongConstraint)
{
  bool retVal = false;
  XrdSysCondVarHelper cHepler(cUpdate);
  
  eos_static_debug("sizeReads=%zu, sizeWrites=%zu, nReferences=%i",
                   sizeReads, sizeWrites, nReferences);\
  
  if (strongConstraint) {
    if ((sizeReads + sizeWrites != 0) || (nReferences >= 1)) {
      retVal =  true;
    }
  } else {
    if ((sizeReads + sizeWrites != 0) || (nReferences > 1)) {
      retVal =  true;
    }
  }
  return retVal;
}

/*----------------------------------------------------------------------------*/
/** 
 *
 * @return id of the file object
 *
 */
/*----------------------------------------------------------------------------*/
int
FileAbstraction::getId() const
{
  return idFile;
}


/*----------------------------------------------------------------------------*/
/** 
 *
 * @return handler to the queue of errors
 *
 */
/*----------------------------------------------------------------------------*/
ConcurrentQueue<error_type>&
FileAbstraction::getErrorQueue() const
{
  return *errorsQueue;
}


/*----------------------------------------------------------------------------*/
/** 
 *
 * @return inode value
 *
 */
/*----------------------------------------------------------------------------*/
unsigned long
FileAbstraction::getInode() const
{
  return inode;
}

