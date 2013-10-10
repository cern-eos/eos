//------------------------------------------------------------------------------
// File: FileAbstraction.cc
// Author: Elvin-Alin Sindrilaru - CERN
//------------------------------------------------------------------------------

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


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------

FileAbstraction::FileAbstraction (int id, int fd) :
mIdFile (id),
mNoReferences (0),
mFd (fd),
mSizeWrites (0),
mSizeReads (0),
mNoWrBlocks (0)
{
  //............................................................................
  // Max file size we can deal with is ~ 90TB
  //............................................................................
  mFirstPossibleKey = static_cast<long long> (1e14 * mIdFile);
  mLastPossibleKey = static_cast<long long> ((1e14 * (mIdFile + 1)));

  eos_static_debug("mIdFile=%i, mFirstPossibleKey=%llu, mLastPossibleKey=%llu",
                   mIdFile, mFirstPossibleKey, mLastPossibleKey);

  errorsQueue = new eos::common::ConcurrentQueue<error_type > ();
  mCondUpdate = XrdSysCondVar(0);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

FileAbstraction::~FileAbstraction ()
{
  delete errorsQueue;
}


//------------------------------------------------------------------------------
// Get sum of the write and read blocks size in cache
//------------------------------------------------------------------------------

size_t
FileAbstraction::GetSizeRdWr ()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  return ( mSizeWrites + mSizeReads);
}


//------------------------------------------------------------------------------
// Get size of write blocks in cache
//------------------------------------------------------------------------------

size_t
FileAbstraction::GetSizeWrites ()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  return mSizeWrites;
}


//------------------------------------------------------------------------------
// Get size of read blocks in cache
//------------------------------------------------------------------------------

size_t
FileAbstraction::GetSizeReads ()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  return mSizeReads;
}


//------------------------------------------------------------------------------
// Get number of write blocks in cache
//------------------------------------------------------------------------------

long long int
FileAbstraction::GetNoWriteBlocks ()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  return mNoWrBlocks;
}


//------------------------------------------------------------------------------
// Get value of the first possible key
//------------------------------------------------------------------------------

long long
FileAbstraction::GetFirstPossibleKey () const
{
  return mFirstPossibleKey;
}


//------------------------------------------------------------------------------
// Get value of the last possible key
//------------------------------------------------------------------------------

long long
FileAbstraction::GetLastPossibleKey () const
{
  return mLastPossibleKey;
}


//------------------------------------------------------------------------------
// Increment the value of accumulated writes size
//------------------------------------------------------------------------------

void
FileAbstraction::IncrementWrites (size_t sizeWrite, bool newBlock)
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mSizeWrites += sizeWrite;

  if (newBlock)
  {
    mNoWrBlocks++;
  }
}


//------------------------------------------------------------------------------
// Increment the value of accumulated reads size
//------------------------------------------------------------------------------

void
FileAbstraction::IncrementReads (size_t sizeRead)
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mSizeReads += sizeRead;
}


//------------------------------------------------------------------------------
// Decrement the value of writes size
//------------------------------------------------------------------------------

void
FileAbstraction::DecrementWrites (size_t sizeWrite, bool fullBlock)
{
  mCondUpdate.Lock();
  eos_static_debug("writes old size=%zu", mSizeWrites);
  mSizeWrites -= sizeWrite;

  if (fullBlock)
  {
    mNoWrBlocks--;
  }

  eos_static_debug("writes new size=%zu", mSizeWrites);

  if (mSizeWrites == 0)
  {
    //..........................................................................
    // Notify pending reading processes
    //..........................................................................
    mCondUpdate.Signal();
  }

  mCondUpdate.UnLock();
}


//------------------------------------------------------------------------------
// Decrement the value of reads size
//------------------------------------------------------------------------------

void
FileAbstraction::DecrementReads (size_t sizeRead)
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mSizeReads -= sizeRead;
}


//------------------------------------------------------------------------------
// Get number of references held to the current file object
//------------------------------------------------------------------------------

int
FileAbstraction::GetNoReferences ()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  return mNoReferences;
}


//------------------------------------------------------------------------------
// Increment the number of references
//------------------------------------------------------------------------------

void
FileAbstraction::IncrementNoReferences ()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mNoReferences++;
}


//------------------------------------------------------------------------------
// Decrement number of references
//------------------------------------------------------------------------------

void
FileAbstraction::DecrementNoReferences ()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mNoReferences--;
}


//------------------------------------------------------------------------------
// Wait to fulsh the writes from cache
//------------------------------------------------------------------------------

void
FileAbstraction::WaitFinishWrites ()
{
  mCondUpdate.Lock();
  eos_static_debug("mSizeWrites=%zu", mSizeWrites);

  if (mSizeWrites != 0)
  {
    mCondUpdate.Wait();
  }

  mCondUpdate.UnLock();
}


//------------------------------------------------------------------------------
// Generate block key
//------------------------------------------------------------------------------

long long int
FileAbstraction::GenerateBlockKey (off_t offset)
{
  offset = (offset / CacheEntry::GetMaxSize()) * CacheEntry::GetMaxSize();
  return static_cast<long long int> ((1e14 * mIdFile) + offset);
}


//------------------------------------------------------------------------------
// Test if file is in use
//------------------------------------------------------------------------------

bool
FileAbstraction::IsInUse (bool strongConstraint)
{
  bool retVal = false;
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  eos_static_debug("mSizeReads=%zu, mSizeWrites=%zu, mNoReferences=%i",
                   mSizeReads, mSizeWrites, mNoReferences);

  if (strongConstraint)
  {
    if ((mSizeReads + mSizeWrites != 0) || (mNoReferences >= 1))
    {
      retVal = true;
    }
  }
  else
  {
    if ((mSizeReads + mSizeWrites != 0) || (mNoReferences > 1))
    {
      retVal = true;
    }
  }

  return retVal;
}

//------------------------------------------------------------------------------
// Get file object id
//------------------------------------------------------------------------------

int
FileAbstraction::GetId () const
{
  return mIdFile;
}


//------------------------------------------------------------------------------
// Get handler to the queue of errors
//------------------------------------------------------------------------------

eos::common::ConcurrentQueue<error_type>&
FileAbstraction::GetErrorQueue () const
{
  return *errorsQueue;
}


//------------------------------------------------------------------------------
// Get inode value
//------------------------------------------------------------------------------

unsigned long
FileAbstraction::GetFd () const
{
  return mFd;
}

