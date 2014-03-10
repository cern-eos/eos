//------------------------------------------------------------------------------
// File: FileAbstraction.cc
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch> CERN
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
#include "fst/layout/Layout.hh"
//------------------------------------------------------------------------------


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileAbstraction::FileAbstraction(int fd, eos::fst::Layout* file) :
  mFd(fd),
  mFile(file),
  mNoReferences(0),
  mSizeWrites(0),
  mNoWrBlocks(0)
{
  // Max file size we can deal with is ~ 90TB
  mFirstPossibleKey = static_cast<long long>(1e14 * mFd);
  mLastPossibleKey = static_cast<long long>((1e14 * (mFd + 1)));
  eos_static_debug("ptr_obj=%p, first_key=%llu, last_key=%llu",
                   this, mFirstPossibleKey, mLastPossibleKey);
  errorsQueue = new eos::common::ConcurrentQueue<error_type > ();
  mCondUpdate = XrdSysCondVar(0);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FileAbstraction::~FileAbstraction()
{
  delete errorsQueue;
  delete mFile;
}


//------------------------------------------------------------------------------
// Get size of write blocks in cache
//------------------------------------------------------------------------------
size_t
FileAbstraction::GetSizeWrites()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  return mSizeWrites;
}


//------------------------------------------------------------------------------
// Get number of write blocks in cache
//------------------------------------------------------------------------------
long long int
FileAbstraction::GetNoWriteBlocks()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  return mNoWrBlocks;
}


//------------------------------------------------------------------------------
// Increment the value of accumulated writes size
//------------------------------------------------------------------------------
void
FileAbstraction::IncrementWrites(size_t size, bool newBlock)
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mSizeWrites += size;

  if (newBlock)
    mNoWrBlocks++;
}


//------------------------------------------------------------------------------
// Decrement the value of writes size
//------------------------------------------------------------------------------
void
FileAbstraction::DecrementWrites(size_t size, bool fullBlock)
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  eos_static_debug("old_sz=%zu, new_sz=%zu", mSizeWrites, mSizeWrites - size);
  mSizeWrites -= size;

  if (fullBlock)
    mNoWrBlocks--;

  // Notify pending reading processes
  if (mSizeWrites == 0)
    mCondUpdate.Signal();
}


//------------------------------------------------------------------------------
// Wait to fulsh the writes from cache
//------------------------------------------------------------------------------
void
FileAbstraction::WaitFinishWrites()
{
  eos_static_debug("mSizeWrites=%zu", mSizeWrites);
  XrdSysCondVarHelper scope_lock(mCondUpdate);

  if (mSizeWrites)
    mCondUpdate.Wait();
}


//------------------------------------------------------------------------------
// Generate block key
//------------------------------------------------------------------------------
long long int
FileAbstraction::GenerateBlockKey(off_t offset)
{
  offset = (offset / CacheEntry::GetMaxSize()) * CacheEntry::GetMaxSize();
  return static_cast<long long int>((1e14 * mFd) + offset);
}


//------------------------------------------------------------------------------
// Increment the number of references
//------------------------------------------------------------------------------
void
FileAbstraction::IncNumRef()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mNoReferences++;
}


//------------------------------------------------------------------------------
// Decrement the number of references
//------------------------------------------------------------------------------
void
FileAbstraction::DecNumRef()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mNoReferences--;
}


//------------------------------------------------------------------------------
// Test if file is in use
//------------------------------------------------------------------------------
bool
FileAbstraction::IsInUse()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  eos_static_debug("write_sz=%zu, num_ref=%i", mSizeWrites, mNoReferences);
  return ((mSizeWrites) || (mNoReferences > 1));
}


//------------------------------------------------------------------------------
// Get handler to the queue of errors
//------------------------------------------------------------------------------
eos::common::ConcurrentQueue<error_type>&
FileAbstraction::GetErrorQueue() const
{
  return *errorsQueue;
}

