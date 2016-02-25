//-----------------------------------------------------------------------
// File: FuseCacheEntry.cc
// Author: Elvin-Alin Sindrilaru - CERN
//-----------------------------------------------------------------------

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

/*----------------------------------------------------------------------------*/
#include "FuseCacheEntry.hh"
/*----------------------------------------------------------------------------*/


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------

FuseCacheEntry::FuseCacheEntry (int noEntries,
                                struct timespec modifTime,
                                struct dirbuf* pBuf) :
mNumEntries (noEntries)
{
  mModifTime.tv_sec = modifTime.tv_sec;
  mModifTime.tv_nsec = modifTime.tv_nsec;
  mBuf.size = pBuf->size;
  mBuf.p = static_cast<char*> (calloc(mBuf.size, sizeof ( char)));
  mBuf.p = static_cast<char*> (memcpy(mBuf.p, pBuf->p, mBuf.size * sizeof ( char)));
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

FuseCacheEntry::~FuseCacheEntry ()
{
  free(mBuf.p);
}


//------------------------------------------------------------------------------
// Test if directory is filled
//------------------------------------------------------------------------------

bool
FuseCacheEntry::IsFilled ()
{
  eos::common::RWMutexReadLock rd_lock(mMutex);
  return ( mSubEntries.size() == static_cast<unsigned int> (mNumEntries - 2));
}


//------------------------------------------------------------------------------
// Update directory information
//------------------------------------------------------------------------------

void
FuseCacheEntry::Update (int noEntries,
                        struct timespec modifTime,
                        struct dirbuf* pBuf)
{
  eos::common::RWMutexWriteLock wr_lock(mMutex);
  mModifTime.tv_sec = modifTime.tv_sec;
  mModifTime.tv_nsec = modifTime.tv_nsec;
  mNumEntries = noEntries;
  mSubEntries.clear();

  if (mBuf.size != pBuf->size)
  {
    mBuf.size = pBuf->size;
    mBuf.p = static_cast<char*> (realloc(mBuf.p, mBuf.size * sizeof ( char)));
  }

  mBuf.p = static_cast<char*> (memcpy(mBuf.p, pBuf->p, mBuf.size * sizeof ( char)));
}

//------------------------------------------------------------------------------
// Get the dirbuf structure
//------------------------------------------------------------------------------

void
FuseCacheEntry::GetDirbuf (struct dirbuf*& rpBuf)
{
  eos::common::RWMutexReadLock rd_lock(mMutex);
  rpBuf->size = mBuf.size;
  rpBuf->p = static_cast<char*> (calloc(rpBuf->size, sizeof ( char)));
  rpBuf->p = static_cast<char*> (memcpy(rpBuf->p, mBuf.p, rpBuf->size * sizeof ( char)));
}


//------------------------------------------------------------------------------
// Get the modification time
//------------------------------------------------------------------------------

struct timespec
FuseCacheEntry::GetModifTime ()
{
  eos::common::RWMutexReadLock rd_lock(mMutex);
  return mModifTime;
}


//------------------------------------------------------------------------------
// Add subentry
//------------------------------------------------------------------------------

void
FuseCacheEntry::AddEntry (unsigned long long inode,
                          struct fuse_entry_param* e)
{
  eos::common::RWMutexWriteLock wr_lock(mMutex);

  if (!mSubEntries.count(inode))
  {
    mSubEntries[inode] = *e;
  }
}


//------------------------------------------------------------------------------
// Get subentry
//------------------------------------------------------------------------------

bool
FuseCacheEntry::GetEntry (unsigned long long inode,
                          struct fuse_entry_param& e)
{
  eos::common::RWMutexReadLock rd_lock(mMutex);

  if (mSubEntries.count(inode))
  {
    e = mSubEntries[inode];
    return true;
  }

  return false;
}

