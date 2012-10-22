// ----------------------------------------------------------------------
// File: FuseCacehEntry.cc
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

/*----------------------------------------------------------------------------*/
#include "FuseCacheEntry.hh"
/*----------------------------------------------------------------------------*/


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FuseCacheEntry::FuseCacheEntry( int             no_entries,
                                struct timespec mt,
                                struct dirbuf*  buf ):
  num_entries( no_entries )
{
  mtime.tv_sec = mt.tv_sec;
  mtime.tv_nsec = mt.tv_nsec;

  b.size = buf->size;
  b.p = ( char* ) calloc( b.size, sizeof( char ) );
  b.p = ( char* ) memcpy( b.p, buf->p, b.size * sizeof( char ) );
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FuseCacheEntry::~FuseCacheEntry()
{
  free( b.p );
}


//------------------------------------------------------------------------------
// Test if directory is filled
//------------------------------------------------------------------------------
bool FuseCacheEntry::IsFilled()
{
  eos::common::RWMutexReadLock rd_lock( mutex );
  return ( children.size() == static_cast<unsigned int>( num_entries - 2 ) );
}


//------------------------------------------------------------------------------
// Update directory information
//------------------------------------------------------------------------------
void FuseCacheEntry::Update( int             no_entries,
                             struct timespec mt,
                             struct dirbuf*  buf )
{
  eos::common::RWMutexWriteLock wLock( mutex );
  mtime.tv_sec = mt.tv_sec;
  mtime.tv_nsec = mt.tv_nsec;
  num_entries = no_entries;
  children.clear();

  if ( b.size != buf->size ) {
    b.size = buf->size;
    b.p = static_cast<char*>( realloc( b.p, b.size * sizeof( char ) ) );
  }

  b.p = static_cast<char*>( memcpy( b.p, buf->p, b.size * sizeof( char ) ) );
}

//------------------------------------------------------------------------------
// Get the dirbuf structure
//------------------------------------------------------------------------------
void FuseCacheEntry::GetDirbuf( struct dirbuf*& buf )
{
  eos::common::RWMutexReadLock rd_lock( mutex );
  buf->size = b.size;
  buf->p = static_cast<char*>( calloc( buf->size, sizeof( char ) ) );
  buf->p = static_cast<char*>( memcpy( buf->p, b.p, buf->size * sizeof( char ) ) );
}


//------------------------------------------------------------------------------
// Get the modification time
//------------------------------------------------------------------------------
struct timespec FuseCacheEntry::GetModifTime()
{
  eos::common::RWMutexReadLock rd_lock( mutex );
  return mtime;
}


//------------------------------------------------------------------------------
// Add subentry
//------------------------------------------------------------------------------
void FuseCacheEntry::AddEntry( unsigned long long       inode,
                               struct fuse_entry_param* e )
{
  eos::common::RWMutexWriteLock wr_lock( mutex );

  if ( !children.count( inode ) ) {
    children[inode] = *e;
  }
}


//------------------------------------------------------------------------------
// Get subentry
//------------------------------------------------------------------------------
bool FuseCacheEntry::GetEntry( unsigned long long       inode,
                               struct fuse_entry_param& e )
{
  eos::common::RWMutexReadLock rd_lock( mutex );

  if ( children.count( inode ) ) {
    e = children[inode];
    return true;
  }

  return false;
}

