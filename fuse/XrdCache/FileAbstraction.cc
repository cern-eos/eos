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
FileAbstraction::FileAbstraction( int id, unsigned long ino ):
  id_file( id ),
  no_references( 0 ),
  inode( ino ),
  size_writes( 0 ),
  size_reads( 0 ),
  no_wr_blocks( 0 )
{
  //----------------------------------------------------------------------------
  // Max file size we can deal with is ~ 90TB
  //----------------------------------------------------------------------------
  first_possible_key = static_cast<long long>( 1e14 * id_file );
  last_possible_key = static_cast<long long>( ( 1e14 * ( id_file + 1 ) ) );

  eos_static_debug( "id_file=%i, first_possible_key=%llu, last_possible_key=%llu",
                    id_file, first_possible_key, last_possible_key );

  errorsQueue = new ConcurrentQueue<error_type>();
  cond_update = XrdSysCondVar( 0 );
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FileAbstraction::~FileAbstraction()
{
  delete errorsQueue;
}


//------------------------------------------------------------------------------
// Get sum of the write and read blocks size in cache
//------------------------------------------------------------------------------
size_t
FileAbstraction::GetSizeRdWr()
{
  XrdSysCondVarHelper cHepler( cond_update );
  return ( size_writes + size_reads );
}


//------------------------------------------------------------------------------
// Get size of write blocks in cache
//------------------------------------------------------------------------------
size_t
FileAbstraction::GetSizeWrites()
{
  XrdSysCondVarHelper cHepler( cond_update );
  return size_writes;
}


//------------------------------------------------------------------------------
// Get size of read blocks in cache
//------------------------------------------------------------------------------
size_t
FileAbstraction::GetSizeReads()
{
  XrdSysCondVarHelper cHepler( cond_update );
  return  size_reads;
}


//------------------------------------------------------------------------------
// Get number of write blocks in cache
//------------------------------------------------------------------------------
long long int
FileAbstraction::GetNoWriteBlocks()
{
  XrdSysCondVarHelper cHepler( cond_update );
  return no_wr_blocks;
}


//------------------------------------------------------------------------------
// Get value of the first possible key
//------------------------------------------------------------------------------
long long
FileAbstraction::GetFirstPossibleKey() const
{
  return first_possible_key;
}


//------------------------------------------------------------------------------
// Get value of the last possible key
//------------------------------------------------------------------------------
long long
FileAbstraction::GetLastPossibleKey() const
{
  return last_possible_key;
}


//------------------------------------------------------------------------------
// Increment the value of accumulated writes size
//------------------------------------------------------------------------------
void
FileAbstraction::IncrementWrites( size_t size, bool new_block )
{
  XrdSysCondVarHelper cHepler( cond_update );
  size_writes += size;

  if ( new_block ) {
    no_wr_blocks++;
  }
}


//------------------------------------------------------------------------------
// Increment the value of accumulated reads size
//------------------------------------------------------------------------------
void
FileAbstraction::IncrementReads( size_t size )
{
  XrdSysCondVarHelper cHepler( cond_update );
  size_reads += size;
}


//------------------------------------------------------------------------------
// Decrement the value of writes size
//------------------------------------------------------------------------------
void
FileAbstraction::DecrementWrites( size_t size, bool full_block )
{
  cond_update.Lock();
  eos_static_debug( "writes old size=%zu", size_writes );
  size_writes -= size;

  if ( full_block ) {
    no_wr_blocks--;
  }

  eos_static_debug( "writes new size=%zu", size_writes );

  if ( size_writes == 0 ) {
    //--------------------------------------------------------------------------
    // Notify pending reading processes
    //--------------------------------------------------------------------------
    cond_update.Signal();
  }

  cond_update.UnLock();
}


//------------------------------------------------------------------------------
// Decrement the value of reads size
//------------------------------------------------------------------------------
void
FileAbstraction::DecrementReads( size_t size )
{
  XrdSysCondVarHelper cHepler( cond_update );
  size_reads -= size;
}


//------------------------------------------------------------------------------
// Get number of references held to the current file object
//------------------------------------------------------------------------------
int
FileAbstraction::GetNoReferences()
{
  XrdSysCondVarHelper cHepler( cond_update );
  return no_references;
}


//------------------------------------------------------------------------------
// Increment the number of references
//------------------------------------------------------------------------------
void
FileAbstraction::IncrementNoReferences()
{
  XrdSysCondVarHelper cHepler( cond_update );
  no_references++;
}


//------------------------------------------------------------------------------
// Decrement number of references
//------------------------------------------------------------------------------
void
FileAbstraction::DecrementNoReferences()
{
  XrdSysCondVarHelper cHepler( cond_update );
  no_references--;
}


//------------------------------------------------------------------------------
// Wait to fulsh the writes from cache
//------------------------------------------------------------------------------
void
FileAbstraction::WaitFinishWrites()
{
  cond_update.Lock();
  eos_static_debug( "size_writes=%zu", size_writes );

  if ( size_writes != 0 ) {
    cond_update.Wait();
  }

  cond_update.UnLock();
}


//------------------------------------------------------------------------------
// Generate block key
//------------------------------------------------------------------------------
long long int
FileAbstraction::GenerateBlockKey( off_t offset )
{
  offset = ( offset / CacheEntry::GetMaxSize() ) * CacheEntry::GetMaxSize();
  return static_cast<long long int>( ( 1e14 * id_file ) + offset );
}


//------------------------------------------------------------------------------
// Test if file is in use
//------------------------------------------------------------------------------
bool
FileAbstraction::IsInUse( bool strong_constraint )
{
  bool retVal = false;
  XrdSysCondVarHelper cHepler( cond_update );

  eos_static_debug( "size_reads=%zu, size_writes=%zu, no_references=%i",
                    size_reads, size_writes, no_references );

  if ( strong_constraint ) {
    if ( ( size_reads + size_writes != 0 ) || ( no_references >= 1 ) ) {
      retVal =  true;
    }
  } else {
    if ( ( size_reads + size_writes != 0 ) || ( no_references > 1 ) ) {
      retVal =  true;
    }
  }

  return retVal;
}

//------------------------------------------------------------------------------
// Get file object id
//------------------------------------------------------------------------------
int
FileAbstraction::GetId() const
{
  return id_file;
}


//------------------------------------------------------------------------------
// Get handler to the queue of errors
//------------------------------------------------------------------------------
ConcurrentQueue<error_type>&
FileAbstraction::GetErrorQueue() const
{
  return *errorsQueue;
}


//------------------------------------------------------------------------------
// Get inode value
//------------------------------------------------------------------------------
unsigned long
FileAbstraction::GetInode() const
{
  return inode;
}

