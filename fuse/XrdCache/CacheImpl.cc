//------------------------------------------------------------------------------
// File: CacheImpl.cc
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
#include "CacheImpl.hh"
//------------------------------------------------------------------------------

const double CacheImpl::max_percent_writes = 0.90;
const double CacheImpl::max_percent_size_blocks = 1.15;

//------------------------------------------------------------------------------
// Construct the cache framework object
//------------------------------------------------------------------------------
CacheImpl::CacheImpl( size_t s_max, XrdFileCache* fc ):
  mgm_cache( fc ),
  size_max( s_max ),
  size_virtual( 0 ),
  size_alloc_blocks( 0 )
{
  recycle_queue = new ConcurrentQueue<CacheEntry*>();
  wr_req_queue = new ConcurrentQueue<CacheEntry*>();
  cache_threshold = max_percent_writes * size_max;
  max_size_alloc_blocks = max_percent_size_blocks * size_max;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
CacheImpl::~CacheImpl()
{
  rw_mutex_map.WriteLock();                                 //write lock map

  for ( key_map_type::iterator it = key2listIter.begin();
        it != key2listIter.end();
        it++ ) {
    delete it->second.first;
  }

  key2listIter.clear();
  rw_mutex_map.UnLock();                                    //unlock map

  //----------------------------------------------------------------------------
  // Delete recyclabe objects
  //----------------------------------------------------------------------------
  CacheEntry* ptr = 0;

  while ( recycle_queue->try_pop( ptr ) ) {
    delete ptr;
  }

  delete recycle_queue;
  delete wr_req_queue;
}


//------------------------------------------------------------------------------
// Method run by the thread doing asynchronous writes
//------------------------------------------------------------------------------
void
CacheImpl::RunThreadWrites()
{
  CacheEntry* pEntry = 0;
  eos::common::Timing rtw( "runThreadWrites" );
  COMMONTIMING( "start", &rtw );

  while ( 1 ) {
    TIMING( "before pop", &rtw );
    wr_req_queue->wait_pop( pEntry );
    TIMING( "after pop", &rtw );

    if ( pEntry == 0 ) {
      break;
    } else {
      //------------------------------------------------------------------------
      // Do write element
      //------------------------------------------------------------------------
      ProcessWriteReq( pEntry );
    }
  }

  //rtw.Print();
}


//------------------------------------------------------------------------------
// Get the object from cache corresponding to the key k
//------------------------------------------------------------------------------
bool
CacheImpl::GetRead( const long long int& k, char* buf, off_t off, size_t len )
{
  //----------------------------------------------------------------------------
  // Block requested is aligned with respect to the maximum CacheEntry size
  //----------------------------------------------------------------------------
  eos::common::Timing gr( "getRead" );
  COMMONTIMING( "start", &gr );

  bool foundPiece = false;
  CacheEntry* pEntry = 0;

  rw_mutex_map.ReadLock();                                  //read lock map
  const key_map_type::iterator it = key2listIter.find( k );

  if ( it != key2listIter.end() ) {
    pEntry = it->second.first;
    COMMONTIMING( "getPiece in", &gr );
    foundPiece = pEntry->GetPiece( buf, off, len );
    COMMONTIMING( "getPiece out", &gr );

    if ( foundPiece ) {
      //------------------------------------------------------------------------
      // Update access record
      //------------------------------------------------------------------------
      XrdSysMutexHelper mHelper( mutex_list );
      key_list.splice( key_list.end(), key_list, it->second.second );
    }
  }

  rw_mutex_map.UnLock();                                    //unlock map
  TIMING( "return", &gr );
  //gr.Print();
  return foundPiece;
}


//------------------------------------------------------------------------------
// Insert a read block in the cache
//------------------------------------------------------------------------------
void
CacheImpl::AddRead( XrdCl::File*&        ref_file,
                    const long long int& k,
                    char*                buf,
                    off_t                off,
                    size_t               len,
                    FileAbstraction&     pFileAbst )
{
  eos::common::Timing ar( "addRead" );
  COMMONTIMING( "start", &ar );
  CacheEntry* pEntry = 0;

  rw_mutex_map.ReadLock();                                  //read lock map
  const key_map_type::iterator it = key2listIter.find( k );

  if ( it != key2listIter.end() ) {
    //--------------------------------------------------------------------------
    // Block entry found
    //--------------------------------------------------------------------------
    size_t sizeAdded;
    pEntry = it->second.first;
    sizeAdded = pEntry->AddPiece( buf, off, len );
    pEntry->GetParentFile()->IncrementReads( sizeAdded );

    mutex_list.Lock();                                      //lock list
    key_list.splice( key_list.end(), key_list, it->second.second );
    mutex_list.UnLock();                                    //unlock list

    rw_mutex_map.UnLock();                                  //unlock map
    COMMONTIMING( "add to old block", &ar );
  } else {
    rw_mutex_map.UnLock();                                  //unlock map

    //--------------------------------------------------------------------------
    // Get new block
    //--------------------------------------------------------------------------
    pEntry = GetRecycledBlock( ref_file, buf, off, len, false, pFileAbst );

    while ( GetSize() + CacheEntry::GetMaxSize() >= size_max ) {
      TIMING( "start evitc", &ar );

      if ( !RemoveReadBlock() ) {
        ForceWrite();
      }
    }
    
    COMMONTIMING( "after evict", &ar );
    XrdSysRWLockHelper rwHelper( rw_mutex_map, false );     //write lock map
    XrdSysMutexHelper mHelper( mutex_list );                //lock list

    //--------------------------------------------------------------------------
    // Update cache and file size
    //--------------------------------------------------------------------------
    IncrementSize( CacheEntry::GetMaxSize() );
    pEntry->GetParentFile()->IncrementReads( pEntry->GetSizeData() );

    //--------------------------------------------------------------------------
    // Update most-recently-used key
    //--------------------------------------------------------------------------
    key_list_type::iterator it = key_list.insert( key_list.end(), k );
    key2listIter.insert( std::make_pair( k, std::make_pair( pEntry, it ) ) );

    //---> unlock map and list
  }

  COMMONTIMING( "return", &ar );
  //ar.Print();
}


//------------------------------------------------------------------------------
// Flush all write requests belonging to a given file
//------------------------------------------------------------------------------
void
CacheImpl::FlushWrites( FileAbstraction& pFileAbst )
{
  CacheEntry* pEntry = 0;

  if ( pFileAbst.GetSizeWrites() == 0 ) {
    eos_static_debug( "info=no writes for this file" );
    return;
  }

  XrdSysRWLockHelper rwHelper( rw_mutex_map );              //read lock map
  key_map_type::iterator iStart = key2listIter.lower_bound( pFileAbst.GetFirstPossibleKey() );
  const key_map_type::iterator iEnd = key2listIter.lower_bound( pFileAbst.GetLastPossibleKey() );

  while ( iStart != iEnd ) {
    pEntry = iStart->second.first;
    assert( pEntry->IsWr() );
    wr_req_queue->push( pEntry );
    key2listIter.erase( iStart++ );
    eos_static_debug( "info=pushing write elem to queue" );
  }

  //---> unlock map
}


//------------------------------------------------------------------------------
// Process a write request
//------------------------------------------------------------------------------
void
CacheImpl::ProcessWriteReq( CacheEntry* pEntry )
{
  int retc = 0;
  error_type error;

  eos_static_debug( "file sizeWrites=%zu size=%lu offset=%llu",
                    pEntry->GetParentFile()->GetSizeWrites(),
                    pEntry->GetSizeData(), pEntry->GetOffsetStart() );

  retc = pEntry->DoWrite();

  //----------------------------------------------------------------------------
  // Put error code in error queue
  //----------------------------------------------------------------------------
  if ( retc ) {
    error = std::make_pair( retc, pEntry->GetOffsetStart() );
    pEntry->GetParentFile()->errorsQueue->push( error );
  }

  pEntry->GetParentFile()->DecrementWrites( pEntry->GetSizeData(), true );
  size_t current_size = DecrementSize( CacheEntry::GetMaxSize() );

  if ( ( current_size < cache_threshold ) &&
       ( current_size + CacheEntry::GetMaxSize() >= cache_threshold ) ) {
    //--------------------------------------------------------------------------
    // Notify possible waiting threads that a write was done
    // (i.e. possible free space in cache available)
    //--------------------------------------------------------------------------
    eos_static_debug( "Thread broadcasting writes done." );
    cond_wr_done.Broadcast();                               //send broadcast
  }

  //----------------------------------------------------------------------------
  // Add block to recycle list
  //----------------------------------------------------------------------------
  recycle_queue->push( pEntry );
}


//------------------------------------------------------------------------------
// Add new write request
//------------------------------------------------------------------------------
void
CacheImpl::AddWrite( XrdCl::File*&        ref_file,
                     const long long int& k,
                     char*                buf,
                     off_t                off,
                     size_t               len,
                     FileAbstraction&     pFileAbst )
{
  CacheEntry* pEntry = 0;

  if ( pFileAbst.GetSizeReads() != 0 ) {
    //--------------------------------------------------------------------------
    // Delete all read blocks from cache
    //--------------------------------------------------------------------------
    XrdSysRWLockHelper rwHelper( rw_mutex_map, false );     //write lock map
    XrdSysMutexHelper mHelper( mutex_list );                //mutex lock

    key_list_type::iterator itList;
    const key_map_type::iterator iStart = key2listIter.lower_bound( pFileAbst.GetFirstPossibleKey() );
    const key_map_type::iterator iEnd = key2listIter.lower_bound( pFileAbst.GetLastPossibleKey() );
    key_map_type::iterator iTmp = iStart;

    while ( iTmp != iEnd ) {
      pEntry = iTmp->second.first;
      itList = iTmp->second.second;

      if ( !pEntry->IsWr() ) {
        eos_static_err( "error=found write bloce, when only reads expected" );
        exit( -1 );
      }

      pEntry->GetParentFile()->DecrementReads( pEntry->GetSizeData() );
      DecrementSize( CacheEntry::GetMaxSize() );
      key_list.erase( itList );
      iTmp++;
    }

    key2listIter.erase( iStart, iEnd );

    //--->unlock map and list
  }

  rw_mutex_map.ReadLock();                                  //read lock map
  assert( pFileAbst.GetSizeReads() == 0 );

  key_map_type::iterator it = key2listIter.find( k );

  if ( it != key2listIter.end() ) {
    size_t sizeAdded;
    pEntry = it->second.first;
    sizeAdded = pEntry->AddPiece( buf, off, len );
    pEntry->GetParentFile()->IncrementWrites( sizeAdded, false );

    eos_static_debug( "info=old_block: key=%lli, off=%zu, len=%zu "
                      "sizeAdded=%zu parentWrites=%zu",
                      k, off, len, sizeAdded,
                      pEntry->GetParentFile()->GetSizeWrites() );

    if ( pEntry->IsFull() ) {
      eos_static_debug( "info=block full add to writes queue" );
      key2listIter.erase( it );
      wr_req_queue->push( pEntry );
    }

    rw_mutex_map.UnLock();                                  //unlock map
  } else {
    rw_mutex_map.UnLock();                                  //unlock map

    //--------------------------------------------------------------------------
    // Get new block
    //--------------------------------------------------------------------------
    pEntry = GetRecycledBlock( ref_file, buf, off, len, true, pFileAbst );

    while ( GetSize() + CacheEntry::GetMaxSize() >= size_max ) {
      eos_static_debug( "size cache=%zu before adding write block", GetSize() );

      if ( !RemoveReadBlock() ) {
        ForceWrite();
      }
    }

    pEntry->GetParentFile()->IncrementWrites( len, true );
    IncrementSize( CacheEntry::GetMaxSize() );

    eos_static_debug( "info=new_block: key=%lli, off=%zu, len=%zu "
                      "sizeAdded=%zu parentWrites=%zu",
                      k, off, len, len, pEntry->GetParentFile()->GetSizeWrites() );

    //--------------------------------------------------------------------------
    // Deal with new entry
    //--------------------------------------------------------------------------
    if ( pEntry->IsFull() ) {
      wr_req_queue->push( pEntry );
    } else {
      std::pair<key_map_type::iterator, bool> ret;
      rw_mutex_map.ReadLock();                              //read lock map

      ret = key2listIter.insert(
              std::make_pair( k, std::make_pair( pEntry, key_list.end() ) ) );

      rw_mutex_map.UnLock();                                //unlock map
    }
  }
}


//------------------------------------------------------------------------------
// Kill the asynchrounous thread doing the writes, by adding a sentinel obj.
//------------------------------------------------------------------------------
void
CacheImpl::KillWriteThread()
{
  CacheEntry* pEntry = 0;
  wr_req_queue->push( pEntry );
  return;
}


//------------------------------------------------------------------------------
// Recycle an used block or create a new one if none available
//------------------------------------------------------------------------------
CacheEntry*
CacheImpl::GetRecycledBlock( XrdCl::File*&    ref_file,
                             char*            buf,
                             off_t            off,
                             size_t           len,
                             bool             iswr,
                             FileAbstraction& pFileAbst )
{
  CacheEntry* pRecycledObj = 0;

  if ( recycle_queue->try_pop( pRecycledObj ) ) {
    //--------------------------------------------------------------------------
    // Got obj from pool
    //--------------------------------------------------------------------------
    pRecycledObj->DoRecycle( ref_file, buf, off, len, pFileAbst, iswr );
  } else {
    XrdSysMutexHelper mHelper( mutex_alloc_size );

    if ( size_alloc_blocks >= max_size_alloc_blocks ) {
      mHelper.UnLock();
      recycle_queue->wait_pop( pRecycledObj );
    } else {
      //------------------------------------------------------------------------
      // No obj in pool, allocate new one
      //------------------------------------------------------------------------
      size_alloc_blocks += CacheEntry::GetMaxSize();
      mHelper.UnLock();
      pRecycledObj = new CacheEntry( ref_file, buf, off, len, pFileAbst, iswr );
    }
  }

  return pRecycledObj;
}


//------------------------------------------------------------------------------
// Method to force the execution of a write even if the block is not full;
// This is done to lower the congestion in the cache when there are many
// sparse writes done
//------------------------------------------------------------------------------
void
CacheImpl::ForceWrite()
{
  CacheEntry* pEntry = 0;

  rw_mutex_map.WriteLock();                                 //write lock map
  key_map_type::iterator iStart = key2listIter.begin();
  const key_map_type::iterator iEnd = key2listIter.end();
  pEntry = iStart->second.first;

  while ( !pEntry->IsWr() && ( iStart != iEnd ) ) {
    iStart++;
    pEntry = iStart->second.first;
  }

  if ( iStart != iEnd ) {
    eos_static_debug( "Force write to be done!\n" );
    wr_req_queue->push( pEntry );
    key2listIter.erase( iStart );
  }

  rw_mutex_map.UnLock();                                    //unlock map

  eos_static_debug( "Thread waiting 250 ms for writes to be done..." );
  cond_wr_done.WaitMS( CacheImpl::GetTimeWait() );
}


//------------------------------------------------------------------------------
// Method to remove the least-recently used read block from cache
//------------------------------------------------------------------------------
bool
CacheImpl::RemoveReadBlock()
{
  bool foundCandidate = false;
  XrdSysRWLockHelper lHelper( rw_mutex_map, 0 );            //write lock map
  XrdSysMutexHelper mHelper( mutex_list );                  //lock list

  key_list_type::iterator iter = key_list.begin();

  if ( iter != key_list.end() ) {
    foundCandidate = true;
    const key_map_type::iterator it = key2listIter.find( *iter );

    if ( it == key2listIter.end() ) {
      eos_static_err( "Iterator to the end" );
      return false;
    }

    CacheEntry* pEntry = static_cast<CacheEntry*>( it->second.first );
    DecrementSize( CacheEntry::GetMaxSize() );
    key2listIter.erase( it );
    key_list.erase( iter );

    //--------------------------------------------------------------------------
    // Remove file id from mapping if no more blocks in cache and
    // there are no references to the file object.
    //--------------------------------------------------------------------------
    pEntry->GetParentFile()->DecrementReads( pEntry->GetSizeData() );

    if ( !pEntry->GetParentFile()->IsInUse( true ) ) {
      mgm_cache->RemoveFileInode( pEntry->GetParentFile()->GetInode(), true );
    }

    //--------------------------------------------------------------------------
    // Add block to the recycle pool
    //--------------------------------------------------------------------------
    recycle_queue->push( pEntry );
  }

  return foundCandidate;
}


//------------------------------------------------------------------------------
// Get current size of the blocks in cache
//------------------------------------------------------------------------------
size_t
CacheImpl::GetSize()
{
  size_t retValue;
  XrdSysMutexHelper mHelper( mutex_size );
  retValue = size_virtual;
  return retValue;
}


//------------------------------------------------------------------------------
// Increment size of the the blocks in cache
//------------------------------------------------------------------------------
size_t
CacheImpl::IncrementSize( size_t value )
{
  size_t retValue;
  XrdSysMutexHelper mHelper( mutex_size );
  size_virtual += value;
  retValue = size_virtual;
  return retValue;
}


//------------------------------------------------------------------------------
// Decrement the size of the the blocks in cache
//------------------------------------------------------------------------------
size_t
CacheImpl::DecrementSize( size_t value )
{
  size_t retValue;
  XrdSysMutexHelper mHelper( mutex_size );
  size_virtual -= value;
  retValue = size_virtual;
  return retValue;
}

