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

const double CacheImpl::msMaxPercentWrites = 0.90;
const double CacheImpl::msMaxPercentSizeBlocks = 1.15;

//------------------------------------------------------------------------------
// Construct the cache framework object
//------------------------------------------------------------------------------
CacheImpl::CacheImpl( size_t sizeMax, XrdFileCache* pMgmCache ):
  mpMgmCache( pMgmCache ),
  mSizeMax( sizeMax ),
  mSizeVirtual( 0 ),
  mSizeAllocBlocks( 0 )
{
  mRecycleQueue = new ConcurrentQueue<CacheEntry*>();
  mWrReqQueue = new ConcurrentQueue<CacheEntry*>();
  mCacheThreshold = msMaxPercentWrites * mSizeMax;
  mMaxSizeAllocBlocks = msMaxPercentSizeBlocks * mSizeMax;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
CacheImpl::~CacheImpl()
{
  mRwMutex.WriteLock();                                //write lock map

  for ( key_map_type::iterator it = mKey2ListIter.begin();
        it != mKey2ListIter.end();
        it++ ) {
    delete it->second.first;
  }

  mKey2ListIter.clear();
  mRwMutex.UnLock();                                   //unlock map

  //............................................................................
  // Delete recyclabe objects
  //............................................................................
  CacheEntry* ptr = 0;

  while ( mRecycleQueue->try_pop( ptr ) ) {
    delete ptr;
  }

  delete mRecycleQueue;
  delete mWrReqQueue;
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
    COMMONTIMING( "before pop", &rtw );
    mWrReqQueue->wait_pop( pEntry );
    COMMONTIMING( "after pop", &rtw );

    if ( pEntry == 0 ) {
      break;
    } else {
      //........................................................................
      // Do write element
      //........................................................................
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
  //............................................................................
  // Block requested is aligned with respect to the maximum CacheEntry size
  //............................................................................
  eos::common::Timing gr( "getRead" );
  COMMONTIMING( "start", &gr );

  bool found_piece = false;
  CacheEntry* pEntry = 0;

  mRwMutex.ReadLock();                                 //read lock map
  const key_map_type::iterator it = mKey2ListIter.find( k );

  if ( it != mKey2ListIter.end() ) {
    pEntry = it->second.first;
    COMMONTIMING( "getPiece in", &gr );
    found_piece = pEntry->GetPiece( buf, off, len );
    COMMONTIMING( "getPiece out", &gr );

    if ( found_piece ) {
      //........................................................................
      // Update access record
      //........................................................................
      XrdSysMutexHelper mutex_helper( mMutexList );
      mKeyList.splice( mKeyList.end(), mKeyList, it->second.second );
    }
  }

  mRwMutex.UnLock();                                   //unlock map
  COMMONTIMING( "return", &gr );
  //gr.Print();
  return found_piece;
}


//------------------------------------------------------------------------------
// Insert a read block in the cache
//------------------------------------------------------------------------------
void
CacheImpl::AddRead( eos::fst::Layout*&   file,
                    const long long int& k,
                    char*                buf,
                    off_t                off,
                    size_t               len,
                    FileAbstraction&     rFileAbst )
{
  eos::common::Timing ar( "addRead" );
  COMMONTIMING( "start", &ar );
  CacheEntry* pEntry = 0;

  mRwMutex.ReadLock();                                 //read lock map
  const key_map_type::iterator it = mKey2ListIter.find( k );

  if ( it != mKey2ListIter.end() ) {
    //..........................................................................
    // Block entry found
    //..........................................................................
    size_t size_added;
    pEntry = it->second.first;
    size_added = pEntry->AddPiece( buf, off, len );
    pEntry->GetParentFile()->IncrementReads( size_added );

    mMutexList.Lock();                                 //lock list
    mKeyList.splice( mKeyList.end(), mKeyList, it->second.second );
    mMutexList.UnLock();                               //unlock list

    mRwMutex.UnLock();                                 //unlock map
    COMMONTIMING( "add to old block", &ar );
  } else {
    mRwMutex.UnLock();                                 //unlock map

    //..........................................................................
    // Get new block
    //..........................................................................
    pEntry = GetRecycledBlock( file, buf, off, len, false, rFileAbst );

    while ( GetSize() + CacheEntry::GetMaxSize() >= mSizeMax ) {
      COMMONTIMING( "start evitc", &ar );

      if ( !RemoveReadBlock() ) {
        ForceWrite();
      }
    }

    COMMONTIMING( "after evict", &ar );
    XrdSysRWLockHelper rw_helper( mRwMutex, false );   //write lock map
    XrdSysMutexHelper mutex_helper( mMutexList );      //lock list

    //..........................................................................
    // Update cache and file size
    //..........................................................................
    IncrementSize( CacheEntry::GetMaxSize() );
    pEntry->GetParentFile()->IncrementReads( pEntry->GetSizeData() );

    //..........................................................................
    // Update most-recently-used key
    //..........................................................................
    key_list_type::iterator it = mKeyList.insert( mKeyList.end(), k );
    mKey2ListIter.insert( std::make_pair( k, std::make_pair( pEntry, it ) ) );

    //---> unlock map and list
  }

  COMMONTIMING( "return", &ar );
  //ar.Print();
}


//------------------------------------------------------------------------------
// Flush all write requests belonging to a given file
//------------------------------------------------------------------------------
void
CacheImpl::FlushWrites( FileAbstraction& rFileAbst )
{
  CacheEntry* pEntry = 0;

  if ( rFileAbst.GetSizeWrites() == 0 ) {
    eos_static_debug( "no writes for this file" );
    return;
  }

  XrdSysRWLockHelper rw_helper( mRwMutex );             //read lock map

  key_map_type::iterator iStart = 
    mKey2ListIter.lower_bound( rFileAbst.GetFirstPossibleKey() );

  const key_map_type::iterator iEnd = 
    mKey2ListIter.lower_bound( rFileAbst.GetLastPossibleKey() );

  while ( iStart != iEnd ) {
    pEntry = iStart->second.first;
    assert( pEntry->IsWr() );
    mWrReqQueue->push( pEntry );
    mKey2ListIter.erase( iStart++ );
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

  //............................................................................
  // Put error code in error queue
  //............................................................................
  if ( retc == -1 ) {
    error = std::make_pair( retc, pEntry->GetOffsetStart() );
    pEntry->GetParentFile()->errorsQueue->push( error );
  }

  pEntry->GetParentFile()->DecrementWrites( pEntry->GetSizeData(), true );
  size_t current_size = DecrementSize( CacheEntry::GetMaxSize() );

  if ( ( current_size < mCacheThreshold ) &&
       ( current_size + CacheEntry::GetMaxSize() >= mCacheThreshold ) ) {
    //..........................................................................
    // Notify possible waiting threads that a write was done
    // (i.e. possible free space in cache available)
    //..........................................................................
    eos_static_debug( "Thread broadcasting writes done." );
    mCondWrDone.Broadcast();                           //send broadcast
  }

  //............................................................................
  // Add block to recycle list
  //............................................................................
  mRecycleQueue->push( pEntry );
}


//------------------------------------------------------------------------------
// Add new write request
//------------------------------------------------------------------------------
void
CacheImpl::AddWrite( eos::fst::Layout*&   file,
                     const long long int& k,
                     char*                buf,
                     off_t                off,
                     size_t               len,
                     FileAbstraction&     rFileAbst )
{
  CacheEntry* pEntry = 0;

  if ( rFileAbst.GetSizeReads() != 0 ) {
    //--------------------------------------------------------------------------
    // Delete all read blocks from cache
    //--------------------------------------------------------------------------
    XrdSysRWLockHelper rw_helper( mRwMutex, false );   //write lock map
    XrdSysMutexHelper mutex_helper( mMutexList );      //mutex lock

    key_list_type::iterator itList;

    const key_map_type::iterator iStart = 
      mKey2ListIter.lower_bound( rFileAbst.GetFirstPossibleKey() );

    const key_map_type::iterator iEnd = 
      mKey2ListIter.lower_bound( rFileAbst.GetLastPossibleKey() );

    key_map_type::iterator iTmp = iStart;

    while ( iTmp != iEnd ) {
      pEntry = iTmp->second.first;
      itList = iTmp->second.second;

      if ( pEntry->IsWr() ) {
        eos_static_err( "error=found write block, when only reads expected" );
        exit( -1 );
      }

      pEntry->GetParentFile()->DecrementReads( pEntry->GetSizeData() );
      DecrementSize( CacheEntry::GetMaxSize() );
      mKeyList.erase( itList );
      iTmp++;
    }

    mKey2ListIter.erase( iStart, iEnd );

    //--->unlock map and list
  }

  mRwMutex.ReadLock();                                 //read lock map
  assert( rFileAbst.GetSizeReads() == 0 );

  key_map_type::iterator it = mKey2ListIter.find( k );

  if ( it != mKey2ListIter.end() ) {
    size_t size_added;
    pEntry = it->second.first;
    size_added = pEntry->AddPiece( buf, off, len );
    pEntry->GetParentFile()->IncrementWrites( size_added, false );

    eos_static_debug( "info=old_block: key=%lli, off=%zu, len=%zu "
                      "size_added=%zu parentWrites=%zu",
                      k, off, len, size_added,
                      pEntry->GetParentFile()->GetSizeWrites() );

    if ( pEntry->IsFull() ) {
      eos_static_debug( "info=block full add to writes queue" );
      mKey2ListIter.erase( it );
      mWrReqQueue->push( pEntry );
    }

    mRwMutex.UnLock();                                 //unlock map
  } else {
    mRwMutex.UnLock();                                 //unlock map

    //..........................................................................
    // Get new block
    //..........................................................................
    pEntry = GetRecycledBlock( file, buf, off, len, true, rFileAbst );

    while ( GetSize() + CacheEntry::GetMaxSize() >= mSizeMax ) {
      eos_static_debug( "size cache=%zu before adding write block", GetSize() );

      if ( !RemoveReadBlock() ) {
        ForceWrite();
      }
    }

    pEntry->GetParentFile()->IncrementWrites( len, true );
    IncrementSize( CacheEntry::GetMaxSize() );

    eos_static_debug( "info=new_block: key=%lli, off=%zu, len=%zu "
                      "size_added=%zu parentWrites=%zu",
                      k, off, len, len, pEntry->GetParentFile()->GetSizeWrites() );

    //..........................................................................
    // Deal with new entry
    //..........................................................................
    if ( pEntry->IsFull() ) {
      mWrReqQueue->push( pEntry );
    } else {
      std::pair<key_map_type::iterator, bool> ret;
      mRwMutex.ReadLock();                             //read lock map

      ret = mKey2ListIter.insert(
              std::make_pair( k, std::make_pair( pEntry, mKeyList.end() ) ) );

      mRwMutex.UnLock();                               //unlock map
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
  mWrReqQueue->push( pEntry );
  return;
}


//------------------------------------------------------------------------------
// Recycle an used block or create a new one if none available
//------------------------------------------------------------------------------
CacheEntry*
CacheImpl::GetRecycledBlock( eos::fst::Layout*& file,
                             char*              buf,
                             off_t              off,
                             size_t             len,
                             bool               isWr,
                             FileAbstraction&   rFileAbst )
{
  CacheEntry* pRecycledObj = 0;

  if ( mRecycleQueue->try_pop( pRecycledObj ) ) {
    //..........................................................................
    // Got obj from pool
    //..........................................................................
    pRecycledObj->DoRecycle( file, buf, off, len, rFileAbst, isWr );
  } else {
    XrdSysMutexHelper mutex_helper( mMutexAllocSize );

    if ( mSizeAllocBlocks >= mMaxSizeAllocBlocks ) {
      mutex_helper.UnLock();
      mRecycleQueue->wait_pop( pRecycledObj );
    } else {
      //........................................................................
      // No obj in pool, allocate new one
      //........................................................................
      mSizeAllocBlocks += CacheEntry::GetMaxSize();
      mutex_helper.UnLock();
      pRecycledObj = new CacheEntry( file, buf, off, len, rFileAbst, isWr );
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

  mRwMutex.WriteLock();                                //write lock map
  key_map_type::iterator iStart = mKey2ListIter.begin();
  const key_map_type::iterator iEnd = mKey2ListIter.end();
  pEntry = iStart->second.first;

  while ( !pEntry->IsWr() && ( iStart != iEnd ) ) {
    iStart++;
    pEntry = iStart->second.first;
  }

  if ( iStart != iEnd ) {
    eos_static_debug( "Force write to be done!\n" );
    mWrReqQueue->push( pEntry );
    mKey2ListIter.erase( iStart );
  }

  mRwMutex.UnLock();                                   //unlock map

  eos_static_debug( "Thread waiting 250 ms for writes to be done..." );
  mCondWrDone.WaitMS( CacheImpl::GetTimeWait() );
}


//------------------------------------------------------------------------------
// Method to remove the least-recently used read block from cache
//------------------------------------------------------------------------------
bool
CacheImpl::RemoveReadBlock()
{
  bool found_candidate = false;
  XrdSysRWLockHelper rw_helper( mRwMutex, 0 );         //write lock map
  XrdSysMutexHelper mutex_helper( mMutexList );        //lock list

  key_list_type::iterator iter = mKeyList.begin();

  if ( iter != mKeyList.end() ) {
    found_candidate = true;
    const key_map_type::iterator it = mKey2ListIter.find( *iter );

    if ( it == mKey2ListIter.end() ) {
      eos_static_err( "Iterator to the end" );
      return false;
    }

    CacheEntry* pEntry = static_cast<CacheEntry*>( it->second.first );
    DecrementSize( CacheEntry::GetMaxSize() );
    mKey2ListIter.erase( it );
    mKeyList.erase( iter );

    //..........................................................................
    // Remove file id from mapping if no more blocks in cache and
    // there are no references to the file object.
    //..........................................................................
    pEntry->GetParentFile()->DecrementReads( pEntry->GetSizeData() );

    if ( !pEntry->GetParentFile()->IsInUse( true ) ) {
      mpMgmCache->RemoveFileInode( pEntry->GetParentFile()->GetInode(), true );
    }

    //..........................................................................
    // Add block to the recycle pool
    //..........................................................................
    mRecycleQueue->push( pEntry );
  }

  return found_candidate;
}


//------------------------------------------------------------------------------
// Get current size of the blocks in cache
//------------------------------------------------------------------------------
size_t
CacheImpl::GetSize()
{
  size_t ret_value;
  XrdSysMutexHelper mutex_helper( mMutexSize );
  ret_value = mSizeVirtual;
  return ret_value;
}


//------------------------------------------------------------------------------
// Increment size of the the blocks in cache
//------------------------------------------------------------------------------
size_t
CacheImpl::IncrementSize( size_t value )
{
  size_t ret_value;
  XrdSysMutexHelper mutex_helper( mMutexSize );
  mSizeVirtual += value;
  ret_value = mSizeVirtual;
  return ret_value;
}


//------------------------------------------------------------------------------
// Decrement the size of the the blocks in cache
//------------------------------------------------------------------------------
size_t
CacheImpl::DecrementSize( size_t value )
{
  size_t ret_value;
  XrdSysMutexHelper mutex_helper( mMutexSize );
  mSizeVirtual -= value;
  ret_value = mSizeVirtual;
  return ret_value;
}

