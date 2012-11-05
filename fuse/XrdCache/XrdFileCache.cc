//------------------------------------------------------------------------------
// File: XrdFileCache.cc
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
#include <cstdio>
#include <cstring>
#include <unistd.h>
//------------------------------------------------------------------------------
#include "common/Logging.hh"
//------------------------------------------------------------------------------
#include "XrdFileCache.hh"
//------------------------------------------------------------------------------

XrdFileCache* XrdFileCache::pInstance = NULL;

//------------------------------------------------------------------------------
// Return a singleton instance of the class
//------------------------------------------------------------------------------
XrdFileCache*
XrdFileCache::GetInstance( size_t sizeMax )
{
  if ( !pInstance ) {
    pInstance  = new XrdFileCache( sizeMax );
    pInstance->Init();
  }

  return pInstance;
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdFileCache::XrdFileCache( size_t sizeMax ):
  mIndexFile( msMaxIndexFiles / 10 ),
  msCacheSizeMax( sizeMax )
{
  mpUsedIndxQueue = new ConcurrentQueue<int>();
}


//------------------------------------------------------------------------------
// Initialization method in which the low-level cache is created and the
// asynchronous thread doing the write operations is started
//------------------------------------------------------------------------------
void
XrdFileCache::Init()
{
  mpCacheImpl = new CacheImpl( msCacheSizeMax, this );
  //............................................................................
  // Start worker thread
  //............................................................................
  XrdSysThread::Run( &mWriteThread,
                     XrdFileCache::WriteThreadProc,
                     static_cast<void*>( this ) );
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdFileCache::~XrdFileCache()
{
  void* ret;
  mpCacheImpl->KillWriteThread();
  XrdSysThread::Join( mWriteThread, &ret );
  delete mpCacheImpl;
  delete mpUsedIndxQueue;
}


//------------------------------------------------------------------------------
// Function ran by the async thread managing the cache
//------------------------------------------------------------------------------
void*
XrdFileCache::WriteThreadProc( void* arg )
{
  XrdFileCache* pfc = static_cast<XrdFileCache*>( arg );
  pfc->mpCacheImpl->RunThreadWrites();
  eos_static_debug( "stopped writer thread" );
  return static_cast<void*>( pfc );
}


//------------------------------------------------------------------------------
// Obtain handler to a file abstraction object
//------------------------------------------------------------------------------
FileAbstraction*
XrdFileCache::GetFileObj( unsigned long inode, bool getNew )
{
  int key = -1;
  FileAbstraction* fRet = NULL;
  mRwLock.ReadLock();                                  //read lock
  std::map<unsigned long, FileAbstraction*>::iterator iter =
    mInode2fAbst.find( inode );

  if ( iter != mInode2fAbst.end() ) {
    fRet = iter->second;
    key = fRet->GetId();
  } else if ( getNew ) {
    mRwLock.UnLock();                                  //unlock map
    mRwLock.WriteLock();                               //write lock

    if ( mIndexFile >= msMaxIndexFiles ) {
      while ( !mpUsedIndxQueue->try_pop( key ) ) {
        mpCacheImpl->RemoveReadBlock();
      }

      fRet = new FileAbstraction( key, inode );
      mInode2fAbst.insert( std::pair<unsigned long, FileAbstraction*>( inode, fRet ) );
    } else {
      key = mIndexFile;
      fRet = new FileAbstraction( key, inode );
      mInode2fAbst.insert( std::pair<unsigned long, FileAbstraction*>( inode, fRet ) );
      mIndexFile++;
    }
  } else {
    mRwLock.UnLock();                                  //unlock map
    return 0;
  }

  //............................................................................
  // Increase the number of references to this file
  //............................................................................
  fRet->IncrementNoReferences();
  mRwLock.UnLock();                                    //unlock map
  eos_static_debug( "inode=%lu, key=%i", inode, key );
  return fRet;
}


//------------------------------------------------------------------------------
// Submit a write request
//------------------------------------------------------------------------------
void
XrdFileCache::SubmitWrite( XrdCl::File*& rpFile,
                           unsigned long inode,
                           void*         buf,
                           off_t         off,
                           size_t        len )
{
  size_t nwrite;
  long long int key;
  off_t written_offset = 0;
  char* pBuf = static_cast<char*>( buf );
  FileAbstraction* pAbst = GetFileObj( inode, true );

  //............................................................................
  // While write bigger than block size, break in smaller blocks
  //............................................................................
  while ( ( ( off % CacheEntry::GetMaxSize() ) + len ) > CacheEntry::GetMaxSize() ) {
    nwrite = CacheEntry::GetMaxSize() - ( off % CacheEntry::GetMaxSize() );
    key = pAbst->GenerateBlockKey( off );
    eos_static_debug( "(1) off=%zu, len=%zu", off, nwrite );
    mpCacheImpl->AddWrite( rpFile, key, pBuf + written_offset, off, nwrite, *pAbst );
    off += nwrite;
    len -= nwrite;
    written_offset += nwrite;
  }

  if ( len != 0 ) {
    nwrite = len;
    key = pAbst->GenerateBlockKey( off );
    eos_static_debug( "(2) off=%zu, len=%zu", off, nwrite );
    mpCacheImpl->AddWrite( rpFile, key, pBuf + written_offset, off, nwrite, *pAbst );
    written_offset += nwrite;
  }

  pAbst->DecrementNoReferences();
  return;
}


//------------------------------------------------------------------------------
// Try to satisfy request from cache
//------------------------------------------------------------------------------
size_t
XrdFileCache::GetRead( FileAbstraction& rFileAbst,
                       void*            buf,
                       off_t            off,
                       size_t           len )
{
  size_t nread;
  long long int key;
  bool found = true;
  off_t read_offset = 0;
  char* pBuf = static_cast<char*>( buf );

  //............................................................................
  // While read bigger than block size, break in smaller blocks
  //............................................................................
  while ( ( ( off % CacheEntry::GetMaxSize() ) + len ) > CacheEntry::GetMaxSize() ) {
    nread = CacheEntry::GetMaxSize() - ( off % CacheEntry::GetMaxSize() );
    key = rFileAbst.GenerateBlockKey( off );
    eos_static_debug( "(1) off=%zu, len=%zu", off, nread );
    found = mpCacheImpl->GetRead( key, pBuf + read_offset, off, nread );

    if ( !found ) {
      return 0;
    }

    off += nread;
    len -= nread;
    read_offset += nread;
  }

  if ( len != 0 ) {
    nread = len;
    key = rFileAbst.GenerateBlockKey( off );
    eos_static_debug( "(2) off=%zu, len=%zu", off, nread );
    found = mpCacheImpl->GetRead( key, pBuf + read_offset, off, nread );

    if ( !found ) {
      return 0;
    }

    read_offset += nread;
  }

  return read_offset;
}


//------------------------------------------------------------------------------
// Save piece in cache
//------------------------------------------------------------------------------
size_t
XrdFileCache::PutRead( XrdCl::File*&    file,
                       FileAbstraction& rFileAbst,
                       void*            buf,
                       off_t            off,
                       size_t           len )
{
  size_t nread;
  long long int key;
  off_t read_offset = 0;
  char* pBuf = static_cast<char*>( buf );

  //............................................................................
  // Read bigger than block size, break in smaller blocks
  //............................................................................
  while ( ( ( off % CacheEntry::GetMaxSize() ) + len ) > CacheEntry::GetMaxSize() ) {
    nread = CacheEntry::GetMaxSize() - ( off % CacheEntry::GetMaxSize() );
    key = rFileAbst.GenerateBlockKey( off );
    eos_static_debug( "(1) off=%zu, len=%zu key=%lli", off, nread, key );
    mpCacheImpl->AddRead( file, key, pBuf + read_offset, off, nread, rFileAbst );
    off += nread;
    len -= nread;
    read_offset += nread;
  }

  if ( len != 0 ) {
    nread = len;
    key = rFileAbst.GenerateBlockKey( off );
    eos_static_debug( "(2) off=%zu, len=%zu key=%lli", off, nread, key );
    mpCacheImpl->AddRead( file, key, pBuf + read_offset, off, nread, rFileAbst );
    read_offset += nread;
  }

  return read_offset;
}


//------------------------------------------------------------------------------
// Remove file from mapping
//------------------------------------------------------------------------------
bool
XrdFileCache::RemoveFileInode( unsigned long inode, bool strongConstraint )
{
  bool do_deletion = false;
  eos_static_debug( "inode=%lu", inode );
  FileAbstraction* ptr =  NULL;
  mRwLock.WriteLock();                                 //write lock map
  std::map<unsigned long, FileAbstraction*>::iterator iter =
    mInode2fAbst.find( inode );

  if ( iter != mInode2fAbst.end() ) {
    ptr = static_cast<FileAbstraction*>( ( *iter ).second );

    if ( strongConstraint ) {
      //........................................................................
      // Strong constraint
      //........................................................................
      do_deletion = ( ptr->GetSizeRdWr() == 0 ) && ( ptr->GetNoReferences() == 0 );
    } else {
      //........................................................................
      // Weak constraint
      //........................................................................
      do_deletion = ( ptr->GetSizeRdWr() == 0 ) && ( ptr->GetNoReferences() <= 1 );
    }

    if ( do_deletion ) {
      //........................................................................
      // Remove file from mapping
      //........................................................................
      int id = ptr->GetId();
      delete ptr;
      mInode2fAbst.erase( iter );
      mpUsedIndxQueue->push( id );
    }
  }

  mRwLock.UnLock();                                    //unlock map
  return do_deletion;
}


//------------------------------------------------------------------------------
// Get errors queue
//------------------------------------------------------------------------------
ConcurrentQueue<error_type>&
XrdFileCache::GetErrorQueue( unsigned long inode )
{
  ConcurrentQueue<error_type>* tmp = NULL;
  FileAbstraction* pAbst = GetFileObj( inode, false );

  if ( pAbst ) {
    *tmp = pAbst->GetErrorQueue();
    pAbst->DecrementNoReferences();
  }

  return *tmp;
}

//------------------------------------------------------------------------------
// Method used to wait for the writes corresponding to a file to be commited.
// It also forces the incompletele (not full) write blocks from cache to be
// added to the writes queue and implicitly to be written to the file.
//------------------------------------------------------------------------------
void
XrdFileCache::WaitFinishWrites( FileAbstraction& rFileAbst )
{
  if ( rFileAbst.GetSizeWrites() != 0 ) {
    mpCacheImpl->FlushWrites( rFileAbst );
    rFileAbst.WaitFinishWrites();

    /*
    if ( !rFileAbst.IsInUse( false ) ) {
      RemoveFileInode( rFileAbst.GetInode(), false );
    }
    */
  }
}


//------------------------------------------------------------------------------
// Method used to wait for the writes corresponding to a file to be commited.
// It also forces the incompletele (not full) write blocks from cache to be added
// to the writes queue and implicitly to be written to the file. 
//------------------------------------------------------------------------------
void
XrdFileCache::WaitWritesAndRemove(FileAbstraction &fileAbst)
{
  if (fileAbst.GetSizeWrites() != 0) {
    mpCacheImpl->FlushWrites(fileAbst);
    fileAbst.WaitFinishWrites();
  }

  if (!fileAbst.IsInUse(false)) {
    RemoveFileInode(fileAbst.GetInode(), false);
  }
}

