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
  cache_size_max( sizeMax ),
  indexFile( max_index_files / 10 )
{
  queue_used_indx = new ConcurrentQueue<int>();
}


//------------------------------------------------------------------------------
// Initialization method in which the low-level cache is created and the
// asynchronous thread doing the write operations is started
//------------------------------------------------------------------------------
void
XrdFileCache::Init()
{
  cache_impl = new CacheImpl( cache_size_max, this );

  //----------------------------------------------------------------------------
  // Start worker thread
  //----------------------------------------------------------------------------
  XrdSysThread::Run( &write_thread,
                     XrdFileCache::WriteThreadProc,
                     static_cast<void*>( this ) );
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdFileCache::~XrdFileCache()
{
  void* ret;
  cache_impl->KillWriteThread();
  XrdSysThread::Join( write_thread, &ret );

  delete cache_impl;
  delete queue_used_indx;
}


//------------------------------------------------------------------------------
// Function run by the async thread managing the cache
//------------------------------------------------------------------------------
void*
XrdFileCache::WriteThreadProc( void* arg )
{
  XrdFileCache* pfc = static_cast<XrdFileCache*>( arg );
  pfc->cache_impl->RunThreadWrites();
  eos_static_debug( "stopped writer thread" );
  return static_cast<void*>( pfc );
}


//------------------------------------------------------------------------------
// Obtain handler to a file abstraction object
//------------------------------------------------------------------------------
FileAbstraction*
XrdFileCache::GetFileObj( unsigned long inode, bool get_new )
{
  int key = -1;
  FileAbstraction* fRet = NULL;

  rw_lock.ReadLock();                                      //read lock

  std::map<unsigned long, FileAbstraction*>::iterator iter = inode2fAbst.find( inode );

  if ( iter != inode2fAbst.end() ) {
    fRet = iter->second;
    key = fRet->GetId();
  } else if ( get_new ) {
    rw_lock.UnLock();                                      //unlock map
    rw_lock.WriteLock();                                   //write lock

    if ( indexFile >= max_index_files ) {
      while ( !queue_used_indx->try_pop( key ) ) {
        cache_impl->RemoveReadBlock();
      }

      fRet = new FileAbstraction( key, inode );
      inode2fAbst.insert( std::pair<unsigned long, FileAbstraction*>( inode, fRet ) );
    } else {
      key = indexFile;
      fRet = new FileAbstraction( key, inode );
      inode2fAbst.insert( std::pair<unsigned long, FileAbstraction*>( inode, fRet ) );
      indexFile++;
    }
  } else {
    rw_lock.UnLock();                                      //unlock map
    return 0;
  }

  //----------------------------------------------------------------------------
  // Increase the number of references to this file
  //----------------------------------------------------------------------------
  fRet->IncrementNoReferences();
  rw_lock.UnLock();                                        //unlock map

  eos_static_debug( "inode=%lu, key=%i", inode, key );

  return fRet;
}


//------------------------------------------------------------------------------
// Submit a write request
//------------------------------------------------------------------------------
void
XrdFileCache::SubmitWrite( XrdCl::File*& file,
                           unsigned long inode,
                           void*         buf,
                           off_t         off,
                           size_t        len )
{
  size_t nwrite;
  long long int key;
  off_t writtenOffset = 0;
  char* pBuf = static_cast<char*>( buf );

  FileAbstraction* fAbst = GetFileObj( inode, true );

  //----------------------------------------------------------------------------
  // While write bigger than block size, break in smaller blocks
  //----------------------------------------------------------------------------
  while ( ( ( off % CacheEntry::GetMaxSize() ) + len ) > CacheEntry::GetMaxSize() ) {
    nwrite = CacheEntry::GetMaxSize() - ( off % CacheEntry::GetMaxSize() );
    key = fAbst->GenerateBlockKey( off );
    eos_static_debug( "(1) off=%zu, len=%zu", off, nwrite );
    cache_impl->AddWrite( file, key, pBuf + writtenOffset, off, nwrite, *fAbst );

    off += nwrite;
    len -= nwrite;
    writtenOffset += nwrite;
  }

  if ( len != 0 ) {
    nwrite = len;
    key = fAbst->GenerateBlockKey( off );
    eos_static_debug( "(2) off=%zu, len=%zu", off, nwrite );
    cache_impl->AddWrite( file, key, pBuf + writtenOffset, off, nwrite, *fAbst );
    writtenOffset += nwrite;
  }

  fAbst->DecrementNoReferences();
  return;
}


//------------------------------------------------------------------------------
// Try to satisfy request from cache
//------------------------------------------------------------------------------
size_t
XrdFileCache::GetRead( FileAbstraction& fileAbst,
                       void*            buf,
                       off_t            off,
                       size_t           len )
{
  size_t nread;
  long long int key;
  bool found = true;
  off_t readOffset = 0;
  char* pBuf = static_cast<char*>( buf );

  //----------------------------------------------------------------------------
  // While read bigger than block size, break in smaller blocks
  //----------------------------------------------------------------------------
  while ( ( ( off % CacheEntry::GetMaxSize() ) + len ) > CacheEntry::GetMaxSize() ) {
    nread = CacheEntry::GetMaxSize() - ( off % CacheEntry::GetMaxSize() );
    key = fileAbst.GenerateBlockKey( off );
    eos_static_debug( "(1) off=%zu, len=%zu", off, nread );
    found = cache_impl->GetRead( key, pBuf + readOffset, off, nread );

    if ( !found ) {
      return 0;
    }

    off += nread;
    len -= nread;
    readOffset += nread;
  }

  if ( len != 0 ) {
    nread = len;
    key = fileAbst.GenerateBlockKey( off );
    eos_static_debug( "(2) off=%zu, len=%zu", off, nread );
    found = cache_impl->GetRead( key, pBuf + readOffset, off, nread );

    if ( !found ) {
      return 0;
    }

    readOffset += nread;
  }

  return readOffset;
}


//------------------------------------------------------------------------------
// Save piece in cache
//------------------------------------------------------------------------------
size_t
XrdFileCache::PutRead( XrdCl::File*&    file,
                       FileAbstraction& fileAbst,
                       void*            buf,
                       off_t            off,
                       size_t           len )
{
  size_t nread;
  long long int key;
  off_t readOffset = 0;
  char* pBuf = static_cast<char*>( buf );

  //------------------------------------------------------------------------------
  // Read bigger than block size, break in smaller blocks
  //------------------------------------------------------------------------------
  while ( ( ( off % CacheEntry::GetMaxSize() ) + len ) > CacheEntry::GetMaxSize() ) {
    nread = CacheEntry::GetMaxSize() - ( off % CacheEntry::GetMaxSize() );
    key = fileAbst.GenerateBlockKey( off );
    eos_static_debug( "(1) off=%zu, len=%zu key=%lli", off, nread, key );
    cache_impl->AddRead( file, key, pBuf + readOffset, off, nread, fileAbst );
    off += nread;
    len -= nread;
    readOffset += nread;
  }

  if ( len != 0 ) {
    nread = len;
    key = fileAbst.GenerateBlockKey( off );
    eos_static_debug( "(2) off=%zu, len=%zu key=%lli", off, nread, key );
    cache_impl->AddRead( file, key, pBuf + readOffset, off, nread, fileAbst );
    readOffset += nread;
  }

  return readOffset;
}


//------------------------------------------------------------------------------
// Remove file from mapping
//------------------------------------------------------------------------------
bool
XrdFileCache::RemoveFileInode( unsigned long inode, bool strong_constraint )
{
  bool doDeletion = false;
  eos_static_debug( "inode=%lu", inode );
  FileAbstraction* ptr =  NULL;

  rw_lock.WriteLock();                                     //write lock map
  std::map<unsigned long, FileAbstraction*>::iterator iter = inode2fAbst.find( inode );

  if ( iter != inode2fAbst.end() ) {
    ptr = static_cast<FileAbstraction*>( ( *iter ).second );

    if ( strong_constraint ) {
      //------------------------------------------------------------------------
      // Strong constraint
      //------------------------------------------------------------------------
      doDeletion = ( ptr->GetSizeRdWr() == 0 ) && ( ptr->GetNoReferences() == 0 );
    } else {
      //------------------------------------------------------------------------
      // Weak constraint
      //------------------------------------------------------------------------
      doDeletion = ( ptr->GetSizeRdWr() == 0 ) && ( ptr->GetNoReferences() <= 1 );
    }

    if ( doDeletion ) {
      //------------------------------------------------------------------------
      // Remove file from mapping
      //------------------------------------------------------------------------
      int id = ptr->GetId();
      delete ptr;
      inode2fAbst.erase( iter );
      queue_used_indx->push( id );
    }
  }

  rw_lock.UnLock();                                        //unlock map
  return doDeletion;
}


//------------------------------------------------------------------------------
// Get errors queue
//------------------------------------------------------------------------------
ConcurrentQueue<error_type>&
XrdFileCache::GetErrorQueue( unsigned long inode )
{
  ConcurrentQueue<error_type>* tmp = NULL;
  FileAbstraction* pFileAbst = GetFileObj( inode, false );

  if ( pFileAbst ) {
    *tmp = pFileAbst->GetErrorQueue();
    pFileAbst->DecrementNoReferences();
  }

  return *tmp;
}

//------------------------------------------------------------------------------
// Method used to wait for the writes corresponding to a file to be commited.
// It also forces the incompletele (not full) write blocks from cache to be
// added to the writes queue and implicitly to be written to the file.
//------------------------------------------------------------------------------
void
XrdFileCache::WaitFinishWrites( FileAbstraction& fileAbst )
{
  if ( fileAbst.GetSizeWrites() != 0 ) {
    cache_impl->FlushWrites( fileAbst );
    fileAbst.WaitFinishWrites();

    if ( !fileAbst.IsInUse( false ) ) {
      RemoveFileInode( fileAbst.GetInode(), false );
    }
  }
}


//------------------------------------------------------------------------------
// Method used to wait for the writes corresponding to a file to be commited.
// It also forces the incompletele (not full) write blocks from cache to be
// added to the writes queue and implicitly to be written to the file.
//------------------------------------------------------------------------------
void
XrdFileCache::WaitFinishWrites( unsigned long inode )
{
  FileAbstraction* pFileAbst = GetFileObj( inode, false );

  if ( pFileAbst && ( pFileAbst->GetSizeWrites() != 0 ) ) {
    cache_impl->FlushWrites( *pFileAbst );
    pFileAbst->WaitFinishWrites();

    if ( !pFileAbst->IsInUse( false ) ) {
      if ( RemoveFileInode( pFileAbst->GetInode(), false ) ) {
        return;
      }
    }
  }

  if ( pFileAbst ) {
    pFileAbst->DecrementNoReferences();
  }

  return;
}


/*
//------------------------------------------------------------------------------
size_t
XrdFileCache::getReadV(unsigned long inode, int filed, void* buf,
off_t* offset, size_t* length, int nbuf)
{
size_t ret = 0;
char* ptrBuf = static_cast<char*>(buf);
long long int key;
CacheEntry* pEntry = NULL;
FileAbstraction* pFileAbst = getFileObj(inode, true);

for (int i = 0; i < nbuf; i++) {
key = pFileAbst->GenerateBlockKey(offset[i]);
pEntry = cache_impl->getRead(key, pFileAbst);

if (pEntry && (pEntry->GetLength() == length[i])) {
ptrBuf = (char*)memcpy(ptrBuf, pEntry->GetDataBuffer(), pEntry->GetLength());
ptrBuf += length[i];
ret += length[i];
} else break;
}

return ret;
}


//------------------------------------------------------------------------------
void
XrdFileCache::putReadV(unsigned long inode, int filed, void* buf,
off_t* offset, size_t* length, int nbuf)
{
char* ptrBuf = static_cast<char*>(buf);
long long int key;
CacheEntry* pEntry = NULL;
FileAbstraction* pFileAbst = getFileObj(inode, true);

for (int i = 0; i < nbuf; i++) {
pEntry = cache_impl->getRecycledBlock(filed, ptrBuf, offset[i], length[i], pFileAbst);
key = pFileAbst->GenerateBlockKey(offset[i]);
cache_impl->Insert(key, pEntry);
ptrBuf += length[i];
}

return;
}
*/
