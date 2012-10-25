//------------------------------------------------------------------------------
// File: AsyncMetaHandler.cc
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

/*----------------------------------------------------------------------------*/
#include "fst/io/ChunkHandler.hh"
#include "fst/io/AsyncMetaHandler.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
AsyncMetaHandler::AsyncMetaHandler():
  mState( true ),
  mNumExpectedResp( 0 ),
  mNumReceivedResp( 0 )
{
  mCond = XrdSysCondVar( 0 );
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
AsyncMetaHandler::~AsyncMetaHandler()
{
  ChunkHandler* ptr_chunk = NULL;
  
  while ( !listReq.empty() ) {
    ptr_chunk = listReq.back();
    listReq.pop_back();
    delete ptr_chunk;
  }

  while ( !listCache.empty() ) {
    ptr_chunk = listCache.back();
    listCache.pop_back();
    delete ptr_chunk;
  }
}


//------------------------------------------------------------------------------
// Register a new handler for the current file
//------------------------------------------------------------------------------
ChunkHandler*
AsyncMetaHandler::Register( uint64_t offset,
                            uint32_t length,
                            bool     isWrite )
{
  ChunkHandler* ptr_chunk = NULL;
  
  mCond.Lock();          // --> 
  if ( listCache.size() ) {
    ptr_chunk = listCache.back();
    listCache.pop_back();
  }

  if ( !ptr_chunk ) {
    ptr_chunk = new ChunkHandler( this, offset, length, isWrite );
  }
  else {
    ptr_chunk->Update( this, offset, length, isWrite );
  }

  listReq.push_back( ptr_chunk );
  mNumExpectedResp++;
  mCond.UnLock();        // <--
  
  return ptr_chunk;
}


//------------------------------------------------------------------------------
// Handle response
//------------------------------------------------------------------------------
void
AsyncMetaHandler::HandleResponse( XrdCl::XRootDStatus* pStatus,
                                  ChunkHandler*        chunk )
{
  mCond.Lock();
  mNumReceivedResp++;

  if ( pStatus->status != XrdCl::stOK )
  {
    mMapErrors.insert( std::make_pair( chunk->GetOffset(), chunk->GetLength() ) );
    mState = false;
  }

  if ( mNumReceivedResp == mNumExpectedResp ) {
    mCond.Signal();
  }

  mCond.UnLock();
}


//------------------------------------------------------------------------------
// Get map of errors
//------------------------------------------------------------------------------
const std::map<uint64_t, uint32_t>&
AsyncMetaHandler::GetErrorsMap()
{
  return mMapErrors;
}


//------------------------------------------------------------------------------
// Wait for responses
//------------------------------------------------------------------------------
bool
AsyncMetaHandler::WaitOK()
{
  mCond.Lock();

  if ( mNumReceivedResp == mNumExpectedResp ) {
    mCond.UnLock();
    return mState;
  }

  mCond.Wait();
  mCond.UnLock();
  return mState;
}


//------------------------------------------------------------------------------
// Reset
//------------------------------------------------------------------------------
void
AsyncMetaHandler::Reset()
{
  ChunkHandler* ptr_chunk = NULL;

  mCond.Lock();          // -->
  mState = true;
  mNumExpectedResp = 0;
  mNumReceivedResp = 0;
  mMapErrors.clear();  

  while ( !listReq.empty() ) {
    ptr_chunk = listReq.back();
    listReq.pop_back();
    if ( listCache.size() < msMaxCacheSize ) {
      listCache.push_back( ptr_chunk );
    }
    else {
      delete ptr_chunk;
    }
  }
  mCond.UnLock();        // <--
}


EOSFSTNAMESPACE_END
