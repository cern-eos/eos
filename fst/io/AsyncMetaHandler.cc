//------------------------------------------------------------------------------
// File: AsyncMetaHandler.cc
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch> - CERN
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
#include "fst/io/VectChunkHandler.hh"
#include "fst/io/AsyncMetaHandler.hh"
/*----------------------------------------------------------------------------*/


EOSFSTNAMESPACE_BEGIN

///! maximum number of obj in cache used for recycling
const unsigned int AsyncMetaHandler::msMaxNumAsyncObj = 20;


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
AsyncMetaHandler::AsyncMetaHandler () :
eos::common::LogId(),
mErrorType (XrdCl::errNone),
mAsyncReq (0),
mAsyncVReq (0),
mChunkToDelete(NULL),
mVChunkToDelete(NULL)
{
  mCond = XrdSysCondVar(0);
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
AsyncMetaHandler::~AsyncMetaHandler ()
{
  // Drop all chunks handlers
  ChunkHandler* ptr_chunk = NULL;

  while (!mQRecycle.empty())
  {
    if (mQRecycle.try_pop(ptr_chunk))
    {
      delete ptr_chunk;
      ptr_chunk = 0;
    }
  }

  // Drop all vector handlers
  VectChunkHandler* ptr_vchunk = NULL;

  while (!mQVRecycle.empty())
  {
    if (mQVRecycle.try_pop(ptr_vchunk))
    {
      delete ptr_vchunk;
      ptr_vchunk = 0;
    }
  }

  
  if (mChunkToDelete)
  {
    delete mChunkToDelete;
    mChunkToDelete = 0;
  }

  if (mVChunkToDelete)
  {
    delete mVChunkToDelete;
    mVChunkToDelete = 0;
  }

  mMapErrors.clear();
}


//------------------------------------------------------------------------------
// Register a new handler for the current file
//------------------------------------------------------------------------------
ChunkHandler*
AsyncMetaHandler::Register (uint64_t offset,
                            uint32_t length,
                            const char* buffer,
                            bool isWrite)
{
  ChunkHandler* ptr_chunk = NULL;
  mCond.Lock();  // -->

  // If any of the the previous requests failed with a timeout then stop trying
  // and return an error
  if (mErrorType == XrdCl::errOperationExpired)
  {
    mCond.UnLock(); // <--
    return NULL;
  }

  mAsyncReq++;
  
  if (mQRecycle.size() + mAsyncReq >= msMaxNumAsyncObj)
  {
    mCond.UnLock();   // <--    
    mQRecycle.wait_pop(ptr_chunk);
    ptr_chunk->Update(this, offset, length, buffer, isWrite);
  }
  else
  {
   // Create new request
    mCond.UnLock();   // <--            
    ptr_chunk = new ChunkHandler(this, offset, length, buffer, isWrite);
  }
  
  return ptr_chunk;
}


//--------------------------------------------------------------------------
//! Register a new vector request for the current file
//--------------------------------------------------------------------------
VectChunkHandler*
AsyncMetaHandler::Register (XrdCl::ChunkList& chunks,
                            const char* wrBuf,
                            bool isWrite)
{
  VectChunkHandler* ptr_vchunk = NULL;
  mCond.Lock();  // -->

  // If any of the the previous requests failed with a timeout then stop trying
  // and return an error
  if (mErrorType == XrdCl::errOperationExpired)
  {
    mCond.UnLock(); // <--
    return NULL;
  }

  mAsyncVReq++;
  
  if (mQVRecycle.size() + mAsyncVReq >= msMaxNumAsyncObj)
  {
    mCond.UnLock();   // <--    
    mQVRecycle.wait_pop(ptr_vchunk);
    ptr_vchunk->Update(this, chunks, wrBuf, isWrite);
  }
  else
  {
   // Create new request
    mCond.UnLock();   // <--            
    ptr_vchunk = new VectChunkHandler(this, chunks, wrBuf, isWrite);
  }
  
  return ptr_vchunk;
}


//------------------------------------------------------------------------------
// Handle response
//------------------------------------------------------------------------------
void
AsyncMetaHandler::HandleResponse (XrdCl::XRootDStatus* pStatus,
                                  ChunkHandler* chunk)
{
  mCond.Lock(); // -->

  // See last comment for motivation
  if (mChunkToDelete)
  {
    delete mChunkToDelete;
    mChunkToDelete = NULL;
  }

  if (pStatus->status != XrdCl::stOK)
  {
    eos_debug("Got error message with status:%u, code:%u, errNo:%lu",
              pStatus->status, pStatus->code, (unsigned long)pStatus->errNo);
    mMapErrors.insert(std::make_pair(chunk->GetOffset(), chunk->GetLength()));

    // If we got a timeout in the previous requests then we keep the error code
    if (mErrorType != XrdCl::errOperationExpired)
    {
      mErrorType = pStatus->code;

      if (mErrorType == XrdCl::errOperationExpired)
      {
        eos_debug("Got a timeout error for request off=%zu, len=%lu",
                  chunk->GetOffset(), (unsigned long)chunk->GetLength());
      }    
    }
  }

  if (--mAsyncReq == 0)
    mCond.Signal();
  
  if (!mQRecycle.push_size(chunk, msMaxNumAsyncObj))
  {
    // Save the pointer to the chunk object to be deleted by the next arriving
    // response. This can not be done here as we are currently called from the
    // same chunk handler object that we want to delete. 
    mChunkToDelete = chunk;
  }

  mCond.UnLock();  // <--
}


//--------------------------------------------------------------------------
//! Handle response vector response
//--------------------------------------------------------------------------
void
AsyncMetaHandler::HandleResponse (XrdCl::XRootDStatus* pStatus,
                                  VectChunkHandler* vchunk)
{

    mCond.Lock(); // -->

  // See last comment for motivation
  if (mVChunkToDelete)
  {
    delete mVChunkToDelete;
    mVChunkToDelete = NULL;
  }

  if (pStatus->status != XrdCl::stOK)
  {
    //eos_debug("Got error message with status:%u, code:%u, errNo:%lu",
    //          pStatus->status, pStatus->code, (unsigned long)pStatus->errNo);
    
    //mMapErrors.insert(std::make_pair(chunk->GetOffset(), chunk->GetLength()));

    // If we got a timeout in the previous requests then we keep the error code
    if (mErrorType != XrdCl::errOperationExpired)
    {
      mErrorType = pStatus->code;

      if (mErrorType == XrdCl::errOperationExpired)
      {
        // eos_debug("Got a timeout error for request off=%zu, len=%lu",
        //          chunk->GetOffset(), (unsigned long)chunk->GetLength());
      }    
    }
  }

  if (--mAsyncVReq == 0)
   mCond.Signal();

  if (!mQVRecycle.push_size(vchunk, msMaxNumAsyncObj))
  {
    // Save the pointer to the chunk object to be deleted by the next arriving
    // response. This can not be done here as we are currently called from the
    // same chunk handler object that we want to delete. 
    mVChunkToDelete = vchunk;
  }

  mCond.UnLock();  // <--
}


//------------------------------------------------------------------------------
// Get map of errors
//------------------------------------------------------------------------------
const std::map<uint64_t, uint32_t>&
AsyncMetaHandler::GetErrors ()
{
  return mMapErrors;
}


//------------------------------------------------------------------------------
// Wait for responses
//------------------------------------------------------------------------------
uint16_t
AsyncMetaHandler::WaitOK ()
{
  uint16_t ret = XrdCl::errNone;
  mCond.Lock();   // -->
  
  while (mAsyncReq > 0)
    mCond.Wait();

  while (mAsyncVReq > 0)
    mCond.Wait();

  ret = mErrorType;
  mCond.UnLock(); // <--
  return ret;
}


//------------------------------------------------------------------------------
// Reset
//------------------------------------------------------------------------------
void
AsyncMetaHandler::Reset ()
{
  mCond.Lock();
  mErrorType = XrdCl::errNone;
  mAsyncReq = 0;
  mAsyncVReq = 0;
  mMapErrors.clear();
  mCond.UnLock();
}


EOSFSTNAMESPACE_END
