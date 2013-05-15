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

///! maximum number of obj in cache used for recycling
const unsigned int AsyncMetaHandler::msMaxNumAsyncObj = 10;


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------

AsyncMetaHandler::AsyncMetaHandler () :
mState (true),
mAsyncReq (0)
{
  mCond = XrdSysCondVar(0);
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

AsyncMetaHandler::~AsyncMetaHandler ()
{
  ChunkHandler* ptr_chunk = NULL;

  while (mQRecycle.try_pop(ptr_chunk))
  {
    delete ptr_chunk;
  }
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
  mAsyncReq++;
 
  if (mAsyncReq >= msMaxNumAsyncObj)
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


//------------------------------------------------------------------------------
// Handle response
//------------------------------------------------------------------------------

void
AsyncMetaHandler::HandleResponse (XrdCl::XRootDStatus* pStatus,
                                  ChunkHandler* chunk)
{
  mCond.Lock(); // -->

  if (pStatus->status != XrdCl::stOK)
  {
    mMapErrors.insert(std::make_pair(chunk->GetOffset(), chunk->GetLength()));
    mState = false;
  }

  if (--mAsyncReq == 0)
  {
    mCond.Signal();
  }

  mCond.UnLock();  // <--
  
  if (!mQRecycle.push_size(chunk, msMaxNumAsyncObj))
  {
    delete chunk;
  }
}


//------------------------------------------------------------------------------
// Get map of errors
//------------------------------------------------------------------------------

const std::map<uint64_t, uint32_t>&
AsyncMetaHandler::GetErrorsMap ()
{
  return mMapErrors;
}


//------------------------------------------------------------------------------
// Wait for responses
//------------------------------------------------------------------------------

bool
AsyncMetaHandler::WaitOK ()
{
  mCond.Lock();   // -->
  while (mAsyncReq)
  {
    mCond.Wait();
  }
  mCond.UnLock(); // <--

  return mState;
}


//------------------------------------------------------------------------------
// Reset
//------------------------------------------------------------------------------

void
AsyncMetaHandler::Reset ()
{
  mCond.Lock();
  mState = true;
  mAsyncReq = 0;
  mMapErrors.clear();
  mCond.UnLock();
}


EOSFSTNAMESPACE_END
