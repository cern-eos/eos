//------------------------------------------------------------------------------
// File: ChunkHandler.cc
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

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ChunkHandler::ChunkHandler (AsyncMetaHandler* metaHandler,
                            uint64_t offset,
                            uint32_t length,
                            const char* buff,
                            bool isWrite) :
mBuffer(0),
mMetaHandler (metaHandler),
mOffset (offset),
mLength (length),
mCapacity (length),
mRespLength (0),
mIsWrite (isWrite),
mErrorNo (0)
{
  if (isWrite)
  {
    mBuffer = static_cast<char*>(calloc(length, sizeof(char)));
    if (mBuffer)
    {
      mBuffer = static_cast<char*>(memcpy(mBuffer, buff, length));
    }
  }
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

ChunkHandler::~ChunkHandler ()
{
  if (mBuffer)
  {
    free(mBuffer);
  }
}


//------------------------------------------------------------------------------
// Update function
//------------------------------------------------------------------------------

void
ChunkHandler::Update (AsyncMetaHandler* metaHandler,
                      uint64_t offset,
                      uint32_t length,
                      const char* buff,
                      bool isWrite)
{
  mMetaHandler = metaHandler;
  mOffset = offset;
  mLength = length;
  mRespLength = 0;
  mIsWrite = isWrite;
  mErrorNo = 0;

  if (isWrite)
  {
    if (length > mCapacity)
    {
      mCapacity = length;
      mBuffer = static_cast<char*>(realloc(mBuffer, mCapacity));
    }
    
    mBuffer = static_cast<char*>(memcpy(mBuffer, buff, length));
  }
}


//------------------------------------------------------------------------------
// Handle response
//------------------------------------------------------------------------------

void
ChunkHandler::HandleResponse (XrdCl::XRootDStatus* pStatus,
                              XrdCl::AnyObject* pResponse)
{
  //............................................................................
  // Do some extra check for the read case
  //............................................................................
  if ((mIsWrite == false) && (pResponse))
  {
    XrdCl::ChunkInfo* chunk = 0;
    pResponse->Get(chunk);
    mRespLength = chunk->length;

    //..........................................................................
    // Notice if we received less then we initially requested - usually this means
    // we reached the end of the file, but we will treat it as an error
    //..........................................................................
    if (mLength != chunk->length)
    {
      pStatus->status = XrdCl::stError;
      pStatus->errNo = EFAULT;
      mErrorNo = EFAULT;
    }
  }

  mMetaHandler->HandleResponse(pStatus, this);
  delete pStatus;

  if (pResponse)
  {
    delete pResponse;
  }
}

EOSFSTNAMESPACE_END
