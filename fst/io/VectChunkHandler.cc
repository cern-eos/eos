//------------------------------------------------------------------------------
// File: VectChunkHandler.cc
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
#include "fst/io/VectChunkHandler.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
VectChunkHandler::VectChunkHandler (AsyncMetaHandler* metaHandler,
                                    XrdCl::ChunkList& chunks,
                                    const char* wrBuf,
                                    bool isWrite) :
XrdCl::ResponseHandler(),
mBuffer(0),
mMetaHandler (metaHandler),
mCapacity (0),
mLength (0),
mRespLength (0),
mIsWrite (isWrite)
{
  // Copy the list of chunks and compute buffer size
  for (auto chunk = chunks.begin(); chunk != chunks.end(); ++chunk)
  {
    mLength += chunk->length;
    mChunkList.push_back(*chunk);
  }

  mCapacity = mLength;

  /*
  NOTE: Vector writes are not supported yet
  if (mIsWrite)
  {
    // Copy the write buffer to the local one
    mBuffer = static_cast<char*>(calloc(mCapacity, sizeof(char)));
    
    if (mBuffer)
      mBuffer = static_cast<char*>(memcpy(mBuffer, wrBuf, mLength));
  }
  */
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
VectChunkHandler::~VectChunkHandler ()
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
VectChunkHandler::Update (AsyncMetaHandler* metaHandler,
                          XrdCl::ChunkList& chunks,
                          const char* wrBuf,
                          bool isWrite)
{
  mMetaHandler = metaHandler;
  mRespLength = 0;
  mLength = 0;
  mIsWrite = isWrite;
  
  // Copy the list of chunks and compute buffer size
  for (auto chunk = chunks.begin(); chunk != chunks.end(); ++chunk)
  {
    mLength += chunk->length;
    mChunkList.push_back(*chunk);
  }

  /*
  NOTE: vector writes are not supported yet
  if (mIsWrite)
  {
    if (mLength > mCapacity)
    {
      mCapacity = mLength;
      mBuffer = static_cast<char*>(realloc(mBuffer, mCapacity));
    }

    mBuffer = static_cast<char*>(memcpy(mBuffer, wrBuf, mLength));
  }
  */
}


//------------------------------------------------------------------------------
// Handle response
//------------------------------------------------------------------------------
void
VectChunkHandler::HandleResponse (XrdCl::XRootDStatus* pStatus,
                                  XrdCl::AnyObject* pResponse)
{
  // Do some extra check for the read case
  if ((mIsWrite == false) && (pResponse))
  {
    XrdCl::VectorReadInfo* vrd_info = 0;
    pResponse->Get(vrd_info);
    mRespLength = vrd_info->GetSize();

    // Notice if we receive less then we initially requested - for readv it
    // means there was an error
    if (mLength != mRespLength)
    {
      pStatus->status = XrdCl::stError;
      pStatus->code = XrdCl::errErrorResponse;
    }
  }

  if (pResponse)
  {
    delete pResponse;
  }
   
  mMetaHandler->HandleResponse(pStatus, this);
  delete pStatus;
}

EOSFSTNAMESPACE_END

