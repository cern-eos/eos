// -----------------------------------------------------------------------------
// File: AsyncReadHandler.cc
// Author: Elvin-Alin Sindrilaru - CERN
// -----------------------------------------------------------------------------

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
#include "AsyncReadHandler.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

// -----------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------
AsyncReadHandler::AsyncReadHandler():
  mNumResponses( 0 )
{
  if ( sem_init( &mSemaphore, 0, 0 ) ) {
    fprintf( stderr, "Error while creating semaphore. \n" );
    return;
  }
}


// -----------------------------------------------------------------------------
// Destructor
// -----------------------------------------------------------------------------
AsyncReadHandler::~AsyncReadHandler()
{
  sem_destroy( &mSemaphore );
}


// -----------------------------------------------------------------------------
// Handle response
// -----------------------------------------------------------------------------
void
AsyncReadHandler::HandleResponse( XrdCl::XRootDStatus* pStatus,
                                  XrdCl::AnyObject*    pResponse )
{
  if ( pStatus->status == XrdCl::stOK ) {
    sem_post( &mSemaphore );
  } else {
    XrdCl::Chunk* chunk = 0;
    pResponse->Get( chunk );
    mMapErrors.insert( std::make_pair( chunk->offset, chunk->length ) );
    sem_post( &mSemaphore );
  }
}


// -----------------------------------------------------------------------------
// Wait for responses
// -----------------------------------------------------------------------------
bool
AsyncReadHandler::WaitOK()
{
  for ( int i = 0; i < mNumResponses; i++ ) {
    sem_wait( &mSemaphore );
  }

  if ( mMapErrors.empty() ) {
    return true;
  }

  return false;
}


// -----------------------------------------------------------------------------
// Get map of errors
// -----------------------------------------------------------------------------
const std::map<uint64_t, uint32_t>&
AsyncReadHandler::GetErrorsMap()
{
  return mMapErrors;
}


// -----------------------------------------------------------------------------
// Increment the number of expected responses
// -----------------------------------------------------------------------------
void
AsyncReadHandler::Increment()
{
  mNumResponses++;
}


// -----------------------------------------------------------------------------
// Get number of expected responses
// -----------------------------------------------------------------------------
const int
AsyncReadHandler::GetNoResponses() const
{
  return mNumResponses;
}


// -----------------------------------------------------------------------------
// Reset
// -----------------------------------------------------------------------------
void
AsyncReadHandler::Reset()
{
  mNumResponses = 0;
  mMapErrors.clear();
}


EOSFSTNAMESPACE_END
