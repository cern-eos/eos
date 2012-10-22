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
  nResponses( 0 )
{
  if ( sem_init( &semaphore, 0, 0 ) ) {
    fprintf( stderr, "Error while creating semaphore. \n" );
    return;
  }
}


// -----------------------------------------------------------------------------
// Destructor
// -----------------------------------------------------------------------------
AsyncReadHandler::~AsyncReadHandler()
{
  sem_destroy( &semaphore );
}


// -----------------------------------------------------------------------------
// Handle response
// -----------------------------------------------------------------------------
void AsyncReadHandler::HandleResponse( XrdCl::XRootDStatus* status,
                                       XrdCl::AnyObject*    response )
{
  if ( status->status == XrdCl::stOK ) {
    sem_post( &semaphore );
  } else {
    XrdCl::Chunk* chunk = 0;
    response->Get( chunk );
    mapErrors.insert( std::make_pair( chunk->offset, chunk->length ) );
    sem_post( &semaphore );
  }
}


// -----------------------------------------------------------------------------
// Wait for responses
// -----------------------------------------------------------------------------
bool AsyncReadHandler::WaitOK()
{
  for ( int i = 0; i < nResponses; i++ ) {
    sem_wait( &semaphore );
  }

  if ( mapErrors.empty() ) {
    return true;
  }

  return false;
}


// -----------------------------------------------------------------------------
// Get map of errors
// -----------------------------------------------------------------------------
const std::map<uint64_t, uint32_t>& AsyncReadHandler::GetErrorsMap()
{
  return mapErrors;
}


// -----------------------------------------------------------------------------
// Increment the number of expected responses
// -----------------------------------------------------------------------------
void AsyncReadHandler::Increment()
{
  nResponses++;
}


// -----------------------------------------------------------------------------
// Get number of expected responses
// -----------------------------------------------------------------------------
const int AsyncReadHandler::GetNoResponses() const
{
  return nResponses;
}


// -----------------------------------------------------------------------------
// Reset
// -----------------------------------------------------------------------------
void AsyncReadHandler::Reset()
{
  nResponses = 0;
  mapErrors.clear();
}


EOSFSTNAMESPACE_END
