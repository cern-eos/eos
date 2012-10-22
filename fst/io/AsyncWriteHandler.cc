// -----------------------------------------------------------------------------
// File: AsyncWriteHandler.cc
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
#include "AsyncWriteHandler.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

// -----------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------
AsyncWriteHandler::AsyncWriteHandler():
  state( true ),
  nExpectedRes( 0 ),
  nReceivedRes( 0 )
{
  cond = XrdSysCondVar( 0 );
}


// -----------------------------------------------------------------------------
// Destructor
// -----------------------------------------------------------------------------
AsyncWriteHandler::~AsyncWriteHandler()
{
  //empty
}


// -----------------------------------------------------------------------------
// Handle response
// -----------------------------------------------------------------------------
void AsyncWriteHandler::HandleResponse( XrdCl::XRootDStatus* status,
                                        XrdCl::AnyObject*    response )
{
  cond.Lock();
  nReceivedRes++;

  if ( status->status != XrdCl::stOK ) {
    state = false;
  }

  if ( nReceivedRes == nExpectedRes ) {
    cond.Signal();
  }

  cond.UnLock();
}


// -----------------------------------------------------------------------------
// Wait for responses
// -----------------------------------------------------------------------------
bool AsyncWriteHandler::WaitOK()
{
  cond.Lock();

  if ( nReceivedRes == nExpectedRes ) {
    cond.UnLock();
    return state;
  }

  cond.Wait();
  cond.UnLock();

  return state;
}


// -----------------------------------------------------------------------------
// Increment the number of expected responses
// -----------------------------------------------------------------------------
void AsyncWriteHandler::Increment()
{
  cond.Lock();
  nExpectedRes++;
  cond.UnLock();
}


// -----------------------------------------------------------------------------
// Reset
// -----------------------------------------------------------------------------
void AsyncWriteHandler::Reset()
{
  state = true;
  nExpectedRes = 0;
  nReceivedRes = 0;
}


EOSFSTNAMESPACE_END
