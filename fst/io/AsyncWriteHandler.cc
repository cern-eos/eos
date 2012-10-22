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
  mState( true ),
  mNumExpectedResp( 0 ),
  mNumReceivedResp( 0 )
{
  mCond = XrdSysCondVar( 0 );
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
void
AsyncWriteHandler::HandleResponse( XrdCl::XRootDStatus* pStatus,
                                   XrdCl::AnyObject*    pResponse )
{
  mCond.Lock();
  mNumReceivedResp++;

  if ( pStatus->status != XrdCl::stOK ) {
    mState = false;
  }

  if ( mNumReceivedResp == mNumExpectedResp ) {
    mCond.Signal();
  }

  mCond.UnLock();
}


// -----------------------------------------------------------------------------
// Wait for responses
// -----------------------------------------------------------------------------
bool
AsyncWriteHandler::WaitOK()
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


// -----------------------------------------------------------------------------
// Increment the number of expected responses
// -----------------------------------------------------------------------------
void
AsyncWriteHandler::Increment()
{
  mCond.Lock();
  mNumExpectedResp++;
  mCond.UnLock();
}


// -----------------------------------------------------------------------------
// Reset
// -----------------------------------------------------------------------------
void
AsyncWriteHandler::Reset()
{
  mState = true;
  mNumExpectedResp = 0;
  mNumReceivedResp = 0;
}


EOSFSTNAMESPACE_END
