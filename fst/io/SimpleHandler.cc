//------------------------------------------------------------------------------
// File: SimpleHandler.cc
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
#include "fst/io/SimpleHandler.hh"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
SimpleHandler::SimpleHandler (uint64_t offset,
                              uint32_t length,
                              bool isWrite) :
XrdCl::ResponseHandler (),
mOffset (offset),
mLength (length),
mRespLength (0),
mIsWrite (isWrite),
mRespOK (false),
mReqDone (false),
mHasReq (false)
{
  mCond = XrdSysCondVar(0);
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

SimpleHandler::~SimpleHandler () {
  // emtpy
}


//------------------------------------------------------------------------------
// Update function
//------------------------------------------------------------------------------

void
SimpleHandler::Update (uint64_t offset,
                       uint32_t length,
                       bool isWrite)
{
  mOffset = offset;
  mLength = length;
  mRespLength = 0;
  mIsWrite = isWrite;
  mRespOK = false;
  mReqDone = false;
  mHasReq = true;
}


//------------------------------------------------------------------------------
// Handle response
//------------------------------------------------------------------------------

void
SimpleHandler::HandleResponse (XrdCl::XRootDStatus* pStatus,
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
  }

  mCond.Lock();
  mRespOK = pStatus->IsOK();
  mReqDone = true;
  mCond.Signal(); //signal
  mCond.UnLock();

  delete pStatus;

  if (pResponse)
  {
    delete pResponse;
  }

}


//------------------------------------------------------------------------------
// Wait for responses
//------------------------------------------------------------------------------

bool
SimpleHandler::WaitOK ()
{
  bool req_status = false;

  mCond.Lock();

  if (mReqDone)
  {
    req_status = mRespOK;
  }
  else
  {
    mCond.Wait();
    req_status = mRespOK;
  }

  mHasReq = false;
  mCond.UnLock();

  return req_status;
}


//------------------------------------------------------------------------------
//! Get if there is any request to process
//------------------------------------------------------------------------------

bool
SimpleHandler::HasRequest ()
{
  bool ret = false;

  mCond.Lock();
  ret = mHasReq;
  mCond.UnLock();

  return ret;
}


EOSFSTNAMESPACE_END
