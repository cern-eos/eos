//------------------------------------------------------------------------------
//! @file SyncResponseHandler.hh
//! @author Andreas-Joachim Peters
//! @brief remote IO filesystem implementation
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

#ifndef FUSE_SYNCRESPONSEHANDLER_HH_
#define FUSE_SYNCRESPONSEHANDLER_HH_

#include "XrdCl/XrdClXRootDResponses.hh"

class SyncResponseHandler : public XrdCl::ResponseHandler
{
public:
  //------------------------------------------------------------------------                                                                                                                                     
  //! Constructor                                                                                                                                                                                                
  //------------------------------------------------------------------------                                                                                                                                     
  SyncResponseHandler():
    pStatus(0),
    pResponse(0),
    pSem( new XrdSysSemaphore(0) ) {}

  //------------------------------------------------------------------------                                                                                                                                     
  //! Destructor                                                                                                                                                                                                 
  //------------------------------------------------------------------------                                                                                                                                     
  virtual ~SyncResponseHandler()
  {
    if (pResponse)
      delete pResponse;
    delete pSem;
  }

  //------------------------------------------------------------------------                                                                                                                                     
  //! Handle the response                                                                                                                                                                                        
  //------------------------------------------------------------------------                                                                                                                                     
  virtual void HandleResponse( XrdCl::XRootDStatus *status,
			       XrdCl::AnyObject    *response )
  {
    pStatus = status;
    pResponse = response;
    pSem->Post();
  }

  //------------------------------------------------------------------------                                                                                                                                     
  //! Get the status                                                                                                                                                                                             
  //------------------------------------------------------------------------                                                                                                                                     
  XrdCl::XRootDStatus *GetStatus()
  {
    return pStatus;
  }

  //------------------------------------------------------------------------                                                                                                                                     
  //! Get the response                                                                                                                                                                                           
  //------------------------------------------------------------------------                                                                                                                                     
  XrdCl::AnyObject *GetResponse()
  {
    return pResponse;
  }

  //------------------------------------------------------------------------                                                                                                                                     
  //! Wait for the arrival of the response                                                                                                                                                                       
  //------------------------------------------------------------------------                                                                                                                                     
  void WaitForResponse()
  {
    pSem->Wait();
  }

  template<class Type>
    XrdCl::XRootDStatus Sync(Type *&response)
  {
    WaitForResponse();

    XrdCl::AnyObject    *resp   = GetResponse();
    XrdCl::XRootDStatus *status = GetStatus();
    XrdCl::XRootDStatus ret( *status );
    delete status;

    if( ret.IsOK() )
      {
	if( !resp )
	  return XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errInternal );
	resp->Get( response );
	resp->Set( (int *)0 );
	if( !response )
	  {
	    return XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errInternal );
	  }
      }

    return ret;
  }

 private:
  XrdCl::XRootDStatus    *pStatus;
  XrdCl::AnyObject       *pResponse;
  XrdSysSemaphore *pSem;
};
#endif
