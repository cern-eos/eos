// ----------------------------------------------------------------------
// File: AsyncRespHandler.hh
// Author: Elvin-Alin Sindrilaru - CERN
// ----------------------------------------------------------------------

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
#include <typeinfo>
/*----------------------------------------------------------------------------*/
#include <semaphore.h>
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClXRootDResponses.hh"
/*----------------------------------------------------------------------------*/

#ifndef __EOS_ASYNCRESPONSEHANDLER_HH__
#define __EOS_ASYNCRESPONSEHANDLER_HH__

EOSFSTNAMESPACE_BEGIN

//----------------------------------------------------------------------------
//! Handle an async response
//----------------------------------------------------------------------------
class AsyncRespHandler: public XrdCl::ResponseHandler
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  AsyncRespHandler() {
    if ( sem_init( &semaphore, 0, 0 ) ) {
      fprintf( stderr, "Error while creating semaphore. \n" );
      return;
    }
  };


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~AsyncRespHandler() {
    sem_destroy( &semaphore );
  }

  virtual void HandleResponse( XrdCl::XRootDStatus* status,
                               XrdCl::AnyObject*    response ) {
    XrdCl::Chunk* chunk = 0;
    response->Get( chunk );

    if ( status->status == XrdCl::stOK ) {
      sem_post( &semaphore );
    }
  };

  virtual void Wait( int nReq ) {
    for ( int i = 0; i < nReq; i++ ) {
      sem_wait( &semaphore );
    }
  };

private:
  sem_t semaphore;
};

EOSFSTNAMESPACE_END

#endif



