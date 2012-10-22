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
#include <map>
/*----------------------------------------------------------------------------*/
#include <semaphore.h>
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/

#ifndef __EOS_ASYNCRESPONSEHANDLER_HH__
#define __EOS_ASYNCRESPONSEHANDLER_HH__

EOSFSTNAMESPACE_BEGIN

//----------------------------------------------------------------------------
//! Class for handling async responses
//----------------------------------------------------------------------------
class AsyncRespHandler: public XrdCl::ResponseHandler
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  AsyncRespHandler() {
    nResponses = 0;
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

  //----------------------------------------------------------------------------
  //! Handle response 
  //----------------------------------------------------------------------------
  virtual void HandleResponse( XrdCl::XRootDStatus* status,
                               XrdCl::AnyObject*    response ) {
    XrdCl::Chunk* chunk = 0;

    if ( status->status == XrdCl::stOK ) {
      //fprintf( stdout, "Response kXR_ok.\n");
      response->Get( chunk );
      sem_post( &semaphore );
    }
    else {
      response->Get( chunk );
      //fprintf( stdout, "Response kXR_err at offset = %zu, lenght = %u. \n",
      //         chunk->offset, chunk->length );
      mapErrors.insert( std::make_pair( chunk->offset, chunk->length ) );
      sem_post( &semaphore );
    }
  };

  
  //----------------------------------------------------------------------------
  //! Wait for responses
  //----------------------------------------------------------------------------
  virtual bool WaitOK() {
    int value;
    for ( int i = 0; i < nResponses; i++ ) {
      sem_getvalue(&semaphore, &value);
      sem_wait( &semaphore );
    }

    //fprintf( stdout, "[%s] Got %i responses and error status is: %i. \n",
    //         __FUNCTION__, nResponses, mapErrors.empty() );
    
    if ( mapErrors.empty() ) {
      return true;
    }

    return false;
  };
  
  //----------------------------------------------------------------------------
  //! Get map of errors
  //----------------------------------------------------------------------------
  std::map<uint64_t, uint32_t>& GetErrorsMap() {
    return mapErrors;
  };


  //----------------------------------------------------------------------------
  //! Increment the number fo expected responses
  //----------------------------------------------------------------------------
  virtual void Increment() {
    nResponses++;
  }


  //----------------------------------------------------------------------------
  //! GetNoRes
  //----------------------------------------------------------------------------
  virtual int GetNoResponses() {
    return nResponses;
  }

  
  //----------------------------------------------------------------------------
  //! Reset
  //----------------------------------------------------------------------------
  virtual void Reset() {
    mapErrors.clear();
    nResponses = 0;
  };

private:

  int nResponses;                            //! expected number of responses
  sem_t semaphore;                           //! semaphore used for synchronising
  std::map<uint64_t, uint32_t> mapErrors;    //! chunks for with the request failed
};

EOSFSTNAMESPACE_END

#endif



