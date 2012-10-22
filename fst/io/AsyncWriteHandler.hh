// -----------------------------------------------------------------------------
// File: AsyncWriteHandler.hh
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
#include "fst/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/

#ifndef __EOS_ASYNCWRITEHANDLER_HH__
#define __EOS_ASYNCWRITEHANDLER_HH__

EOSFSTNAMESPACE_BEGIN

// -----------------------------------------------------------------------------
//! Class for handling async write responses
// -----------------------------------------------------------------------------
class AsyncWriteHandler: public XrdCl::ResponseHandler
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    AsyncWriteHandler();

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~AsyncWriteHandler();

    //--------------------------------------------------------------------------
    //! Handle response
    //--------------------------------------------------------------------------
    void HandleResponse( XrdCl::XRootDStatus* status,
                         XrdCl::AnyObject*    response );

    //--------------------------------------------------------------------------
    // Wait for responses
    //--------------------------------------------------------------------------
    bool WaitOK();

    //--------------------------------------------------------------------------
    //! Increment the number of expected responses
    //--------------------------------------------------------------------------
    void Increment();

    //--------------------------------------------------------------------------
    //! Reset
    //--------------------------------------------------------------------------
    void Reset();

  private:

    bool state;          //< true if all requests are ok, otherwise false
    int nExpectedRes;    //< expected number of responses
    int nReceivedRes;    //< received number of responses
    XrdSysCondVar cond;  //< condition variable to signal the receival of
                         //< all responses
};

EOSFSTNAMESPACE_END

#endif // __EOS_ASYNCWRITEHANDLER_HH__ 



