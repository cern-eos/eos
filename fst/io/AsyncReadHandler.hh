// -----------------------------------------------------------------------------
//! @file AsyncReadHandler.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Class for handling async read responses from xrootd
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
#include <map>
#include <semaphore.h>
/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
/*----------------------------------------------------------------------------*/

#ifndef __EOS_ASYNCREADHANDLER_HH__
#define __EOS_ASYNCREADHANDLER_HH__

EOSFSTNAMESPACE_BEGIN

// -----------------------------------------------------------------------------
//! Class for handling async read responses
// -----------------------------------------------------------------------------
class AsyncReadHandler: public XrdCl::ResponseHandler
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    AsyncReadHandler();

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~AsyncReadHandler();

    //--------------------------------------------------------------------------
    //! Handle response
    //--------------------------------------------------------------------------
    virtual void HandleResponse( XrdCl::XRootDStatus* pStatus,
                                 XrdCl::AnyObject*    pResponse );

    //--------------------------------------------------------------------------
    //! Wait for responses
    //--------------------------------------------------------------------------
    virtual bool WaitOK();

    //--------------------------------------------------------------------------
    //! Get map of errors
    //--------------------------------------------------------------------------
    const std::map<uint64_t, uint32_t>& GetErrorsMap();

    //--------------------------------------------------------------------------
    //! Increment the number fo expected responses
    //--------------------------------------------------------------------------
    void Increment();

    //--------------------------------------------------------------------------
    //! Get number of expected responses
    //--------------------------------------------------------------------------
    const int GetNoResponses() const;

    //--------------------------------------------------------------------------
    //! Reset
    //--------------------------------------------------------------------------
    virtual void Reset();

  private:

    int mNumResponses;                       ///< expected number of responses
    sem_t mSemaphore;                        ///< semaphore used for synchronisations
    std::map<uint64_t, uint32_t> mMapErrors; ///< chunks for which the request failed
};

EOSFSTNAMESPACE_END

#endif  //__EOS_ASYNCREADHANDLER_HH__



