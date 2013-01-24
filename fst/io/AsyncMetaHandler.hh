//------------------------------------------------------------------------------
// File: AsyncMetaHandler.hh
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

//------------------------------------------------------------------------------
//! @file AsyncMetaHandler.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Class for handling async responses from xrootd for one file
//------------------------------------------------------------------------------

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
/*----------------------------------------------------------------------------*/

#ifndef __EOS_ASYNCMETAHANDLER_HH__
#define __EOS_ASYNCMETAHANDLER_HH__

EOSFSTNAMESPACE_BEGIN

// Forward declaration 
class ChunkHandler;

//------------------------------------------------------------------------------
//! Class for handling async responses
//------------------------------------------------------------------------------

class AsyncMetaHandler
{
public:

  //--------------------------------------------------------------------------
  //! Constructor
  //--------------------------------------------------------------------------
  AsyncMetaHandler ();


  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  virtual ~AsyncMetaHandler ();


  //--------------------------------------------------------------------------
  //! Handle response
  //!
  //! @param pStatus status of the request
  //! @param chunk received chunk response
  //!
  //--------------------------------------------------------------------------
  virtual void HandleResponse (XrdCl::XRootDStatus* pStatus,
                               ChunkHandler* chunk);


  //--------------------------------------------------------------------------
  //! Wait for responses
  //--------------------------------------------------------------------------
  bool WaitOK ();


  //--------------------------------------------------------------------------
  //! Register a new request for the current file
  //!
  //! @param offset request offset
  //! @param length request length
  //! @param isWrite set if it is a write request
  //!
  //! @return new chunk async handler object
  //! 
  //--------------------------------------------------------------------------
  ChunkHandler* Register (uint64_t offset, uint32_t length, bool isWrite);


  //--------------------------------------------------------------------------
  //! Get map of errors
  //--------------------------------------------------------------------------
  const std::map<uint64_t, uint32_t>& GetErrorsMap ();


  //--------------------------------------------------------------------------
  //! Reset
  //--------------------------------------------------------------------------
  void Reset ();

private:

  bool mState; ///< true if all requests are ok, otherwise false
  int mNumExpectedResp; ///< expected number of responses
  int mNumReceivedResp; ///< received number of responses
  XrdSysCondVar mCond; ///< condition variable to signal the receival of
  ///< all responses

  std::list<ChunkHandler*> listReq; ///< list of registered async requests
  std::list<ChunkHandler*> listCache; ///< list of cached request objects
  std::map<uint64_t, uint32_t> mMapErrors; ///< chunks for which the request failed

  ///! maximum number of obj in cache used for recycling
  static const unsigned int msMaxCacheSize;

};

EOSFSTNAMESPACE_END

#endif // __EOS_ASYNCMETAHANDLER_HH__ 



