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
#include "common/ConcurrentQueue.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/


#ifndef __EOS_ASYNCMETAHANDLER_HH__
#define __EOS_ASYNCMETAHANDLER_HH__

EOSFSTNAMESPACE_BEGIN

// Forward declaration 
class ChunkHandler;

//------------------------------------------------------------------------------
//! Class for handling async responses
//------------------------------------------------------------------------------

class AsyncMetaHandler: public eos::common::LogId
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
  //!
  //! @return error type, if no error occurs return XrdCl::errNone
  //!  For further details on possible error codes look into XrdClStatus.hh
  //!
  //--------------------------------------------------------------------------
  uint16_t WaitOK ();


  //--------------------------------------------------------------------------
  //! Register a new request for the current file
  //!
  //! @param offset request offset
  //! @param length request length
  //! @param buffer holder for the data
  //! @param isWrite set if it is a write request
  //!
  //! @return new chunk async handler object
  //! 
  //--------------------------------------------------------------------------
  ChunkHandler* Register (uint64_t offset,
                          uint32_t length,
                          const char* buffer,
                          bool isWrite);


  //--------------------------------------------------------------------------
  //! Get map of errors
  //!
  //! @return map of errors
  //!
  //--------------------------------------------------------------------------
  const std::map<uint64_t, uint32_t>& GetErrors ();


  //--------------------------------------------------------------------------
  //! Reset
  //--------------------------------------------------------------------------
  void Reset ();

private:

  uint16_t mErrorType; ///< type of error, we are mostly interested in timeouts
  unsigned int mAsyncReq; ///< number of async requests in flight (for which no response was received)
  XrdSysCondVar mCond; ///< condition variable to signal the receival of all responses
  ChunkHandler* mChunkToDelete; ///< pointer to the ChunkHandler to be deleted

  eos::common::ConcurrentQueue<ChunkHandler*> mQRecycle; ///< recyclable obj
  std::map<uint64_t, uint32_t> mMapErrors; ///< chunks for which the request failed

  //! maxium number of async requests in flight and also the maximum number
  //! of ChunkHandler object that can be saved in cache
  static const unsigned int msMaxNumAsyncObj; 

};

EOSFSTNAMESPACE_END

#endif // __EOS_ASYNCMETAHANDLER_HH__ 



