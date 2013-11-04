//------------------------------------------------------------------------------
//! @file AsyncMetaHandler.hh
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//! @brief Class for handling async responses from xrootd for one file
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
class VectChunkHandler;

//------------------------------------------------------------------------------
//! Class for handling async responses
//------------------------------------------------------------------------------
class AsyncMetaHandler: public eos::common::LogId
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  AsyncMetaHandler ();


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~AsyncMetaHandler ();


  //----------------------------------------------------------------------------
  //! Register a new request for the current file
  //!
  //! @param offset request offset
  //! @param length request length
  //! @param buffer holder for the data
  //! @param isWrite set if it is a write request
  //!
  //! @return new chunk async handler object
  //! 
  //----------------------------------------------------------------------------
  ChunkHandler* Register (uint64_t offset,
                          uint32_t length,
                          char* buffer,
                          bool isWrite);
                          

  //----------------------------------------------------------------------------
  //! Register a new vector request for the current file
  //!
  //! @param chunks list of chunks used for the vector request
  //! @param wrBuff write buffer, ignored if it's a read operation
  //! @param isWrite set if it is a write request
  //!
  //! @return new vector chunk async handler object
  //! 
  //----------------------------------------------------------------------------
  VectChunkHandler* Register (XrdCl::ChunkList& chunks,
                              const char* wrBuf,
                              bool isWrite);

  
  //----------------------------------------------------------------------------
  //! Handle response normal response
  //!
  //! @param pStatus status of the request
  //! @param chunk received chunk response
  //!
  //----------------------------------------------------------------------------
  virtual void HandleResponse (XrdCl::XRootDStatus* pStatus,
                               ChunkHandler* chunk);


  //----------------------------------------------------------------------------
  //! Handle response vector response
  //!
  //! @param pStatus status of the request
  //! @param chunks received vector response
  //!
  //----------------------------------------------------------------------------
  virtual void HandleResponse (XrdCl::XRootDStatus* pStatus,
                               VectChunkHandler* chunks);

  
  //----------------------------------------------------------------------------
  //! Wait for responses
  //!
  //! @return error type, if no error occurs return XrdCl::errNone
  //!  For further details on possible error codes look into XrdClStatus.hh
  //!
  //----------------------------------------------------------------------------
  uint16_t WaitOK ();

  
  //----------------------------------------------------------------------------
  //! Get map of errors
  //!
  //! @return map of errors
  //!
  //----------------------------------------------------------------------------
  const XrdCl::ChunkList& GetErrors ();


  //----------------------------------------------------------------------------
  //! Reset
  //----------------------------------------------------------------------------
  void Reset ();
  

private:

  uint16_t mErrorType; ///< type of error, we are mostly interested in timeouts
  uint32_t mAsyncReq; ///< number of async requests in flight (for which no response was received)
  uint32_t mAsyncVReq; ///< number of async VECTOR req. in flight (for which no response was received)
  XrdSysCondVar mCond; ///< condition variable to signal the receival of all responses
  ChunkHandler* mHandlerDel; ///< pointer to handler to be deleted
  VectChunkHandler* mVHandlerDel; ///< pointer to VECTOR handler to be deleted

  eos::common::ConcurrentQueue<ChunkHandler*> mQRecycle; ///< recyclable normal handlers
  eos::common::ConcurrentQueue<VectChunkHandler*> mQVRecycle; ///< recyclable vector handlers
  XrdCl::ChunkList mErrors; ///< chunks for which the request failed

  //! Maxium number of async requests in flight and also the maximum number
  //! of ChunkHandler object that can be saved in cache
  static const unsigned int msMaxNumAsyncObj; 

};

EOSFSTNAMESPACE_END

#endif // __EOS_ASYNCMETAHANDLER_HH__ 

