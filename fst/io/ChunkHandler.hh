//------------------------------------------------------------------------------
//! @file ChunkHandler.hh
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//! @brief Class holding information about an asynchronous request and a pointer
//!        to the file the request belongs to. This class notifies the
//!        AsyncMetaHandler corresponding to the file object of any errors during
//!        transfers
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

#ifndef __EOS_CHUNKHANDLER_HH__
#define __EOS_CHUNKHANDLER_HH__

/*----------------------------------------------------------------------------*/
#include "fst/io/AsyncMetaHandler.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class holding information about an asynchronous request
//------------------------------------------------------------------------------
class ChunkHandler : public XrdCl::ResponseHandler
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param reqHandler handler to the file meta handler
  //! @param offset request offset
  //! @param length request length
  //! @param buff pointer to the read or write buffer
  //! @param isWrite chunk belongs to a write request
  //!
  //----------------------------------------------------------------------------
  ChunkHandler (AsyncMetaHandler* reqHandler,
                uint64_t offset,
                uint32_t length,
                char* buff,
                bool isWrite);


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ChunkHandler ();


  //----------------------------------------------------------------------------
  //! Update function
  //!
  //! @param reqHandler handler to the file meta handler
  //! @param offset request offset
  //! @param length request length
  //! @param buffer pointer to the read or write buffer
  //! @param isWrite chunk belongs to a write request
  //!
  //----------------------------------------------------------------------------
  void Update (AsyncMetaHandler* reqHandler,
               uint64_t offset,
               uint32_t length,
               char* buff,
               bool isWrite);


  //----------------------------------------------------------------------------
  //! Handle response
  //!
  //! @param pStatus status of the response
  //! @param pResponse object containing extra info about the response
  //! 
  //----------------------------------------------------------------------------
  virtual void HandleResponse (XrdCl::XRootDStatus* pStatus,
                               XrdCl::AnyObject* pResponse);



  //----------------------------------------------------------------------------
  //! Get buffer pointer
  //----------------------------------------------------------------------------
  inline char*
  GetBuffer() const
  {
    return mBuffer;
  };
  

  //----------------------------------------------------------------------------
  //! Get request chunk offset
  //----------------------------------------------------------------------------
  inline uint64_t
  GetOffset () const
  {
    return mOffset;
  };


  //----------------------------------------------------------------------------
  //! Get request chunk length
  //----------------------------------------------------------------------------
  inline uint32_t
  GetLength () const
  {
    return mLength;
  };


  //----------------------------------------------------------------------------
  //! Get response chunk length
  //----------------------------------------------------------------------------
  inline uint32_t
  GetRespLength () const
  {
    return mRespLength;
  };

  
  //----------------------------------------------------------------------------
  //! Test if chunk is from a write operation 
  //----------------------------------------------------------------------------
  inline bool
  IsWrite () const
  {
    return mIsWrite;
  };

private:

  char* mBuffer;  ///< holder for data for write requests
  AsyncMetaHandler* mMetaHandler; ///< handler to the whole file meta handler
  uint64_t mOffset; ///< offset of the request
  uint32_t mLength; ///< length of the request
  uint32_t mCapacity; ///< capacity of the buffer
  uint32_t mRespLength; ///< length of response received, only for reads
  bool mIsWrite; ///< operation type is write
};

EOSFSTNAMESPACE_END

#endif   // __EOS_CHUNKHANDLER_HH__
