//------------------------------------------------------------------------------
//! @file VectChunkHandler.hh
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//! @brief Class holding information about an asynchronous vector request and
//!        a pointer to the file the request belongs to. This class notifies
//!        the AsyncMetaHandler corresponding to the file object of of any
//!        errors during transfer.
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

#ifndef __EOS_VECTCHUNKHANDLER_HH__
#define __EOS_VECTCHUNKHANDLER_HH__

/*----------------------------------------------------------------------------*/
#include "fst/io/AsyncMetaHandler.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

// -----------------------------------------------------------------------------
//! Class holding information about an asynchronous vector request
// -----------------------------------------------------------------------------
class VectChunkHandler : public XrdCl::ResponseHandler
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param reqHandler handler to the file meta handler
  //! @param chunkList chunks concerning the vector operation
  //! @param isWrite chunk belongs to a write request
  //!
  //----------------------------------------------------------------------------
  VectChunkHandler (AsyncMetaHandler* reqHandler,
                    XrdCl::ChunkList& chunkList,
                    const char* wrBuf,
                    bool isWrite);


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~VectChunkHandler ();


  //----------------------------------------------------------------------------
  //! Update function
  //!
  //! @param reqHandler handler to the file meta handler
  //! @param offset request offset
  //! @param length request length
  //! @param buffer holder for data 
  //! @param isWrite chunk belongs to a write request
  //!
  //----------------------------------------------------------------------------
  void Update (AsyncMetaHandler* reqHandler,
               XrdCl::ChunkList& chunks,
               const char* wrBuf,
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
  //! Get buffer
  //----------------------------------------------------------------------------
  inline char*
  GetBuffer() const
  {
    return mBuffer;
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
  //! Get total length of vector request
  //----------------------------------------------------------------------------
  inline uint32_t
  GetLength () const
  {
    return mLength;
  };


  //----------------------------------------------------------------------------
  //! Get the list of chunks
  //----------------------------------------------------------------------------
  inline XrdCl::ChunkList&
  GetChunkList ()
  {
    return mChunkList;
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
  XrdCl::ChunkList mChunkList; ///< vector operation chunks
  uint32_t mCapacity; ///< capacity of the buffer
  uint32_t mLength; ///< length of the vector request
  uint32_t mRespLength; ///< length of response received, only for reads
  bool mIsWrite; ///< operation type is write
};

EOSFSTNAMESPACE_END

#endif   // __EOS_VECTCHUNKHANDLER_HH__
