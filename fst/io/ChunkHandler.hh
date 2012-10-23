//------------------------------------------------------------------------------
// File: ChunkHandler.hh
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
//! @file ChunkHandler.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Class holding information about an asynchronous request
//------------------------------------------------------------------------------

#ifndef __EOS_CHUNKHANDLER_HH__
#define __EOS_CHUNKHANDLER_HH__

/*----------------------------------------------------------------------------*/
#include "fst/io/AsyncMetaHandler.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

// -----------------------------------------------------------------------------
//! Class holding information about an asynchronous request
// -----------------------------------------------------------------------------
class ChunkHandler: public XrdCl::ResponseHandler
{
 public:

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param reqHandler handler to the file meta handler
  //! @param offset request offset
  //! @param length request length
  //!
  //----------------------------------------------------------------------------
  ChunkHandler( AsyncMetaHandler* reqHandler,
                uint64_t          offset,
                uint32_t          length );


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ChunkHandler();


  //--------------------------------------------------------------------------
  //! Handle response
  //!
  //! @param pStatus status of the response
  //! @param pResponse object containing extra info about the response
  //! 
  //--------------------------------------------------------------------------
  virtual void HandleResponse( XrdCl::XRootDStatus* pStatus,
                               XrdCl::AnyObject*    pResponse );


 private:

  AsyncMetaHandler* mMetaHandler; ///< handler to the whole file meta handler
  uint64_t          mOffset;      ///< offset of the request
  uint32_t          mLength;      ///< length of the request
  
};

EOSFSTNAMESPACE_END

#endif   // __EOS_CHUNKHANDLER_HH__
