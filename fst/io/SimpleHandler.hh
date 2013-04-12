//------------------------------------------------------------------------------
// File: SimpleHandler.hh
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
//! @file SimpleHandler.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Class holding information about an asynchronous request
//------------------------------------------------------------------------------

#ifndef __EOS_SIMPLEHANDLER_HH__
#define __EOS_SIMPLEHANDLER_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

// -----------------------------------------------------------------------------
//! Class holding information about an asynchronous request
// -----------------------------------------------------------------------------
class SimpleHandler : public XrdCl::ResponseHandler
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param offset request offset
  //! @param length request length
  //! @param isWrite chunk belongs to a write request
  //!
  //----------------------------------------------------------------------------
  SimpleHandler (uint64_t offset = 0,
                 uint32_t length = 0,
                 bool isWrite = false);


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~SimpleHandler ();


  //----------------------------------------------------------------------------
  //! Update function
  //!
  //! @param offset request offset
  //! @param length request length
  //! @param isWrite chunk belongs to a write request
  //!
  //----------------------------------------------------------------------------
  void Update (uint64_t offset,
               uint32_t length,
               bool isWrite);


  //----------------------------------------------------------------------------
  //! Wait for request to be done 
  //!
  //! @return status of the request
  //!
  //----------------------------------------------------------------------------
  bool WaitOK ();


  //----------------------------------------------------------------------------
  //! Get if there is any request to process
  //!
  //! @return true if there is a request, false otherwise
  //!
  //----------------------------------------------------------------------------
  bool HasRequest ();


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
  //! Get response chunk status
  //----------------------------------------------------------------------------

  inline bool
  GetRespStatus () const
  {
    return mRespOK;
  };


  //----------------------------------------------------------------------------
  //! Test if chunk is from a write operation 
  //----------------------------------------------------------------------------

  inline bool
  IsWrite () const
  {
    return mIsWrite;
  };


  //----------------------------------------------------------------------------
  //! Handle response
  //!
  //! @param pStatus status of the response
  //! @param pResponse object containing extra info about the response
  //! 
  //----------------------------------------------------------------------------
  virtual void HandleResponse (XrdCl::XRootDStatus* pStatus,
                               XrdCl::AnyObject* pResponse);


private:

  uint64_t mOffset; ///< offset of the request
  uint32_t mLength; ///< length of the request
  uint32_t mRespLength; ///< length of response received, only for reads
  bool mIsWrite; ///< operation type is write
  bool mRespOK; ///< mark if the resp status is ok
  bool mReqDone; ///< mark if the request was done
  bool mHasReq; ///< mark if there is any request to proceess
  XrdSysCondVar mCond; ///< cond. variable used for synchronisation

};

EOSFSTNAMESPACE_END

#endif   // __EOS_SIMPLEHANDLER_HH__
