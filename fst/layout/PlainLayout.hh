//------------------------------------------------------------------------------
//! @file PlainLayout.hh
//! @author Elvin-Alin Sindrilaru / Andreas-Joachim Peters - CERN
//! @brief Layout of a plain file without any replication or striping
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

#ifndef __EOSFST_PLAINLAYOUT_HH__
#define __EOSFST_PLAINLAYOUT_HH__

#include "fst/layout/Layout.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include <pthread.h>

EOSFSTNAMESPACE_BEGIN

//! Forward declaration
class PlainLayout;

//------------------------------------------------------------------------------
//! Class used for handling asynchronous open responses for this layout
//------------------------------------------------------------------------------
class AsyncLayoutOpenHandler:
  public XrdCl::ResponseHandler, public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param layout layout object
  //----------------------------------------------------------------------------
  AsyncLayoutOpenHandler(PlainLayout* layout): mPlainLayout(layout) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~AsyncLayoutOpenHandler() {}

  //----------------------------------------------------------------------------
  //! Called when a response to associated request arrives or an error occurs
  //!
  //! @param status   status of the request
  //! @param response an object associated with the response (request dependent)
  //! @param hostList list of hosts the request was redirected to
  //---------------------------------------------------------------------------
  virtual void HandleResponseWithHosts(XrdCl::XRootDStatus* status,
                                       XrdCl::AnyObject* response,
                                       XrdCl::HostList* hostList);

private:
  PlainLayout* mPlainLayout; ///< Layout object corresponding to this handler
};

//------------------------------------------------------------------------------
//! Class abstracting the physical layout of a plain file
//------------------------------------------------------------------------------
class PlainLayout : public Layout
{
  friend class AsyncLayoutOpenHandler;
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param file file handler
  //! @param lid layout id
  //! @param client security information
  //! @param error error information
  //! @param io io access type ( ofs/xrd )
  //! @param timeout timeout value
  //!
  //----------------------------------------------------------------------------
  PlainLayout(XrdFstOfsFile* file,
              unsigned long lid,
              const XrdSecEntity* client,
              XrdOucErrInfo* outError,
              const char* path,
              uint16_t timeout = 0);


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~PlainLayout();

  // -------------------------------------------------------------------------
  // Redirect to new target
  // -------------------------------------------------------------------------
  virtual void Redirect(const char* path);

  //--------------------------------------------------------------------------
  //! Open file
  //!
  //! @param flags open flags
  //! @param mode open mode
  //! @param opaque opaque information
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //----------------------------------------------------------------------------
  virtual int Open(
    XrdSfsFileOpenMode flags,
    mode_t mode,
    const char* opaque = "");

  //--------------------------------------------------------------------------
  //! Open file asynchronously
  //!
  //! @param path file path
  //! @param flags open flags
  //! @param mode open mode
  //! @param handler open handler
  //! @param opaque opaque information
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  virtual int OpenAsync(XrdSfsFileOpenMode flags,
                        mode_t mode, XrdCl::ResponseHandler* handler,
                        const char* opaque = "");

  //--------------------------------------------------------------------------
  //! Wait for the asynchronous open response
  //!
  //! @return true if open successful, false otherwise
  //--------------------------------------------------------------------------
  bool WaitOpenAsync();

  //--------------------------------------------------------------------------
  //! Read from file
  //!
  //! @param offset offset
  //! @param buffer place to hold the read data
  //! @param length length
  //! @param readahead readahead switch
  //!
  //! @return number of bytes read or -1 if error
  //!
  //----------------------------------------------------------------------------
  virtual int64_t Read(XrdSfsFileOffset offset,
                       char* buffer,
                       XrdSfsXferSize length,
                       bool readahead = false);


  //----------------------------------------------------------------------------
  //! Vector read
  //!
  //! @param chunkList list of chunks for the vector read
  //! @param len total length of the vector read
  //!
  //! @return number of bytes read of -1 if error
  //!
  //----------------------------------------------------------------------------
  virtual int64_t ReadV(XrdCl::ChunkList& chunkList,
                        uint32_t len);


  //----------------------------------------------------------------------------
  //! Write to file
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //!
  //! @return number of bytes written or -1 if error
  //!
  //----------------------------------------------------------------------------
  virtual int64_t Write(XrdSfsFileOffset offset,
                        const char* buffer,
                        XrdSfsXferSize length);

  //--------------------------------------------------------------------------
  //! Wait for all async IO and return global response code
  //--------------------------------------------------------------------------
  virtual int WaitAsyncIO()
  {
    return mFileIO->fileWaitAsyncIO();
  }

  //----------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //----------------------------------------------------------------------------
  virtual int Truncate(XrdSfsFileOffset offset);


  //----------------------------------------------------------------------------
  //! Allocate file space
  //!
  //! @param length space to be allocated
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //----------------------------------------------------------------------------
  virtual int Fallocate(XrdSfsFileOffset length);


  //----------------------------------------------------------------------------
  //! Deallocate file space
  //!
  //! @param fromOffset offset start
  //! @param toOffset offset end
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //----------------------------------------------------------------------------
  virtual int Fdeallocate(XrdSfsFileOffset fromOffset,
                          XrdSfsFileOffset toOffset);


  //----------------------------------------------------------------------------
  //! Remove file
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //----------------------------------------------------------------------------
  virtual int Remove();


  //----------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //----------------------------------------------------------------------------
  virtual int Sync();


  //----------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //----------------------------------------------------------------------------
  virtual int Stat(struct stat* buf);


  //----------------------------------------------------------------------------
  //! Close file
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //----------------------------------------------------------------------------
  virtual int Close();

private:

  uint64_t mFileSize; ///< file size
  bool mDisableRdAhead; ///< if any write operations is done, disable rdahead
  bool mHasAsyncResponse; ///< true if async open response arrived
  bool mAsyncResponse; ///< True if successful, otherwise false
  pthread_mutex_t mMutex; ///< Mutex to be used with the condition variable
  pthread_cond_t mCondVar; ///< Condition variable for async notifications
  eos::fst::AsyncIoOpenHandler* mIoOpenHandler; ///< Open handler for IO layer
  XrdSfsFileOpenMode mFlags; ///< Open flags

  //----------------------------------------------------------------------------
  //! Disable copy constructor
  //----------------------------------------------------------------------------
  PlainLayout(const PlainLayout&) = delete;


  //----------------------------------------------------------------------------
  //! Disable assign operator
  //----------------------------------------------------------------------------
  PlainLayout& operator = (const PlainLayout&) = delete;

};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_PLAINLAYOUT_HH__
