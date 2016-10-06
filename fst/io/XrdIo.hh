//------------------------------------------------------------------------------
//! @file XrdIo.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Class used for doing remote IO operations unsing the xrd client
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

#ifndef __EOSFST_XRDFILEIO_HH__
#define __EOSFST_XRDFILEIO_HH__

#include "fst/io/FileIo.hh"
#include "fst/io/SimpleHandler.hh"
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdCl/XrdClURL.hh"

EOSFSTNAMESPACE_BEGIN

//! Forward declarations
class XrdIo;
class AsyncMetaHandler;
struct ReadaheadBlock;

typedef std::map<uint64_t, ReadaheadBlock*> PrefetchMap;

//------------------------------------------------------------------------------
//! Struct that holds a readahead buffer and corresponding handler
//------------------------------------------------------------------------------

struct ReadaheadBlock
{
  static const uint64_t sDefaultBlocksize; ///< default value for readahead

  //----------------------------------------------------------------------------
  //! Constuctor
  //!
  //! @param blocksize the size of the readahead
  //!
  //----------------------------------------------------------------------------
  ReadaheadBlock(uint64_t blocksize = sDefaultBlocksize)
  {
    buffer = new char[blocksize];
    handler = new SimpleHandler();
  }

  //----------------------------------------------------------------------------
  //! Update current request
  //!
  //! @param offset offset
  //! @param length length
  //! @param isWrite true if write request, otherwise false
  //!
  //----------------------------------------------------------------------------
  void Update(uint64_t offset, uint32_t length, bool isWrite)
  {
    handler->Update(offset, length, isWrite);
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ReadaheadBlock()
  {
    delete[] buffer;
    delete handler;
  }

  char* buffer; ///< pointer to where the data is read
  SimpleHandler* handler; ///< async handler for the requests
};

//------------------------------------------------------------------------------
//! Class used for handling asynchronous open responses
//------------------------------------------------------------------------------
class AsyncIoOpenHandler: public XrdCl::ResponseHandler,
                          public eos::common::LogId
{
 public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param io_file file object
  //! @param layout_handler handler for the layout object
  //----------------------------------------------------------------------------
  AsyncIoOpenHandler(XrdIo* io_file, XrdCl::ResponseHandler* layout_handler):
      mFileIo(io_file), mLayoutOpenHandler(layout_handler) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~AsyncIoOpenHandler() {}

  //----------------------------------------------------------------------------
  //! Called when a response to associated request arrives or an error  occurs
  //!
  //! @param status   status of the request
  //! @param response an object associated with the response (request dependent)
  //! @param hostList list of hosts the request was redirected to
  //---------------------------------------------------------------------------
  virtual void HandleResponseWithHosts(XrdCl::XRootDStatus* status,
                                       XrdCl::AnyObject* response,
                                       XrdCl::HostList* hostList);

 private:
  XrdIo* mFileIo; ///< File IO object corresponding to this handler
  XrdCl::ResponseHandler* mLayoutOpenHandler; ///< Open handler for the layout
};


//------------------------------------------------------------------------------
//! Class used for doing remote IO operations using the Xrd client
//------------------------------------------------------------------------------
class XrdIo : public FileIo
{
  friend class AsyncIoOpenHandler;
public:

  static const uint32_t sNumRdAheadBlocks; ///< no. of blocks used for readahead

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param handle to logical file
  //! @param client security entity
  //! @param error error information
  //!
  //----------------------------------------------------------------------------
  XrdIo ();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdIo ();

  //----------------------------------------------------------------------------
  //! Open file - synchronously
  //!
  //! @param path file path
  //! @param flags open flags
  //! @param mode open mode
  //! @param opaque opaque information
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int Open (const std::string& path,
                    XrdSfsFileOpenMode flags,
                    mode_t mode = 0,
                    const std::string& opaque = "",
                    uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Open file - asynchronously. This call is to be used from one of the file
  //! layout classes and not on its own, as there is not mechanims buit into
  //! this class to wait for the response.
  //!
  //! @param path file path
  //! @param handler handler called asynchronously
  //! @param flags open flags
  //! @param mode open mode
  //! @param opaque opaque information
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int OpenAsync (const std::string& path, XrdCl::ResponseHandler* io_handler,
                         XrdSfsFileOpenMode flags, mode_t mode = 0,
                         const std::string& opaque = "", uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Read from file - sync
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param timeout timeout value
  //!
  //! @return number of bytes read or -1 if error
  //!
  //----------------------------------------------------------------------------
  virtual int64_t Read (XrdSfsFileOffset offset,
                        char* buffer,
                        XrdSfsXferSize length,
                        uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Write to file - sync
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //! @param timeout timeout value
  //!
  //! @return number of bytes written or -1 if error
  //!
  //--------------------------------------------------------------------------
  virtual int64_t Write (XrdSfsFileOffset offset,
                         const char* buffer,
                         XrdSfsXferSize length,
                         uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Read from file - async
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param readahead true if readahead is to be enabled, otherwise false
  //! @param timeout timeout value
  //!
  //! @return number of bytes read or -1 if error
  //!
  //--------------------------------------------------------------------------
  virtual int64_t ReadAsync (XrdSfsFileOffset offset,
                             char* buffer,
                             XrdSfsXferSize length,
                             bool readahead = false,
                             uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Write to file - async
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //! @param timeout timeout value
  //!
  //! @return number of bytes written or -1 if error
  //!
  //--------------------------------------------------------------------------
  virtual int64_t WriteAsync (XrdSfsFileOffset offset,
                              const char* buffer,
                              XrdSfsXferSize length,
                              uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Truncate (XrdSfsFileOffset offset,
                        uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Remove file
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Remove (uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Sync (uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Close file
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Close (uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Stat (struct stat* buf, uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Get pointer to async meta handler object
  //!
  //! @return pointer to async handler, NULL otherwise
  //!
  //--------------------------------------------------------------------------
  virtual void* GetAsyncHandler ();


private:

  bool mDoReadahead; ///< mark if readahead is enabled
  uint32_t mBlocksize; ///< block size for rd/wr opertations
  std::string mPath; ///< path to file
  XrdCl::File* mXrdFile; ///< handler to xrd file
  AsyncMetaHandler* mMetaHandler; ///< async requests meta handler
  PrefetchMap mMapBlocks; ///< map of block read/prefetched
  std::queue<ReadaheadBlock*> mQueueBlocks; ///< queue containing available blocks
  XrdSysMutex mPrefetchMutex; ///< mutex to serialise the prefetch step

  // connection pool handler
  static XrdSysMutex sConnectionPoolMutex;
  static std::map<std::string, std::map<int, size_t> > sConnectionPool;
  static size_t sConnectionPoolMaxSize;

  void AssignConnection(); ///< if needed assign a connection from the connection pool
  void DropConnection(); ///< if needed return a conection back to the pool
  void DumpConnectionPool(); ///< dump current state of connection pool

  XrdCl::URL mTargetUrl;
  size_t mConnectionId;
  

  //--------------------------------------------------------------------------
  //! Method used to prefetch the next block using the readahead mechanism
  //!
  //! @param offset begin offset of the current block we are reading
  //! @param isWrite true if block is for write, false otherwise
  //! @param timeout timeout value
  //!
  //! @return true if prefetch request was sent, otherwise false
  //!
  //--------------------------------------------------------------------------
  bool PrefetchBlock (int64_t offset,
                      bool isWrite,
                      uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Try to find a block in cache with contains the provided offset
  //!
  //! @param offset offset to be searched for
  //!
  //! @return iterator to the block containing the offset or if no such block
  //!         is found we return the iterator to the end of the map
  //!
  //--------------------------------------------------------------------------
  PrefetchMap::iterator FindBlock(uint64_t offset);

  //--------------------------------------------------------------------------
  //! Disable copy constructor
  //--------------------------------------------------------------------------
  XrdIo (const XrdIo&) = delete;

  //--------------------------------------------------------------------------
  //! Disable assign operator
  //--------------------------------------------------------------------------
  XrdIo& operator = (const XrdIo&) = delete;
};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_XRDFILEIO_HH__
