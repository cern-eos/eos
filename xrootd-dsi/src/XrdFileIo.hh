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
//! @file XrdFileIo.hh
//! @author Elvin-Alin Sindrilaru - CERN , Geoffray Adde - CERN
//! @brief Class used for doing remote IO operations using the Xrd client
//!
//! @details The following code has been extracted from EOS.
//! The API signatures have been slightly modified to better fit the needs.
//! The logic itself remains unchanged.
//------------------------------------------------------------------------------

#ifndef __EOSFST_XRDFILEIO_HH__
#define __EOSFST_XRDFILEIO_HH__

/*----------------------------------------------------------------------------*/
#include "SimpleHandler.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFile.hh"
/*----------------------------------------------------------------------------*/
#include <queue>


class AsyncMetaHandler;

//------------------------------------------------------------------------------
//! Struct that holds a readahead buffer and corresponding handler
//------------------------------------------------------------------------------

struct ReadaheadBlock
{
  static uint64_t sDefaultBlocksize; ///< default value for readahead

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
//! Class used for doing remote IO operations using the Xrd client
//------------------------------------------------------------------------------

class XrdFileIo 
{
public:

  static uint32_t sNumRdAheadBlocks; //< no. of blocks used for readahead

  XrdFileIo ();


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdFileIo ();


  //----------------------------------------------------------------------------
  //! Open file
  //!
  //! @param path file path
  //! @param flags open flags
  //! @param mode open mode
  //! @param readahead enable read ahead
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //----------------------------------------------------------------------------
  virtual XrdCl::XRootDStatus Open (const std::string& path,
                    XrdCl::OpenFlags::Flags flags,
                    mode_t mode = 0,
                    bool readahead = true);


  //----------------------------------------------------------------------------
  //! Read from file - sync
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //!
  //! @return number of bytes read or -1 if error
  //!
  //----------------------------------------------------------------------------
  virtual int64_t Read (uint64_t offset,
                        char* buffer,
                        uint32_t length);


  //--------------------------------------------------------------------------
  //! Write to file - sync
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //!
  //! @return number of bytes written or -1 if error
  //!
  //--------------------------------------------------------------------------
  virtual int64_t Write (uint64_t offset,
                         const char* buffer,
                         uint32_t length);


  //--------------------------------------------------------------------------
  //! Read from file - async
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param pFileHandler async handler for file
  //! @param readahead true if readahead is to be enabled, otherwise false
  //!
  //! @return number of bytes read or -1 if error
  //!
  //--------------------------------------------------------------------------
  virtual int64_t Read (uint64_t offset,
                        char* buffer,
                        uint32_t length,
                        void* pFileHandler,
                        bool readahead = false,
                        bool *usedCallBack = 0);


  //--------------------------------------------------------------------------
  //! Write to file - async
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //! @param handler async write handler
  //!
  //! @return number of bytes written or -1 if error
  //!
  //--------------------------------------------------------------------------
  virtual int64_t Write (uint64_t offset,
                         const char* buffer,
                         uint32_t length,
                         void* pFileHandler);


  //--------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Truncate (uint64_t offset);


  //--------------------------------------------------------------------------
  //! Remove file
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Remove ();


  //--------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Sync ();


  //--------------------------------------------------------------------------
  //! Close file
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Close ();


  //--------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Stat (struct stat* buf);

private:
  int mIndex; ///< inded of readahead block in use ( 0 or 1 )
  bool mDoReadahead; ///< mark if readahead is enabled
  uint32_t mBlocksize; ///< block size for rd/wr opertations
  std::string mPath; ///< path to file
  XrdCl::File* mXrdFile; ///< handler to xrd file
  std::map<uint64_t, ReadaheadBlock*> mMapBlocks; ///< map of block read/prefetched
  std::queue<ReadaheadBlock*> mQueueBlocks; ///< queue containing available blocks

  //--------------------------------------------------------------------------
  //! Method used to prefetch the next block using the readahead mechanism
  //!
  //! @param offset begin offset of the current block we are reading
  //! @param isWrite true if block is for write, false otherwise
  //!
  //--------------------------------------------------------------------------
  void PrefetchBlock (int64_t offset, bool isWrite);


  //--------------------------------------------------------------------------
  //! Disable copy constructor
  //--------------------------------------------------------------------------
  //XrdFileIo (const XrdFileIo&) = delete;
  XrdFileIo (const XrdFileIo&) {}


  //--------------------------------------------------------------------------
  //! Disable assign operator
  //--------------------------------------------------------------------------
  //XrdFileIo& operator = (const XrdFileIo&) = delete;
  XrdFileIo& operator = (const XrdFileIo&) {return *this;}

};

#endif  // __EOSFST_XRDFILEIO_HH__

