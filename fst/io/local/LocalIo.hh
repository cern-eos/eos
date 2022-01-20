//------------------------------------------------------------------------------
//! @file LocalIo.hh
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//! @brief Class used for doing local IO operations
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

#ifndef __EOSFST_LOCALFILEIO__HH__
#define __EOSFST_LOCALFILEIO__HH__

#include "fst/io/FileIo.hh"
#include "FsIo.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class used for doing local IO operations
//------------------------------------------------------------------------------
class LocalIo : public FsIo
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param handle to logical file
  //! @param client security entity
  //----------------------------------------------------------------------------
  LocalIo(std::string path, XrdFstOfsFile* file = 0,
          const XrdSecEntity* client = 0);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~LocalIo();

  //--------------------------------------------------------------------------
  //! Open file
  //!
  //! @param flags open flags
  //! @param mode open mode
  //! @param opaque opaque information
  //! @param timeout timeout value
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileOpen(XrdSfsFileOpenMode flags,
               mode_t mode = 0,
               const std::string& opaque = "",
               uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Open file asynchronously
  //!
  //! @param flags open flags
  //! @param mode open mode
  //! @param opaque opaque info to be appended to the request
  //! @param timeout operation timeout
  //!
  //! @return future holding the status response
  //--------------------------------------------------------------------------
  std::future<XrdCl::XRootDStatus>
  fileOpenAsync(XrdSfsFileOpenMode flags, mode_t mode = 0,
                const std::string& opaque = "", uint16_t timeout = 0) override;

  //----------------------------------------------------------------------------
  //! Read from file - sync
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param timeout timeout value
  //!
  //! @return number of bytes read or -1 if error
  //--------------------------------------------------------------------------
  int64_t fileRead(XrdSfsFileOffset offset,
                   char* buffer,
                   XrdSfsXferSize length,
                   uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Read from file asynchronously
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param timeout timeout value
  //!
  //! @return number of bytes read or -1 if error
  //! @note The buffer given by the user is not neccessarily populated with
  //!       any meaningful data when this function returns. The user should call
  //!       fileWaitAsyncIO to enforce this guarantee.
  //----------------------------------------------------------------------------
  int64_t fileReadAsync(XrdSfsFileOffset offset, char* buffer,
                        XrdSfsXferSize length, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Read from file with prefetching
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param timeout timeout value
  //!
  //! @return number of bytes read or -1 if error
  //----------------------------------------------------------------------------
  int64_t fileReadPrefetch(XrdSfsFileOffset offset, char* buffer,
                           XrdSfsXferSize length, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Vector read - sync
  //!
  //! @param chunkList list of chunks for the vector read
  //! @param timeout timeout value
  //!
  //! @return number of bytes read of -1 if error
  //----------------------------------------------------------------------------
  int64_t fileReadV(XrdCl::ChunkList& chunkList, uint16_t timeout = 0);

  //------------------------------------------------------------------------------
  //! Vector read - async
  //!
  //! @param chunkList list of chunks for the vector read
  //! @param timeout timeout value
  //!
  //! @return number of bytes read of -1 if error; this actually calls the
  //!         ReadV sync method
  //------------------------------------------------------------------------------
  int64_t fileReadVAsync(XrdCl::ChunkList& chunkList, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Write to file - sync
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //! @param timeout timeout value
  //!
  //! @return number of bytes written or -1 if error
  //----------------------------------------------------------------------------
  int64_t fileWrite(XrdSfsFileOffset offset, const char* buffer,
                    XrdSfsXferSize length, uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Write to file - async
  //!
  //! @return number of bytes written or -1 if error
  //--------------------------------------------------------------------------
  int64_t fileWriteAsync(XrdSfsFileOffset offset, const char* buffer,
                         XrdSfsXferSize length, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Write to file - async
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //!
  //! @return future holding the status response
  //--------------------------------------------------------------------------
  std::future<XrdCl::XRootDStatus>
  fileWriteAsync(const char* buffer, XrdSfsFileOffset offset,
                 XrdSfsXferSize length);

  //----------------------------------------------------------------------------
  //! Truncate
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileTruncate(XrdSfsFileOffset offset, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Truncate asynchronous
  //!
  //! @param offset truncate file to this value
  //! @param timeout timeout value
  //!
  //! @return future holding the status response
  //----------------------------------------------------------------------------
  std::future<XrdCl::XRootDStatus>
  fileTruncateAsync(XrdSfsFileOffset offset, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Allocate file space
  //!
  //! @param length space to be allocated
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileFallocate(XrdSfsFileOffset length);

  //----------------------------------------------------------------------------
  //! Deallocate file space
  //!
  //! @param fromOffset offset start
  //! @param toOffset offset end
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileFdeallocate(XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset);

  //----------------------------------------------------------------------------
  //! Remove file
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileRemove(uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileSync(uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Get pointer to async meta handler object
  //!
  //! @return pointer to async handler, NULL otherwise
  //----------------------------------------------------------------------------
  void* fileGetAsyncHandler()
  {
    return 0;
  }

  //----------------------------------------------------------------------------
  //! Check for the existence of a file
  //!
  //! @param path to the file
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileExists();

  //----------------------------------------------------------------------------
  //! Close file
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileClose(uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileStat(struct stat* buf, uint16_t timeout = 0);

private:
  XrdFstOfsFile* mLogicalFile; ///< handler to logical file
  const XrdSecEntity* mSecEntity; ///< security entity

  //----------------------------------------------------------------------------
  //! Disable copy constructor
  //----------------------------------------------------------------------------
  LocalIo(const LocalIo&) = delete;

  //----------------------------------------------------------------------------
  //! Disable assign operator
  //----------------------------------------------------------------------------
  LocalIo& operator = (const LocalIo&) = delete;
};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_LOCALFILEIO_HH__
