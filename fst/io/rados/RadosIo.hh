//------------------------------------------------------------------------------
//! @file RadosIo.hh
//! @author Elvin-Alin Sindrilaru - CERN
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

#ifndef __EOSFST_RADOSFILEIO__HH__
#define __EOSFST_RADOSFILEIO__HH__

/*----------------------------------------------------------------------------*/
#include "fst/io/FileIo.hh"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class used for doing local IO operations
//------------------------------------------------------------------------------
class RadosIo : public FileIo
{
public:
  //--------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param handle to logical file
  //! @param client security entity
  //!
  //--------------------------------------------------------------------------
  RadosIo(std::string path) :
    FileIo(path, "RadosIO")
  {};


  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  virtual ~RadosIo() {}

  //--------------------------------------------------------------------------
  //! Open file
  //!
  //! @param flags open flags
  //! @param mode open mode
  //! @param opaque opaque information
  //! @param timeout timeout value
  //! @return 0 if successful, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileOpen(XrdSfsFileOpenMode flags,
               mode_t mode = 0,
               const std::string& opaque = "",
               uint16_t timeout = 0)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //--------------------------------------------------------------------------
  //! Read from file - sync
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param timeout timeout value
  //! @return number of bytes read or -1 if error
  //--------------------------------------------------------------------------
  int64_t fileRead(XrdSfsFileOffset offset,
                   char* buffer,
                   XrdSfsXferSize length,
                   uint16_t timeout = 0)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

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
                        XrdSfsXferSize length, uint16_t timeout = 0)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

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
                           XrdSfsXferSize length, uint16_t timeout = 0)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //--------------------------------------------------------------------------
  //! Write to file - sync
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //! @param timeout timeout value
  //! @return number of bytes written or -1 if error
  //--------------------------------------------------------------------------
  int64_t fileWrite(XrdSfsFileOffset offset,
                    const char* buffer,
                    XrdSfsXferSize length,
                    uint16_t timeout = 0)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //----------------------------------------------------------------------------
  //! Vector read - sync
  //!
  //! @param chunkList list of chunks for the vector read
  //! @param timeout timeout value
  //!
  //! @return number of bytes read of -1 if error
  //----------------------------------------------------------------------------
  virtual int64_t fileReadV(XrdCl::ChunkList& chunkList,
                            uint16_t timeout = 0)
  {
    errno = EOPNOTSUPP;
    return -1;
  }

  //------------------------------------------------------------------------------
  //! Vector read - async
  //!
  //! @param chunkList list of chunks for the vector read
  //! @param timeout timeout value
  //!
  //! @return 0(SFS_OK) if request successfully sent, otherwise -1(SFS_ERROR)
  //------------------------------------------------------------------------------
  virtual int64_t fileReadVAsync(XrdCl::ChunkList& chunkList,
                                 uint16_t timeout = 0)
  {
    errno = EOPNOTSUPP;
    return -1;
  }

  //--------------------------------------------------------------------------
  //! Write to file - async
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //! @param timeout timeout value
  //! @return number of bytes written or -1 if error
  //--------------------------------------------------------------------------
  int64_t fileWriteAsync(XrdSfsFileOffset offset,
                         const char* buffer,
                         XrdSfsXferSize length,
                         uint16_t timeout = 0)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

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
                 XrdSfsXferSize length)
  {
    std::promise<XrdCl::XRootDStatus> wr_promise;
    std::future<XrdCl::XRootDStatus> wr_future = wr_promise.get_future();
    wr_promise.set_value(XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errUnknown,
                         ENOSYS, "operation not supported"));
    return wr_future;
  }

  //--------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //! @param timeout timeout value
  //! @return 0 if successful, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileTruncate(XrdSfsFileOffset offset, uint16_t timeout = 0)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //--------------------------------------------------------------------------
  //! Allocate file space
  //!
  //! @param length space to be allocated
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileFallocate(XrdSfsFileOffset length)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //--------------------------------------------------------------------------
  //! Deallocate file space
  //!
  //! @param fromOffset offset start
  //! @param toOffset offset end
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileFdeallocate(XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //--------------------------------------------------------------------------
  //! Remove file
  //!
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileRemove(uint16_t timeout = 0)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //--------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileSync(uint16_t timeout = 0)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //--------------------------------------------------------------------------
  //! Get pointer to async meta handler object
  //!
  //! @return pointer to async handler, NULL otherwise
  //--------------------------------------------------------------------------
  void* fileGetAsyncHandler()
  {
    errno = ENOSYS;
    return NULL;
  }

  //--------------------------------------------------------------------------
  //! Check for the existence of a file
  //!
  //! @param path to the file
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileExists()
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //--------------------------------------------------------------------------
  //! Close file
  //!
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileClose(uint16_t timeout = 0)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //--------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileStat(struct stat* buf, uint16_t timeout = 0)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //----------------------------------------------------------------------------
  //! Execute implementation dependant command
  //!
  //! @param cmd command
  //! @param client client identity
  //!
  //! @return 0 if successful, -1 otherwise
  //----------------------------------------------------------------------------
  int fileFctl(const std::string& cmd, uint16_t timeout = 0)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  // ------------------------------------------------------------------------
  //! Set a binary attribute (name has to start with 'user.' !!!)
  //!
  //! @param name attribute name
  //! @param value attribute value
  //! @param len value length
  //! @return 0 on success, -1 otherwise and error code is set
  // ------------------------------------------------------------------------
  int attrSet(const char* name, const char* value, size_t len)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  // ------------------------------------------------------------------------
  //! Set a binary attribute (name has to start with 'user.' !!!)
  //!
  //! @param name attribute name
  //! @param value attribute value
  //! @return 0 on success, -1 otherwise and error code is set
  // ------------------------------------------------------------------------
  int attrSet(string name, std::string value)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  // ------------------------------------------------------------------------
  //! Get a binary attribute by name
  //!
  //! @param name attribute name
  //! @param value contains attribute value upon success
  //! @param size the buffer size, after success the value size
  //! @return 0 on success, -1 otherwise and error code is set
  // ------------------------------------------------------------------------
  int attrGet(const char* name, char* value, size_t& size)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  // ------------------------------------------------------------------------
  //! Get a binary attribute by name
  //!
  //! @param name attribute name
  //! @param value contains attribute value upon success
  //! @return 0 on success, -1 otherwise and error code is set
  // ------------------------------------------------------------------------
  int attrGet(string name, std::string& value)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  // ------------------------------------------------------------------------
  //! Delete a binary attribute by name
  //!
  //! @param name attribute name
  //! @return 0 on success, -1 otherwise and error code is set
  // ------------------------------------------------------------------------
  int attrDelete(const char* name)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  // ------------------------------------------------------------------------
  //! List all attributes for the associated path
  //!
  //! @param list contains all attribute names for the set path upon success
  //! @return 0 on success, -1 otherwise and error code is set
  // ------------------------------------------------------------------------
  int attrList(std::vector<std::string>& list)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //--------------------------------------------------------------------------
  //! Open a cursor to traverse a storage system
  //!
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------
  FileIo::FtsHandle* ftsOpen()
  {
    errno = ENOSYS;
    return NULL;
  }

  //--------------------------------------------------------------------------
  //! Return the next path related to a traversal cursor obtained with ftsOpen
  //!
  //! @param fts_handle cursor obtained by ftsOpen
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------
  std::string ftsRead(FileIo::FtsHandle* handle)
  {
    return "";
  }

  //--------------------------------------------------------------------------
  //! Close a traversal cursor
  //!
  //! @param fts_handle cursor to close
  //! @return 0 if fts_handle was an open cursor, otherwise -1
  //--------------------------------------------------------------------------
  int ftsClose(FileIo::FtsHandle* handle)
  {
    errno = ENOSYS;
    return SFS_ERROR;
  }


  //--------------------------------------------------------------------------
  //! Plug-in function to fill a statfs structure about the storage filling
  //! state
  //! @param path to statfs
  //! @param statfs return struct
  //! @return 0 if successful otherwise errno
  //--------------------------------------------------------------------------
  int Statfs(struct statfs* statFs)
  {
    //! IMPLEMENT ME PROPERLY!
    statFs->f_type = 0xceff;
    statFs->f_bsize = 1 * 1024 * 1024;
    statFs->f_blocks = 4 * 1024 * 1024;
    statFs->f_bfree = 4 * 1024 * 1024;
    statFs->f_bavail = 4 * 1024 * 1024;
    statFs->f_files = 4 * 1024 * 1024;
    statFs->f_ffree = 4 * 1024 * 1024;
    return 0;
  }

private:
  //--------------------------------------------------------------------------
  //! Disable copy constructor
  //--------------------------------------------------------------------------
  RadosIo(const RadosIo&) = delete;


  //--------------------------------------------------------------------------
  //! Disable assign operator
  //--------------------------------------------------------------------------
  RadosIo& operator = (const RadosIo&) = delete;


};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_LOCALFILEIO_HH__
