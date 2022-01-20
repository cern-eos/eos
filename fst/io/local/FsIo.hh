//------------------------------------------------------------------------------
//! @file FsIo.hh
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

#ifndef __EOSFST_FSFILEIO__HH__
#define __EOSFST_FSFILEIO__HH__

#include "fst/io/FileIo.hh"

EOSFSTNAMESPACE_BEGIN
//------------------------------------------------------------------------------
//! Class used for doing local IO operations
//------------------------------------------------------------------------------
class FsIo : public FileIo
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param path file path
  //----------------------------------------------------------------------------
  FsIo(std::string path);

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param path file path
  //! @param iotype type of underlying file
  //----------------------------------------------------------------------------
  FsIo(std::string path, std::string iotype);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FsIo();

  //----------------------------------------------------------------------------
  //! Open file
  //!
  //! @param flags open flags
  //! @param mode open mode
  //! @param opaque opaque information
  //! @param timeout timeout value
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int fileOpen(XrdSfsFileOpenMode flags,
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
  virtual std::future<XrdCl::XRootDStatus>
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
  //----------------------------------------------------------------------------
  virtual int64_t fileRead(XrdSfsFileOffset offset,
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
  //----------------------------------------------------------------------------
  virtual int64_t fileReadAsync(XrdSfsFileOffset offset, char* buffer,
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
  virtual int64_t fileReadPrefetch(XrdSfsFileOffset offset, char* buffer,
                                   XrdSfsXferSize length, uint16_t timeout = 0);

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
  virtual int64_t fileWrite(XrdSfsFileOffset offset,
                            const char* buffer,
                            XrdSfsXferSize length,
                            uint16_t timeout = 0);

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

  //----------------------------------------------------------------------------
  //! Vector read - async
  //!
  //! @param chunkList list of chunks for the vector read
  //! @param timeout timeout value
  //!
  //! @return 0(SFS_OK) if request successfully sent, otherwise -1 (SFS_ERROR)
  //----------------------------------------------------------------------------
  virtual int64_t fileReadVAsync(XrdCl::ChunkList& chunkList,
                                 uint16_t timeout = 0)
  {
    errno = EOPNOTSUPP;
    return -1;
  }

  //----------------------------------------------------------------------------
  //! Write to file - async
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //! @param timeout timeout value
  //!
  //! @return number of bytes written or -1 if error
  //----------------------------------------------------------------------------
  virtual int64_t fileWriteAsync(XrdSfsFileOffset offset,
                                 const char* buffer,
                                 XrdSfsXferSize length,
                                 uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Write to file - async
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //!
  //! @return future holding the status response
  //--------------------------------------------------------------------------
  virtual std::future<XrdCl::XRootDStatus>
  fileWriteAsync(const char* buffer, XrdSfsFileOffset offset,
                 XrdSfsXferSize length);

  //----------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //! @param timeout timeout value
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int fileTruncate(XrdSfsFileOffset offset, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Truncate asynchronous
  //!
  //! @param offset truncate file to this value
  //! @param timeout timeout value
  //!
  //! @return future holding the status response
  //----------------------------------------------------------------------------
  virtual std::future<XrdCl::XRootDStatus>
  fileTruncateAsync(XrdSfsFileOffset offset, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Allocate file space
  //!
  //! @param length space to be allocated
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int fileFallocate(XrdSfsFileOffset length);

  //----------------------------------------------------------------------------
  //! Deallocate file space
  //!
  //! @param fromOffset offset start
  //! @param toOffset offset end
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int fileFdeallocate(XrdSfsFileOffset fromOffset,
                              XrdSfsFileOffset toOffset);

  //----------------------------------------------------------------------------
  //! Remove file
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int fileRemove(uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int fileSync(uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Get pointer to async meta handler object
  //!
  //! @return pointer to async handler, NULL otherwise
  //----------------------------------------------------------------------------
  virtual void* fileGetAsyncHandler();

  //----------------------------------------------------------------------------
  //! Check for the existence of a file
  //!
  //! @param path to the file
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int fileExists();

  //----------------------------------------------------------------------------
  //! Close file
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int fileClose(uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int fileStat(struct stat* buf, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Execute implementation dependant commands
  //!
  //! @param buf stat buffer
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int fileFctl(const std::string& cmd, uint16_t timeout = 0)
  {
    return SFS_OK;
  }

  //----------------------------------------------------------------------------
  //! Set a binary attribute (name has to start with 'user.' !!!)
  //!
  //! @param name attribute name
  //! @param value attribute value
  //! @param len value length
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int attrSet(const char* name, const char* value, size_t len);

  //----------------------------------------------------------------------------
  //! Set a binary attribute (name has to start with 'user.' !!!)
  //!
  //! @param name attribute name
  //! @param value attribute value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int attrSet(string name, std::string value);

  //----------------------------------------------------------------------------
  //! Get a binary attribute by name
  //!
  //! @param name attribute name
  //! @param value contains attribute value upon success
  //! @param size the buffer size, after success the value size
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int attrGet(const char* name, char* value, size_t& size);

  //----------------------------------------------------------------------------
  //! Get a binary attribute by name
  //!
  //! @param name attribute name
  //! @param value contains attribute value upon success
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int attrGet(string name, std::string& value);

  //----------------------------------------------------------------------------
  //! Delete a binary attribute by name
  //!
  //! @param name attribute name
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int attrDelete(const char* name);

  //----------------------------------------------------------------------------
  //! List all attributes for the associated path
  //!
  //! @param list contains all attribute names for the set path upon success
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int attrList(std::vector<std::string>& list);

  //----------------------------------------------------------------------------
  //! Plug-in function to fill a statfs structure about the storage filling
  //! state
  //!
  //! @param path to statfs
  //! @param statfs return struct
  //!
  //! @return 0 if successful otherwise errno
  //----------------------------------------------------------------------------
  virtual int Statfs(struct statfs* statFs);

  //----------------------------------------------------------------------------
  //! Class implementing extended attribute support
  //----------------------------------------------------------------------------
  class FtsHandle : public FileIo::FtsHandle
  {
    friend class FsIo;

  protected:
    char** paths;
    void* tree;
  public:

    FtsHandle(const char* dirp) : FileIo::FtsHandle(dirp)
    {
      paths = (char**) calloc(2, sizeof(char*));
      paths[0] = (char*) dirp;
      paths[1] = 0;
      tree = 0;
    }

    virtual ~FtsHandle()
    {
      if (paths) {
        free(paths);
      }

      paths = 0;
    }
  };

  //----------------------------------------------------------------------------
  //! Open a cursor to traverse a storage system
  //!
  //! @param subtree where to start traversing
  //!
  //! @return returns implementation dependent handle or 0 in case of error
  //----------------------------------------------------------------------------
  virtual FileIo::FtsHandle* ftsOpen();

  //----------------------------------------------------------------------------
  //! Return the next path related to a traversal cursor obtained with ftsOpen
  //!
  //! @param fts_handle cursor obtained by ftsOpen
  //!
  //! @return returns implementation dependent handle or 0 in case of error
  //----------------------------------------------------------------------------
  virtual std::string ftsRead(FileIo::FtsHandle* fts_handle);

  //----------------------------------------------------------------------------
  //! Close a traversal cursor
  //!
  //! @param fts_handle cursor to close
  //!
  //! @return 0 if fts_handle was an open cursor, otherwise -1
  //----------------------------------------------------------------------------
  virtual int ftsClose(FileIo::FtsHandle* fts_handle);

private:
  int mFd; //< file descriptor to filesystem file

  //----------------------------------------------------------------------------
  //! Disable copy constructor
  //----------------------------------------------------------------------------
  FsIo(const FsIo&) = delete;

  //----------------------------------------------------------------------------
  //! Disable assign operator
  //----------------------------------------------------------------------------
  FsIo& operator = (const FsIo&) = delete;
};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_FSFILEIO_HH__
