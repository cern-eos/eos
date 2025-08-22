//------------------------------------------------------------------------------
//! @file NfsIo.hh
//! @author Robert-Paul Pasca - CERN
//! @brief Abstract class modelling an IO plugin
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#pragma once

#ifdef HAVE_NFS
#include "fst/io/FileIo.hh"
#include "common/FileMap.hh"
#include <string>
#include <mutex>
#include <nfsc/libnfs.h>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! NFS IO plug-in
//------------------------------------------------------------------------------

class NfsIo : public FileIo
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param path path to use
  //! @param file XrdFstOfsFile pointer
  //! @param client XrdSecEntity pointer
  //!
  //----------------------------------------------------------------------------
  NfsIo(std::string path, XrdFstOfsFile* file, const XrdSecEntity* client);


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~NfsIo();


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

  //--------------------------------------------------------------------------
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
  int64_t fileReadV(XrdCl::ChunkList& chunkList,
                    uint16_t timeout = 0)
  {
    // Operation not supported in NfsIo
    return -ENOTSUP;
  }

  //------------------------------------------------------------------------------
  //! Vector read - async
  //!
  //! @param chunkList list of chunks for the vector read
  //! @param timeout timeout value
  //!
  //! @return 0(SFS_OK) if request successfully sent, otherwise -1(SFS_ERROR)
  //------------------------------------------------------------------------------
  int64_t fileReadVAsync(XrdCl::ChunkList& chunkList,
                         uint16_t timeout = 0)
  {
    // Operation not supported in NfsIo
    return -ENOTSUP;
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
  //--------------------------------------------------------------------------
  int64_t fileWriteAsync(XrdSfsFileOffset offset,
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
  std::future<XrdCl::XRootDStatus>
  fileWriteAsync(const char* buffer, XrdSfsFileOffset offset,
                 XrdSfsXferSize length);

  //--------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileSync(uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Get pointer to async meta handler object
  //!
  //! @return pointer to async handler, NULL otherwise
  //--------------------------------------------------------------------------
  void* fileGetAsyncHandler();

  //--------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //! @param timeout timeout value
  //! @return 0 if successful, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
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

  //--------------------------------------------------------------------------
  //! Allocate file space
  //!
  //! @param length space to be allocated
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileFallocate(XrdSfsFileOffset length)
  {
    return 0;
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
    return 0;
  }

  //--------------------------------------------------------------------------
  //! Remove file
  //!
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileRemove(uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  int fileDelete(const char* path);

  //--------------------------------------------------------------------------
  //! Check for the existence of a file
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileExists();

  //--------------------------------------------------------------------------
  int Mkdir(const char* path, mode_t mode);
  int Rmdir(const char* path);

  //--------------------------------------------------------------------------
  //! Close file
  //!
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is e is set
  // ------------------------------------------------------------------------
  int fileClose(uint16_t  timeout = 0);

  //--------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileStat(struct stat* buf, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Execute implementation dependant commands
  //!
  //! @param buf stat buffer
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileFctl(const std::string& cmd, uint16_t timeout = 0)
  {
    // Operation not supported in NfsIO
    return -ENOTSUP;
  };

  // ------------------------------------------------------------------------
  //! Set a binary attribute (name has to start with 'user.' !!!)
  //!
  //! @param name attribute name
  //! @param value attribute value
  //! @param len value length
  //! @return 0 on success, -1 otherwise and error code is set
  // ------------------------------------------------------------------------
  int attrSet(const char* name, const char* value, size_t len);

  // ------------------------------------------------------------------------
  //! Set a binary attribute (name has to start with 'user.' !!!)
  //!
  //! @param name attribute name
  //! @param value attribute value
  //! @return 0 on success, -1 otherwise and error code is set
  // ------------------------------------------------------------------------
  int attrSet(std::string name, std::string value);

  // ------------------------------------------------------------------------
  //! Get a binary attribute by name
  //!
  //! @param name attribute name
  //! @param value contains attribute value upon success
  //! @param size the buffer size, after success the value size
  //! @return 0 on success, -1 otherwise and error code is set
  // ------------------------------------------------------------------------
  int attrGet(const char* name, char* value, size_t& size);

  // ------------------------------------------------------------------------
  //! Get a binary attribute by name
  //!
  //! @param name attribute name
  //! @param value contains attribute value upon success
  //! @return 0 on success, -1 otherwise and error code is set
  // ------------------------------------------------------------------------
  int attrGet(std::string name, std::string& value);

  // ------------------------------------------------------------------------
  //! Delete a binary attribute by name
  //!
  //! @param name attribute name
  //! @return 0 on success, -1 otherwise and error code is set
  // ------------------------------------------------------------------------
  int attrDelete(const char* name);

  // ------------------------------------------------------------------------
  //! List all attributes for the associated path
  //!
  //! @param list contains all attribute names for the set path upon success
  //! @return 0 on success, -1 otherwise and error code is set
  // ------------------------------------------------------------------------
  int attrList(std::vector<std::string>& list);

  // ------------------------------------------------------------------------
  //! Set attribute synchronization mode
  //!
  //! @param mode if true, every set attributes runs 'pull-modify-push',
  //!             otherwise runs just once in the destructor,
  //!             doing a 'pull-modify-modify-....-push' sequence
  // ------------------------------------------------------------------------
  void setAttrSync(bool mode = false)
  {
    mAttrSync = mode;
  }

  //--------------------------------------------------------------------------
  //! traversing filesystem/storage routines
  //--------------------------------------------------------------------------

  class FtsHandle : public FileIo::FtsHandle
  {
  public:

    FtsHandle(const char* dirp) : FileIo::FtsHandle(dirp)
    {
    }

    virtual ~FtsHandle();
  };

  //--------------------------------------------------------------------------
  //! Open a cursor to traverse a storage system
  //!
  //! @param options options for traversing the hierarchy
  //
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------
  FileIo::FtsHandle* ftsOpen(int options = 0) override
  {
    return 0;
  }

  //--------------------------------------------------------------------------
  //! Return the next path related to a traversal cursor obtained with ftsOpen
  //! @param fts_handle cursor obtained by ftsOpen
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------
  std::string ftsRead(FileIo::FtsHandle* fts_handle) override
  {
    return "";
  }

  //--------------------------------------------------------------------------
  //! Close a traversal cursor
  //! @param fts_handle cursor to close
  //! @return 0 if fts_handle was an open cursor, otherwise -1
  //--------------------------------------------------------------------------
  int ftsClose(FileIo::FtsHandle* fts_handle) override
  {
    return -1;
  }

  //--------------------------------------------------------------------------
  //! Plug-in function to fill a statfs structure about the storage filling
  //! state
  //! @param statfs return struct
  //! @return 0 if successful otherwise errno
  //--------------------------------------------------------------------------

  int Statfs(struct statfs* statFs);

  // NFS context used when you have a NFS Server configuration - not supported
  // static struct nfs_context* gContext;
  // static std::mutex gContextMutex; ///< mutex for thread-safe context initialization
  // static std::string gMountedPath; ///< path where NFS is mounted

private:
  std::string xattrPath() const; //< path to xattr file
  int loadAttrFile(); ///< pull attributes from disk
  int flushAttrFile(); ///< push modified attributes

  int mFd {-1};
  bool mCreated {false};
  bool mOpen {false};
  off_t seq_offset {0};
  off_t short_read_offset {0};

  std::string mFilePath; ///< file path
  bool mAttrLoaded;
  bool mAttrDirty;
  bool mAttrSync;

  eos::common::FileMap mFileMap; ///< extended attribute file map
};

EOSFSTNAMESPACE_END

#endif  // HAVE_NFS

