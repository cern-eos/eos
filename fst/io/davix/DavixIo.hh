//------------------------------------------------------------------------------
//! @file DavixIo.hh
//! @author Andreas-Joachim Peters - CERN
//! @brief Abstract class modelling an IO plugin
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

#pragma once

#ifdef HAVE_DAVIX
#include <string>
#include "fst/io/FileIo.hh"
#include "common/FileMap.hh"
#include <davix/davix.hpp>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! DAVIX Web IO plug-in
//------------------------------------------------------------------------------

class DavixIo : public FileIo
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param url url to use
  //!
  //----------------------------------------------------------------------------
  DavixIo(std::string url, std::string s3credentials = "");


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~DavixIo();


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
               uint16_t timeout = 0);

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
    // Operation not supported in DavixIo
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
    // Operation not supported in DavixIo
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
  int fileDelete(const char* url);

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
    // Operation not supported in DavixIO
    return -ENOTSUP;
  };

  //--------------------------------------------------------------------------
  //! Download a remote file into a string object
  //! @param url from where to download
  //! @param download string where to place the contents
  //! @return 0 success, otherwise -1 and errno
  //--------------------------------------------------------------------------
  int Download(std::string url, std::string& download);

  //--------------------------------------------------------------------------
  //! Upload a string object into a remote file
  //! @param url from where to upload
  //! @param upload string to store into remote file
  //! @return 0 success, otherwise -1 and errno
  //--------------------------------------------------------------------------
  int Upload(std::string url, std::string& upload);

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
  int attrSet(string name, std::string value);

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
  int attrGet(string name, std::string& value);

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
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------

  FileIo::FtsHandle* ftsOpen()
  {
    return 0;
  }

  //--------------------------------------------------------------------------
  //! Return the next path related to a traversal cursor obtained with ftsOpen
  //! @param fts_handle cursor obtained by ftsOpen
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------

  std::string ftsRead(FileIo::FtsHandle* fts_handle)
  {
    return "";
  }

  //--------------------------------------------------------------------------
  //! Close a traversal cursor
  //! @param fts_handle cursor to close
  //! @return 0 if fts_handle was an open cursor, otherwise -1
  //--------------------------------------------------------------------------

  int ftsClose(FileIo::FtsHandle* fts_handle)
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

  static Davix::Context gContext;

private:
  int SetErrno(int errcode, Davix::DavixError** err, bool free_error = true);
  std::string RetrieveS3Credentials();
  bool mCreated;
  std::string mAttrUrl;
  std::string mOpaque;
  std::string mParent;
  off_t seq_offset;
  off_t short_read_offset;
  bool mShortRead;

  Davix::DavPosix mDav;
  DAVIX_FD* mFd;
  bool mAttrLoaded;
  bool mAttrDirty;
  bool mAttrSync;

  Davix::RequestParams mParams;;

  eos::common::FileMap mFileMap; ///< extended attribute file map
  bool mIsS3; ///< indicates an s3 protocol flavour
};

EOSFSTNAMESPACE_END

#endif  // HAVE_DAVIX

