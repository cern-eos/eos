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

#ifndef __EOSFST_DAVIXIO_HH__
#define __EOSFST_DAVIXIO_HH__

/*----------------------------------------------------------------------------*/
#include <string>
/*----------------------------------------------------------------------------*/
#include "fst/io/FileIo.hh"
#include <davix/davix.hpp>

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! DAVIX Web IO plug-in
//------------------------------------------------------------------------------

class DavixIo : public FileIo {
public:

  //--------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param file original OFS file
  //! @param client security entity
  //!
  //--------------------------------------------------------------------------

  DavixIo ();

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------

  virtual
  ~DavixIo ();


  //--------------------------------------------------------------------------
  //! Open file
  //!
  //! @param path file path
  //! @param flags open flags
  //! @param mode open mode
  //! @param opaque opaque information
  //! @param timeout timeout value
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  int Open (const std::string& path,
            XrdSfsFileOpenMode flags,
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
  //!
  //! @return number of bytes read or -1 if error
  //!
  //--------------------------------------------------------------------------
  int64_t Read (XrdSfsFileOffset offset,
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
  int64_t Write (XrdSfsFileOffset offset,
                 const char* buffer,
                 XrdSfsXferSize length,
                 uint16_t timeout = 0);
  ;


  //--------------------------------------------------------------------------
  //! Read from file - async
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param readahead set if readahead is to be used
  //! @param timeout timeout value
  //!
  //! @return number of bytes read or -1 if error
  //!
  //--------------------------------------------------------------------------
  int64_t ReadAsync (XrdSfsFileOffset offset,
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
  int64_t WriteAsync (XrdSfsFileOffset offset,
                      const char* buffer,
                      XrdSfsXferSize length,
                      uint16_t timeout = 0);


  //--------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //! @param timeout timeout value
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  int Truncate (XrdSfsFileOffset offset, uint16_t timeout = 0);


  //--------------------------------------------------------------------------
  //! Allocate file space
  //!
  //! @param length space to be allocated
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------

  int
  Fallocate (XrdSfsFileOffset length)
  {
    return 0;
  }


  //--------------------------------------------------------------------------
  //! Deallocate file space
  //!
  //! @param fromOffset offset start
  //! @param toOffset offset end
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------

  int
  Fdeallocate (XrdSfsFileOffset fromOffset,
               XrdSfsFileOffset toOffset)
  {
    return 0;
  }


  //--------------------------------------------------------------------------
  //! Remove file
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------

  int
  Remove (uint16_t timeout = 0);


  //--------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------

  int Sync (uint16_t timeout = 0)
  {
    return 0;
  }


  //--------------------------------------------------------------------------
  //! Close file
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------

  int Close (uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //! @param timeout timeout value
  //!
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  int Stat (struct stat* buf, uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Check for the existance of a file
  //!
  //! @param path to the file
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Exists (const char* path);

  //--------------------------------------------------------------------------
  //! Delete a file
  //!
  //! @param path to the file to be deleted
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Delete (const char* path);

  //--------------------------------------------------------------------------
  //! Create a directory
  //!
  //! @param path to the directory to create
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Mkdir (const char* path, mode_t mode = S_IRWXU);

  //--------------------------------------------------------------------------
  //! Delete a directory
  //!
  //! @param path to the directory to delete
  //! @param mode to be set
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Rmdir (const char* path);

  //--------------------------------------------------------------------------
  //! Get pointer to async meta handler object
  //!
  //! @return pointer to async handler, NULL otherwise
  //!
  //--------------------------------------------------------------------------
  void* GetAsyncHandler ();

  //--------------------------------------------------------------------------
  //! traversing filesystem/storage routines
  //--------------------------------------------------------------------------

  class FtsHandle : public FileIo::FtsHandle {
  public:

    FtsHandle (const char* dirp) : FileIo::FtsHandle (dirp)
    {
    }

    virtual ~FtsHandle ();
  };

  //--------------------------------------------------------------------------
  //! Open a cursor to traverse a storage system
  //! @param subtree where to start traversing
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------

  FileIo::FtsHandle* ftsOpen (std::string subtree)
  {
    return 0;
  }

  //--------------------------------------------------------------------------
  //! Return the next path related to a traversal cursor obtained with ftsOpen
  //! @param fts_handle cursor obtained by ftsOpen
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------

  std::string ftsRead (FileIo::FtsHandle* fts_handle)
  {
    return "";
  }

  //--------------------------------------------------------------------------
  //! Close a traversal cursor
  //! @param fts_handle cursor to close
  //! @return 0 if fts_handle was an open cursor, otherwise -1
  //--------------------------------------------------------------------------

  int ftsClose (FileIo::FtsHandle* fts_handle)
  {
    return -1;
  }

  //--------------------------------------------------------------------------
  //! Plug-in function to fill a statfs structure about the storage filling
  //! state
  //! @param path to statfs
  //! @param statfs return struct
  //! @return 0 if successful otherwise errno
  //--------------------------------------------------------------------------

  int Statfs (const char* path, struct statfs* statFs);

  static Davix::Context gContext;

private:
  int SetErrno (int errcode, Davix::DavixError *err);
  bool mCreated;
  std::string mUrl;
  std::string mParent;
  off_t seq_offset;

  Davix::DavPosix mDav;
  DAVIX_FD* mFd;
};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_DAVIXIO_HH__

