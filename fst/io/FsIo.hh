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

/*----------------------------------------------------------------------------*/
#include "fst/io/FileIo.hh"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN
//------------------------------------------------------------------------------
//! Class used for doing local IO operations
//------------------------------------------------------------------------------

class FsIo : public FileIo {
public:
  //--------------------------------------------------------------------------
  //! Constructor
  //!
  //!
  //--------------------------------------------------------------------------
  FsIo ();


  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  virtual ~FsIo ();


  //--------------------------------------------------------------------------
  //! Open file
  //!
  //! @param path file path to local file
  //! @param flags open flags
  //! @param mode open mode
  //! @param opaque opaque information
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Open (const std::string& path,
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
  virtual int64_t Read (XrdSfsFileOffset offset,
                        char* buffer,
                        XrdSfsXferSize length,
                        uint16_t timeout = 0);


  //--------------------------------------------------------------------------
  //! Write to file - sync
  //!
  //! @param offset offset in file
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
  //! Read from file async - falls back to synchrounous mode
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
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
  //! Write to file async - falls back to synchronous mode
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
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Truncate (XrdSfsFileOffset offset, uint16_t timeout = 0);


  //--------------------------------------------------------------------------
  //! Allocate file space
  //!
  //! @param length space to be allocated
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Fallocate (XrdSfsFileOffset lenght);


  //--------------------------------------------------------------------------
  //! Deallocate file space
  //!
  //! @param fromOffset offset start
  //! @param toOffset offset end
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Fdeallocate (XrdSfsFileOffset fromOffset,
                           XrdSfsFileOffset toOffset);


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
  //! Check for the existance of a file
  //!
  //! @param path to the file
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  virtual int Exists (const char* path);

  //--------------------------------------------------------------------------                                                                                                                                 //! Delete a file
  //!
  //! @param path to the file to be deleted
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  virtual int Delete (const char* path);

  //--------------------------------------------------------------------------
  //! Get pointer to async meta handler object
  //!
  //! @return pointer to async handler, NULL otherwise
  //!
  //--------------------------------------------------------------------------
  virtual void* GetAsyncHandler ();

  virtual int Statfs (const char* path, struct statfs* statFs)
  {
    return ::statfs(path, statFs);
  }

  //--------------------------------------------------------------------------
  //! Class implementing extended attribute support
  //--------------------------------------------------------------------------

  class Attr : public eos::common::Attr {
  public:
    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    Attr () : eos::common::Attr (0)
    {
    }

    Attr (const char* path) : eos::common::Attr (path)
    {
    }

    // -----------------------------------------------------------------------
    // Destructor
    // -----------------------------------------------------------------------

    virtual ~Attr ()
    {
    }
  };

  class FtsHandle : public FileIo::FtsHandle {
    friend class FsIo;

  protected:
    char **paths;
    void *tree;
  public:

    FtsHandle (const char* dirp) : FileIo::FtsHandle (dirp)
    {
      paths = (char**) calloc(2, sizeof (char*));
      paths[0] = (char*) dirp;
      paths[1] = 0;
      tree = 0;
    }

    virtual ~FtsHandle ()
    {
      if (paths)
        free(paths);
      paths = 0;
    }
  };
  //--------------------------------------------------------------------------
  //! Open a cursor to traverse a storage system
  //! @param subtree where to start traversing
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------

  virtual FileIo::FtsHandle* ftsOpen (std::string subtree);

  //--------------------------------------------------------------------------
  //! Return the next path related to a traversal cursor obtained with ftsOpen
  //! @param fts_handle cursor obtained by ftsOpen
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------

  virtual std::string ftsRead (FileIo::FtsHandle* fts_handle);

  //--------------------------------------------------------------------------
  //! Close a traversal cursor
  //! @param fts_handle cursor to close
  //! @return 0 if fts_handle was an open cursor, otherwise -1
  //--------------------------------------------------------------------------

  virtual int ftsClose (FileIo::FtsHandle* fts_handle);

private:

  int mFd; //< file descriptor to filesystem file

  //--------------------------------------------------------------------------
  //! Disable copy constructor
  //--------------------------------------------------------------------------
  FsIo (const FsIo&) = delete;


  //--------------------------------------------------------------------------
  //! Disable assign operator
  //--------------------------------------------------------------------------
  FsIo& operator = (const FsIo&) = delete;


};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_FSFILEIO_HH__


