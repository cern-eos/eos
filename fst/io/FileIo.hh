//------------------------------------------------------------------------------
//! @file FileIo.hh
//! @author Elvin-Alin Sindrilaru - CERN
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

#ifndef __EOSFST_FILEIO_HH__
#define __EOSFST_FILEIO_HH__

/*----------------------------------------------------------------------------*/
#include <string>
/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/Statfs.hh"
#include "common/Attr.hh"
#include "fst/Namespace.hh"
#include "fst/XrdFstOfsFile.hh"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! The truncate offset (1TB) is used to indicate that a file should be deleted
//! during the close as there is no better interface usable via XrdCl to
//! communicate a deletion on a open file
//------------------------------------------------------------------------------
#define EOS_FST_DELETE_FLAG_VIA_TRUNCATE_LEN 1024 * 1024 * 1024 * 1024ll
#define EOS_FST_NOCHECKSUM_FLAG_VIA_TRUNCATE_LEN ((1024 * 1024 * 1024 * 1024ll)+1)

//! Forward declaration
class XrdFstOfsFile;

//------------------------------------------------------------------------------
//! Abstract class modelling an IO plugin
//------------------------------------------------------------------------------

class FileIo : public eos::common::LogId {
public:

  //--------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param file original OFS file
  //! @param client security entity
  //!
  //--------------------------------------------------------------------------

  FileIo () :
  eos::common::LogId (),
  mFilePath (""),
  mLastUrl (""),
  mType ("FileIo"),
  mLastErrMsg("")
  {
    //empty
  }


  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------

  virtual
  ~FileIo ()
  {
    //empty
  }


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
  virtual int Open (const std::string& path,
                    XrdSfsFileOpenMode flags,
                    mode_t mode = 0,
                    const std::string& opaque = "",
                    uint16_t timeout = 0) = 0;


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
                        uint16_t timeout = 0) = 0;


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
                         uint16_t timeout = 0) = 0;


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
  virtual int64_t ReadAsync (XrdSfsFileOffset offset,
                             char* buffer,
                             XrdSfsXferSize length,
                             bool readahead = false,
                             uint16_t timeout = 0) = 0;


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
                              uint16_t timeout = 0) = 0;


  //--------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //! @param timeout timeout value
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Truncate (XrdSfsFileOffset offset, uint16_t timeout = 0) = 0;


  //--------------------------------------------------------------------------
  //! Allocate file space
  //!
  //! @param length space to be allocated
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------

  virtual int
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

  virtual int
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

  virtual int
  Remove (uint16_t timeout = 0)
  {
    return 0;
  }


  //--------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Sync (uint16_t timeout = 0) = 0;


  //--------------------------------------------------------------------------
  //! Close file
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Close (uint16_t timeout = 0) = 0;


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
  virtual int Stat (struct stat* buf, uint16_t timeout = 0) = 0;

  //--------------------------------------------------------------------------
  //! Check for the existance of a file
  //!
  //! @param path to the file
  //!
  //! @return true if exists, otherwise false
  //--------------------------------------------------------------------------
  virtual bool Exists(const char* path) = 0;

  //--------------------------------------------------------------------------
  //! Delete a file
  //!
  //! @param path to the file to be deleted
  //!
  //! @return 0 if successfull, otherwise errno
  //--------------------------------------------------------------------------
  virtual int Delete(const char* path) = 0;

  //--------------------------------------------------------------------------
  //! Get pointer to async meta handler object
  //!
  //! @return pointer to async handler, NULL otherwise
  //!
  //--------------------------------------------------------------------------
  virtual void* GetAsyncHandler () = 0;


  //--------------------------------------------------------------------------
  //! Get path to current file
  //--------------------------------------------------------------------------

  const std::string&
  GetPath ()
  {
    return mFilePath;
  }

  //--------------------------------------------------------------------------
  //! Get last used URL to current file
  //--------------------------------------------------------------------------

  const std::string&
  GetLastUrl ()
  {
    return mLastUrl;
  }

  //--------------------------------------------------------------------------
  //! traversing filesystem/storage routines
  //--------------------------------------------------------------------------

  //--------------------------------------------------------------------------
  //! Open a curser to traverse a storage system
  //! @param subtree where to start traversing
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------

  virtual void* ftsOpen(std::string subtree) {return 0;}

  //--------------------------------------------------------------------------
  //! Return the next path related to a traversal cursor obtained with ftsOpen
  //! @param fts_handle cursor obtained by ftsOpen
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------
  virtual std::string ftsRead(void* fts_handle) {return "";}

  //--------------------------------------------------------------------------
  //! Close a traversal cursor
  //! @param fts_handle cursor to close
  //! @return 0 if fts_handle was an open cursor, otherwise -1
  //--------------------------------------------------------------------------
  virtual int ftsClose(void* fts_handle) {return -1;}

  //--------------------------------------------------------------------------
  //! Callback function to fill a statfs structure about the storage filling
  //! state
  //! @param data containing path, return code and statfs structure
  //! @return 0 if successful otherwise errno
  //--------------------------------------------------------------------------

  static int StatfsCB (eos::common::Statfs::Callback::callback_data* data)
  {
    data->retc = ((FileIo*) (data->caller))->Statfs(data->path, data->statfs);
    return data->retc;
  }

  //--------------------------------------------------------------------------
  //! Plug-in function to fill a statfs structure about the storage filling
  //! state
  //! @param path to statfs
  //! @param statfs return struct
  //! @return 0 if successful otherwise errno
  //--------------------------------------------------------------------------

  virtual int Statfs (const char* path, struct statfs* statFs)
  {
    eos_warning("msg=\"base class statfs called\"");
    return -ENODATA;
  }

  //--------------------------------------------------------------------------
  //! Class implementing extended attribute support
  //--------------------------------------------------------------------------

  class Attr : public eos::common::Attr {
  public:
    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    Attr ()
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

  Attr& DoAttr ()
  {
    return mAttr;
  }


protected:
  Attr mAttr;
  std::string mFilePath; ///< path to current physical file
  std::string mLastUrl; ///< last used url if remote file
  std::string mType; ///< type
  std::string mLastErrMsg; ///< last error message seen

  //--------------------------------------------------------------------------
  //! Get last error message
  //--------------------------------------------------------------------------
  const std::string&
  GetLastErrMsg ()
  {
    return mLastErrMsg;
  }
};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_FILEIO_HH__

