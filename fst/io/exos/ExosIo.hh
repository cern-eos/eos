//------------------------------------------------------------------------------
//! @file ExosIo.hh
//! @author Andreas Joachim Peters - CERN
//! @brief Class used for doing IO on rados clusters
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#ifndef __EOSFST_EXOSFILEIO__HH__
#define __EOSFST_EXOSFILEIO__HH__

/*----------------------------------------------------------------------------*/
#include "fst/io/FileIo.hh"
#include "exosfile.hh"
#include <sys/types.h>
#include <attr/xattr.h>

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class used for doing local IO operations
//------------------------------------------------------------------------------
class ExosIo : public FileIo
{
public:
  //--------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param handle to logical file
  //! @param client security entity
  //!
  //--------------------------------------------------------------------------
  ExosIo(std::string path) :
    FileIo(path, "ExosIO")
  {
    XrdOucString lpath = path.c_str();
    int qpos = lpath.find("?");
    if (qpos != STR_NPOS) {
      while (lpath.replace(":","&", qpos)) {}
    }

    mURL.FromString(lpath.c_str());
    XrdCl::URL::ParamsMap lparams = mURL.GetParams();

    if (getenv("EXOSIO_MD_POOL")) {
      lparams["rados.md"] = getenv("EXOSIO_MD_POOL");
    }
    if (getenv("EXOSIO_DATA_POOL")) {
      lparams["rados.data"] = getenv("EXOSIO_DATA_POOL");
    }
    if (getenv("EXOSIO_USER")) {
      lparams["rados.user"] = getenv("EXOSIO_USER");
    }
    if (getenv("EXOSIOS_CONFIG")) {
      lparams["rados.config"] = getenv("EXOSIO_CONFIG");
    }

    // opt. set debug mode
    if (getenv("EXOSIOS_DEBUG")) {
      mEXOS.debug();
    }

    mURL.SetParams(lparams);

    mCGI = mURL.GetParamsAsString().c_str();
    mCGI.erase(0,1);
    path = mURL.GetPath();
    // initialize exosfile object
    mEXOS.init(path, mCGI);
  };


  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  virtual ~ExosIo() {}

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
  //! Read from file - async
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param readahead set if readahead is to be used
  //! @param timeout timeout value
  //! @return number of bytes read or -1 if error
  //--------------------------------------------------------------------------
  int64_t fileReadAsync(XrdSfsFileOffset offset,
                        char* buffer,
                        XrdSfsXferSize length,
                        bool readahead = false,
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
                         uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Wait for all async IO
  //!
  //! @return global return code of async IO
  //--------------------------------------------------------------------------
  virtual int fileWaitAsyncIO();


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
  int fileFallocate(XrdSfsFileOffset length) ;

  //--------------------------------------------------------------------------
  //! Deallocate file space
  //!
  //! @param fromOffset offset start
  //! @param toOffset offset end
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileFdeallocate(XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset);

  //--------------------------------------------------------------------------
  //! Remove file
  //!
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileRemove(uint16_t timeout = 0);

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
  int fileExists();

  //--------------------------------------------------------------------------
  //! Close file
  //!
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileClose(uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int fileStat(struct stat* buf, uint16_t timeout = 0);

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


  class FtsHandle : public FileIo::FtsHandle
  {
  public:
    
    FtsHandle(const char* dirp) : FileIo::FtsHandle(dirp)
    {
    }
    
    ~FtsHandle()
    { }
    
    void set(void* opaque) {opaqueptr = opaque;}
    void* get() const {return opaqueptr;}
    
  private:
    void* opaqueptr;
  };


  //--------------------------------------------------------------------------
  //! Open a cursor to traverse a storage system
  //!
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------
  FileIo::FtsHandle* ftsOpen();

  //--------------------------------------------------------------------------
  //! Return the next path related to a traversal cursor obtained with ftsOpen
  //!
  //! @param fts_handle cursor obtained by ftsOpen
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------
  std::string ftsRead(FileIo::FtsHandle* handle);

  //--------------------------------------------------------------------------
  //! Close a traversal cursor
  //!
  //! @param fts_handle cursor to close
  //! @return 0 if fts_handle was an open cursor, otherwise -1
  //--------------------------------------------------------------------------
  int ftsClose(FileIo::FtsHandle* handle);


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

  ssize_t Ret2Errno(ssize_t rc) 
  {
    if (rc>=0) {
      rc = rc;
      errno = 0;
    } else {
      errno = -rc;
      rc = -1;
    }
    if (errno == EALREADY) {
      errno = 0; return 0;
    }
    return rc;
  }
private:
  //--------------------------------------------------------------------------
  //! Disable copy constructor
  //--------------------------------------------------------------------------
  ExosIo(const ExosIo&) = delete;
  
  
  //--------------------------------------------------------------------------
  //! Disable assign operator
  //--------------------------------------------------------------------------
  ExosIo& operator = (const ExosIo&) = delete;
  
  std::string mCGI;
  XrdCl::URL mURL;
  exosfile mEXOS;
};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_EXOSFILEIO_HH__
