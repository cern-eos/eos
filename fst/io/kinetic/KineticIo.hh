//------------------------------------------------------------------------------
//! @file KineticIO.hh
//! @author Paul Hermann Lensing <paul.lensing@cern.ch>
//! @brief Intermediate class used to forward Kinetic IO operations
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#ifndef __EOS_FST_KINETICFILEIO__HH__
#define __EOS_FST_KINETICFILEIO__HH__

#include "fst/io/FileIo.hh"
#include "common/plugin_manager/DynamicLibrary.hh"
#include "kio/KineticIoFactory.hh"
#include <memory>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class KineticLib use to load dynamically the Kinetic library
//------------------------------------------------------------------------------
class KineticLib
{
public:
  //----------------------------------------------------------------------------
  //! Acess a factory object. Function throws if if kineticio library has not
  //! been loaded correctly.
  //----------------------------------------------------------------------------
  static kio::LoadableKineticIoFactoryInterface* access();

private:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  KineticLib();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~KineticLib();

  //! Pointer to factory object, set in constructor
  kio::LoadableKineticIoFactoryInterface* mFactory;
  //! Keep the library object around so it will be unloaded correctly
  std::unique_ptr<eos::common::DynamicLibrary> mLibrary;
};

//------------------------------------------------------------------------------
//! Class KineticIo
//------------------------------------------------------------------------------
class KineticIo : public FileIo
{
public:
  //----------------------------------------------------------------------------
  //! Open file
  //!
  //! @param flags open flags
  //! @param mode open mode
  //! @param opaque opaque information
  //! @param timeout timeout value
  //! @return 0 if successful, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileOpen(XrdSfsFileOpenMode flags,
               mode_t mode = 0,
               const std::string& opaque = "",
               uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Read from file - sync
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param timeout timeout value
  //! @return number of bytes read or -1 if error
  //----------------------------------------------------------------------------
  int64_t fileRead(XrdSfsFileOffset offset,
                   char* buffer,
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
  //! @return 0(SFS_OK) if request successfully sent, otherwise -1(SFS_ERROR)
  //----------------------------------------------------------------------------
  virtual int64_t fileReadVAsync(XrdCl::ChunkList& chunkList,
                                 uint16_t timeout = 0)
  {
    errno = EOPNOTSUPP;
    return -1;
  }

  //----------------------------------------------------------------------------
  //! Write to file - sync
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //! @param timeout timeout value
  //! @return number of bytes written or -1 if error
  //----------------------------------------------------------------------------
  int64_t fileWrite(XrdSfsFileOffset offset,
                    const char* buffer,
                    XrdSfsXferSize length,
                    uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Read from file - async
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param readahead set if readahead is to be used
  //! @param timeout timeout value
  //! @return number of bytes read or -1 if error
  //----------------------------------------------------------------------------
  int64_t fileReadAsync(XrdSfsFileOffset offset,
                        char* buffer,
                        XrdSfsXferSize length,
                        bool readahead = false,
                        uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Write to file - async
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //! @param timeout timeout value
  //! @return number of bytes written or -1 if error
  //----------------------------------------------------------------------------
  int64_t fileWriteAsync(XrdSfsFileOffset offset,
                         const char* buffer,
                         XrdSfsXferSize length,
                         uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //! @param timeout timeout value
  //! @return 0 if successful, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileTruncate(XrdSfsFileOffset offset, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Allocate file space
  //!
  //! @param length space to be allocated
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileFallocate(XrdSfsFileOffset length);

  //----------------------------------------------------------------------------
  //! Deallocate file space
  //!
  //! @param fromOffset offset start
  //! @param toOffset offset end
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileFdeallocate(XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset);

  //----------------------------------------------------------------------------
  //! Remove file
  //!
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileRemove(uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileSync(uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Get pointer to async meta handler object
  //!
  //! @return pointer to async handler, NULL otherwise
  //----------------------------------------------------------------------------
  void* fileGetAsyncHandler();

  //----------------------------------------------------------------------------
  //! Check for the existence of a file
  //!
  //! @param path to the file
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileExists();

  //----------------------------------------------------------------------------
  //! Close file
  //!
  //! @param timeout timeout value
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
    // TODO: this needs a proper implementation for the Kinetic drives
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
  int attrSet(const char* name, const char* value, size_t len);

  //----------------------------------------------------------------------------
  //! Set a binary attribute (name has to start with 'user.' !!!)
  //!
  //! @param name attribute name
  //! @param value attribute value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int attrSet(string name, std::string value);

  //----------------------------------------------------------------------------
  //! Get a binary attribute by name
  //!
  //! @param name attribute name
  //! @param value contains attribute value upon success
  //! @param size the buffer size, after success the value size
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int attrGet(const char* name, char* value, size_t& size);

  //----------------------------------------------------------------------------
  //! Get a binary attribute by name
  //!
  //! @param name attribute name
  //! @param value contains attribute value upon success
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int attrGet(string name, std::string& value);

  //----------------------------------------------------------------------------
  //! Delete a binary attribute by name
  //!
  //! @param name attribute name
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int attrDelete(const char* name);

  //----------------------------------------------------------------------------
  //! List all attributes for the associated path
  //!
  //! @param list contains all attribute names for the set path upon success
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int attrList(std::vector<std::string>& list);

  class FtsHandle : public FileIo::FtsHandle
  {
  public:
    std::vector<std::string> cached;
    std::size_t current_index;

    FtsHandle(const char* dirp) : FileIo::FtsHandle(dirp)
    {
      cached.push_back(dirp);
      current_index = 1;
    }

    ~FtsHandle()
    { }
  };

  //----------------------------------------------------------------------------
  //! Open a cursor to traverse a storage system
  //!
  //! @return returns implementation dependent handle or 0 in case of error
  //----------------------------------------------------------------------------
  FileIo::FtsHandle* ftsOpen();

  //----------------------------------------------------------------------------
  //! Return the next path related to a traversal cursor obtained with ftsOpen
  //!
  //! @param fts_handle cursor obtained by ftsOpen
  //! @return returns implementation dependent handle or 0 in case of error
  //----------------------------------------------------------------------------
  std::string ftsRead(FileIo::FtsHandle* handle);

  //----------------------------------------------------------------------------
  //! Close a traversal cursor
  //!
  //! @param fts_handle cursor to close
  //! @return 0 if fts_handle was an open cursor, otherwise -1
  //----------------------------------------------------------------------------
  int ftsClose(FileIo::FtsHandle* handle);

  //----------------------------------------------------------------------------
  //! Constructor. May throw if the underlying kinetic io library object
  //! can not be constructed.
  //!
  //! @param path the path associated with this plugin instance
  //----------------------------------------------------------------------------
  KineticIo(std::string path);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~KineticIo();

  //----------------------------------------------------------------------------
  //! Plug-in function to fill a statfs structure about the storage filling
  //! state.
  //!
  //! @param path to statfs
  //! @param statfs return struct
  //!
  //! @return 0 if successful otherwise errno
  //----------------------------------------------------------------------------
  int Statfs(struct statfs* statFs);

private:
  //! Actual implementation class
  std::unique_ptr<kio::FileIoInterface> kio;
  //! No copy constructor
  KineticIo(const KineticIo&) = delete;
  // No copy assignment operator.
  KineticIo& operator=(const KineticIo&) = delete;
};

EOSFSTNAMESPACE_END

#endif  // __EOS_FST_KINETICFILEIO__HH__
