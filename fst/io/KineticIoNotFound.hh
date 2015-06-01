//------------------------------------------------------------------------------
//! @file KineticIoNotFound.hh
//! @author Paul Hermann Lensing
//! @brief Enable compilation without kineticIO library
//------------------------------------------------------------------------------
#ifndef __EOSFST_KINETICFILEIO__HH__
#define __EOSFST_KINETICFILEIO__HH__

/*----------------------------------------------------------------------------*/
#include "fst/io/FileIo.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class KineticIo : public FileIo
{
public:
  class Attr : public eos::common::Attr
  {
  public:
    // ------------------------------------------------------------------------
    //! Set a binary attribute (name has to start with 'user.' !!!)
    // ------------------------------------------------------------------------
    bool Set (const char* name, const char* value, size_t len){
      return false;
    }

    // ------------------------------------------------------------------------
    //! Set a string attribute (name has to start with 'user.' !!!)
    // ------------------------------------------------------------------------
    bool Set (std::string key, std::string value){
      return false;
    }

    // ------------------------------------------------------------------------
    //! Get a binary attribute by name (name has to start with 'user.' !!!)
    // ------------------------------------------------------------------------
    bool Get (const char* name, char* value, size_t &size){
      return false;
    }

    // ------------------------------------------------------------------------
    //! Get a string attribute by name (name has to start with 'user.' !!!)
    // ------------------------------------------------------------------------
    std::string Get (std::string name){
      return "";
    }

    // ------------------------------------------------------------------------
    //! Factory function to create an attribute object
    // ------------------------------------------------------------------------
    static Attr* OpenAttr (const char* path){
      return NULL;
    }

    // ------------------------------------------------------------------------
    //! Non static Factory function to create an attribute object
    // ------------------------------------------------------------------------
    Attr* OpenAttribute (const char* path){
      return NULL;
    }
  };

  //--------------------------------------------------------------------------
  //! Open file
  //!
  //! @param path file path
  //! @param flags open flags
  //! @param mode open mode
  //! @param opaque opaque information
  //! @param timeout timeout value
  //! @return 0 if successful, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Open (const std::string& path, XrdSfsFileOpenMode flags, mode_t mode = 0,
      const std::string& opaque = "", uint16_t timeout = 0){
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
  int64_t Read (XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length,
      uint16_t timeout = 0){
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
  int64_t Write (XrdSfsFileOffset offset, const char* buffer,
      XrdSfsXferSize length, uint16_t timeout = 0){
    errno = ENOSYS;
    return SFS_ERROR;
  }

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
  int64_t ReadAsync (XrdSfsFileOffset offset, char* buffer,
      XrdSfsXferSize length, bool readahead = false, uint16_t timeout = 0){
    errno = ENOSYS;
    return SFS_ERROR;
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
  int64_t WriteAsync (XrdSfsFileOffset offset, const char* buffer,
      XrdSfsXferSize length, uint16_t timeout = 0){
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //--------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //! @param timeout timeout value
  //! @return 0 if successful, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Truncate (XrdSfsFileOffset offset, uint16_t timeout = 0){
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //--------------------------------------------------------------------------
  //! Allocate file space
  //!
  //! @param length space to be allocated
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Fallocate (XrdSfsFileOffset lenght){
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
  int Fdeallocate (XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset){
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //--------------------------------------------------------------------------
  //! Remove file
  //!
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Remove (uint16_t timeout = 0){
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //--------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Sync (uint16_t timeout = 0){
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //--------------------------------------------------------------------------
  //! Close file
  //!
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Close (uint16_t timeout = 0){
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
  int Stat (struct stat* buf, uint16_t timeout = 0){
    errno = ENOSYS;
    return SFS_ERROR;
  }

  //--------------------------------------------------------------------------
  //! Get pointer to async meta handler object
  //!
  //! @return pointer to async handler, NULL otherwise
  //--------------------------------------------------------------------------
  void* GetAsyncHandler (){
    return NULL;
  }

  //--------------------------------------------------------------------------
  //! Plug-in function to fill a statfs structure about the storage filling
  //! state
  //!
  //! @param path to statfs
  //! @param statfs return struct
  //! @return 0 if successful otherwise errno
  //--------------------------------------------------------------------------
  int Statfs (const char* path, struct statfs* statFs){
    return ENOSYS;
  }

  //--------------------------------------------------------------------------
  //! Open a curser to traverse a storage system
  //! @param subtree where to start traversing
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------
  void* ftsOpen(std::string subtree){
    return NULL;
  }

  //--------------------------------------------------------------------------
  //! Return the next path related to a traversal cursor obtained with ftsOpen
  //! @param fts_handle cursor obtained by ftsOpen
  //! @return returns full path (including mountpoint) for the next path
  //!         indicated by traversal cursor, empty string if there is no next
  //--------------------------------------------------------------------------
  std::string ftsRead(void* fts_handle){
    return "";
  }

  //--------------------------------------------------------------------------
  //! Close a traversal cursor
  //! @param fts_handle cursor to close
  //! @return 0 if fts_handle was an open cursor, otherwise -1
  //--------------------------------------------------------------------------
  int ftsClose(void* fts_handle){
    return -1;
  }

  //--------------------------------------------------------------------------
  //! Constructor
  //--------------------------------------------------------------------------
  KineticIo(){}

  //--------------------------------------------------------------------------
  //! Desructor
  //--------------------------------------------------------------------------
  ~KineticIo(){}

private:
  //! No copy constructor
  KineticIo (const KineticIo&) = delete;

  // No copy assignment operator.
  KineticIo& operator = (const KineticIo&) = delete;

};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_KINETICFILEIO__HH__
