//------------------------------------------------------------------------------
//! @file KineticIO.hh
//! @author Paul Hermann Lensing
//! @brief Intermediate class used to forward Kinetic IO operations
//------------------------------------------------------------------------------
#ifndef __EOSFST_KINETICFILEIO__HH__
#define __EOSFST_KINETICFILEIO__HH__

/*----------------------------------------------------------------------------*/
#include "fst/io/FileIo.hh"
#include <kio/FileIoInterface.hh>
#include <kio/FileAttrInterface.hh>
#include <memory>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class KineticIo : public FileIo
{
public:
  class Attr : public eos::common::Attr, public eos::common::LogId
  {
  private:
    //! The actual implementation class.
    std::unique_ptr<kio::FileAttrInterface> kattr;
  public:
    // ------------------------------------------------------------------------
    //! Set a binary attribute (name has to start with 'user.' !!!)
    // ------------------------------------------------------------------------
    bool Set (const char* name, const char* value, size_t len);

    // ------------------------------------------------------------------------
    //! Set a string attribute (name has to start with 'user.' !!!)
    // ------------------------------------------------------------------------
    bool Set (std::string key, std::string value);

    // ------------------------------------------------------------------------
    //! Get a binary attribute by name (name has to start with 'user.' !!!)
    // ------------------------------------------------------------------------
    bool Get (const char* name, char* value, size_t &size);

    // ------------------------------------------------------------------------
    //! Get a string attribute by name (name has to start with 'user.' !!!)
    // ------------------------------------------------------------------------
    std::string Get (std::string name);

    // ------------------------------------------------------------------------
    //! Factory function to create an attribute object
    // ------------------------------------------------------------------------
    static Attr* OpenAttr (const char* path);

    // ------------------------------------------------------------------------
    //! Non static Factory function to create an attribute object
    // ------------------------------------------------------------------------
    Attr* OpenAttribute (const char* path);

    // ------------------------------------------------------------------------
    // Constructor
    // ------------------------------------------------------------------------
    explicit Attr (std::unique_ptr<kio::FileAttrInterface> a);

    // ------------------------------------------------------------------------
    // Destructor
    // ------------------------------------------------------------------------
    virtual ~Attr ();
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
      const std::string& opaque = "", uint16_t timeout = 0);

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
  int64_t Write (XrdSfsFileOffset offset, const char* buffer,
      XrdSfsXferSize length, uint16_t timeout = 0);

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
      XrdSfsXferSize length, bool readahead = false, uint16_t timeout = 0);

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
      XrdSfsXferSize length, uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //! @param timeout timeout value
  //! @return 0 if successful, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Truncate (XrdSfsFileOffset offset, uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Allocate file space
  //!
  //! @param length space to be allocated
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Fallocate (XrdSfsFileOffset lenght);

  //--------------------------------------------------------------------------
  //! Deallocate file space
  //!
  //! @param fromOffset offset start
  //! @param toOffset offset end
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Fdeallocate (XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset);

  //--------------------------------------------------------------------------
  //! Remove file
  //!
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Remove (uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Sync (uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Close file
  //!
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Close (uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //! @param timeout timeout value
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Stat (struct stat* buf, uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Get pointer to async meta handler object
  //!
  //! @return pointer to async handler, NULL otherwise
  //--------------------------------------------------------------------------
  void* GetAsyncHandler ();

  //--------------------------------------------------------------------------
  //! Plug-in function to fill a statfs structure about the storage filling
  //! state
  //!
  //! @param path to statfs
  //! @param statfs return struct
  //! @return 0 if successful otherwise errno
  //--------------------------------------------------------------------------
  int Statfs (const char* path, struct statfs* statFs);

  //--------------------------------------------------------------------------
  //! Check for the existence of a file
  //!
  //! @param path to the file
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Exists(const char* path);

  //--------------------------------------------------------------------------
  //! Delete a file
  //!
  //! @param path to the file to be deleted
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  int Delete(const char* path);

  //--------------------------------------------------------------------------
  //! Open a curser to traverse a storage system
  //! @param subtree where to start traversing
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------
  void* ftsOpen(std::string subtree);

  //--------------------------------------------------------------------------
  //! Return the next path related to a traversal cursor obtained with ftsOpen
  //! @param fts_handle cursor obtained by ftsOpen
  //! @return returns full path (including mountpoint) for the next path
  //!         indicated by traversal cursor, empty string if there is no next
  //--------------------------------------------------------------------------
  std::string ftsRead(void* fts_handle);

  //--------------------------------------------------------------------------
  //! Close a traversal cursor
  //! @param fts_handle cursor to close
  //! @return 0 if fts_handle was an open cursor, otherwise -1
  //--------------------------------------------------------------------------
  int ftsClose(void* fts_handle);

  //--------------------------------------------------------------------------
  //! Constructor
  //! @param cache_capacity maximum cache size
  //--------------------------------------------------------------------------
  explicit KineticIo ();

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  ~KineticIo ();

private:
  //! the actual implementation class
  std::unique_ptr<kio::FileIoInterface> kio;

  //! No copy constructor
  KineticIo (const KineticIo&) = delete;

  // No copy assignment operator.
  KineticIo& operator = (const KineticIo&) = delete;

};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_KINETICFILEIO__HH__
