//------------------------------------------------------------------------------
//! @file KineticIO.hh
//! @author Paul Hermann Lensing
//! @brief Class used for doing Kinetic IO operations
//------------------------------------------------------------------------------
#ifndef __EOSFST_KINETICFILEIO__HH__
#define __EOSFST_KINETICFILEIO__HH__

/*----------------------------------------------------------------------------*/
#include "fst/io/FileIo.hh"
#include "kinetic/kinetic.h"
#include "KineticChunk.hh"
#include "KineticClusterMap.hh"
#include <condition_variable>
#include <unordered_map>
#include <chrono>
#include <mutex>
#include <queue>
#include <list>

/* <cstdatomic> is part of gcc 4.4.x experimental C++0x support... <atomic> is
 * what actually made it into the standard.
#if __GNUC__ == 4 && (__GNUC_MINOR__ == 4)
    #include <cstdatomic>
#else
    #include <atomic>
#endif
 */
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

typedef std::shared_ptr<KineticClusterInterface> ClusterPointer;


//------------------------------------------------------------------------------
//! String utility functions. Don't really fit in any class; used by
//! KineticIo and nested classes. KineticIo::Attr does not require a KineticIo
//! object.
//------------------------------------------------------------------------------
namespace path_util{
  //--------------------------------------------------------------------------
  //! Create the kinetic key from the supplied path and chunk number.
  //!
  //! @param path base path
  //! @param chunk_number the chunk number
  //! @return the kinetic key for the requested chunk
  //--------------------------------------------------------------------------
  std::shared_ptr<const std::string> chunkKey(const char * path,
          int chunk_number);
  
  //--------------------------------------------------------------------------
  //! Extract the location id from the supplied path.
  //!
  //! @param path base path of the form kinetic:ID:path
  //! @return the location drive id
  //--------------------------------------------------------------------------
  std::string extractID(const std::string& path);
}


//------------------------------------------------------------------------------
//! Class used for doing Kinetic IO operations
//------------------------------------------------------------------------------
class KineticIo  : public FileIo
{
public:
  class Attr : public eos::common::Attr, public eos::common::LogId
  {
  private:
    ClusterPointer cluster;
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
    explicit Attr (const char* path, ClusterPointer con);

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
  explicit KineticIo (size_t cache_capacity=10);

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  ~KineticIo ();

private:
    //--------------------------------------------------------------------------
    //! Implementation of read and write functionality as most of the code is
    //! shared.
    //--------------------------------------------------------------------------
    int64_t doReadWrite (XrdSfsFileOffset offset, char* buffer,
		XrdSfsXferSize length, uint16_t timeout, int mode);

private:
  class LastChunkNumber : public eos::common::LogId {

  public:
    //--------------------------------------------------------------------------
     //! Checks if the chunk number stored in last_chunk_number is still valid,
     //! if not it will query the drive to obtain the up-to-date last chunk and
     //! store it (so it can get requested with get() by the user).
     //!
     //! @return 0 if successful otherwise errno
     //-------------------------------------------------------------------------
     int verify();

     //-------------------------------------------------------------------------
     //! Get the chunk number of the last chunk.
     //!
     //! @return currently set last chunk number
     //-------------------------------------------------------------------------
     int get() const;

     //-------------------------------------------------------------------------
     //! Set the supplied chunk number as last chunk.
     //!
     //! @param chunk_number the chunk number to be set
     //-------------------------------------------------------------------------
     void set(int chunk_number);

     //-------------------------------------------------------------------------
     //! Constructor
     //!
     //! @param parent reference to the enclosing KineticIo object
     //-------------------------------------------------------------------------
     explicit LastChunkNumber(KineticIo & parent);

     //-------------------------------------------------------------------------
     //! Destructor.
     //-------------------------------------------------------------------------
     ~LastChunkNumber();

  private:
      //! reference to the enclosing KineticIo object
      KineticIo & parent;

      //! currently set last chunk number
      int last_chunk_number;

      //! time point it was verified that the last_chunk_number is correct
      //! (another client might have created a later chunk)
      std::chrono::system_clock::time_point last_chunk_number_timestamp;
  };

  //----------------------------------------------------------------------------
  //! LRU cache for KineticChunks with background flushing. Obtains chunks
  //! from the drive automatically if not cached. Flushes chunks in the
  //! background if requested. Is not threadsafe (KineticIo is called single
  //! threaded...).
  //----------------------------------------------------------------------------
  class KineticChunkCache {

  public:
    //--------------------------------------------------------------------------
    //! Obtain 1 MB chunk associated with the file path set in constructor, chunk
    //! numbers start at 0
    //!
    //! @param chunk_number specifies which 1 MB chunk in the file is requested,
    //! @param chunk points to chunk on success, otherwise not changed
    //! @param create if set implies the chunk (probably) does not exist on the
    //!        cluster yet
    //! @return 0 if successful otherwise errno
    //--------------------------------------------------------------------------
    int get (int chunk_number, std::shared_ptr<KineticChunk>& chunk,
            bool create=false);

    //--------------------------------------------------------------------------
    //! Blocking flush of the entire cache.
    //!
    //! @return 0 if successful otherwise errno
    //--------------------------------------------------------------------------
    int flush();

    //--------------------------------------------------------------------------
    //! Drop everything. Don't flush dirty chunks.
    //--------------------------------------------------------------------------
    void clear();

    //--------------------------------------------------------------------------
    //! Add chunk number to the todo list of the background thread.
    //--------------------------------------------------------------------------
    void requestFlush(int chunk_number);

    //--------------------------------------------------------------------------
    //! Constructor.
    //!
    //! @param parent reference to the enclosing KineticIo object
    //! @param cache_capacity maximum number of items in chache
    //--------------------------------------------------------------------------
    explicit KineticChunkCache(KineticIo & parent, size_t cache_capacity);

    //--------------------------------------------------------------------------
    //! Destructor.
    //--------------------------------------------------------------------------
    ~KineticChunkCache();

private:
    //--------------------------------------------------------------------------
    //! Function executed by the background flushing thread....
    //--------------------------------------------------------------------------
    void background();

private:
    //! reference to the enclosing KineticIo object
    KineticIo & parent;

    //! maximum number of items allowed in the cache
    size_t capacity;

    //! keeping track of lru order
    std::list<int> lru_order;

    //! the cache... could increase performance a little bit using
    //! <ListIterator, KineticChunk> elements
    std::unordered_map<int, std::shared_ptr<KineticChunk>> cache;

    //! contains all chunk numbers scheduled for a background flush
    std::queue<int> background_queue;

    //! signal when a new item was queued for background flush
    std::condition_variable background_trigger;

    //! thread safe access to background_queue
    std::mutex background_mutex;

    //! background thread loops until run is set to false
    bool background_run;

    //! background thread sets shutdown to true on exit, signaling that the
    //! cache destructor can safely complete
    bool background_shutdown;
  };

private:
  //! we don't want to have to look in the drive map for every access...
  ClusterPointer cluster;

  //! cache & background flush functionality.
  KineticChunkCache cache;

  //! keep track of the last chunk to answer stat requests reasonably
  LastChunkNumber lastChunkNumber;

  //! mFilePath has form kinetic:ID:path, just store pointer to path.
  const char * chunkNameBase;

private:
    KineticIo (const KineticIo&) = delete;
    KineticIo& operator = (const KineticIo&) = delete;
};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_KINETICFILEIO_HH__
