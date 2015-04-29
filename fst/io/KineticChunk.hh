//------------------------------------------------------------------------------
//! @file KineticChunk.hh
//! @author Paul Hermann Lensing
//! @brief High(er) level API for Kinetic keys. 
//------------------------------------------------------------------------------
#ifndef KINETICCHUNK_HH_
#define KINETICCHUNK_HH_

/*----------------------------------------------------------------------------*/
#include <chrono>
#include <string>
#include <list>
#include "kinetic/kinetic.h"
/*----------------------------------------------------------------------------*/

typedef std::shared_ptr<kinetic::BlockingKineticConnectionInterface> ConnectionPointer;


//------------------------------------------------------------------------------
//! High(er) level API for Kinetic keys. Handles incremental updates and resolves concurrency
//! on chunk-basis. For multi-chunk atomic writes the caller will have to do appropriate locking
//! himself.
//------------------------------------------------------------------------------
class KineticChunk {
public:
    //! Initialized to 1 second staleness (in milliseconds)
    static const int expiration_time;  
    //! Initialized to 1 MB chunk capacity (in bytes)
    static const int capacity; 

public:
    //--------------------------------------------------------------------------
    //! Reading is guaranteed up-to-date within expiration_time limits.
    //! 
    //! @param buffer output buffer 
    //! @param offset offset in the chunk to start reading
    //! @param length number of bytes to read 
    //! @return 0 if successful otherwise errno
    //--------------------------------------------------------------------------
    int read(char* const buffer, off_t offset, size_t length);

    //--------------------------------------------------------------------------
    //! Writing in-memory only, never flushes to the drive. 
    //! 
    //! @param buffer input buffer 
    //! @param offset offset in the chunk to start writing
    //! @param length number of bytes to write 
    //! @return 0 if successful otherwise errno
    //--------------------------------------------------------------------------
    int write(const char* const buffer, off_t offset, size_t length);

    //--------------------------------------------------------------------------
    //! Truncate in-memory only, never flushes to the drive. 
    //! 
    //! @param offset the new target chunk size 
    //! @return 0 if successful otherwise errno
    //--------------------------------------------------------------------------
    int truncate(off_t offset);

    //--------------------------------------------------------------------------
    //! Flush flushes all changes to the drive.  
    //!
    //! @return 0 if successful otherwise errno
    //--------------------------------------------------------------------------
    int flush();

    //--------------------------------------------------------------------------
    //! Return the actual value size. Is up-to-date within expiration_time limits.
    //!
    //! @return size in bytes of underlying value 
    //--------------------------------------------------------------------------
    int size(); 
    
    //--------------------------------------------------------------------------
    //! Test for your flushing needs.
    //!
    //! @return true if unflushed changes exist, false otherwise
    //--------------------------------------------------------------------------
    bool dirty();
    
    //--------------------------------------------------------------------------
    //! Checking virgin status is usually frowned upon, but this is an exception. 
    //!
    //! @return true if the chunk has never been flushed, false otherwise
    //--------------------------------------------------------------------------
    bool virgin();
        
    //--------------------------------------------------------------------------
    //! Constructor. 
    //! 
    //! @param con a connection to the drive the chunk is (to be) stored on
    //! @param key the name of the chunk 
    //! @param skip_initial_get if true assume that the key does not yet exist 
    //--------------------------------------------------------------------------
    explicit KineticChunk(ConnectionPointer con, std::string key, bool skip_initial_get=false);
    ~KineticChunk();

private:
    //--------------------------------------------------------------------------
    //! (Re)reads the on-drive value, merges in any existing changes made via 
    //! write and / or truncate on the local copy of the value. 
    //! 
    //! @return 0 if successful otherwise errno
    //--------------------------------------------------------------------------
    int get();

private:
    //! the name of the chunk 
    std::string key;

    //! the on-drive version of the chunk 
    std::string version;

    //! the contents of the chunk 
    std::string data;

    //! time the chunk was last verified to be up to date
    std::chrono::system_clock::time_point timestamp;

    //! connection to kinetic drive this chunk is (to be) stored on 
    ConnectionPointer connection;

    //! a list of bit-regions that have been changed since this data block has last been flushed
    std::list<std::pair<off_t, size_t> > updates;
};


#endif

