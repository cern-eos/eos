//------------------------------------------------------------------------------
//! @file KineticChunk.hh
//! @author Paul Hermann Lensing
//! @brief High(er) level API for Kinetic keys. 
//------------------------------------------------------------------------------
#ifndef KINETICCHUNK_HH_
#define KINETICCHUNK_HH_

/*----------------------------------------------------------------------------*/
#include <memory>
#include <chrono>
#include <string>
#include <mutex>
#include <list>
#include "KineticClusterInterface.hh"
/*----------------------------------------------------------------------------*/


//------------------------------------------------------------------------------
//! High(er) level API for Kinetic keys. Handles incremental updates and
//! resolves concurrency on chunk-basis. For multi-chunk atomic writes the
//! caller will have to do appropriate locking himself. Chunk size depends on
//! cluster configuration. Is threadsafe to enable background flushing.
//------------------------------------------------------------------------------
class KineticChunk {
public:
    //! Initialized to 1 second staleness (in milliseconds)
    static const int expiration_time;

public:
  //--------------------------------------------------------------------------
  //! Reading is guaranteed up-to-date within expiration_time limits. Note that
  //! any read up to the value size limit of the assigned cluster is legal. If
  //! nothing has been written to the requested memory region, 0s will be
  //! returned.
  //!
  //! @param buffer output buffer
  //! @param offset offset in the chunk to start reading
  //! @param length number of bytes to read
  //! @return 0 if successful otherwise errno
  //--------------------------------------------------------------------------
  int read(char* const buffer, off_t offset, size_t length);

  //--------------------------------------------------------------------------
  //! Writing in-memory only, never flushes to the backend. Any write up to the
  //! value size limit of the assigned cluster is legal. Writes do not have to
  //! be consecutive.
  //!
  //! @param buffer input buffer
  //! @param offset offset in the chunk to start writing
  //! @param length number of bytes to write
  //! @return 0 if successful otherwise errno
  //--------------------------------------------------------------------------
  int write(const char* const buffer, off_t offset, size_t length);

  //--------------------------------------------------------------------------
  //! Truncate in-memory only, never flushes to the backend storage.
  //!
  //! @param offset the new target chunk size
  //! @return 0 if successful otherwise errno
  //--------------------------------------------------------------------------
  int truncate(off_t offset);

  //--------------------------------------------------------------------------
  //! Flush flushes all changes to the backend. Does not incur IO if chunk is
  //! not dirty.
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
  //! Test for your flushing needs. Chunk is considered dirty if it is either
  //! freshly created or it has been written since its last flush.
  //!
  //! @return true if dirty, false otherwise.
  //--------------------------------------------------------------------------
  bool dirty() const;

  //--------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param cluster the cluster that this chunk is (to be) stored on
  //! @param key the name of the chunk
  //! @param skip_initial_get if true assume that the key does not yet exist
  //--------------------------------------------------------------------------
  explicit KineticChunk(std::shared_ptr<KineticClusterInterface> cluster,
                        const std::shared_ptr<const std::string> key,
                        bool skip_initial_get=false);
  ~KineticChunk();

private:
  //--------------------------------------------------------------------------
  //! Validate the in-memory version against the version stored in the cluster
  //! assigned to this chunk.
  //!
  //! @return true if remote version could be verified to be equal, false
  //!         otherwise
  //--------------------------------------------------------------------------
  bool validateVersion();

  //--------------------------------------------------------------------------
  //! (Re)reads the value from the backend, merges in any existing changes made 
  //! via write and / or truncate on the local copy of the value.
  //!
  //! @return 0 if successful otherwise errno
  //--------------------------------------------------------------------------
  int getRemoteValue();

private:
  //! cluster this chunk is (to be) stored on
  const std::shared_ptr<KineticClusterInterface> cluster;

  //! the key of the chunk
  const std::shared_ptr<const std::string> key;

  //! the latest known version of the key that is stored in the cluster
  std::shared_ptr<const std::string> version;

  //! the actual data
  std::shared_ptr<std::string> value;

  //! time the chunk was last verified to be up to date
  std::chrono::system_clock::time_point timestamp;

  //! a list of bit-regions that have been changed since this data block has last been flushed
  std::list<std::pair<off_t, size_t> > updates;

  //! thread-safety
  std::recursive_mutex mutex;
};

#endif

