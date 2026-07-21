//------------------------------------------------------------------------------
//! @file ReadThroughCache.hh
//! @brief Helpers for FST read-through cache scheduling
//------------------------------------------------------------------------------

#pragma once

#include "common/FileSystem.hh"
#include "mgm/Namespace.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IFileMD.hh"
#include <string>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Utilities for space-linked read-through caching
//------------------------------------------------------------------------------
class ReadThroughCache
{
public:
  //----------------------------------------------------------------------------
  //! Return configured cache space name for a backend space (empty if none).
  //! Directory policy sys.forced.cachespace overrides the space config when
  //! present in attrmap: a non-empty value selects that cache space;
  //! "none" / empty disables caching for that directory tree.
  //----------------------------------------------------------------------------
  static std::string GetCacheSpaceName(
    const std::string& backend_space,
    const eos::IContainerMD::XAttrMap* attrmap = nullptr);

  //----------------------------------------------------------------------------
  //! Select a cache filesystem for a read-only open.
  //! Rendezvous-hash of fid over the current online FS set of the cache space
  //! so that adding/removing NVMes remaps only ~1/N of files. cache_location
  //! is not sticky - the next read may remap and rewrite the pointer; the old
  //! journal is truncated on remapping.
  //!
  //! @note Caller must hold FsView::gFsView.ViewMutex (read)
  //!
  //! @return cache fsid or 0 if none available
  //----------------------------------------------------------------------------
  static eos::common::FileSystem::fsid_t
  SelectCacheFs(const std::string& backend_space,
                const std::shared_ptr<eos::IFileMD>& fmd,
                const eos::IContainerMD::XAttrMap* attrmap = nullptr);

  //----------------------------------------------------------------------------
  //! Best-effort notify the cache FST to truncate the journal for fid
  //----------------------------------------------------------------------------
  static bool NotifyJournalTruncate(eos::common::FileSystem::fsid_t fsid,
                                    eos::IFileMD::id_t fid);

  //----------------------------------------------------------------------------
  //! If fmd has a cache_location, notify that FST to truncate its journal.
  //! If the notification fails the cache replica reference is dropped
  //! (cache_location cleared and persisted).
  //!
  //! @note Performs a synchronous FST query and may take the namespace write
  //!       lock - must NOT be called with eosViewRWMutex held
  //----------------------------------------------------------------------------
  static void TruncateOnMutation(const std::shared_ptr<eos::IFileMD>& fmd);

  //----------------------------------------------------------------------------
  //! Explicitly drop the cache replica reference from fmd and best-effort
  //! truncate the journal on the cache FST.
  //!
  //! @return previous cache_location fsid (0 if none was set)
  //! @throws eos::MDException if clearing the namespace pointer fails
  //!
  //! @note May take the namespace write lock and perform a synchronous FST
  //!       query - must NOT be called with eosViewRWMutex held
  //----------------------------------------------------------------------------
  static eos::common::FileSystem::fsid_t
  DropCacheLocation(const std::shared_ptr<eos::IFileMD>& fmd);
};

EOSMGMNAMESPACE_END
