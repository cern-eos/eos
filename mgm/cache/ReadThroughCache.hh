//------------------------------------------------------------------------------
//! @file ReadThroughCache.hh
//! @brief Helpers for FST read-through cache scheduling
//------------------------------------------------------------------------------

#pragma once

#include "common/FileSystem.hh"
#include "mgm/Namespace.hh"
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
  //! Return configured cache space name for a backend space (empty if none)
  //----------------------------------------------------------------------------
  static std::string GetCacheSpaceName(const std::string& backend_space);

  //----------------------------------------------------------------------------
  //! Select a cache filesystem for a read-only open.
  //! Prefers an existing online cache_location, otherwise picks an online FS
  //! from the cache space (fid-based round robin).
  //!
  //! @note Caller must hold FsView::gFsView.ViewMutex (read)
  //!
  //! @return cache fsid or 0 if none available
  //----------------------------------------------------------------------------
  static eos::common::FileSystem::fsid_t
  SelectCacheFs(const std::string& backend_space,
                const std::shared_ptr<eos::IFileMD>& fmd);

  //----------------------------------------------------------------------------
  //! Best-effort notify the cache FST to truncate the journal for fid
  //----------------------------------------------------------------------------
  static bool NotifyJournalTruncate(eos::common::FileSystem::fsid_t fsid,
                                    eos::IFileMD::id_t fid);

  //----------------------------------------------------------------------------
  //! If fmd has a cache_location, notify that FST to truncate its journal
  //----------------------------------------------------------------------------
  static void TruncateOnMutation(const std::shared_ptr<eos::IFileMD>& fmd);
};

EOSMGMNAMESPACE_END
