//------------------------------------------------------------------------------
//! @file ReadThroughCache.cc
//------------------------------------------------------------------------------

#include "mgm/cache/ReadThroughCache.hh"
#include "common/Constants.hh"
#include "common/FileId.hh"
#include "common/Logging.hh"
#include "common/RWMutex.hh"
#include "common/SymKeys.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/misc/Constants.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "namespace/MDException.hh"
#include "namespace/interface/IView.hh"
#include <vector>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Resolve cache space: directory sys.forced.cachespace overrides space config
//------------------------------------------------------------------------------
std::string
ReadThroughCache::GetCacheSpaceName(const std::string& backend_space,
                                    const eos::IContainerMD::XAttrMap* attrmap)
{
  if (attrmap) {
    auto it = attrmap->find(SYS_FORCED_CACHESPACE);

    if (it != attrmap->end()) {
      // Explicit directory policy: "none"/empty disables caching for this tree
      if (it->second.empty() || (it->second == "none")) {
        return {};
      }

      // Same-space caching is rejected (mirrors space.cachespace config rules)
      if (it->second == backend_space) {
        eos_static_warning("msg=\"sys.forced.cachespace equals backend space, "
                           "ignoring\" backend_space=%s", backend_space.c_str());
        return {};
      }

      return it->second;
    }
  }

  auto sit = FsView::gFsView.mSpaceView.find(backend_space);

  if (sit == FsView::gFsView.mSpaceView.end() || !sit->second) {
    return {};
  }

  return sit->second->GetConfigMember(eos::common::SPACE_CACHE_SPACE_NAME);
}

//------------------------------------------------------------------------------
// Select a cache filesystem for a read-only open
//------------------------------------------------------------------------------
eos::common::FileSystem::fsid_t
ReadThroughCache::SelectCacheFs(const std::string& backend_space,
                                const std::shared_ptr<eos::IFileMD>& fmd,
                                const eos::IContainerMD::XAttrMap* attrmap)
{
  if (!fmd) {
    return 0;
  }

  const std::string cache_space = GetCacheSpaceName(backend_space, attrmap);

  if (cache_space.empty()) {
    return 0;
  }

  auto is_online_cache_fs = [](eos::common::FileSystem::fsid_t fsid) {
    auto* fs = FsView::gFsView.mIdView.lookupByID(fsid);

    if (!fs) {
      return false;
    }

    if (fs->GetStatus() != eos::common::BootStatus::kBooted) {
      return false;
    }

    if (fs->GetConfigStatus() < eos::common::ConfigStatus::kRO) {
      return false;
    }

    if (fs->GetActiveStatus() != eos::common::ActiveStatus::kOnline) {
      return false;
    }

    return true;
  };

  // Rendezvous-hash over the *current* online cache FS set. Preferring a
  // sticky cache_location would pin files to the NVMes that existed when they
  // were first cached and leave newly added cache FS idle after an expansion.
  // Rendezvous hashing remaps only ~1/N of fids when a member is added/removed.
  auto sit = FsView::gFsView.mSpaceView.find(cache_space);

  if (sit == FsView::gFsView.mSpaceView.end() || !sit->second) {
    return 0;
  }

  const auto fid = fmd->getId();
  eos::common::FileSystem::fsid_t best_fsid = 0;
  uint64_t best_score = 0;

  for (auto it = sit->second->begin(); it != sit->second->end(); ++it) {
    const auto fsid = *it;

    if (!is_online_cache_fs(fsid)) {
      continue;
    }

    // SplitMix64-style mix of (fid, fsid) - deterministic, no seed needed
    uint64_t score = fid;
    score ^= (uint64_t) fsid + 0x9e3779b97f4a7c15ULL + (score << 6) +
             (score >> 2);
    score = (score ^ (score >> 30)) * 0xbf58476d1ce4e5b9ULL;
    score = (score ^ (score >> 27)) * 0x94d049bb133111ebULL;
    score = score ^ (score >> 31);

    if (!best_fsid || (score > best_score) ||
        ((score == best_score) && (fsid < best_fsid))) {
      best_fsid = fsid;
      best_score = score;
    }
  }

  return best_fsid;
}

//------------------------------------------------------------------------------
// Best-effort notify the cache FST to truncate the journal for fid
//------------------------------------------------------------------------------
bool
ReadThroughCache::NotifyJournalTruncate(eos::common::FileSystem::fsid_t fsid,
                                        eos::IFileMD::id_t fid)
{
  using namespace eos::common;

  if (!fsid || !fid || !gOFS) {
    return false;
  }

  std::string fst_host;
  int fst_port = 1095;
  XrdOucString capability = "";
  {
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    auto* fs = FsView::gFsView.mIdView.lookupByID(fsid);

    if (!fs) {
      eos_static_err("msg=\"cache truncate: no filesystem\" fsid=%lu", fsid);
      return false;
    }

    capability += "&mgm.access=cachetruncate";
    capability += "&mgm.manager=";
    capability += gOFS->ManagerId.c_str();
    capability += "&mgm.fsid=";
    capability += (int) fs->GetId();
    capability += "&mgm.fid=";
    capability += FileId::Fid2Hex(fid).c_str();
    fst_host = fs->GetHost();
    fst_port = fs->getCoreParams().getLocator().getPort();
  }

  XrdOucEnv incapenv(capability.c_str());
  XrdOucEnv* outcapenv = nullptr;
  SymKey* symkey = gSymKeyStore.GetCurrentKey();
  int caprc = SymKey::CreateCapability(&incapenv, outcapenv, symkey,
                                       gOFS->mCapabilityValidity);

  if (caprc) {
    eos_static_err("msg=\"cache truncate: capability failed\" errno=%d", caprc);
    return false;
  }

  int caplen = 0;
  std::string qreq = "/?fst.pcmd=cachetruncate";
  qreq += outcapenv->Env(caplen);
  std::string qresp;
  bool ok = true;

  if (gOFS->SendQuery(fst_host, fst_port, qreq, qresp)) {
    eos_static_err("msg=\"cache truncate notify failed\" fsid=%lu fxid=%08llx",
                   fsid, fid);
    ok = false;
  }

  delete outcapenv;
  return ok;
}

//------------------------------------------------------------------------------
// Truncate cache journal on mutation of authoritative file. If the cache FST
// cannot be reached, drop the cache replica reference (cache_location) so no
// future read is steered towards the stale journal.
//------------------------------------------------------------------------------
void
ReadThroughCache::TruncateOnMutation(const std::shared_ptr<eos::IFileMD>& fmd)
{
  if (!fmd) {
    return;
  }

  const auto cache_fsid = fmd->getCacheLocation();

  if (!cache_fsid) {
    return;
  }

  if (!NotifyJournalTruncate(cache_fsid, fmd->getId())) {
    eos_static_warning("msg=\"cache truncate notify failed, dropping cache "
                       "replica\" fxid=%08llx cache_fsid=%u", fmd->getId(),
                       cache_fsid);

    try {
      eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex);
      fmd->setCacheLocation(0);
      gOFS->eosView->updateFileStore(fmd.get());
    } catch (eos::MDException& e) {
      eos_static_err("msg=\"failed to drop cache replica\" fxid=%08llx "
                     "errno=%d", fmd->getId(), e.getErrno());
    }
  }
}

//------------------------------------------------------------------------------
// Drop cache_location from FMD and truncate the journal on the cache FST
//------------------------------------------------------------------------------
eos::common::FileSystem::fsid_t
ReadThroughCache::DropCacheLocation(const std::shared_ptr<eos::IFileMD>& fmd)
{
  if (!fmd) {
    return 0;
  }

  eos::common::FileSystem::fsid_t cache_fsid = 0;
  {
    eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex);
    cache_fsid = fmd->getCacheLocation();

    if (!cache_fsid) {
      return 0;
    }

    fmd->setCacheLocation(0);
    gOFS->eosView->updateFileStore(fmd.get());
  }

  if (!NotifyJournalTruncate(cache_fsid, fmd->getId())) {
    eos_static_warning("msg=\"cache truncate notify failed after drop\" "
                       "fxid=%08llx cache_fsid=%u", fmd->getId(), cache_fsid);
  }

  return cache_fsid;
}

EOSMGMNAMESPACE_END
