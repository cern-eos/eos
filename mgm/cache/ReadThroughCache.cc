//------------------------------------------------------------------------------
//! @file ReadThroughCache.cc
//------------------------------------------------------------------------------

#include "mgm/cache/ReadThroughCache.hh"
#include "common/Constants.hh"
#include "common/FileId.hh"
#include "common/Logging.hh"
#include "common/SymKeys.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include <vector>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Get configured cache space name for a backend space
//------------------------------------------------------------------------------
std::string
ReadThroughCache::GetCacheSpaceName(const std::string& backend_space)
{
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
                                const std::shared_ptr<eos::IFileMD>& fmd)
{
  if (!fmd) {
    return 0;
  }

  const std::string cache_space = GetCacheSpaceName(backend_space);

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

  const auto existing = fmd->getCacheLocation();

  if (existing && is_online_cache_fs(existing)) {
    auto* fs = FsView::gFsView.mIdView.lookupByID(existing);

    if (fs && (fs->GetSpace() == cache_space)) {
      return existing;
    }
  }

  auto sit = FsView::gFsView.mSpaceView.find(cache_space);

  if (sit == FsView::gFsView.mSpaceView.end() || !sit->second) {
    return 0;
  }

  std::vector<eos::common::FileSystem::fsid_t> candidates;

  for (auto it = sit->second->begin(); it != sit->second->end(); ++it) {
    if (is_online_cache_fs(*it)) {
      candidates.push_back(*it);
    }
  }

  if (candidates.empty()) {
    return 0;
  }

  return candidates[fmd->getId() % candidates.size()];
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
// Truncate cache journal on mutation of authoritative file
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

  (void) NotifyJournalTruncate(cache_fsid, fmd->getId());
}

EOSMGMNAMESPACE_END
