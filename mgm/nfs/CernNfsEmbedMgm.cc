// MGM-only CernNfsEmbed::StartMgm — references EosEmbedMgmFS, which is
// compiled into XrdEosMgm, not libcernnfs or FST targets.

#include "common/CernNfsEmbed.hh"
#include "common/CernNfsEmbedDetail.hh"

#ifdef EOS_HAVE_CERN_NFS
#include <cernnfs/nfs_server.hpp>
#include <cernnfs/vfs.hpp>

#include <thread>
#endif

namespace eos::common {

bool
CernNfsEmbed::StartMgm(void* mgm_ofs,
                       int port,
                       const std::string& bind_host,
                       const std::string& mount_path,
                       std::string* err)
{
#ifndef EOS_HAVE_CERN_NFS
  if (err) {
    *err = "cern-nfs support was not built in";
  }

  return false;
#else
  if (Running()) {
    return true;
  }

  if (!detail::ConfigureSpdlogLogging("EOS_MGM_NFS_LOG", "/var/log/eos/mgm",
                                      "EOS_MGM_NFS_LOG_LEVEL", err)) {
    return false;
  }

  if (!detail::ValidateEmbedNfsKeytab(err)) {
    return false;
  }

  if (!cernnfs::EosEmbedMgmFS::configure_mgm(mgm_ofs)) {
    if (err) {
      *err = "EosEmbedMgmFS::configure_mgm failed";
    }

    return false;
  }

  auto root = cernnfs::EosEmbedMgmFS::open_root(mount_path);

  if (!root) {
    if (err) {
      *err = "EosEmbedMgmFS::open_root failed for " + mount_path;
    }

    return false;
  }

  const std::string bind = bind_host + ":" + std::to_string(port);
  mImpl->server = cernnfs::ServerBuilder(root)
                      .bind(bind)
                      .domain("CERN.CH")
                      .workers(std::thread::hardware_concurrency())
                      .nfs_minor(1)
                      .server_id("eos-mgm-nfs")
                      .pnfs_exchange_role(cernnfs::PnfsExchangeRole::Mds)
                      .build();

  if (!mImpl->server) {
    if (err) {
      *err = "failed to build embedded MGM NFS server";
    }

    return false;
  }

  cernnfs::NFSServer* server_ptr = mImpl->server.get();
  mImpl->thread = std::thread([server_ptr]() {
    server_ptr->start();
  });

  return true;
#endif
}

} // namespace eos::common
