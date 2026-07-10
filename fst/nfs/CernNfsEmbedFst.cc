// FST-only CernNfsEmbed::StartFst — references EosEmbedFstFS, which is
// compiled into XrdEosFst, not libcernnfs or MGM targets.

#include "common/CernNfsEmbed.hh"
#include "common/CernNfsEmbedDetail.hh"

#ifdef EOS_HAVE_CERN_NFS
#include <cernnfs/nfs_server.hpp>
#include <cernnfs/vfs.hpp>

#include <thread>
#endif

namespace eos::common {

bool
CernNfsEmbed::StartFst(void* fst_ofs,
                       int port,
                       const std::string& bind_host,
                       const std::string& replica_root,
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

  if (!detail::ConfigureSpdlogLogging("EOS_FST_NFS_LOG", "/var/log/eos/fst",
                                      "EOS_FST_NFS_LOG_LEVEL", err)) {
    return false;
  }

  if (!detail::ValidateEmbedNfsKeytab(err)) {
    return false;
  }

  if (!cernnfs::EosEmbedFstFS::configure_fst(fst_ofs)) {
    if (err) {
      *err = "EosEmbedFstFS::configure_fst failed";
    }

    return false;
  }

  auto root = cernnfs::EosEmbedFstFS::open_root(replica_root);

  if (!root) {
    if (err) {
      *err = "EosEmbedFstFS::open_root failed for " + replica_root;
    }

    return false;
  }

  const std::string bind = bind_host + ":" + std::to_string(port);
  mImpl->server = cernnfs::ServerBuilder(root)
                      .bind(bind)
                      .domain("CERN.CH")
                      .workers(std::thread::hardware_concurrency())
                      .nfs_minor(1)
                      .server_id("eos-fst-nfs")
                      .pnfs_exchange_role(cernnfs::PnfsExchangeRole::Ds)
                      .build();

  if (!mImpl->server) {
    if (err) {
      *err = "failed to build embedded FST NFS server";
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
