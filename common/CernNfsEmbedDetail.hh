#ifndef EOS_COMMON_CERNNFS_EMBED_DETAIL_HH
#define EOS_COMMON_CERNNFS_EMBED_DETAIL_HH

// Internal helpers shared by CernNfsEmbed translation units (common + mgm/fst).

#include <memory>
#include <string>
#include <thread>

#ifdef EOS_HAVE_CERN_NFS
#include <cernnfs/nfs_server.hpp>
#endif

namespace eos::common {

struct CernNfsEmbedHandle
{
#ifdef EOS_HAVE_CERN_NFS
  std::unique_ptr<cernnfs::NFSServer> server;
  std::thread thread;
#endif
};

namespace detail {

#ifdef EOS_HAVE_CERN_NFS
bool ConfigureSpdlogLogging(const char* log_env_var,
                            const char* default_base_dir,
                            const char* service_level_env,
                            std::string* err);

//! Validate the pNFS DS signing keytab before starting embedded NFS.
//! Sets @a err to a provisioning hint when missing or invalid.
bool ValidateEmbedNfsKeytab(std::string* err = nullptr);
#endif

} // namespace detail
} // namespace eos::common

#endif
