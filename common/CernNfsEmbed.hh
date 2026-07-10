#ifndef EOS_COMMON_CERNNFS_EMBED_HH
#define EOS_COMMON_CERNNFS_EMBED_HH

// ************************************************************************
// * EOS - the CERN Disk Storage System                                   *
// * Copyright (C) 2026 CERN/Switzerland                                  *
// ************************************************************************

#include <memory>
#include <string>
#include <cstdlib>
#include <cstring>

namespace eos::common {

struct CernNfsEmbedHandle;

namespace detail {

inline bool
NfsLogUsesZstdEnv()
{
  const char* zenv = getenv("EOS_ZSTD_LOGGING");

  return zenv && (!strcmp(zenv, "1") || !strcasecmp(zenv, "true") ||
                  !strcasecmp(zenv, "yes") || !strcasecmp(zenv, "on"));
}

inline std::string
DefaultNfsLogBaseDir(const char* default_base_dir)
{
  const char* xrd_logdir = getenv("XRDLOGDIR");

  if (xrd_logdir && *xrd_logdir) {
    return xrd_logdir;
  }

  return default_base_dir ? default_base_dir : "/var/log/eos";
}

} // namespace detail

//! Hosts an in-process cern-nfs server backed by EosEmbedMgmFS or EosEmbedFstFS.
class CernNfsEmbed
{
public:
  CernNfsEmbed();
  ~CernNfsEmbed();

  CernNfsEmbed(const CernNfsEmbed&) = delete;
  CernNfsEmbed& operator=(const CernNfsEmbed&) = delete;

  //! Parse a positive TCP port from an environment variable value.
  static int PortFromEnv(const char* env_value);

  //! Resolve the operational NFS log path from @a log_env_var or defaults.
  //! Returns @c nfs41.zstd when EOS_ZSTD_LOGGING is enabled, otherwise
  //! @c nfs41.log (or the explicit env override in plain mode).
  static std::string LogPathFromEnv(const char* log_env_var,
                                    const char* default_base_dir)
  {
    if (!detail::NfsLogUsesZstdEnv()) {
      if (log_env_var) {
        const char* explicit_path = getenv(log_env_var);

        if (explicit_path && *explicit_path) {
          return explicit_path;
        }
      }
    }

    std::string base = detail::DefaultNfsLogBaseDir(default_base_dir);

    if (!base.empty() && base.back() != '/') {
      base += '/';
    }

    base += detail::NfsLogUsesZstdEnv() ? "nfs41.zstd" : "nfs41.log";
    return base;
  }

  bool Running() const;

  bool StartMgm(void* mgm_ofs,
                int port,
                const std::string& bind_host,
                const std::string& mount_path,
                std::string* err = nullptr);

  bool StartFst(void* fst_ofs,
                int port,
                const std::string& bind_host,
                const std::string& replica_root,
                std::string* err = nullptr);

  void Stop();

private:
  std::unique_ptr<CernNfsEmbedHandle> mImpl;
};

} // namespace eos::common

#endif
