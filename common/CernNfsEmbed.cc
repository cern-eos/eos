// ************************************************************************
// * EOS - the CERN Disk Storage System                                   *
// * Copyright (C) 2026 CERN/Switzerland                                  *
// ************************************************************************

#include "common/CernNfsEmbed.hh"
#include "common/CernNfsEmbedDetail.hh"

#include <cstdlib>
#include <string>
#include <thread>

#ifdef EOS_HAVE_CERN_NFS
#include "common/Logging.hh"
#include "common/NfsLogSink.hh"
#include "common/Path.hh"

#include <cernnfs/ds_fh.hpp>
#include <cernnfs/logging.hpp>
#include <cernnfs/nfs_server.hpp>
#include <cernnfs/vfs.hpp>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>
#endif

namespace eos::common {

namespace {

constexpr const char* kNfsZstdTag = "nfs41";
constexpr const char* kNfsPlainLogName = "nfs41.log";

std::string
JoinBaseDir(const char* dir)
{
  std::string base = dir ? dir : "";

  if (!base.empty() && base.back() != '/') {
    base += '/';
  }

  return base;
}

std::string
ParentDirectory(const std::string& file_path)
{
  const auto slash = file_path.find_last_of('/');
  if (slash == std::string::npos) {
    return ".";
  }
  if (slash == 0) {
    return "/";
  }
  return file_path.substr(0, slash);
}

} // namespace

#ifdef EOS_HAVE_CERN_NFS
namespace detail {

std::string
EmbedNfsKeytabPath()
{
  const char* env = std::getenv("EOS_EMBED_NFS_KEYTAB");

  if (env && *env) {
    return env;
  }

  return "/etc/eos.nfs.keytab";
}

bool
ValidateEmbedNfsKeytab(std::string* err)
{
  const std::string path = EmbedNfsKeytabPath();
  auto loaded = cernnfs::ds_fh::load_keytab_file(path);

  if (loaded.is_err()) {
    if (err) {
      *err =
        "embedded NFS requires a pNFS DS signing keytab at \"" + path +
        "\" (" + loaded.error() + "). "
        "Install the same file on every MGM and FST node, for example:\n"
        "  printf '%s %s\\n' 1 \"$(head -c 32 /dev/urandom | xxd -p -c 64)\" "
        "| sudo tee " +
        path +
        "\n"
        "  sudo chmod 600 " +
        path +
        "\n"
        "Override the path with EOS_EMBED_NFS_KEYTAB.";
    }

    return false;
  }

  if (loaded.value().size() == 0) {
    if (err) {
      *err = "embedded NFS keytab \"" + path + "\" contains no signing keys";
    }

    return false;
  }

  return true;
}

spdlog::level::level_enum
ResolveNfsLogLevel(const char* service_level_env)
{
  const char* level_env = nullptr;

  if (service_level_env) {
    level_env = getenv(service_level_env);
  }

  if (!level_env || !*level_env) {
    level_env = getenv("LOG_LEVEL");
  }

  if (level_env && *level_env) {
    return cernnfs::log_level_from_string(level_env);
  }

  return spdlog::level::info;
}

void
InstallSpdlogLogger(const std::shared_ptr<spdlog::sinks::sink>& sink,
                    const char* service_level_env)
{
  cernnfs::configure_logging(sink, ResolveNfsLogLevel(service_level_env));
}

std::string
PlainNfsLogPath(const char* log_env_var, const char* default_base_dir)
{
  if (log_env_var) {
    const char* explicit_path = getenv(log_env_var);

    if (explicit_path && *explicit_path) {
      return explicit_path;
    }
  }

  return JoinBaseDir(detail::DefaultNfsLogBaseDir(default_base_dir).c_str()) +
         kNfsPlainLogName;
}

bool
ConfigureSpdlogLogging(const char* log_env_var,
                       const char* default_base_dir,
                       const char* service_level_env,
                       std::string* err)
{
  const std::string log_path =
    CernNfsEmbed::LogPathFromEnv(log_env_var, default_base_dir);
  auto& logging = Logging::GetInstance();

  if (logging.IsZstdEnabled()) {
    const std::string plain_path =
      PlainNfsLogPath(log_env_var, default_base_dir);
    logging.ZstdMigratePlainFile(kNfsZstdTag, plain_path.c_str());

    try {
      auto sink = std::make_shared<NfsZstdLogSink_mt>(kNfsZstdTag);
      InstallSpdlogLogger(sink, service_level_env);
      spdlog::info("embedded cern-nfs logging to {} (zstd)", log_path);
      return true;
    } catch (const std::exception& e) {
      if (err) {
        *err = std::string("failed to configure ZSTD NFS logging: ") + e.what();
      }

      return false;
    }
  }

  const std::string log_dir = ParentDirectory(log_path);
  if (log_dir.empty()) {
    if (err) {
      *err = "invalid NFS log path (no directory): " + log_path;
    }
    return false;
  }
  eos::common::Path parent((log_dir + "/.keep").c_str());

  if (!parent.MakeParentPath(S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)) {
    if (err) {
      *err = "failed to create parent directory for " + log_path;
    }

    return false;
  }

  try {
    auto sink =
      std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_path, true);
    InstallSpdlogLogger(sink, service_level_env);
    spdlog::info("embedded cern-nfs logging to {}", log_path);
    return true;
  } catch (const std::exception& e) {
    if (err) {
      *err = std::string("failed to open NFS log file ") + log_path + ": " +
             e.what();
    }

    return false;
  }
}

} // namespace detail
#endif

CernNfsEmbed::CernNfsEmbed()
    : mImpl(std::make_unique<CernNfsEmbedHandle>())
{
}

CernNfsEmbed::~CernNfsEmbed()
{
  Stop();
}

int
CernNfsEmbed::PortFromEnv(const char* env_value)
{
  if (!env_value || !*env_value) {
    return 0;
  }

  char* end = nullptr;
  const long port = strtol(env_value, &end, 10);

  if ((end == env_value) || (*end != '\0') || (port <= 0) || (port > 65535)) {
    return 0;
  }

  return static_cast<int>(port);
}

bool
CernNfsEmbed::Running() const
{
#ifdef EOS_HAVE_CERN_NFS
  return mImpl && mImpl->server && mImpl->server->is_running();
#else
  return false;
#endif
}

void
CernNfsEmbed::Stop()
{
#ifdef EOS_HAVE_CERN_NFS
  if (!mImpl) {
    return;
  }

  if (mImpl->server) {
    mImpl->server->stop();
  }

  if (mImpl->thread.joinable()) {
    mImpl->thread.join();
  }

  mImpl->server.reset();
#endif
}

} // namespace eos::common
