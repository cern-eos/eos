#pragma once

#include "common/ParseUtils.hh"

#include <cstdint>
#include <limits>
#include <string>

namespace eos::mgm::monitoring {

constexpr const char* kPrometheusEnabledConfig = "monitoring.prometheus.enabled";
constexpr const char* kPrometheusPortConfig = "monitoring.prometheus.port";
constexpr const char* kPrometheusCacheTtlConfig =
    "monitoring.prometheus.cache_ttl_seconds";
constexpr uint32_t kDefaultPrometheusPort = 9987;
constexpr uint32_t kDefaultPrometheusCacheTtlSeconds = 5;
constexpr uint32_t kMaxPrometheusCacheTtlSeconds = 3600;

inline bool
ParseUint32Config(const std::string& value, uint32_t& parsed)
{
  if (value.empty()) {
    return false;
  }

  uint64_t result = 0;

  if (!eos::common::ParseUInt64(value, result) ||
      result > std::numeric_limits<uint32_t>::max()) {
    return false;
  }

  parsed = static_cast<uint32_t>(result);
  return true;
}

inline bool
ParsePortConfig(const std::string& value, uint16_t& parsed)
{
  uint32_t port = 0;

  if (!ParseUint32Config(value, port) || port == 0 ||
      port > std::numeric_limits<uint16_t>::max()) {
    return false;
  }

  parsed = static_cast<uint16_t>(port);
  return true;
}

inline bool
IsValidCacheTtl(const uint32_t seconds)
{
  return seconds <= kMaxPrometheusCacheTtlSeconds;
}

} // namespace eos::mgm::monitoring
