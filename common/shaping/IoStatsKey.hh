#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <tuple>

namespace eos::common::traffic_shaping {

// Placeholder used wherever a missing string identifier (app name, node id) needs
// to be rendered to the user. Kept here so MGM/FST agree on the same token.
inline constexpr const char* kUnknownId = "<unknown>";

// 64-bit fractional part of the golden ratio (floor(2^64 / phi)). Used as the
// mixing constant in HashCombine() to spread bits across the accumulated seed.
inline constexpr std::size_t kHashCombineMix = 0x9e3779b97f4a7c15ULL;

inline std::size_t
HashCombine(const std::size_t seed, const std::size_t val)
{
  return seed ^ (val + kHashCombineMix + (seed << 6) + (seed >> 2));
}

// Uniquely identifies a traffic stream by (app, uid, gid, fsid).
struct IoStatsKey {
  std::string app = kUnknownId;
  uint32_t uid = 0;
  uint32_t gid = 0;
  uint32_t fsid = 0;

  bool
  operator==(const IoStatsKey& other) const
  {
    return uid == other.uid && gid == other.gid && fsid == other.fsid && app == other.app;
  }

  bool
  operator<(const IoStatsKey& other) const
  {
    return std::tie(app, uid, gid, fsid) <
           std::tie(other.app, other.uid, other.gid, other.fsid);
  }
};

struct IoStatsKeyHash {
  std::size_t
  operator()(const IoStatsKey& k) const
  {
    std::size_t h = std::hash<std::string>{}(k.app);
    h = HashCombine(h, std::hash<uint32_t>{}(k.uid));
    h = HashCombine(h, std::hash<uint32_t>{}(k.gid));
    h = HashCombine(h, std::hash<uint32_t>{}(k.fsid));
    return h;
  }
};

} // namespace eos::common::traffic_shaping
