#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

namespace eos::common::traffic_shaping {

// Uniquely identifies a traffic stream by (app, uid, gid, fsid).
struct IoStatsKey {
  std::string app = "<unknown>";
  uint32_t uid = 0;
  uint32_t gid = 0;
  uint32_t fsid = 0;

  bool
  operator==(const IoStatsKey& other) const
  {
    return uid == other.uid && gid == other.gid && fsid == other.fsid && app == other.app;
  }
};

struct IoStatsKeyHash {
  std::size_t
  operator()(const IoStatsKey& k) const
  {
    // Use hash_combine to avoid XOR's cancellation/commutativity pitfalls
    // when mixing several fields into one hash value.
    //
    // The constant is the 64-bit fractional part of the golden ratio:
    //   floor(2^64 / phi) = 0x9e3779b97f4a7c15
    //
    // It is commonly used in hash-combine functions, including Boost-style
    // hash_combine, because it helps spread bits when successive field hashes
    // are mixed into the accumulated seed.

    auto combine = [](const std::size_t seed, const std::size_t val) -> std::size_t {
      return seed ^ (val + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2));
    };

    std::size_t h = std::hash<std::string>{}(k.app);
    h = combine(h, std::hash<uint32_t>{}(k.uid));
    h = combine(h, std::hash<uint32_t>{}(k.gid));
    h = combine(h, std::hash<uint32_t>{}(k.fsid));
    return h;
  }
};

} // namespace eos::common::traffic_shaping
