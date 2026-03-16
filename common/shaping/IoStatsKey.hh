#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

namespace eos::common::traffic_shaping {

// Uniquely identifies a traffic stream by (app, uid, gid).
// Used by both FST (for recording I/O) and MGM (for policy look-ups and rate tracking).
struct IoStatsKey {
  std::string app;
  uint32_t uid;
  uint32_t gid;

  bool
  operator==(const IoStatsKey& other) const
  {
    return uid == other.uid && gid == other.gid && app == other.app;
  }
};

struct IoStatsKeyHash {
  std::size_t
  operator()(const IoStatsKey& k) const
  {
    // Use hash_combine to avoid XOR's cancellation/commutativity pitfalls
    // (e.g. uid==gid would collapse to just hash(app) with plain XOR).
    // The magic constant is the golden-ratio fractional bits, same as
    // boost::hash_combine.
    auto combine = [](const std::size_t seed, const std::size_t val) -> std::size_t {
      return seed ^ (val + 0x9e3779b9 + (seed << 6) + (seed >> 2));
    };
    std::size_t h = std::hash<std::string>{}(k.app);
    h = combine(h, std::hash<uint32_t>{}(k.uid));
    h = combine(h, std::hash<uint32_t>{}(k.gid));
    return h;
  }
};

} // namespace eos::common::traffic_shaping
