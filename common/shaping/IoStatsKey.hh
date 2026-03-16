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
    return std::hash<std::string>{}(k.app) ^ (std::hash<uint32_t>{}(k.uid) << 1) ^
           (std::hash<uint32_t>{}(k.gid) << 2);
  }
};

} // namespace eos::common::traffic_shaping
