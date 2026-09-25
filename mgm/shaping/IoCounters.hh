#pragma once

#include <json/json.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <map>
#include <string>
#include <tuple>

namespace eos::mgm::traffic_shaping {

// Monitoring only. The caller holds TrafficShapingManager::mMutex. Retain idle
// identities so rate-estimator GC cannot reset an application's exported total.
class IoCounters {
public:
  using Key = std::tuple<std::string, std::string, uint32_t, uint32_t>;
  using Values = std::array<uint64_t, 4>; // read/write bytes, read/write operations
  static constexpr size_t kMaxEntries = 50000;
  static constexpr size_t kMaxEstimatedBytes = 64 * 1024 * 1024;

  void
  Observe(const Key& key, uint64_t generation, int64_t timestamp_ms, int64_t now_ms,
          const Values& values)
  {
    if (timestamp_ms > now_ms && timestamp_ms - now_ms > 3000) {
      return;
    }

    auto it = mEntries.find(key);
    const bool first = it == mEntries.end();
    if (first) {
      const size_t bytes = sizeof(Key) + sizeof(Entry) + 4 * sizeof(void*) +
                           std::get<0>(key).size() + std::get<1>(key).size();
      if (mEntries.size() >= kMaxEntries || bytes > kMaxEstimatedBytes - mBytes) {
        ++mRejected;
        return;
      }
      it = mEntries.try_emplace(key).first;
      mBytes += bytes;
    }

    auto& entry = it->second;
    if (!first && timestamp_ms > 0 && entry.timestamp_ms > 0 &&
        timestamp_ms <= entry.timestamp_ms) {
      return;
    }
    const bool new_generation = first || entry.generation != generation;
    // Legacy reports without a timestamp can still use the 5.4 stream creation
    // time as an ordering guard when changing generations.
    if (!first && new_generation && timestamp_ms <= 0 && generation <= entry.generation) {
      return;
    }
    const bool baseline = first && now_ms > 0 &&
                          static_cast<uint64_t>(now_ms) > generation &&
                          static_cast<uint64_t>(now_ms) - generation > 3000;
    for (size_t i = 0; i < values.size(); ++i) {
      if (new_generation) {
        if (!baseline) {
          entry.total[i] += values[i];
        }
        entry.last[i] = values[i];
      } else {
        if (values[i] >= entry.last[i]) {
          entry.total[i] += values[i] - entry.last[i];
        }
        entry.last[i] = std::max(entry.last[i], values[i]);
      }
    }
    entry.generation = generation;
    entry.timestamp_ms = std::max(entry.timestamp_ms, timestamp_ms);
  }

  Json::Value
  ToJson() const
  {
    Json::Value result;
    result["version"] = 1;
    result["entries"] = Json::Value(Json::arrayValue);
    result["rejected_entries_total"] = Json::UInt64(mRejected);
    result["limit_entries"] = Json::UInt64(kMaxEntries);
    for (const auto& [key, entry] : mEntries) {
      Json::Value row;
      row["node_id"] = std::get<0>(key);
      row["app"] = std::get<1>(key);
      row["uid"] = std::get<2>(key);
      row["gid"] = std::get<3>(key);
      row["bytes_read_total"] = Json::UInt64(entry.total[0]);
      row["bytes_written_total"] = Json::UInt64(entry.total[1]);
      row["read_ops_total"] = Json::UInt64(entry.total[2]);
      row["write_ops_total"] = Json::UInt64(entry.total[3]);
      result["entries"].append(std::move(row));
    }
    return result;
  }

private:
  struct Entry {
    Values last{};
    Values total{};
    uint64_t generation = 0;
    int64_t timestamp_ms = 0;
  };
  std::map<Key, Entry> mEntries;
  size_t mBytes = 0;
  uint64_t mRejected = 0;
};

} // namespace eos::mgm::traffic_shaping
