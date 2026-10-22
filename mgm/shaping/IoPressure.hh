#pragma once

#include <algorithm>
#include <optional>

namespace eos::mgm::traffic_shaping {

// Readers return only fresh, usable pressures in [0, 1]. When individual
// filesystem results are needed, every source must still be visited.
template <typename Sources, typename Reader>
std::optional<double>
CollectMaximumIoPressure(const Sources& sources, Reader&& read_pressure,
                         const bool allow_short_circuit)
{
  std::optional<double> maximum;
  for (const auto& source : sources) {
    const auto pressure = read_pressure(source);
    if (!pressure) {
      continue;
    }
    maximum = maximum ? std::max(*maximum, *pressure) : *pressure;
    if (allow_short_circuit && *maximum == 1.0) {
      break;
    }
  }
  return maximum;
}

} // namespace eos::mgm::traffic_shaping
