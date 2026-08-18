#pragma once

#include "mgm/shaping/TrafficShaping.hh"

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace eos::mgm::monitoring {

inline void
EscapeLabelValue(std::string& out, std::string_view val)
{
  for (char c : val) {
    switch (c) {
    case '\\':
      out += "\\\\";
      break;
    case '"':
      out += "\\\"";
      break;
    case '\n':
      out += "\\n";
      break;
    default:
      out.push_back(c);
      break;
    }
  }
}

inline void
FormatHeader(std::string& out, std::string_view name, std::string_view type,
             std::string_view help)
{
  if (!help.empty()) {
    out += "# HELP ";
    out += name;
    out += ' ';
    for (char c : help) {
      if (c == '\\') {
        out += "\\\\";
      } else if (c == '\n') {
        out += "\\n";
      } else {
        out.push_back(c);
      }
    }
    out += '\n';
  }

  out += "# TYPE ";
  out += name;
  out += ' ';
  out += type;
  out += '\n';
}

inline void
FormatValue(std::string& out, double value)
{
  if (std::isnan(value)) {
    out += "NaN";
  } else if (std::isinf(value)) {
    out += (value < 0 ? "-Inf" : "+Inf");
  } else {
    char buf[64];
    if (value == std::floor(value) && std::abs(value) < 1e15) {
      std::snprintf(buf, sizeof(buf), "%.0f", value);
    } else {
      std::snprintf(buf, sizeof(buf), "%.6g", value);
    }
    out += buf;
  }
}

inline void
FormatLabels(std::string& out, const std::map<std::string, std::string>& labels)
{
  if (labels.empty()) {
    return;
  }

  out += '{';
  bool first = true;
  for (const auto& [k, v] : labels) {
    if (!first) {
      out += ',';
    }
    first = false;
    out += k;
    out += "=\"";
    EscapeLabelValue(out, v);
    out += '"';
  }
  out += '}';
}

inline void
FormatGaugeMetric(std::string& out, std::string_view name,
                  const std::map<std::string, std::string>& labels, double value)
{
  out += name;
  FormatLabels(out, labels);
  out += ' ';
  FormatValue(out, value);
  out += '\n';
}

inline void
FormatCounterMetric(std::string& out, std::string_view name,
                    const std::map<std::string, std::string>& labels, uint64_t value)
{
  out += name;
  FormatLabels(out, labels);
  out += ' ';
  out += std::to_string(value);
  out += '\n';
}

inline void
FormatCounterMetric(std::string& out, std::string_view name,
                    const std::map<std::string, std::string>& labels, double value)
{
  out += name;
  FormatLabels(out, labels);
  out += ' ';
  FormatValue(out, value);
  out += '\n';
}

inline void
FormatHistogramMetric(
    std::string& out, std::string_view name,
    const std::map<std::string, std::string>& labels,
    const eos::mgm::traffic_shaping::DurationHistogramSnapshot& snapshot)
{
  const size_t bucket_count = std::min(snapshot.upper_bounds_seconds.size(),
                                       snapshot.cumulative_bucket_counts.size());

  for (size_t i = 0; i < bucket_count; ++i) {
    out += name;
    out += "_bucket{";
    for (const auto& [k, v] : labels) {
      out += k;
      out += "=\"";
      EscapeLabelValue(out, v);
      out += "\",";
    }
    out += "le=\"";
    FormatValue(out, snapshot.upper_bounds_seconds[i]);
    out += "\"} ";
    out += std::to_string(snapshot.cumulative_bucket_counts[i]);
    out += '\n';
  }

  // +Inf bucket
  out += name;
  out += "_bucket{";
  for (const auto& [k, v] : labels) {
    out += k;
    out += "=\"";
    EscapeLabelValue(out, v);
    out += "\",";
  }
  out += "le=\"+Inf\"} ";
  out += std::to_string(snapshot.sample_count);
  out += '\n';

  // _sum
  out += name;
  out += "_sum";
  FormatLabels(out, labels);
  out += ' ';
  FormatValue(out, snapshot.sample_sum_seconds);
  out += '\n';

  // _count
  out += name;
  out += "_count";
  FormatLabels(out, labels);
  out += ' ';
  out += std::to_string(snapshot.sample_count);
  out += '\n';
}

} // namespace eos::mgm::monitoring
