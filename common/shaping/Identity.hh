// ----------------------------------------------------------------------
// File: Identity.hh
// Author: CERN
// ----------------------------------------------------------------------

#pragma once

#include <cstdint>
#include <string>

namespace eos::common::traffic_shaping {

std::string FormatResolvedId(uint32_t id, const std::string& name, int errc);
std::string NodeLabel(const std::string& node_id);
std::string UidLabel(uint32_t uid);
std::string GidLabel(uint32_t gid);

} // namespace eos::common::traffic_shaping
