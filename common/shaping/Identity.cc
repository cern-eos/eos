// ----------------------------------------------------------------------
// File: Identity.cc
// Author: CERN
// ----------------------------------------------------------------------

#include "common/shaping/Identity.hh"

#include "common/Mapping.hh"

#include <string_view>

namespace eos::common::traffic_shaping {

std::string
FormatResolvedId(const uint32_t id, const std::string& name, const int errc)
{
  const std::string id_string = std::to_string(id);

  if (errc || name.empty() || name == id_string) {
    return id_string;
  }

  return id_string + "(" + name + ")";
}

std::string
NodeLabel(const std::string& node_id)
{
  constexpr std::string_view prefix = "/eos/";
  constexpr std::string_view suffix = "/fst";

  if (node_id.size() > prefix.size() + suffix.size() &&
      node_id.compare(0, prefix.size(), prefix) == 0 &&
      node_id.compare(node_id.size() - suffix.size(), suffix.size(), suffix) == 0) {
    return node_id.substr(prefix.size(), node_id.size() - prefix.size() - suffix.size());
  }

  return node_id;
}

std::string
UidLabel(const uint32_t uid)
{
  int errc = 0;
  const auto name = eos::common::Mapping::UidToUserName(static_cast<uid_t>(uid), errc);
  return FormatResolvedId(uid, name, errc);
}

std::string
GidLabel(const uint32_t gid)
{
  int errc = 0;
  const auto name = eos::common::Mapping::GidToGroupName(static_cast<gid_t>(gid), errc);
  return FormatResolvedId(gid, name, errc);
}

} // namespace eos::common::traffic_shaping
