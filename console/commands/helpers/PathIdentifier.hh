#pragma once

#include <charconv>
#include <cstdint>
#include <string_view>

namespace eos::console {

enum class IdentifierType { Path, File, Container, Inode, Invalid };

struct PathIdentifier {
  IdentifierType type = IdentifierType::Path;
  uint64_t value = 0;
};

inline PathIdentifier
ParsePathIdentifier(std::string_view text)
{
  struct Prefix {
    std::string_view name;
    IdentifierType type;
    int base;
  };
  constexpr Prefix prefixes[] = {
      {"fid:", IdentifierType::File, 10},      {"fxid:", IdentifierType::File, 16},
      {"cid:", IdentifierType::Container, 10}, {"cxid:", IdentifierType::Container, 16},
      {"pid:", IdentifierType::Container, 10}, {"pxid:", IdentifierType::Container, 16},
      {"inode:", IdentifierType::Inode, 10}};
  for (const auto& prefix : prefixes) {
    if (text.substr(0, prefix.name.size()) != prefix.name) {
      continue;
    }
    const auto number = text.substr(prefix.name.size());
    uint64_t value = 0;
    const auto parsed =
        std::from_chars(number.data(), number.data() + number.size(), value, prefix.base);
    if (parsed.ec != std::errc{} || parsed.ptr != number.data() + number.size()) {
      return {IdentifierType::Invalid, 0};
    }
    return {prefix.type, value};
  }
  return {};
}

} // namespace eos::console
