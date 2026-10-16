#pragma once

#include <string_view>

namespace eos::console {

inline bool
IsSafeCgiMetadata(std::string_view value)
{
  for (const unsigned char character : value) {
    if (character == '&' || character < 0x20 || character == 0x7f) {
      return false;
    }
  }
  return true;
}

} // namespace eos::console
