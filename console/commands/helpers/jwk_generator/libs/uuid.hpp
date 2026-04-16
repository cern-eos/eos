// https://stackoverflow.com/a/60198074
#pragma once

#include "common/utils/RandUtils.hh"
#include <sstream>

namespace jwk_generator
{
namespace detail
{
static inline std::string generate_uuid_v4()
{
  std::stringstream ss;
  int i;
  ss << std::hex;

  for (i = 0; i < 8; i++) {
    ss << eos::common::getRandom(0, 15);
  }

  ss << "-";

  for (i = 0; i < 4; i++) {
    ss << eos::common::getRandom(0, 15);
  }

  ss << "-4";

  for (i = 0; i < 3; i++) {
    ss << eos::common::getRandom(0, 15);
  }

  ss << "-";
  ss << eos::common::getRandom(8, 11);

  for (i = 0; i < 3; i++) {
    ss << eos::common::getRandom(0, 15);
  }

  ss << "-";

  for (i = 0; i < 12; i++) {
    ss << eos::common::getRandom(0, 15);
  };

  return ss.str();
}
};
};
