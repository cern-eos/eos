#pragma once

#include <random>

namespace eos::common {

template <typename IntType=uint32_t>
auto getRandom(IntType start, IntType end) -> IntType
{
  thread_local std::random_device tlrd;
  thread_local std::mt19937 generator(tlrd());

  std::uniform_int_distribution<IntType> distrib(start, end);
  return distrib(generator);
}

} // eos::common
