#pragma once

#include <random>

namespace eos::common {

static auto get_local_generator() {
  thread_local std::random_device tlrd;
  thread_local std::mt19937 generator(tlrd());
  return generator();
}



template <typename IntType=uint32_t>
auto getRandom(IntType start, IntType end) -> IntType
{
  std::uniform_int_distribution<IntType> distrib(start, end);
  return distrib(get_local_generator());
}

} // eos::common
