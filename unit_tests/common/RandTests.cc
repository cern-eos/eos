#include <set>
#include "common/utils/RandUtils.hh"
#include "gtest/gtest.h"

// This is not an actual test of random distributions
// it is simply a test to confirm that both the [start,end]
// ranges are generated
TEST(getRandom, Limits)
{
  std::set<int> numbers;
  for (int i=0; i < 1000; i++) {
    int n = eos::common::getRandom(1,6);
    numbers.insert(n);
  }
  ASSERT_EQ(numbers.size(),6);
  for (int i=1;i<7;i++) {
    ASSERT_FALSE(numbers.find(i) == numbers.end());
  }
}
