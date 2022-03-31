
#include "gtest/gtest.h"
#include "common/utils/ContainerUtils.hh"
#include <map>
#include <unordered_map>
#include <set>
#include <unordered_set>
#include <vector>
#include <list>

bool is_even(int i) { return i%2 == 0; }

TEST(EraseIf, Map)
{
  std::map<int,std::string> m = { {1, "one"},
                                   {2, "two"},
                                   {3, "three"},
                                   {4, "four"}};

  auto m2 = m; // copy
  std::map<int,std::string> expected = {{1, "one"}, {3, "three"}};
  eos::common::erase_if(m, [](const auto& p) { return is_even(p.first);});
  ASSERT_EQ(expected, m);

  eos::common::erase_if(m2, [](const auto& p) { return !is_even(p.first);});
  std::map<int, std::string> expected2 = {{2, "two"}, {4, "four"}};
  ASSERT_EQ(expected2, m2);
}

TEST(EraseIf, UnorderedMap)
{
  std::unordered_map<int,std::string> m = { {1, "one"},
                                  {2, "two"},
                                  {3, "three"},
                                  {4, "four"}};

  auto m2 = m; // copy
  std::unordered_map<int,std::string> expected = {{1, "one"}, {3, "three"}};
  eos::common::erase_if(m, [](const auto& p) { return is_even(p.first);});
  ASSERT_EQ(expected, m);

  eos::common::erase_if(m2, [](const auto& p) { return !is_even(p.first);});
  std::unordered_map<int, std::string> expected2 = {{2, "two"}, {4, "four"}};
  ASSERT_EQ(expected2, m2);
}

TEST(EraseIf, Set)
{
  std::set<int> s = {1,2,3,4};
  eos::common::erase_if(s, is_even);
  //s.erase(std::remove_if(s.begin(),s.end(),is_even));
  std::set<int> expected = {1,3};
  ASSERT_EQ(expected, s);
}

TEST(EraseIf, UnorderedSet)
{
  std::unordered_set<int> s = {1,2,3,4};
  eos::common::erase_if(s, is_even);
  //s.erase(std::remove_if(s.begin(),s.end(),is_even));
  std::unordered_set<int> expected = {1,3};
  ASSERT_EQ(expected, s);
}

using eos::common::pickIndexRR;

TEST(pickIndexRR, list)
{
  using Cont = std::list<int>;
  Cont C {1,2,3};
  // repeat this 4 times
  Cont expected {
      1,2,3,
      1,2,3,
      1,2,3,
      1,2,3
  };

  Cont actual;
  for (int i=0; i < 12; ++i) {
    actual.emplace_back(pickIndexRR(C,i));
  }
  ASSERT_EQ(expected, actual);

  Cont C2{1};
  for (int i = 0; i < 12; ++i) {
    EXPECT_EQ(pickIndexRR(C2,i), 1);
  }
}

TEST(pickIndexRR, exception)
{
  std::list<int> C;
  EXPECT_THROW(pickIndexRR(C,0),std::out_of_range);
  std::vector<int> V{};
  EXPECT_THROW(pickIndexRR(V,0), std::out_of_range);
  std::vector<int> v(1);
  ASSERT_EQ(v.size(), 1);
  EXPECT_NO_THROW(pickIndexRR(v,1));
  EXPECT_EQ(pickIndexRR(v,2),0);
}

TEST(pickIndexRR, Set)
{
  using Cont = std::set<int>;
  Cont C {1,2,3};
  // repeat this 4 times
  std::vector<int> expected {
      1,2,3,
      1,2,3,
      1,2,3,
      1,2,3
  };

  std::vector<int> actual;
  for (int i=0; i < 12; ++i) {
    actual.emplace_back(pickIndexRR(C,i));
  }
  ASSERT_EQ(expected, actual);
}

TEST(pickIndexRR, UnorderedSet)
{
  using Cont = std::unordered_set<int>;
  Cont C {1,2,3};

  std::vector<int> base;
  base.insert(base.end(), C.begin(), C.end());

  ASSERT_EQ(base.size(), 3);
  ASSERT_EQ(C.size(), 3);
  // Copy contents back to another hash set to check that we are equal ie
  // base == C
  Cont base_set;
  for (const auto &item: base) {
    base_set.emplace(item);
  }
  ASSERT_EQ(C, base_set);

  // repeat this 4 times for our good ol RR
  std::vector<int> expected;
  for (int i=0; i < 4; i++) {
    expected.insert(std::end(expected), std::begin(base), std::end(base));
  }

  std::vector<int> actual;
  for (int i=0; i < 12; ++i) {
    actual.emplace_back(pickIndexRR(C,i));
  }
  ASSERT_EQ(expected, actual);
}

// TESTING for remove_if
#include <algorithm>
TEST(StdEraseIf, vector)
{
  std::vector<int> v = {1,2,3,4};
  //eos::common::erase_if(v, is_even); will not compile
  v.erase(std::remove_if(v.begin(),v.end(), is_even));
  std::vector<int> expected = {1,3,4}; // remove_if only does [first, last);
  ASSERT_EQ(expected, v);
}