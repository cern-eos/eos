
#include "gtest/gtest.h"
#include "common/utils/ContainerUtils.hh"
#include <map>
#include <unordered_map>
#include <set>
#include <unordered_set>
#include <vector>

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