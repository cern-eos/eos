#include "unit_tests/common/MemConfigStore.hh"
#include "gtest/gtest.h"

using std::string_literals::operator ""s;
const std::string default_val = "DEFAULT";

TEST(ConfigStore, strkeys)
{
  MemConfigStore mconf;
  EXPECT_TRUE(mconf.save("key1","val1"));
  EXPECT_EQ("val1", mconf.get("key1"s, default_val));

  float pi = 3.1428;
  EXPECT_TRUE(mconf.save("pi", std::to_string(pi)));
  // std::to_string defaults to 6 decimal places though this might be impl defined,
  // recommended to use the other overload
  EXPECT_EQ("3.142800", mconf.get("pi"s, default_val));
  EXPECT_FLOAT_EQ(pi, mconf.get("pi", (float)0));

  uint32_t nthreads = 1000;
  EXPECT_TRUE(mconf.save("nthreads", std::to_string(nthreads)));
  EXPECT_EQ("1000", mconf.get("nthreads", default_val));
  EXPECT_EQ(nthreads, mconf.get("nthreads",(uint32_t)0));
}

TEST(ConfigStore, nullkeys)
{
  MemConfigStore mconf;
  EXPECT_EQ(default_val, mconf.get("somekey", default_val));

  EXPECT_EQ(1000, mconf.get("nthreads",1000));
  EXPECT_DOUBLE_EQ(3.1, mconf.get("pi",3.1));
}