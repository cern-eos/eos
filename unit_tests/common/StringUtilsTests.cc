#include "common/StringUtils.hh"
#include <string_view>
#include "gtest/gtest.h"

using eos::common::StringToNumeric;
using std::string_literals::operator ""s;
using std::string_view_literals::operator ""sv;

TEST(StringUtils, GetNumericBasic)
{

  uint32_t test_u32;
  ASSERT_TRUE(StringToNumeric("100"sv, test_u32));
  EXPECT_EQ(test_u32, 100);

  ASSERT_TRUE(StringToNumeric("0"s, test_u32));
  EXPECT_EQ(test_u32, 0);

  ASSERT_TRUE(StringToNumeric("10units"s, test_u32));
  EXPECT_EQ(test_u32, 10);

  // Test end ranges
  char test_c;
  ASSERT_TRUE(StringToNumeric("127"sv, test_c));
  EXPECT_EQ(test_c, 127);
  ASSERT_FALSE(StringToNumeric("128"sv, test_c));

  ASSERT_TRUE(StringToNumeric("4294967295"sv, test_u32));
  EXPECT_EQ(test_u32, 4294967295u);

  ASSERT_FALSE(StringToNumeric("4294967296"sv, test_u32));

  uint64_t test_u64;
  ASSERT_TRUE(StringToNumeric("4294967296"sv, test_u64));
  EXPECT_EQ(test_u64, 4294967296);

  ASSERT_TRUE(
      StringToNumeric("9007199254740993"sv, test_u64));
  EXPECT_EQ(test_u64, 9007199254740993);
  // Now for some
  ASSERT_FALSE(StringToNumeric("pickles"sv, test_u32));
  ASSERT_FALSE(StringToNumeric("value=10"sv, test_u32));

}

TEST(StringUtils, GetNumericNegative)
{
  using std::string_literals::operator ""s;
  int test;
  ASSERT_TRUE(StringToNumeric("-10"sv, test));
  EXPECT_EQ(test, -10);


  char testc;
  ASSERT_TRUE(StringToNumeric("-128"sv, testc));
  EXPECT_EQ(testc, -128);

  ASSERT_FALSE(StringToNumeric("-129"sv, testc));
  EXPECT_EQ(testc, 0);

  // Test default val
  char default_c=-100;
  ASSERT_FALSE(StringToNumeric("-129"sv, testc, default_c));
  EXPECT_EQ(testc, -100);

}


TEST(StringUtils, GetNumericDouble)
{
  float testf;
  ASSERT_TRUE(StringToNumeric("1.0f"s, testf));
  EXPECT_FLOAT_EQ(testf, 1.0);

  ASSERT_TRUE(StringToNumeric("1e5"s, testf));
  EXPECT_FLOAT_EQ(testf, 100000);

  ASSERT_TRUE(StringToNumeric("3.14159265359"s, testf));

  // you get ~7 decimal places beyond which it is approximated
  // Note that FLOAT_EQ will pass once you're 4e away, so any further
  // decimal places will more or less pass
  // Some of these may be platform specific, esp failure tests at
  // limits is compiler + machine dependant
  EXPECT_FLOAT_EQ(testf, 3.1415927);
  std::string err;
  float defaultf = 10;
  ASSERT_FALSE(
      StringToNumeric("1e129"s, testf, defaultf, &err));
  EXPECT_FLOAT_EQ(testf, 10);

#if __cpp_lib_to_chars < 201611
  EXPECT_EQ(err.find("\"msg=Failed float"), 0);
#else
  EXPECT_EQ(err.find("\"msg=Failed Numeric"), 0);
#endif

  ASSERT_FALSE(
      StringToNumeric("garbage"s, testf, defaultf, &err));
  EXPECT_FLOAT_EQ(testf, 10);

  double testd;
  ASSERT_TRUE(StringToNumeric("3.14159265358979"s, testd));
  EXPECT_DOUBLE_EQ(testd, 3.14159265358979);

  ASSERT_TRUE(StringToNumeric("9007199254740992"s, testd));
  EXPECT_DOUBLE_EQ(testd, 9007199254740992);
  // And we're going to the approximation territory from this point on
  ASSERT_TRUE(StringToNumeric("9007199254740993"s, testd));
  EXPECT_DOUBLE_EQ(testd, 9007199254740992);

  ASSERT_TRUE(StringToNumeric("1.023e129"s, testd));
  EXPECT_DOUBLE_EQ(testd, 1.023e129);

  ASSERT_TRUE(StringToNumeric("1e308"s, testd));
  EXPECT_DOUBLE_EQ(testd, 1e308);

  double defaultd=3.14;
  ASSERT_FALSE(StringToNumeric("1e309"s, testd, defaultd));
  EXPECT_DOUBLE_EQ(testd, 3.14);

  long double testld;
  ASSERT_TRUE(StringToNumeric("3.1415926535897932384626433832795028"s,
                              testld));
  EXPECT_EQ(testld, 3.1415926535897932384626433832795028L);
}


TEST(StringUtils, StringToNumericErrorMessage)
{
  uint32_t test_u32;
  std::string err_msg;
  ASSERT_FALSE(
      StringToNumeric("pickles"sv, test_u32, 0u, &err_msg));
  ASSERT_TRUE(!err_msg.empty());
  // FIXME: not sure if cpp guarantees error message strings will be maintained
  // across standards/ OSes, so probably better to search for invalid arg/ out of
  // range etc in case this test keeps failing on update
  EXPECT_EQ(err_msg,
            "\"msg=Failed Numeric conversion\" key=pickles error_msg=Invalid argument");
  EXPECT_EQ(test_u32, 0);

  err_msg.clear();
  char test_c;
  ASSERT_FALSE(StringToNumeric("128"sv, test_c, (char)0, &err_msg));
  ASSERT_TRUE(!err_msg.empty());
  EXPECT_EQ(err_msg,
            "\"msg=Failed Numeric conversion\" key=128 error_msg=Numerical result out of range");

}