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
  char default_c = -100;
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
  double defaultd = 3.14;
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

using eos::common::replace_all;

TEST(StringUtils, ReplaceAllEmpty) {
  std::string str = "the quick brown fox jumps over the lazy dog";
  std::string orig_str {str};

  replace_all(orig_str, "","");
  EXPECT_EQ(orig_str, str);

  replace_all(orig_str, "", "foo");
  EXPECT_EQ(orig_str, str);

  replace_all(orig_str, "fox", "charlie", 1000,2000);
  EXPECT_EQ(orig_str, str);

  replace_all(orig_str, "fox", "charlie", 10,5);
  EXPECT_EQ(orig_str, str);

}

TEST(StringUtils, ReplaceAllBasic) {
  std::string str = "the quick brown fox jumps over the lazy dog";

  replace_all(str, "fox", "charlie");
  EXPECT_EQ(str, "the quick brown charlie jumps over the lazy dog");

  str = "the quick brown fox jumps over the lazy dog";
  replace_all(str, "the", "a");
  EXPECT_EQ(str, "a quick brown fox jumps over a lazy dog");

  str = "the quick brown fox jumps over the lazy dog";
  replace_all(str, "o", "O");
  EXPECT_EQ(str, "the quick brOwn fOx jumps Over the lazy dOg");

  str = "the quick brown fox jumps over the lazy dog";
  replace_all(str, " ", "_");
  EXPECT_EQ(str, "the_quick_brown_fox_jumps_over_the_lazy_dog");

  str = "the quick brown fox jumps over the lazy dog";
  replace_all(str, "the", "a", 10, 43);
  EXPECT_EQ(str, "the quick brown fox jumps over a lazy dog");

  str = "the quick brown fox jumps over the lazy dog";
  replace_all(str, "o", "O", 12, 25);
  EXPECT_EQ(str, "the quick brOwn fOx jumps over the lazy dog");
}

TEST(StringUtils, ReplaceAllReduce) {
  std::string str = "aaaaaa";
  replace_all(str, "aa", "b");
  EXPECT_EQ(str, "bbb");

  str = "aaaaa";
  replace_all(str, "aa","b");
  EXPECT_EQ(str, "bba");
}

TEST(StringUtils, ReplaceAllEmptyIn) {
  std::string str = "hello world";
  replace_all(str, "hello","");
  EXPECT_EQ(str, " world");
}

// Test with 'from' parameter
TEST(StringUtils, ReplaceAllFromParameter) {
    std::string str = "abc def abc ghi abc";
    replace_all(str, "abc", "xyz", 4); // Start from position 4
    EXPECT_EQ(str, "abc def xyz ghi xyz");
}

// Test with 'to' parameter
TEST(StringUtils, ReplaceAllToParameter) {
    std::string str = "abc def abc ghi abc";
    replace_all(str, "abc", "xyz", 0, 10); // Only replace up to position 10
    EXPECT_EQ(str, "xyz def xyz ghi abc");
}

// Test with both 'from' and 'to' parameters
TEST(StringUtils, ReplaceAllFromAndToParameters) {
    std::string str = "abc def abc ghi abc jkl abc";
    replace_all(str, "abc", "xyz", 4, 15); // Replace from pos 4 to pos 15
    EXPECT_EQ(str, "abc def xyz ghi abc jkl abc");
}

// Test invalid range parameters
TEST(StringUtils, ReplaceAllInvalidRangeParameters) {
    std::string str = "hello world hello";
    std::string original = str;

    // from >= string size
    replace_all(str, "hello", "hi", 20);
    EXPECT_EQ(str, original);

    // from > to
    std::string str2 = "hello world hello";
    replace_all(str2, "hello", "hi", 10, 5);
    EXPECT_EQ(str2, original);
}

// Test overlapping patterns
TEST(StringUtils, ReplaceAllOverlappingPatterns) {
    std::string str = "aaaa";
    replace_all(str, "aa", "b");
    // Should replace non-overlapping occurrences: "aaaa" -> "bb"
    EXPECT_EQ(str, "bb");
}

// Test single character replacement
TEST(StringUtils, ReplaceAllSingleCharacterReplacement) {
    std::string str = "a b a c a";
    replace_all(str, "a", "x");
    EXPECT_EQ(str, "x b x c x");
}

// Test replacement at string boundaries
TEST(StringUtils, ReplaceAllBoundaryReplacement) {
    // At beginning
    std::string str1 = "hello world";
    replace_all(str1, "hello", "hi");
    EXPECT_EQ(str1, "hi world");

    // At end
    std::string str2 = "world hello";
    replace_all(str2, "hello", "hi");
    EXPECT_EQ(str2, "world hi");

    // Entire string
    std::string str3 = "hello";
    replace_all(str3, "hello", "hi");
    EXPECT_EQ(str3, "hi");
}

// Test with special characters
TEST(StringUtils, ReplaceAllSpecialCharacters) {
    std::string str = "a\nb\ta\nc";
    replace_all(str, "a", "x");
    EXPECT_EQ(str, "x\nb\tx\nc");
}

// Test case sensitivity
TEST(StringUtils, ReplaceAllCaseSensitivity) {
    std::string str = "Hello world HELLO";
    replace_all(str, "Hello", "Hi");
    EXPECT_EQ(str, "Hi world HELLO"); // Should not replace "HELLO"
}

// Test with long strings and patterns
TEST(StringUtils, ReplaceAllLongStringsAndPatterns) {
    std::string str = "this is a long pattern and this is another long pattern";
    replace_all(str, "long pattern", "short");
    EXPECT_EQ(str, "this is a short and this is another short");
}

// Test performance with many replacements
TEST(StringUtils, ReplaceAllManyReplacements) {
    std::string str;
    for (int i = 0; i < 1000; ++i) {
        str += "a ";
    }
    replace_all(str, "a", "bb");

    // Verify the result
    std::string expected;
    for (int i = 0; i < 1000; ++i) {
        expected += "bb ";
    }
    EXPECT_EQ(str, expected);
}

// Test with substring that appears within the replacement
TEST(StringUtils, ReplaceAllSubstringInReplacement) {
    std::string str = "abc abc abc";
    replace_all(str, "abc", "abcdef");
    EXPECT_EQ(str, "abcdef abcdef abcdef");
}

// Test edge case where search pattern is larger than available range
TEST(StringUtils, ReplaceAllSearchPatternLargerThanRange) {
    std::string str = "hello world";
    std::string original = str;
    replace_all(str, "hello world!", "hi", 0, 5); // Pattern longer than range
    EXPECT_EQ(str, original); // Should not change
}

// Test replacement with exact range boundaries
TEST(StringUtils, ReplaceAllExactRangeBoundaries) {
    std::string str = "abcdefabc";
    replace_all(str, "abc", "x", 0, 2); // Range exactly matches first "abc"
    EXPECT_EQ(str, "xdefabc");
}

// Test string_view parameters
TEST(StringUtils, ReplaceAllStringViewParameters) {
    std::string str = "hello world hello";
    std::string_view search = "hello";
    std::string_view replacement = "hi";
    replace_all(str, search, replacement);
    EXPECT_EQ(str, "hi world hi");
}

// Test with to parameter as string::npos (default)
TEST(StringUtils, ReplaceAllDefaultToParameter) {
    std::string str = "abc def abc ghi abc";
    replace_all(str, "abc", "xyz", 4, std::string::npos);
    EXPECT_EQ(str, "abc def xyz ghi xyz");
}

// Test contraction that would make 'to' go below search pattern length
TEST(StringUtils, ReplaceAllContractionBelowPatternLength) {
    std::string str = "abcabcabc";
    replace_all(str, "abc", "x", 0, 8); // This should handle the contraction properly
    EXPECT_EQ(str, "xxx");
}
