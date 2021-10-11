#include "gtest/gtest.h"
#include "common/StringSplit.hh"

using namespace eos::common;
using sv_vector = std::vector<std::string_view>;
using sv_ilist = std::initializer_list<std::string_view>;
using s_vector = std::vector<std::string>;

TEST(StringSplit, Empty)
{
  sv_vector emptyv;
  ASSERT_EQ(StringSplit("",""), emptyv);
  ASSERT_EQ(StringSplit("////","/"), emptyv);
  ASSERT_EQ(StringSplit("abcd",""), sv_vector({"abcd"}));
  ASSERT_EQ(CharSplitIt("abcd",'/'), sv_vector({"abcd"}));
  char un_init;
  ASSERT_EQ(CharSplitIt("abcd",un_init),sv_vector({"abcd"}));
  ASSERT_EQ(CharSplitIt("abcd",'\0'),sv_vector({"abcd"}));
}

TEST(StringSplit, BasicIt)
{

  sv_vector expect_v{"eos","foo","bar"};
  ASSERT_EQ(StringSplitIt("/eos/foo/bar/","/"),
            expect_v);
  ASSERT_EQ(StringSplitIt("/////eos//foo//bar////","/"),
            expect_v);
  ASSERT_EQ(StringSplitIt("eos//foo//bar","/"),
            expect_v);

  ASSERT_EQ(CharSplitIt("/eos/foo/bar/",'/'),
            expect_v);
  ASSERT_EQ(CharSplitIt("/////eos//foo//bar////",'/'),
            expect_v);
  ASSERT_EQ(CharSplitIt("eos//foo//bar",'/'),
            expect_v);
}

TEST(StringSplit, BasicStdString)
{

  s_vector expect_v{"eos","foo","bar"};

  ASSERT_EQ(StringSplit<s_vector>("/eos/foo/bar/","/"),
            expect_v);
  ASSERT_EQ(StringSplit<s_vector>("/////eos//foo//bar////","/"),
            expect_v);
  ASSERT_EQ(StringSplit<s_vector>("eos//foo//bar","/"),
            expect_v);

}

TEST(StringSplit, NullSplit)
{
  std::string null_string;
  null_string += '\0';

  ASSERT_EQ(CharSplitIt(null_string, '\0'),sv_vector());
  // Verify this works with std::string also
  ASSERT_EQ(StringSplitIt(null_string, null_string), sv_vector());

  s_vector expect_v{"eos","foo","bar"};
  std::string s = "eos";
  s += '\0';
  s += "foo";
  s += '\0';
  s += "bar";

  ASSERT_EQ(CharSplitIt(s,'\0'), expect_v);
  ASSERT_EQ(StringSplitIt(s, null_string), expect_v);
  std::string s2 = s;
  s2 += '\0';
  ASSERT_EQ(CharSplitIt(s2,'\0'), expect_v);
  ASSERT_EQ(StringSplitIt(s2, null_string), expect_v);

  std::string s3 = null_string + s2;
  ASSERT_EQ(CharSplitIt(s3,'\0'), expect_v);
  ASSERT_EQ(StringSplitIt(s3, null_string), expect_v);

  std::string s4;
  for (int i =0; i < 1024; i++) {
    s4 += '\0';
  }
  s4 += s2;

  ASSERT_EQ(CharSplitIt(s4,'\0'), expect_v);
  ASSERT_EQ(StringSplitIt(s4, null_string), expect_v);
  ASSERT_EQ(StringSplitIt(s4, "/"), sv_vector({s4}));
}

TEST(StringSplit, EmptyIter)
{
  auto empty = StringSplit("////","/");
  ASSERT_EQ(empty.begin(),empty.end());
  const auto segments = StringSplit("/eos/foo/bar/",",");
  ASSERT_NE(segments.begin(),segments.end());

}

TEST(StringSplit, Iterator)
{
  const auto segments = StringSplit("/eos/foo/bar/","/");
  auto iter = segments.begin();
  ASSERT_EQ("eos",*iter);
  ASSERT_EQ(3, iter->size());
  ASSERT_EQ("eos", *iter++);
  ASSERT_EQ("foo", *iter);
  ASSERT_EQ("bar",*++iter);
  ASSERT_EQ(segments.end(),++iter);
}

TEST(StringSplit, IteratorCopy)
{
  const auto segments = StringSplitIt("/eos/foo/bar","/");
  auto start_segment = segments.begin();
  ASSERT_NE(start_segment, segments.end());
  auto start_copy = start_segment;
  ASSERT_EQ(start_copy, start_segment);

  ASSERT_EQ("eos", *start_segment);
  ASSERT_NE(start_copy, ++start_segment);
  ASSERT_EQ("foo", *start_segment++);
  ASSERT_EQ(segments.end(), ++start_segment);

  ASSERT_EQ("eos",*start_copy);
  ASSERT_EQ("foo",*++start_copy);
}

TEST(StringSplit, StrCopy)
{
  std::vector<std::string> expected = {"eos","foo","bar"};
  std::vector<std::string> actual;
  for (std::string_view part: StringSplit("/eos/foo/bar/", "/")) {
    actual.emplace_back(part);
  }
  ASSERT_EQ(expected, actual);
}

TEST(StringSplit, MultiSplit)
{
  sv_vector expect_v {"key1","val1","key2","val2"};
  ASSERT_EQ(StringSplit("key1=val1;\nkey2=val2",";=\n"),expect_v);
  std::string k = "?key1=val1";
  k+='\0';
  k+=";key2=val2;";

  std::string delim = "?=;\n";
  delim += '\0';

  ASSERT_EQ(StringSplit(k,delim), expect_v);

  // Check that a delim starting with null doesn't break parsing
  std::string delim2;
  delim2 += '\0';
  delim2 += delim;
  ASSERT_EQ(StringSplit(k,delim2), expect_v);

  std::string k2;
  for (int i = 0; i <= 10; i++) {
    k2 += '\0';
  }
  k2 += k;
  ASSERT_EQ(StringSplit(k2,delim), expect_v);
}

TEST(StringSplit, get_delim_p)
{
  using namespace std::literals;
  // pos search is left inclusive [start,end)
  ASSERT_EQ(detail::get_delim_p("foo;;bar;baz"sv,";"sv,0),3);
  ASSERT_EQ(detail::get_delim_p("foo;;bar;baz"sv,";"sv,3),3);
  ASSERT_EQ(detail::get_delim_p("foo;;bar;baz"sv,";"sv,4),4);
  ASSERT_EQ(detail::get_delim_p("foo;;bar;baz"sv,";"sv,5),8);

  // Test the char variant
  ASSERT_EQ(detail::get_delim_p("foo;;bar;baz"sv,';',0),3);
  ASSERT_EQ(detail::get_delim_p("foo;;bar;baz"sv,';',3),3);
  ASSERT_EQ(detail::get_delim_p("foo;;bar;baz"sv,';',4),4);
  ASSERT_EQ(detail::get_delim_p("foo;;bar;baz"sv,';',5),8);
}

TEST(StringSplit, PathSplitter)
{
  s_vector expect_v{"eos","foo","bar"};
  s_vector empty;
  ASSERT_EQ(SplitPath(""), empty);
  ASSERT_EQ(SplitPath("/"), empty);
  ASSERT_EQ(SplitPath("///"), empty);
  ASSERT_EQ(SplitPath("/eos/foo/bar/"), expect_v);
  ASSERT_EQ(SplitPath("/////eos//foo//bar////"), expect_v);
  ASSERT_EQ(SplitPath("eos//foo//bar"), expect_v);
}
