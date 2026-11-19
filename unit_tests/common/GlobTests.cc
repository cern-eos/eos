//------------------------------------------------------------------------------
// File: GlobTests.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "common/Glob.hh"
#include "gtest/gtest.h"
#include <atomic>
#include <string>
#include <thread>
#include <vector>

EOSCOMMONNAMESPACE_BEGIN

TEST(Glob, Wildcards)
{
  EXPECT_TRUE(Glob::Match("asdf*.txt", "asdf1.txt"));
  EXPECT_TRUE(Glob::Match("asdf*.txt", "asdf.txt"));
  EXPECT_FALSE(Glob::Match("asdf*.txt", "bsdf1.txt"));
  EXPECT_TRUE(Glob::Match("*.txt", "asdf1.txt"));
  EXPECT_TRUE(Glob::Match("a?df1.txt", "asdf1.txt"));
  EXPECT_FALSE(Glob::Match("a?df1.txt", "adf1.txt"));
  EXPECT_FALSE(Glob::Match("a?df1.txt", "assdf1.txt"));
  EXPECT_FALSE(Glob::Match("*.txt", "file.TXT"));
  EXPECT_TRUE(Glob::Match("file*", "file with spaces"));
  EXPECT_TRUE(Glob::Match("*", ""));
}

TEST(Glob, BracketExpressions)
{
  EXPECT_TRUE(Glob::Match("[ab]sdf*", "bsdf1.txt"));
  EXPECT_FALSE(Glob::Match("[ab]sdf*", "csdf1.txt"));
  EXPECT_TRUE(Glob::Match("file[1-3].txt", "file2.txt"));
  EXPECT_FALSE(Glob::Match("file[1-3].txt", "file4.txt"));
  EXPECT_TRUE(Glob::Match("[!a]*", "beta"));
  EXPECT_FALSE(Glob::Match("[!a]*", "alpha"));
  EXPECT_TRUE(Glob::Match("[[]*", "[abc"));

  // Unclosed brackets follow the platform's fnmatch behavior.
#if defined(__GLIBC__)
  EXPECT_TRUE(Glob::Match("a[b", "a[b"));
  EXPECT_TRUE(Glob::Match("a[b*", "a[bc"));
#elif defined(__APPLE__)
  EXPECT_FALSE(Glob::Match("a[b", "a[b"));
  EXPECT_FALSE(Glob::Match("a[b*", "a[bc"));
#endif
  EXPECT_FALSE(Glob::Match("a[b*", "abc"));
  EXPECT_TRUE(Glob::Match("file[[:digit:]].txt", "file7.txt"));
  EXPECT_FALSE(Glob::Match("file[[:digit:]].txt", "filex.txt"));
}

TEST(Glob, LeadingDots)
{
  EXPECT_FALSE(Glob::Match("*", ".hidden"));
  EXPECT_FALSE(Glob::Match("?*", ".x"));
  EXPECT_FALSE(Glob::Match("[!.]*", ".x"));
  EXPECT_FALSE(Glob::Match("[.]*", ".x"));
  EXPECT_TRUE(Glob::Match(".*", ".hidden"));
  EXPECT_TRUE(Glob::Match(".*", "."));
  EXPECT_TRUE(Glob::Match(".*", ".."));
  EXPECT_TRUE(Glob::Match(R"(\.*)", ".hidden"));
}

TEST(Glob, EscapesAndLiteralPatterns)
{
  EXPECT_TRUE(Glob::Match(R"(a\*b*)", "a*bc"));
  EXPECT_FALSE(Glob::Match(R"(a\*b*)", "axbc"));
  EXPECT_TRUE(Glob::Match(R"(a\?b*)", "a?bc"));
  EXPECT_TRUE(Glob::Match(R"(a\\*)", R"(a\bc)"));
  EXPECT_FALSE(Glob::Match(R"(regexx*p.tt\.ern)", R"(regexx*p.tt\.ern)"));

  EXPECT_TRUE(Glob::Match(R"(a\*)", "a*"));
  EXPECT_FALSE(Glob::Match(R"(a*\)", R"(abc\)"));

  EXPECT_TRUE(Glob::Match("abc", "abc"));
  EXPECT_FALSE(Glob::Match("abc", "abcd"));
  EXPECT_FALSE(Glob::Match("test", ""));
  EXPECT_TRUE(Glob::Match("", ""));
  EXPECT_FALSE(Glob::Match("{one,two}*", "one.txt"));
  EXPECT_TRUE(Glob::Match("{one,two}*", "{one,two}.txt"));
  EXPECT_FALSE(Glob::Match("@(one|two)*", "one.txt"));
  EXPECT_TRUE(Glob::Match("@(one|two)*", "@(one|two).txt"));
}

TEST(Glob, BasenamePatterns)
{
  EXPECT_TRUE(Glob::Match("*.txt", "file.txt"));
  EXPECT_FALSE(Glob::Match("dir/*.txt", "file.txt"));
}

TEST(Glob, MatchesFullBasename)
{
  const std::string prefix(254, 'a');
  const std::string longName = prefix + 'b';
  EXPECT_TRUE(Glob::Match("*a", prefix.c_str()));
  EXPECT_FALSE(Glob::Match("*b", prefix.c_str()));
  EXPECT_FALSE(Glob::Match("*a", longName.c_str()));
  EXPECT_TRUE(Glob::Match("*b", longName.c_str()));

  // The two-byte character straddles the legacy truncation boundary.
  const std::string utf8Name = std::string(253, 'a') + "\xc3\xa9";
  EXPECT_TRUE(Glob::Match("*\xc3\xa9", utf8Name.c_str()));
}

TEST(Glob, ConcurrentMatches)
{
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;

  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&failures, t]() {
      const std::string prefix = "file" + std::to_string(t) + "-";
      const std::string pattern = prefix + "*.txt";
      const std::string match = prefix + "data.txt";
      const std::string other = "file" + std::to_string((t + 1) % 8) + "-data.txt";

      for (int i = 0; i < 10000; ++i) {
        if (!Glob::Match(pattern.c_str(), match.c_str()) ||
            Glob::Match(pattern.c_str(), other.c_str())) {
          ++failures;
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(0, failures.load());
}
EOSCOMMONNAMESPACE_END
