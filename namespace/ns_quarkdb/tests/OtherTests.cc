/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Other tests
//------------------------------------------------------------------------------

#include <vector>
#include "namespace/ns_quarkdb/ConfigurationParser.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "namespace/ns_quarkdb/LRU.hh"
#include "namespace/utils/PathProcessor.hh"
#include <gtest/gtest.h>
#include <sstream>

//------------------------------------------------------------------------------
// Check the path
//------------------------------------------------------------------------------
bool
checkPath(const std::vector<std::string>& elements, size_t depth)
{
  if (elements.size() < depth) {
    return false;
  }

  for (size_t i = 1; i <= depth; ++i) {
    std::ostringstream o;
    o << "test" << i;

    if (elements[i - 1] != o.str()) {
      return false;
    }
  }

  return true;
}

TEST(PathSplitter, BasicSanity)
{
  std::string path1 = "/test1/test2/test3/test4/";
  std::string path2 = "/test1/test2/test3/test4";
  std::string path3 = "test1/test2/test3/test4/";
  std::string path4 = "test1/test2/test3/test4";
  std::vector<std::string> elements;
  elements = eos::common::SplitPath(path1);
  ASSERT_TRUE(checkPath(elements, 4));
  elements.clear();
  elements = eos::common::SplitPath(path2);
  ASSERT_TRUE(checkPath(elements, 4));
  elements.clear();
  elements = eos::common::SplitPath(path3);
  ASSERT_TRUE(checkPath(elements, 4));
  elements.clear();
  elements = eos::common::SplitPath(path4);
  ASSERT_TRUE(checkPath(elements, 4));
  elements.clear();
  elements = eos::common::SplitPath("/");
  ASSERT_TRUE(elements.empty());
  elements.clear();
  elements = eos::common::SplitPath("");
  ASSERT_TRUE(elements.empty());
}

TEST(PathSplitter, DequeTests)
{
  std::string path1 = "/test1/test2/test3/test4/";
  std::string path2 = "/test1/test2/test3/test4";
  std::string path3 = "test1/test2/test3/test4/";
  std::string path4 = "test1/test2/test3/test4";
  std::deque<std::string> dq1,dq2,dq3,dq4;
  std::deque<std::string> expected {
    "test1","test2","test3","test4"
  };
  eos::PathProcessor::insertChunksIntoDeque(dq1,
                                            path1);
  eos::PathProcessor::insertChunksIntoDeque(dq2,
                                            path2);
  eos::PathProcessor::insertChunksIntoDeque(dq3,
                                            path3);
  eos::PathProcessor::insertChunksIntoDeque(dq4,
                                            path4);

  ASSERT_EQ(dq1, expected);
  ASSERT_EQ(dq2, expected);
  ASSERT_EQ(dq3, expected);
  ASSERT_EQ(dq4, expected);
}

TEST(PathSplitter, DequeTestsNonEmpty)
{
  std::string path1 = "/test1/test2/test3/test4/";
  std::string path2 = "/test1/test2/test3/test4";
  std::string path3 = "test1/test2/test3/test4/";
  std::string path4 = "test1/test2/test3/test4";
  std::deque<std::string> dq1,dq2,dq3,dq4;
  dq1 = dq2 = dq3 = dq4 = {"foo","bar"};
  std::deque<std::string> expected {
    "test1","test2","test3","test4","foo","bar"
  };
  eos::PathProcessor::insertChunksIntoDeque(dq1,
                                            path1);
  eos::PathProcessor::insertChunksIntoDeque(dq2,
                                            path2);
  eos::PathProcessor::insertChunksIntoDeque(dq3,
                                            path3);
  eos::PathProcessor::insertChunksIntoDeque(dq4,
                                            path4);

  ASSERT_EQ(dq1, expected);
  ASSERT_EQ(dq2, expected);
  ASSERT_EQ(dq3, expected);
  ASSERT_EQ(dq4, expected);
}

TEST(LRU, BasicSanity)
{
  struct Entry {
    explicit Entry(std::uint64_t id) : id_(id) {}

    ~Entry() = default;

    std::uint64_t
    getId() const
    {
      return id_;
    }

    std::uint64_t id_;
  };
  std::uint64_t max_size = 1000;
  std::uint64_t delta = 55;
  eos::LRU<std::uint64_t, Entry> cache{max_size};

  // Fill completely the cache
  for (std::uint64_t id = 0; id < max_size; ++id) {
    ASSERT_TRUE(cache.put(id, std::make_shared<Entry>(id)));
  }

  ASSERT_EQ(max_size, cache.size());

  for (std::uint64_t id = 0; id < max_size; ++id) {
    ASSERT_TRUE(cache.get(id)->getId() == id);
  }

  // This triggers a purge of the first 100 elements
  for (auto extra_id = max_size; extra_id < max_size + delta; ++extra_id) {
    ASSERT_TRUE(cache.put(extra_id, std::make_shared<Entry>(extra_id)));
  }

  ASSERT_EQ((std::uint64_t)955, cache.size());
  std::shared_ptr<Entry> elem = cache.get(101);
  ASSERT_TRUE(elem);

  // Add another max_size elements
  for (std::uint64_t id = 2 * max_size; id < 3 * max_size; ++id) {
    ASSERT_TRUE(cache.put(id, std::make_shared<Entry>(id)));
  }

  // Object 101 should still be in cache as we hold a reference to it
  ASSERT_TRUE(cache.get(101));
  // Obect 102 should have been evicted from the cache
  ASSERT_TRUE(!cache.get(100));
}

TEST(PathProcessor, AbsPathTest)
{
  std::string path = "/a/b/c/d/";
  eos::PathProcessor::absPath(path);
  EXPECT_EQ("/a/b/c/d", path);
  path = "/a/./b/./c/././d";
  eos::PathProcessor::absPath(path);
  EXPECT_EQ("/a/b/c/d", path);
  path = "/a/./b/./c/././d/../d/../d/e/../";
  eos::PathProcessor::absPath(path);
  EXPECT_EQ("/a/b/c/d", path);
  path = "/";
  eos::PathProcessor::absPath(path);
  EXPECT_EQ("/", path);
  path = ".././../../.";
  eos::PathProcessor::absPath(path);
  EXPECT_EQ("/", path);
  path = "/a/./b//./c/////./././d";
  eos::PathProcessor::absPath(path);
  EXPECT_EQ("/a/b/c/d", path);
  path = "/a/b/././././/../../c/d/.././../e/./f/";
  eos::PathProcessor::absPath(path);
  EXPECT_EQ("/e/f", path);
}

TEST(QdbContactDetails, BasicSanity)
{
  std::map<std::string, std::string> configuration;
  ASSERT_THROW(eos::ConfigurationParser::parse(configuration), eos::MDException);
  configuration["qdb_cluster"] =
    "example1.cern.ch:1234 example2.cern.ch:2345 example3.cern.ch:3456";
  eos::QdbContactDetails cd = eos::ConfigurationParser::parse(configuration);
  ASSERT_EQ(cd.members.toString(),
            "example1.cern.ch:1234,example2.cern.ch:2345,example3.cern.ch:3456");
  ASSERT_TRUE(cd.password.empty());
  configuration["qdb_password"] = "turtles_turtles_etc";
  cd = eos::ConfigurationParser::parse(configuration);
  ASSERT_EQ(cd.members.toString(),
            "example1.cern.ch:1234,example2.cern.ch:2345,example3.cern.ch:3456");
  ASSERT_EQ(cd.password, "turtles_turtles_etc");
}
