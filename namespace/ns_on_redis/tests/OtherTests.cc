/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#include <cppunit/extensions/HelperMacros.h>
#include <sstream>
#include "namespace/utils/TestHelpers.hh"
#include "namespace/utils/PathProcessor.hh"
#include "namespace/ns_on_redis/LRU.hh"

//------------------------------------------------------------------------------
// Declaration
//------------------------------------------------------------------------------
class OtherTests: public CppUnit::TestCase
{
public:
  CPPUNIT_TEST_SUITE(OtherTests);
  CPPUNIT_TEST(pathSplitterTest);
  CPPUNIT_TEST(lruTest);
  CPPUNIT_TEST_SUITE_END();

  void pathSplitterTest();
  void lruTest();
};

CPPUNIT_TEST_SUITE_REGISTRATION(OtherTests);

//------------------------------------------------------------------------------
// Check the path
//------------------------------------------------------------------------------
bool checkPath(const std::vector<std::string>& elements, size_t depth)
{
  if (elements.size() < depth)
    return false;

  for (size_t i = 1; i <= depth; ++i)
    {
      std::ostringstream o;
      o << "test" << i;

      if (elements[i - 1] != o.str())
	return false;
    }

  return true;
}

//------------------------------------------------------------------------------
// Test the path splitter
//------------------------------------------------------------------------------
void OtherTests::pathSplitterTest()
{
  std::string path1 = "/test1/test2/test3/test4/";
  std::string path2 = "/test1/test2/test3/test4";
  std::string path3 = "test1/test2/test3/test4/";
  std::string path4 = "test1/test2/test3/test4";
  std::vector<std::string> elements;
  eos::PathProcessor::splitPath(elements, path1);
  CPPUNIT_ASSERT(checkPath(elements, 4));
  elements.clear();
  eos::PathProcessor::splitPath(elements, path2);
  CPPUNIT_ASSERT(checkPath(elements, 4));
  elements.clear();
  eos::PathProcessor::splitPath(elements, path3);
  CPPUNIT_ASSERT(checkPath(elements, 4));
  elements.clear();
  eos::PathProcessor::splitPath(elements, path4);
  CPPUNIT_ASSERT(checkPath(elements, 4));
  elements.clear();
  eos::PathProcessor::splitPath(elements, "/");
  CPPUNIT_ASSERT(elements.size() == 0);
  elements.clear();
  eos::PathProcessor::splitPath(elements, "");
  CPPUNIT_ASSERT(elements.size() == 0);
}

//------------------------------------------------------------------------------
// Test namespace LRU basic operations
//------------------------------------------------------------------------------
void OtherTests::lruTest()
{
  struct Entry
  {
    Entry (std::uint64_t id): id_(id) {}
    ~Entry() {}
    std::uint64_t getId() const { return id_; }
    std::uint64_t id_;
  };

  std::uint64_t max_size = 1000;
  std::uint64_t delta = 55;
  eos::LRU<std::uint64_t, Entry> cache {max_size};

  // Fill completely the cache
  for (std::uint64_t id = 0; id < max_size; ++id)
    CPPUNIT_ASSERT(cache.put(id, std::make_shared<Entry>(id)));

  CPPUNIT_ASSERT_EQUAL(max_size, cache.size());

  for (std::uint64_t id = 0; id < max_size; ++id)
    CPPUNIT_ASSERT(cache.get(id)->getId() == id);

  // This triggers a purge of the first 100 elements
  for (auto extra_id = max_size; extra_id < max_size + delta; ++extra_id)
    CPPUNIT_ASSERT(cache.put(extra_id, std::make_shared<Entry>(extra_id)));

  CPPUNIT_ASSERT_EQUAL((std::uint64_t)955, cache.size());

  std::shared_ptr<Entry> elem = cache.get(101);
  CPPUNIT_ASSERT(elem);

  // Add another max_size elements
  for (std::uint64_t id = 2 * max_size; id < 3 * max_size; ++id)
    CPPUNIT_ASSERT(cache.put(id, std::make_shared<Entry>(id)));

  // Object 101 should still be in cache as we hold a reference to it
  CPPUNIT_ASSERT(cache.get(101));
  // Obect 102 should have been evicted from the cache
  CPPUNIT_ASSERT(!cache.get(100));
}
