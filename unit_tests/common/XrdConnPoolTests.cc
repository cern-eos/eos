//------------------------------------------------------------------------------
// File: XrdConnPoolTests.cc
// Author: Elvin-Alin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "gtest/gtest.h"
#include "Namespace.hh"
#include "common/XrdConnPool.hh"
#include <list>

EOSCOMMONTESTING_BEGIN

TEST(XrdConnPool, DefaultDisabled)
{
  XrdCl::URL url;
  std::string surl = "root://eospps.cern.ch:1094//path/test.dat";
  url.FromString(surl);
  eos::common::XrdConnPool pool;
  ASSERT_EQ(pool.AssignConnection(url), 0);
  ASSERT_EQ(url.GetURL(), surl);
}

TEST(XrdConnPool, EvenDistribuion)
{
  XrdCl::URL url("root://eospps.cern.ch:1094/path/test.dat");
  uint32_t max_size = 10;
  eos::common::XrdConnPool pool(true, max_size);
  std::ostringstream oss;

  // Add one user per connection id
  for (uint32_t i = 0; i < max_size; ++i) {
    ASSERT_EQ(pool.AssignConnection(url), i + 1);
    oss << "root://" << i + 1 << "@eospps.cern.ch:1094/path/test.dat";
    ASSERT_EQ(url.GetURL(), oss.str());
    oss.str("");
  }

  // Add two more users per connection id
  for (int j = 0; j < 2; ++j) {
    for (uint32_t i = 0; i < max_size; ++i) {
      ASSERT_EQ(pool.AssignConnection(url), i + 1);
      oss << "root://" << i + 1 << "@eospps.cern.ch:1094/path/test.dat";
      ASSERT_EQ(url.GetURL(), oss.str());
      oss.str("");
    }
  }

  // Free one connection id 5 at a time and allocate a new one
  oss.str("root://5@eospps.cern.ch:1094/path/test.dat");

  for (int i = 0; i < 3; ++i) {
    pool.ReleaseConnection(oss.str());
    ASSERT_EQ(pool.AssignConnection(url), 5);
  }

  // Free all connection ids 5 in one go
  for (int i = 0; i < 3; ++i) {
    pool.ReleaseConnection(oss.str());
  }

  ASSERT_EQ(pool.AssignConnection(url), 5);
  ASSERT_EQ(pool.AssignConnection(url), 5);
  ASSERT_EQ(pool.AssignConnection(url), 5);
  // Now the pool should allocate from id 1 since they all have 3 clients
  // per id
  ASSERT_EQ(pool.AssignConnection(url), 1);
}

TEST(XrdConnPool, ConnIdHelper)
{
  XrdCl::URL url("root://eospps.cern.ch:1094/path/test.dat");
  uint32_t max_size = 10;
  eos::common::XrdConnPool pool(true, max_size);
  std::ostringstream oss;

  // Each gets the same id since it's released at the end of each loop
  for (uint32_t i = 0; i < max_size; ++i) {
    XrdConnIdHelper id_helper(pool, url);
    ASSERT_EQ(id_helper.GetId(), 1);
  }

  // Allocate them dynamically
  std::list<XrdConnIdHelper*> lst;

  for (uint32_t i = 0; i < max_size; ++i) {
    auto elem = new XrdConnIdHelper(pool, url);
    lst.push_back(elem);
    ASSERT_EQ(elem->GetId(), i + 1);
  }

  // std::string out;
  // pool.Dump(out);
  // std::cout << out << std::endl;

  // Release the last two ids and try new assignments
  for (int i = 0; i < 2; ++i) {
    auto elem = lst.back();
    delete elem;
    lst.pop_back();
  }

  for (int i = 0; i < 2; ++i) {
    auto elem = new XrdConnIdHelper(pool, url);
    lst.push_back(elem);
    ASSERT_EQ(elem->GetId(), max_size - 1 + i);
  }

  for (auto& elem : lst) {
    delete elem;
  }

  lst.clear();
  // Add one connection it should get the id 1
  auto elem = new XrdConnIdHelper(pool, url);
  lst.push_back(elem);
  ASSERT_EQ(elem->GetId(), 1);
  delete elem;
  lst.clear();
}

EOSCOMMONTESTING_END
