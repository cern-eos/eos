//------------------------------------------------------------------------------
// File: RouteEndpointTests.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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
#include "mgm/XrdMgmOfs.hh"
#include "common/Mapping.hh"

TEST(RouteEndpoint, Construction)
{
  using namespace eos::mgm;
  XrdMgmOfs ofs(nullptr);
  std::vector<std::string> inputs {
    "eos_dummy1.cern.ch:1094:8000",
    "eos_dummy2.cern.ch:2094:9000",
    "eos_dummy3.cern.ch:3094:1000",
    "eos_dummy4.cern.ch:4094:11000"};
  {
    // Check parsing and equality operators
    RouteEndpoint endpoint1, endpoint2;
    ASSERT_FALSE(endpoint1.ParseFromString("wrong.cern.ch"));
    ASSERT_FALSE(endpoint1.ParseFromString("wrong.cern.ch:94"));
    ASSERT_FALSE(endpoint1.ParseFromString("wrong.cern.ch:94:number"));
    ASSERT_FALSE(endpoint1.ParseFromString("wrong.cern.ch:number:number"));
    ASSERT_TRUE(endpoint1.ParseFromString(inputs[0]));
    ASSERT_TRUE(endpoint2.ParseFromString(inputs[1]));
    ASSERT_NE(endpoint1, endpoint2);
    ASSERT_TRUE(endpoint2.ParseFromString(inputs[0]));
    ASSERT_EQ(endpoint1, endpoint2);
  }

  for (const auto& input : inputs) {
    RouteEndpoint endpoint;
    ASSERT_TRUE(endpoint.ParseFromString(input));
    ASSERT_TRUE(ofs.AddPathRoute("/eos/", std::move(endpoint)));
    ASSERT_FALSE(ofs.AddPathRoute("/eos/", std::move(endpoint)));
  }

  ASSERT_TRUE(ofs.RemovePathRoute("/eos/"));
  ASSERT_FALSE(ofs.RemovePathRoute("/eos/unkown/dir/"));
  ofs.ClearPathRoutes();
  int count = 0;

  // Add several routes to test out the routing
  for (const auto& input : inputs) {
    ++count;
    RouteEndpoint endpoint;
    ASSERT_TRUE(endpoint.ParseFromString(input));
    ASSERT_TRUE(ofs.AddPathRoute("/eos/dir" + std::to_string(count) + "/",
                                 std::move(endpoint)));
  }

  eos::common::Mapping::VirtualIdentity vid;
  eos::common::Mapping::Root(vid);
  std::string host;
  int port;
  ASSERT_FALSE(ofs.PathReroute("", nullptr, vid, host, port));
  ASSERT_FALSE(ofs.PathReroute("/", nullptr, vid, host, port));
  ASSERT_FALSE(ofs.PathReroute("/unkown", nullptr, vid, host, port));
  ASSERT_FALSE(ofs.PathReroute("/eos/", nullptr, vid, host, port));
  // Test http/https redirection
  vid.prot = "http";
  ASSERT_TRUE(ofs.PathReroute("/eos/dir1/", nullptr, vid, host, port));
  ASSERT_TRUE(ofs.PathReroute("/eos/dir1", nullptr, vid, host, port));
  ASSERT_TRUE(host == "eos_dummy1.cern.ch");
  ASSERT_TRUE(port == 8000);
  vid.prot = "https";
  ASSERT_TRUE(ofs.PathReroute("/eos/dir1", nullptr, vid, host, port));
  ASSERT_TRUE(host == "eos_dummy1.cern.ch");
  ASSERT_TRUE(port == 8000);
  // Test xrd redirection
  vid.prot = "";
  ASSERT_TRUE(ofs.PathReroute("/eos/dir2", nullptr, vid, host, port));
  ASSERT_TRUE(host == "eos_dummy2.cern.ch");
  ASSERT_TRUE(port == 2094);
  // Test redirection diven a longer path
  ASSERT_TRUE(ofs.PathReroute("/eos/dir3/subdir1/subdir2", nullptr, vid, host,
                              port));
  ASSERT_TRUE(host == "eos_dummy3.cern.ch");
  ASSERT_TRUE(port == 3094);
}
