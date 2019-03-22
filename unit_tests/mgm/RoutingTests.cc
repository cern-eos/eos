//------------------------------------------------------------------------------
// File: RoutingTests.cc
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
#include "mgm/RouteEndpoint.hh"
#include "mgm/PathRouting.hh"

//------------------------------------------------------------------------------
// Test basic RoutEndpoint construction and parsing
//------------------------------------------------------------------------------
TEST(Routing, Construction)
{
  using namespace eos::mgm;
  using eos::mgm::PathRouting;
  std::string stat_info;
  eos::mgm::PathRouting route(std::chrono::seconds(0));
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
    ASSERT_TRUE(route.Add("/eos/", std::move(endpoint)));
    RouteEndpoint endpoint1;
    ASSERT_TRUE(endpoint1.ParseFromString(input));
    ASSERT_FALSE(route.Add("/eos/", std::move(endpoint1)));
  }

  ASSERT_TRUE(route.Remove("/eos/"));
  ASSERT_FALSE(route.Remove("/eos/unkown/dir/"));
  route.Clear();
}

//------------------------------------------------------------------------------
// Test basic
//------------------------------------------------------------------------------
TEST(Routing, StallRedirect)
{
  using namespace eos::mgm;
  using eos::mgm::PathRouting;
  std::string stat_info;
  // Routing without async updates
  eos::mgm::PathRouting route(std::chrono::seconds(0));
  std::vector<std::string> inputs {
    "eos_dummy1.cern.ch:1094:8000",
    "eos_dummy2.cern.ch:2094:9000",
    "eos_dummy3.cern.ch:3094:10000",
    "eos_dummy4.cern.ch:4094:11000"};
  int count = 0;

  // Add several routes to test out the routing
  for (const auto& input : inputs) {
    ++count;
    RouteEndpoint endpoint;
    endpoint.mIsOnline.store(true); // for testin purposes
    ASSERT_TRUE(endpoint.ParseFromString(input));
    ASSERT_TRUE(route.Add("/eos/dir" + std::to_string(count) + "/",
                          std::move(endpoint)));
  }

  eos::common::VirtualIdentity vid;
  eos::common::Mapping::Root(vid);
  std::string host;
  int port;
  ASSERT_TRUE(PathRouting::Status::NOROUTING ==
              route.Reroute("", nullptr, vid, host, port, stat_info));
  ASSERT_TRUE(PathRouting::Status::NOROUTING ==
              route.Reroute("/", nullptr, vid, host, port, stat_info));
  ASSERT_TRUE(PathRouting::Status::NOROUTING ==
              route.Reroute("/unkown", nullptr, vid, host, port, stat_info));
  ASSERT_TRUE(PathRouting::Status::NOROUTING ==
              route.Reroute("/eos/", nullptr, vid, host, port, stat_info));
  // Test http/https redirection
  vid.prot = "http";
  ASSERT_TRUE(PathRouting::Status::REROUTE ==
              route.Reroute("/eos/dir1/", nullptr, vid, host, port, stat_info));
  ASSERT_TRUE(PathRouting::Status::REROUTE ==
              route.Reroute("/eos/dir1", nullptr, vid, host, port, stat_info));
  ASSERT_TRUE(host == "eos_dummy1.cern.ch");
  ASSERT_TRUE(port == 8000);
  vid.prot = "https";
  ASSERT_TRUE(PathRouting::Status::REROUTE ==
              route.Reroute("/eos/dir1", nullptr, vid, host, port, stat_info));
  ASSERT_TRUE(host == "eos_dummy1.cern.ch");
  ASSERT_TRUE(port == 8000);
  // Test xrd redirection
  vid.prot = "";
  ASSERT_TRUE(PathRouting::Status::REROUTE ==
              route.Reroute("/eos/dir2", nullptr, vid, host, port, stat_info));
  ASSERT_TRUE(host == "eos_dummy2.cern.ch");
  ASSERT_TRUE(port == 2094);
  // Test redirection diven a longer path
  ASSERT_TRUE(PathRouting::Status::REROUTE ==
              route.Reroute("/eos/dir3/subdir1/subdir2", nullptr, vid, host,
                            port, stat_info));
  ASSERT_TRUE(host == "eos_dummy3.cern.ch");
  ASSERT_TRUE(port == 3094);
  ASSERT_TRUE(PathRouting::Status::NOROUTING == route.Reroute("/",
              "&mgm.fsid=3452&mgm.fid=0e98cc49&mgm.localprefix=/data13",
              vid, host, port, stat_info));
  ASSERT_TRUE(PathRouting::Status::NOROUTING == route.Reroute("/",
              "", vid, host, port, stat_info));

  // Put all endpoints as not online and not master to trigger stall response
  for (const auto& input : inputs) {
    RouteEndpoint endpoint;
    endpoint.mIsOnline.store(false);
    endpoint.mIsMaster.store(false);
    ASSERT_TRUE(endpoint.ParseFromString(input));
    ASSERT_TRUE(route.Add("/eos/dir/multi_ep/", std::move(endpoint)));
  }

  ASSERT_TRUE(PathRouting::Status::STALL ==
              route.Reroute("/eos/dir/multi_ep/", nullptr, vid, host, port,
                            stat_info));
  RouteEndpoint endpoint;
  endpoint.mIsOnline.store(true);
  endpoint.mIsMaster.store(true);
  ASSERT_TRUE(endpoint.ParseFromString("eos_dummy5.cern.ch:5094:12000"));
  ASSERT_TRUE(route.Add("/eos/dir/multi_ep/", std::move(endpoint)));
  ASSERT_TRUE(PathRouting::Status::REROUTE ==
              route.Reroute("/eos/dir/multi_ep/", nullptr, vid, host, port,
                            stat_info));
  ASSERT_STREQ("eos_dummy5.cern.ch", host.c_str());
  ASSERT_EQ(5094, port);
  route.Clear();
}
