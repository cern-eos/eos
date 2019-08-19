//------------------------------------------------------------------------------
// File: QoSClassTests.cc
// Author: Mihai Patrascoiu <mihai.patrascoiu@cern.ch>
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
#include "mgm/qos/QoSClass.hh"
#include "mgm/qos/QoSConfig.hh"
#include "namespace/interface/IFileMD.hh"

#include <json/json.h>

//------------------------------------------------------------------------------
// Utility function to convert JSON array into vector container
//------------------------------------------------------------------------------
std::vector<std::string> makeArray(const Json::Value& jsonArray)
{
  std::vector<std::string> array;

  for (auto& it: jsonArray) {
    array.emplace_back(it.asString());
  }

  return array;
}

//------------------------------------------------------------------------------
// Utility function to convert JSON object into map  container
//------------------------------------------------------------------------------
std::map<std::string, std::string> makeMap(const Json::Value& json)
{
  std::map<std::string, std::string> map;

  for (auto& it: json.getMemberNames()) {
    map.emplace(it, json[it].asString());
  }

  return map;
}

//------------------------------------------------------------------------------
// Utility functions to check whether two containers are identical:
//   -- Template function to print two mismatched entries
//   -- Template function overload to print two mismatched pairs
//   -- Template function to identify container elements mismatches
//------------------------------------------------------------------------------
template<typename T>
inline std::string mismatchString(const T& t1, const T& t2)
{
  return SSTR(t1 << " expected " << t2);
}

template<typename T1, typename T2>
inline std::string mismatchString(const pair<T1, T2>& p1,
                                  const pair<T1, T2>& p2)
{
  return SSTR("[" << p1.first << ", " << p1.second << "] expected ["
                  << p2.first << ", " << p2.second << "]");
}

template<typename Container>
void ASSERT_CONTAINER_EQ(const Container& c1, const Container& c2)
{
  auto pair = std::mismatch(c1.begin(), c1.end(), c2.begin());
  bool equal = (pair.first == c1.end() && pair.second == c2.end());

  if (!equal) {
    std::cout << SSTR("[  INFO    ] Received "
                        << mismatchString(*pair.first, *pair.second)
                        << std::endl);
  }

  ASSERT_TRUE(equal);
}

//------------------------------------------------------------------------------
// Provide a dummy QoS Class JSON
//------------------------------------------------------------------------------
Json::Value makeJson(const std::string& name = "QoSTest",
                     const std::vector<std::string>& transitions = {"disk", "tape"},
                     const std::vector<std::string>& locations = {"CH", "HU"},
                     const eos::IFileMD::QoSAttrMap& attrMap = {{"layout","replica"},
                                                                {"replica", "2"},
                                                                {"checksum", "adler32"},
                                                                {"placement", "scattered"}})
{
  Json::Value json;

  json["name"] = name;
  json["transition"] = Json::arrayValue;
  for (auto& transition: transitions) {
    json["transition"].append(transition);
  }

  json["metadata"] = Json::objectValue;
  json["metadata"][CDMI_REDUNDANCY_TAG] = (Json::UInt) 1;
  json["metadata"][CDMI_LATENCY_TAG] = (Json::UInt) 100;
  json["metadata"][CDMI_PLACEMENT_TAG] = Json::arrayValue;
  for (auto& location: locations) {
    json["metadata"][CDMI_PLACEMENT_TAG].append(location);
  }

  json["attributes"] = Json::objectValue;
  for (auto& it: attrMap) {
    json["attributes"][it.first] = it.second;
  }

  return json;
}

//------------------------------------------------------------------------------
// Test factory method - valid JSON
//------------------------------------------------------------------------------
TEST(QoSConfigFactory, ValidJson)
{
  using eos::mgm::QoSConfig;
  Json::Value json = makeJson();

  auto qos = QoSConfig::CreateQoSClass(json);
  ASSERT_NE(qos, nullptr);
  ASSERT_STREQ(qos->name.c_str(), "QoSTest");
  ASSERT_STREQ(qos->name.c_str(), json["name"].asString().c_str());

  ASSERT_CONTAINER_EQ(qos->transitions, {"disk", "tape"});
  ASSERT_CONTAINER_EQ(qos->transitions, makeArray(json["transition"]));

  ASSERT_EQ(qos->cdmi_redundancy, 1);
  ASSERT_EQ(qos->cdmi_redundancy, json["metadata"][CDMI_REDUNDANCY_TAG].asInt());
  ASSERT_EQ(qos->cdmi_latency, 100);
  ASSERT_EQ(qos->cdmi_latency, json["metadata"][CDMI_LATENCY_TAG].asInt());

  ASSERT_CONTAINER_EQ(qos->locations, {"CH", "HU"});
  ASSERT_CONTAINER_EQ(qos->locations, makeArray(json["metadata"][CDMI_PLACEMENT_TAG]));

  ASSERT_CONTAINER_EQ(qos->attributes, {{"layout",    "replica"},
                                        {"replica",   "2"},
                                        {"checksum",  "adler32"},
                                        {"placement", "scattered"}});
  ASSERT_CONTAINER_EQ(qos->attributes, makeMap(json["attributes"]));
}

//------------------------------------------------------------------------------
// Test factory method - valid JSON - empty arrays
//------------------------------------------------------------------------------
TEST(QoSConfigFactory, ValidJsonEmptyArrays)
{
  using eos::mgm::QoSConfig;
  Json::Value json = makeJson("EmptyArrays", {}, {});

  auto qos = QoSConfig::CreateQoSClass(json);
  ASSERT_NE(qos, nullptr);
  ASSERT_CONTAINER_EQ(qos->transitions, {});
  ASSERT_CONTAINER_EQ(qos->locations, {});
}

//------------------------------------------------------------------------------
// Test factory method - invalid JSON
//------------------------------------------------------------------------------
TEST(QoSConfigFactory, InvalidJson)
{
  using eos::mgm::QoSConfig;

  auto removeMember = [](auto&& json, const std::string& key) -> Json::Value {
    json.removeMember(key);
    return json;
  };

  auto assertInvalid = [](auto&& json) {
    auto qos = QoSConfig::CreateQoSClass(json);
    ASSERT_EQ(qos, nullptr);
  };

  assertInvalid(removeMember(makeJson(), "name"));
  assertInvalid(removeMember(makeJson(), "transition"));
  assertInvalid(removeMember(makeJson(), "metadata"));
  assertInvalid(removeMember(makeJson(), "attributes"));

  auto json = makeJson();
  json["metadata"].removeMember(CDMI_PLACEMENT_TAG);
  assertInvalid(json);

  json = makeJson();
  json["attributes"].removeMember("layout");
  assertInvalid(json);

  assertInvalid(Json::objectValue);
  assertInvalid(Json::arrayValue);
  assertInvalid(Json::nullValue);
}
