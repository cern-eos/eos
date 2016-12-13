//------------------------------------------------------------------------------
//! @file HealthCommandTest.cc
//! @author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
//------------------------------------------------------------------------------

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

#include "HealthCommandTest.hh"

CPPUNIT_TEST_SUITE_REGISTRATION(HealthCommandTest);

void HealthCommandTest::DumpStringData(const std::string& path,
                                       const std::string& data)
{
  std::ofstream file;
  file.open(std::string("/root/dumped_data/") + path);
  file <<  data;
  file.close();
}

void HealthCommandTest::GroupEqualityTest(HealthCommand& health,
    std::string type)
{
  auto iter1 = health.m_group_data.begin();
  auto iter2 = m_mock_data.m_info_data[type].begin();

  while (iter1 != health.m_group_data.end() &&
         iter2 != m_mock_data.m_info_data[type].end()) {
    CPPUNIT_ASSERT(iter1->first == iter2->first);
    auto iter3 = iter1->second.begin();
    auto iter4 = iter2->second.begin();

    while (iter3 != iter1->second.end() && iter4 != iter2->second.end()) {
      CPPUNIT_ASSERT(*iter3 ==  *iter4);
      ++iter3;
      ++iter4;
    }

    ++iter1;
    ++iter2;
  }
}


void HealthCommandTest::setUp()
{
  m_mock_data.GenerateInfoData();
  m_mock_data.GenerateOutputs();
  m_mock_data.GenerateMgms();
}

void HealthCommandTest::tearDown()
{
}

void HealthCommandTest::DeadNodesTest()
{
  HealthCommand health("");
  health.m_mgm_execute = m_mock_data.m_mexecs["good_nodes"];
  CPPUNIT_ASSERT_NO_THROW(health.DeadNodesCheck());
  CPPUNIT_ASSERT(health.m_output.str() ==
                 m_mock_data.m_outputs["nodes_good"]);
  health.m_output.str("");
  health.m_mgm_execute = m_mock_data.m_mexecs["bad_nodes"];
  CPPUNIT_ASSERT_NO_THROW(health.DeadNodesCheck());
  CPPUNIT_ASSERT(health.m_output.str() ==
                 m_mock_data.m_outputs["nodes_bad"]);
  health.m_output.str("");
  health.m_all = true;
  health.m_mgm_execute = m_mock_data.m_mexecs["good_nodes"];
  CPPUNIT_ASSERT_NO_THROW(health.DeadNodesCheck());
  CPPUNIT_ASSERT(health.m_output.str() ==
                 m_mock_data.m_outputs["nodes_good_-a"]);
  health.m_output.str("");
  health.m_mgm_execute = m_mock_data.m_mexecs["bad_nodes"];
  CPPUNIT_ASSERT_NO_THROW(health.DeadNodesCheck());
  CPPUNIT_ASSERT(health.m_output.str() ==
                 m_mock_data.m_outputs["nodes_bad_-a"]);
}
void HealthCommandTest::GetGroupsInfoTest()
{
  HealthCommand health("");
  health.m_mgm_execute = m_mock_data.m_mexecs["good"];
  health.GetGroupsInfo();
  GroupEqualityTest(health, std::string("good"));
  health.m_mgm_execute = m_mock_data.m_mexecs["bad"];
  health.GetGroupsInfo();
  GroupEqualityTest(health, "bad");
  health.m_mgm_execute = m_mock_data.m_mexecs["good"];
  health.GetGroupsInfo();
  GroupEqualityTest(health, "bad_drain");
}

void HealthCommandTest::ParseCommandTest()
{
  HealthCommand health("");
  health.m_comm = const_cast<char*>("");
  CPPUNIT_ASSERT_NO_THROW(health.ParseCommand());
  health.m_comm = const_cast<char*>("all");
  CPPUNIT_ASSERT_NO_THROW(health.ParseCommand());
  CPPUNIT_ASSERT(health.m_section == "all");
  health.m_comm = const_cast<char*>("nodes");
  CPPUNIT_ASSERT_NO_THROW(health.ParseCommand());
  CPPUNIT_ASSERT(health.m_section == "nodes");
  health.m_comm = const_cast<char*>("drain");
  CPPUNIT_ASSERT_NO_THROW(health.ParseCommand());
  CPPUNIT_ASSERT(health.m_section == "drain");
  health.m_comm = const_cast<char*>("placement");
  CPPUNIT_ASSERT_NO_THROW(health.ParseCommand());
  CPPUNIT_ASSERT(health.m_section == "placement");
  health.m_comm = const_cast<char*>("--help");
  CPPUNIT_ASSERT_NO_THROW(health.ParseCommand());
  CPPUNIT_ASSERT(health.m_section == "/");
  health.m_comm = const_cast<char*>("placement nodes --help");
  CPPUNIT_ASSERT_NO_THROW(health.ParseCommand());
  CPPUNIT_ASSERT(health.m_section == "/");
  health.m_comm = const_cast<char*>("banana smurf placement");
  CPPUNIT_ASSERT_THROW(health.ParseCommand(),  std::string);
}

void HealthCommandTest::PlacementTest()
{
  HealthCommand health("/");
  health.m_group_data = m_mock_data.m_info_data["good"];
  health.PlacementContentionCheck();
  DumpStringData("11", health.m_output.str());
  DumpStringData("22", m_mock_data.m_outputs["placement_good"]);
  CPPUNIT_ASSERT(health.m_output.str() ==
                 m_mock_data.m_outputs["placement_good"]);
  health.m_output.str(std::string(""));
  health.m_monitoring = true;
  health.PlacementContentionCheck();
  CPPUNIT_ASSERT(health.m_output.str() ==
                 m_mock_data.m_outputs["placement_good_-m"]);
  health.m_output.str(std::string(""));
  health.m_group_data = m_mock_data.m_info_data["bad"];
  health.m_monitoring = false;
  health.PlacementContentionCheck();
  DumpStringData("11", health.m_output.str());
  DumpStringData("22", m_mock_data.m_outputs["placement_bad"]);
  CPPUNIT_ASSERT(health.m_output.str() ==
                 m_mock_data.m_outputs["placement_bad"]);
  health.m_output.str(std::string(""));
  health.m_monitoring = true;
  health.PlacementContentionCheck();
  CPPUNIT_ASSERT(health.m_output.str() ==
                 m_mock_data.m_outputs["placement_bad_-m"]);
  health.m_output.str(std::string(""));
  health.m_monitoring = false;
  health.m_all = true;
  health.m_group_data = m_mock_data.m_info_data["good"];
  health.PlacementContentionCheck();
  CPPUNIT_ASSERT(health.m_output.str() ==
                 m_mock_data.m_outputs["placement_good_-a"]);
  health.m_output.str(std::string(""));
  health.m_group_data = m_mock_data.m_info_data["bad"];
  health.PlacementContentionCheck();
  DumpStringData("11", health.m_output.str());
  DumpStringData("22", m_mock_data.m_outputs["placement_bad_-a"]);
  CPPUNIT_ASSERT(health.m_output.str() ==
                 m_mock_data.m_outputs["placement_bad_-a"]);
}

void HealthCommandTest::TooFullDrainTest()
{
  HealthCommand health("/");
  health.m_group_data = m_mock_data.m_info_data["good"];
  health.TooFullForDrainingCheck();
  CPPUNIT_ASSERT(health.m_output.str() ==
                 m_mock_data.m_outputs["drain_good"]);
  health.m_output.str(std::string(""));
  health.m_monitoring = true;
  health.TooFullForDrainingCheck();
  CPPUNIT_ASSERT(health.m_output.str() ==
                 m_mock_data.m_outputs["drain_good_-m"]);
  health.m_output.str(std::string(""));
  health.m_group_data = m_mock_data.m_info_data["bad_drain"];
  health.m_monitoring = false;
  health.TooFullForDrainingCheck();
  CPPUNIT_ASSERT(health.m_output.str() ==
                 m_mock_data.m_outputs["drain_bad"]);
  health.m_output.str(std::string(""));
  health.m_monitoring = true;
  health.TooFullForDrainingCheck();
  CPPUNIT_ASSERT(health.m_output.str() ==
                 m_mock_data.m_outputs["drain_bad_-m"]);
  health.m_monitoring = false;
  health.m_all = true;
  health.m_output.str(std::string(""));
  health.m_group_data = m_mock_data.m_info_data["good"];
  health.TooFullForDrainingCheck();
  CPPUNIT_ASSERT(health.m_output.str() ==
                 m_mock_data.m_outputs["drain_good_-a"]);
  health.m_output.str(std::string(""));
  health.m_group_data = m_mock_data.m_info_data["bad_drain"];
  health.TooFullForDrainingCheck();
  CPPUNIT_ASSERT(health.m_output.str() ==
                 m_mock_data.m_outputs["drain_bad_-a"]);
}
