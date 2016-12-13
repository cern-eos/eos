//------------------------------------------------------------------------------
//! @file HealthCommandTest.hh
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

#ifndef __HEALTHCOMMANDTEST__HH__
#define __HEALTHCOMMANDTEST__HH__

#include <cppunit/extensions/HelperMacros.h>
#include <cstdio>
#include <string>
#include <vector>
#include <unordered_map>
#include "HealthMockData.hh"
#include "../commands/HealthCommand.hh"
#include "MgmExecuteTest.hh"

using FSInfoVec     = std::vector<FSInfo>;
using GroupsInfo    = std::unordered_map<std::string, FSInfoVec>;
using TestOutputs   = std::unordered_map<std::string,  std::string>;

class HealthCommandTest : public CppUnit::TestCase
{
  CPPUNIT_TEST_SUITE(HealthCommandTest);
  CPPUNIT_TEST(DeadNodesTest);
  CPPUNIT_TEST(TooFullDrainTest);
  CPPUNIT_TEST(PlacementTest);
  CPPUNIT_TEST(ParseCommandTest);
  CPPUNIT_TEST_SUITE_END();

  void DumpStringData(const std::string& path,  const std::string& data);
  void GroupEqualityTest(HealthCommand& health, std::string type);

  HealthMockData m_mock_data;

public:
  void setUp();
  void tearDown();

  void DeadNodesTest();
  void TooFullDrainTest();
  void PlacementTest();
  void ParseCommandTest();
  void GetGroupsInfoTest();

};

#endif //__HEALTHCOMMANDTEST__HH__
