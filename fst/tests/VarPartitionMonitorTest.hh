//------------------------------------------------------------------------------
//! @file VarPartitionMonitorTest.hh
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

#ifndef __EOSFST_TESTS_VARPARTITIONMONITORTEST__HH__
#define __EOSFST_TESTS_VARPARTITIONMONITORTEST__HH__

#include <cppunit/extensions/HelperMacros.h>
#include <cstdint>
#include <fstream>
#include <vector>
#include <thread>
#include "common/FileSystem.hh"
#include "common/RWMutex.hh"
#include "fst/storage/MonitorVarPartition.hh"

typedef eos::common::FileSystem::eConfigStatus fsstatus_t;

//------------------------------------------------------------------------------
// Mock class implementing only relevant methods related to unit testing
//------------------------------------------------------------------------------
struct MockFileSystem {
  fsstatus_t status;

  MockFileSystem() : status(eos::common::FileSystem::kRW) {}

  void SetConfigStatus(fsstatus_t status) {
    this->status = status;
  }

  fsstatus_t GetConfigStatus(bool cached = false) {
    return this->status;
  }
};

typedef eos::fst::MonitorVarPartition<std::vector<MockFileSystem*>> VarMonitorT;

//------------------------------------------------------------------------------
//! Class VarPartitionMonitorTest
//------------------------------------------------------------------------------
class VarPartitionMonitorTest : public CppUnit::TestCase
{
  CPPUNIT_TEST_SUITE(VarPartitionMonitorTest);
  CPPUNIT_TEST(VarMonitorTest);
  CPPUNIT_TEST_SUITE_END();

  eos::common::RWMutex fsMutex;
  std::vector<MockFileSystem*> fsVector;
  VarMonitorT monitor;
  std::thread monitor_thread;
  std::ofstream fill;
  static std::int32_t mMonitorInterval;

public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  VarPartitionMonitorTest();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~VarPartitionMonitorTest() = default;

  //----------------------------------------------------------------------------
  //! CPPUNIT required methods
  //----------------------------------------------------------------------------
  void setUp(void);
  void tearDown(void);

  //----------------------------------------------------------------------------
  //! Method starting the monitoring thread
  //----------------------------------------------------------------------------
  static void* StartFstPartitionMonitor(void* pp);

  //----------------------------------------------------------------------------
  //! Method implementing the test
  //----------------------------------------------------------------------------
  void VarMonitorTest();
};

#endif // __EOSFST_TESTS_VARPARTITIONMONITORTEST__HH__
