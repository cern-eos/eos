//------------------------------------------------------------------------------
// File: CapsTests.cc
// Author: Abhishek Lekshmanan <abhishek.lekshmanan@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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
#include "mgm/FuseServer/Caps.hh"
#include "mgm/XrdMgmOfs.hh"

using namespace eos::mgm::FuseServer;

class FakeStats {
public:
  template <typename... Args>
  void Add(Args&&... args) {
  }
};

class FakeXrdMgmOFS: public XrdMgmOfs
{
public:
  FakeXrdMgmOFS(XrdSysError* lp) : XrdMgmOfs(lp)
  {
  }
  ~FakeXrdMgmOFS() {
    mDoneOrderlyShutdown = true;
  }
  // TODO shadow + mock?
  FakeStats MgmStats;
};

// use a fixture, we'll retain the same Caps across the various tests
class CapsTest : public ::testing::Test
{
protected:
  // TODO: When you update gtest remember to change this to SetUpTestSuite
  // We need these resources shared across all the test suites!
  static void SetUpTestCase() {
    // TODO: delegate this to a helper class that sets up and tearsdown the env
    // saving any vars and resetting them.
    setenv("EOS_MGM_HTTP_PORT", "0", 1);
    setenv("EOS_MGM_GRPC_PORT", "0", 1);
    fake_sys_error = new XrdSysError(nullptr, "fake");
    fake_ofs = new FakeXrdMgmOFS(fake_sys_error);
    gOFS = fake_ofs;
  }

  static void TearDownTestCase() {
    delete fake_sys_error;
    delete fake_ofs;
  }

  static XrdSysError* fake_sys_error;
  static XrdMgmOfs* fake_ofs;

  Caps mCaps;
};

XrdSysError* CapsTest::fake_sys_error = nullptr;
XrdMgmOfs* CapsTest::fake_ofs = nullptr;

TEST_F(CapsTest, EmptyCapsInit) {
  EXPECT_EQ(mCaps.ncaps(), 0);
}

TEST_F(CapsTest, StoreCaps) {
  eos::common::VirtualIdentity vid;
  eos::fusex::cap c;
  mCaps.Store(c,&vid);
  EXPECT_EQ(mCaps.ncaps(), 1);
}
