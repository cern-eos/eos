//------------------------------------------------------------------------------
// File: DrainFsTest.cc
// Author: Andrea Manzi <amanzi@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include <gtest/gtest.h>
#include "mgm/Drainer.hh"

namespace {

class DrainFsTest : public ::testing::Test {
 protected:
  virtual void SetUp() {

    drainer = new eos::mgm::Drainer();
  }

  virtual void TearDown() {}

  eos::mgm::Drainer * drainer;
};

TEST_F(DrainFsTest, StartFSDrain)
{
  XrdOucString out, err;
  XrdOucEnv env;
  int fsId =5;
  env.PutInt("mgm.drain.fsid",fsId);
   
  bool result = drainer->StartFSDrain(env, err);
  cout << err.c_str();
  ASSERT_TRUE(result);
  ASSERT_FALSE(drainer->StartFSDrain(env, err));
  
}

}
