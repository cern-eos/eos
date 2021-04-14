//------------------------------------------------------------------------------
// File: DynamicECTests.cc
// Author: Andreas Stoeve Cern
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
//test
#include "gtest/gtest.h"
#include "mgm/DynamicEC.hh"
#include "common/LayoutId.hh"
#include "time.h"
#include "common/Logging.hh"
#include <random>
#include <ctime>

using namespace eos;

//#define GTEST;

TEST(DynamicECProduction, TestForSetterAndGetter)
{
  const char* str = "default";
  eos::mgm::DynamicEC UUT(str, 3600, 1024 * 1024, 95, 92, false, 30, 1);
  UUT.setWaitTime(100);
  EXPECT_EQ(UUT.getWaitTime(), 100);
  UUT.setMinThresHold(90);
  EXPECT_EQ(UUT.getMinThresHold(), 90);
  UUT.setMinForDeletion(256 * 256);
  EXPECT_EQ(UUT.getMinForDeletion(), 65536);
  UUT.setMaxThresHold(99);
  EXPECT_EQ(UUT.getMaxThresHold(), 99);
  UUT.setAgeFromWhenToDelete(40000);
  EXPECT_EQ(UUT.getAgeFromWhenToDelete(), 40000);
  UUT.setSecurity(3);
  EXPECT_EQ(UUT.getSecutiry(), 3);
  UUT.TestFunction();
}

TEST(DynamicECProduction, TestForSetterAndGetter2)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 1, 1, false);
}

