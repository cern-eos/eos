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
#include "mgm/dynamicec/DynamicEC.hh"
#include "common/LayoutId.hh"
#include "time.h"
#include "common/Logging.hh"
#include <random>
#include <ctime>

using namespace eos;

//#define GTEST;


TEST(DynamicEC, LookInMap)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 1, 1, false);
  UUT.fillFiles();
  //This is for 100000 files in the system
  ASSERT_EQ(UUT.mSimulatedFiles.size(), 100000);
  ASSERT_FALSE(UUT.mSimulatedFiles.empty());
}



TEST(DynamicEC, CheckingForAnythingInTheFile)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 1, 1, false);
  UUT.fillFiles();

  for (int i = 0;  i < UUT.mSimulatedFiles.size(); i++) {
    if (UUT.mSimulatedFiles[i]->getSize() <= 0) {
      std::cerr << "Loop number" << i << std::endl;
      ASSERT_TRUE(false);
    }
  }
}



TEST(DynamicEC, CheckOnWhatTheTimeIsIn)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 1, 1, false);
  UUT.fillFiles();
  eos::IFileMD::ctime_t time;
  UUT.mSimulatedFiles[1].get()->getCTime(time);
  std::cerr << time.tv_sec << " seconds " << " nano seconds " << time.tv_nsec <<
            std::endl;
}



TEST(DynamicEC, TestForIfAnyWillHaveToBeDeleted)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 1, 1, false);
  UUT.fillFiles();
  int number = 0;

  for (int i = 0;  i < 100000; i++) {
    auto file = UUT.mSimulatedFiles[i];

    if (UUT.DeletionOfFileID(UUT.mSimulatedFiles[i], time(0) - 11556926)) {
      number++;
    }
  }

  ASSERT_TRUE(number > 50000);
}










TEST(DynamicEC, TestForGetSmallSizedFill)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 1, 1, false);
  UUT.fillSingleSmallFile(time(0) - 21556926, 5000000, 10);
  ASSERT_FALSE(UUT.DeletionOfFileID(UUT.mSimulatedFiles[0], time(0) - 11556926));
}

TEST(DynamicEC, TestForGetSmallSizedFillAtTheEdge)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 1, 1, false);
  UUT.fillSingleSmallFile(time(0) - 21556926, 5000000, 8);
  ASSERT_FALSE(UUT.DeletionOfFileID(UUT.mSimulatedFiles[0], time(0) - 11556926));
}













TEST(DynamicEC, TestForSpaceStatus)
{
  const char* str = "DynamicTest1";
  eos::mgm::DynamicEC UUT(str, 11556926, 1000000, 95, 92, false);
  UUT.fillFiles();
  eos::mgm::statusForSystem status {0, 0, 0, 0};
  status = UUT.SpaceStatus();
  EXPECT_EQ(status.totalSize, UUT.mCreatedFileSize);
  EXPECT_EQ(status.deletedSize, 0);
  ASSERT_EQ(status.usedSize, UUT.mCreatedFileSize);
  ASSERT_EQ(status.deletedSize, UUT.mDeletedFileSize);
  ASSERT_TRUE(status.undeletedSize > UUT.mCreatedFileSize * 0.079);
  ASSERT_TRUE(status.undeletedSize < UUT.mCreatedFileSize * 0.081);
}













TEST(DynamicEC, TestForFillingInMoreFiles)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 1000000, 95, 92, false);
  UUT.fillFiles();
  ASSERT_EQ(UUT.mSimulatedFiles.size(), 100000);
  int a = UUT.mSimulatedFiles.size();
  UUT.fillFiles(100000);
  ASSERT_EQ(UUT.mSimulatedFiles.size(), 200000);
}

TEST(DynamicEC, TestGetAndSetFunction)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 95, 92, false);
  uint64_t before = time(0) - 11556926;
  ASSERT_EQ(UUT.getMaxThresHold(), 95);
  ASSERT_EQ(UUT.getMinThresHold(), 92);
  UUT.setMaxThresHold(10);
  ASSERT_EQ(UUT.getMaxThresHold(), 95);
  UUT.setMaxThresHold(94);
  ASSERT_EQ(UUT.getMaxThresHold(), 94);
  UUT.setMaxThresHold(92);
  ASSERT_EQ(UUT.getMaxThresHold(), 92);
  UUT.setMaxThresHold(100);
  ASSERT_EQ(UUT.getMaxThresHold(), 92);
  UUT.setMaxThresHold(101);
  ASSERT_EQ(UUT.getMaxThresHold(), 92);
  UUT.setMaxThresHold(95);
  UUT.setMinThresHold(100);
  ASSERT_EQ(UUT.getMinThresHold(), 92);
  UUT.setMinThresHold(101);
  ASSERT_EQ(UUT.getMinThresHold(), 92);
  UUT.setMinThresHold(93);
  ASSERT_EQ(UUT.getMinThresHold(), 93);
  UUT.setMinThresHold(0);
  ASSERT_EQ(UUT.getMinThresHold(), 93);
  UUT.setMinThresHold(-1);
  ASSERT_EQ(UUT.getMinThresHold(), 93);
  UUT.setMinThresHold(10);
  ASSERT_EQ(UUT.getMinThresHold(), 10);
  UUT.setMinThresHold(95);
  ASSERT_EQ(UUT.getMinThresHold(), 95);
  UUT.setMinThresHold(96);
  ASSERT_EQ(UUT.getMinThresHold(), 95);
  ASSERT_EQ(UUT.getMinForDeletion(), 10000000);
  UUT.setMinForDeletion(12345678910);
  ASSERT_EQ(UUT.getMinForDeletion(), 12345678910);
}

TEST(DynamicEC, TestForWaitTime)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 95, 92, false);
  ASSERT_EQ(UUT.getWaitTime(), 30);
  UUT.setWaitTime(-2);
  ASSERT_EQ(UUT.getWaitTime(), 30);
  UUT.setWaitTime(2);
  ASSERT_EQ(UUT.getWaitTime(), 2);
}


TEST(DynamicEC, TestForFailToDeleteAll)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 80, 40, false);
  UUT.fillFiles();
  UUT.CleanupMD();
  eos::mgm::statusForSystem status;
  status = UUT.SpaceStatus();
  ASSERT_TRUE(status.undeletedSize > 10000);
}




