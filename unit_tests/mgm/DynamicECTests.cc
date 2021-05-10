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


TEST(DynamicEC, LookInMap)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 1, 1, false);
  UUT.fillFiles();
  //This is for 100000 files in the system
  ASSERT_EQ(UUT.simulatedFiles.size(), 100000);
  ASSERT_FALSE(UUT.simulatedFiles.empty());
  //ASSERT_EQ(UUT.simulatedFiles.max_size(),100000);
}



TEST(DynamicEC, CheckingForAnythingInTheFile)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 1, 1, false);
  UUT.fillFiles();

  for (int i = 0;  i < UUT.simulatedFiles.size(); i++) {
    if (UUT.simulatedFiles[i]->getSize() <= 0) {
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
  UUT.simulatedFiles[1].get()->getCTime(time);
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
    auto file = UUT.simulatedFiles[i];

    if (UUT.DeletionOfFileID(UUT.simulatedFiles[i], time(0) - 11556926)) {
      number++;
    }
  }

  ASSERT_TRUE(number > 50000);
}

TEST(DynamicEC, TestForErasingFiles)
{
  const char* str = "DynamicTest";
  testing::internal::CaptureStdout();
  std::cout << "My test";
  std::string output = testing::internal::GetCapturedStdout();
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 1, 1, false);
  UUT.fillFiles();

  //int number = 0;
  for (int i = 0;  i < 100000; i++) {
    auto file = UUT.simulatedFiles[i];

    if (UUT.DeletionOfFileID(UUT.simulatedFiles[i], time(0) - 11556926)) {
      UUT.SingleDeletion(UUT.simulatedFiles[i]);
    }
  }

  ASSERT_TRUE(UUT.simulatedFiles.size() < 50000);
}

/*
TEST(DynamicEC, TestForASingleFileWithMultiplePartisions)
{
  const char* str = "DynamicTest";
  //eos::common::Logging::GetInstance().SetUnit("DynamicECTest");
  eos::mgm::DynamicEC UUT(str, 11556927, 10000000, 1, 1, false);
  UUT.fillSingleSmallFile(time(0) - 21556926, 2000000000, 10);
  auto ii = eos::common::LayoutId::GetSizeFactor(
              UUT.simulatedFiles[0]->getLayoutId());

  if (UUT.DeletionOfFileID(UUT.simulatedFiles[0], time(0) - 11556926)) {
    UUT.kQrainReduction(UUT.simulatedFiles[0]);
  }

  //ASSERT_EQ(UUT.GetSizeFactor1(UUT.simulatedFiles[0]), 9);
  //ASSERT_EQ(UUT.simulatedFiles[0]->getLocations().size(), 8);
  //ASSERT_NE(eos::common::LayoutId::GetActualSizeFactor(UUT.simulatedFiles[0].get()),ii);
  //ASSERT_EQ(eos::common::LayoutId::GetSizeFactor(UUT.simulatedFiles[0]->getLayoutId()),400);
}
*/

/*
TEST(DynamicEC, TestForASingleFileWithMultiplePartisionsForTheSizeFactor)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 1, 1, false, 30, 1);
  UUT.fillSingleSmallFile(time(0) - 21556926, 49000000000, 10);
  //ASSERT_EQ(UUT.simulatedFiles[0]->getActualSizeFactor(), 8.0);
  //auto ii = eos::common::LayoutId::GetSizeFactor(UUT.simulatedFiles[0]->getLayoutId());
  //if (UUT.DeletionOfFileID(UUT.simulatedFiles[0], time(0) - 11556926)) {
  UUT.kQrainReduction(UUT.simulatedFiles[0]);
  //}
  ASSERT_EQ(UUT.simulatedFiles[0]->getLocations().size(), 0);
  //ASSERT_EQ(UUT.simulatedFiles[0]->getLocations().size() - eos::common::LayoutId::GetRedundancy(eos::common::LayoutId::GetLayoutType(UUT.simulatedFiles[0]->getLayoutId()), UUT.simulatedFiles[0]->getLocations().size()),111);
  ASSERT_EQ(UUT.simulatedFiles[0]->getActualSizeFactor(), 8.0 / 6.0);
  ASSERT_EQ(UUT.simulatedFiles[0]->getActualSizeFactor(), 8.0);
  //ASSERT_EQ(UUT.GetSizeFactor1(UUT.simulatedFiles[0]), 9);
  //ASSERT_EQ(UUT.simulatedFiles[0]->getLocations().size(), 7);
  //ASSERT_NE(eos::common::LayoutId::GetActualSizeFactor(UUT.simulatedFiles[0].get()),ii);
  //ASSERT_EQ(eos::common::LayoutId::GetSizeFactor(UUT.simulatedFiles[0]->getLayoutId()),400);
}


TEST(DynamicEC, TestForGetSizeOfFileFromRealComponents)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 1, 1, false);
  UUT.fillSingleSmallFile(time(0) - 21556926, 49000000000, 10);

  //auto ii = eos::common::LayoutId::GetSizeFactor(UUT.simulatedFiles[0]->getLayoutId());
  if (UUT.DeletionOfFileID(UUT.simulatedFiles[0], time(0) - 11556926)) {
    UUT.kQrainReduction(UUT.simulatedFiles[0]);
  }

  ASSERT_EQ(UUT.GetSizeOfFile(UUT.simulatedFiles[0]), 49000000000 * 8 / 6);
  //ASSERT_EQ(UUT.GetSizeFactor1(UUT.simulatedFiles[0]), 9);
  //ASSERT_EQ(UUT.simulatedFiles[0]->getLocations().size(), 7);
  //ASSERT_NE(eos::common::LayoutId::GetActualSizeFactor(UUT.simulatedFiles[0].get()),ii);
  //ASSERT_EQ(eos::common::LayoutId::GetSizeFactor(UUT.simulatedFiles[0]->getLayoutId()),400);
}
*/


TEST(DynamicEC, TestForGetSmallSizedFill)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 1, 1, false);
  UUT.fillSingleSmallFile(time(0) - 21556926, 5000000, 10);
  ASSERT_FALSE(UUT.DeletionOfFileID(UUT.simulatedFiles[0], time(0) - 11556926));
}

TEST(DynamicEC, TestForGetSmallSizedFillAtTheEdge)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 1, 1, false);
  UUT.fillSingleSmallFile(time(0) - 21556926, 5000000, 8);
  //ASSERT_EQ(UUT.GetSizeOfFile(UUT.simulatedFiles[0]),2);
  ASSERT_FALSE(UUT.DeletionOfFileID(UUT.simulatedFiles[0], time(0) - 11556926));
}

/*
///Test for multi purpose of this.
TEST(DynamicEC, TestForTheDelitionForSingleFile)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 1000000, 1, 1, false);
  UUT.fillSingleSmallFile(time(0) - 21556926, 5000000, 8);

  if (UUT.DeletionOfFileID(UUT.simulatedFiles[0], time(0) - 11556926)) {
    UUT.kQrainReduction(UUT.simulatedFiles[0]);
  }

  ASSERT_EQ(UUT.deletedFileSize, 2500000);
}
*/

/*
TEST(DynamicEC, TestForTheDelitionForMultiFiles)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 1000000, 1, 1, false);
  UUT.fillSingleSmallFile(time(0) - 21556926, 5000000, 8);

  if (UUT.DeletionOfFileID(UUT.simulatedFiles[0], time(0) - 11556926)) {
    UUT.kQrainReduction(UUT.simulatedFiles[0]);
  }

  UUT.fillSingleSmallFile(time(0) - 21556926, 5000000, 8);

  if (UUT.DeletionOfFileID(UUT.simulatedFiles[0], time(0) - 11556926)) {
    UUT.kQrainReduction(UUT.simulatedFiles[0]);
  }

  ASSERT_EQ(UUT.deletedFileSize, 5000000);
}
*/

/*
TEST(DynamicEC, TestForTheDelitionForMultiFilesForTheSameFiles)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 1000000, 1, 1, false);
  UUT.fillSingleSmallFile(time(0) - 21556926, 5000000, 8);

  if (UUT.DeletionOfFileID(UUT.simulatedFiles[0], time(0) - 11556926)) {
    UUT.kQrainReduction(UUT.simulatedFiles[0]);
  }

  UUT.fillSingleSmallFile(time(0) - 21556926, 5000000, 8);

  if (UUT.DeletionOfFileID(UUT.simulatedFiles[0], time(0) - 11556926)) {
    UUT.kQrainReduction(UUT.simulatedFiles[0]);
  }

  if (UUT.DeletionOfFileID(UUT.simulatedFiles[0], time(0) - 11556926)) {
    UUT.kQrainReduction(UUT.simulatedFiles[0]);
  }

  ASSERT_EQ(UUT.deletedFileSize, 5000000);
}
*/


TEST(DynamicEC, TestForSpaceStatus)
{
  const char* str = "DynamicTest1";
  eos::mgm::DynamicEC UUT(str, 11556926, 1000000, 95, 92, false);
  UUT.fillFiles();
  eos::mgm::statusForSystem status {0, 0, 0, 0};
  status = UUT.SpaceStatus();
  //EXPECT_EQ(1,0);
  EXPECT_EQ(status.totalSize, UUT.createdFileSize);
  EXPECT_EQ(status.deletedSize, 0);
  ASSERT_EQ(status.usedSize, UUT.createdFileSize);
  ASSERT_EQ(status.deletedSize, UUT.deletedFileSize);
  ASSERT_TRUE(status.undeletedSize > UUT.createdFileSize * 0.079);
  ASSERT_TRUE(status.undeletedSize < UUT.createdFileSize * 0.081);
  //EXPECT_THAT(2, IsBetween(1,3));
  //EXPECT_
  //EXPECT_THAT(status.undeletedSize, AllOf(Ge(UUT.createdFileSize*0.919),Le(UUT.createdFileSize*0.921)));
}

/*
TEST(DynamicEC, TestForSpaceStatusWithDeletionOfFiles)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 1000000, 95, 92, false, 30 ,1);
  UUT.fillFiles();
  eos::mgm::statusForSystem status;
  status = UUT.SpaceStatus();
  ASSERT_EQ(status.usedSize, UUT.createdFileSize);
  ASSERT_EQ(status.deletedSize, UUT.deletedFileSize);
  ASSERT_TRUE(status.undeletedSize > UUT.createdFileSize * 0.079);
  ASSERT_TRUE(status.undeletedSize < UUT.createdFileSize * 0.081);
  uint64_t before = status.undeletedSize;
  UUT.Cleanup();
  //ASSERT_EQ(UUT.deletedFileSize,43647012370960);
  status = UUT.SpaceStatus();
  ASSERT_EQ(status.totalSize, UUT.createdFileSize);
  ASSERT_EQ(status.usedSize, UUT.createdFileSize - UUT.deletedFileSize);
  ASSERT_EQ(status.deletedSize, UUT.deletedFileSize);
  //ASSERT_EQ(status.deletedSize, before);
  ASSERT_TRUE(status.deletedSize > before * 0.99);
  ASSERT_TRUE(status.deletedSize < before * 1.01);
  ASSERT_EQ(status.undeletedSize, 0);
  //ASSERT_TRUE(status.undeletedSize > UUT.createdFileSize*0.919);
  //ASSERT_TRUE(status.undeletedSize < UUT.createdFileSize*0.921);
  //317839436332068 this is before, and how much to delete
  //43647012370960 this is what have been deleted.
  //345477648187031 total size
  //without checking for for if the file is the right kQrain layout it has 43647012370960 bytes to delete
  //with checking it only has this to delete 2737004993059
  //2737004993059
  //43647012370960
  //345477648187031
  //31037677227
  //2748500162275 for 6 layout
  //2737004993059 for 5 layout
  //2677857698579 for 4 layout
  //2674594450333 for 3 layout
  //2742356921872 for 2 layout
  //2720364804809 for 1 layout
  //2751183656702 for 0 layout
  //figure this out makes no sense in the way that it is used for this program and will not have to work with the other part of the program.
  //EXPECT_THAT(2, IsBetween(1,3));
  //EXPECT_
  //EXPECT_THAT(status.undeletedSize, AllOf(Ge(UUT.createdFileSize*0.919),Le(UUT.createdFileSize*0.921)));
}
*/

///Test what the different stuff is on my files, in order to know what to check on them.
///something for the stripenumber, redunancynumber
//43647012370960
//2737004993059
/*
TEST(DynamicEC, TestForTest)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 1000000, 95, 92, false);
  UUT.fillSingleSmallFile(time(0) - 21556926, 5000000, 8);
  //ASSERT_EQ(eos::common::LayoutId::GetStripeNumber(UUT.simulatedFiles[0]->getLayoutId()),12);
  ASSERT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
              UUT.simulatedFiles[0]->getLayoutId()), 4);
  //ASSERT_EQ(eos::common::LayoutId:: GetExcessStripeNumber(UUT.simulatedFiles[0]->getLayoutId()),12);
  UUT.kQrainReduction(UUT.simulatedFiles[0]);
  //ASSERT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(UUT.simulatedFiles[0]->getLayoutId()),3);
}
*/

/*
TEST(DynamicEC, TestForkQrainReductionForDifferentRainStripes)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT1(str, 11556926, 1000000, 95, 92, false);
  UUT1.fillSingleSmallFile(time(0) - 21556926, 5000000, 8);
  UUT1.kQrainReduction(UUT1.simulatedFiles[0]);
  ASSERT_EQ(UUT1.simulatedFiles[0]->getLocations().size(), 6);
  eos::mgm::DynamicEC UUT2(str, 11556926, 1000000, 95, 92, false);
  UUT2.fillSingleSmallFile(time(0) - 21556926, 5000000, 7);
  UUT2.kQrainReduction(UUT2.simulatedFiles[0]);
  ASSERT_EQ(UUT2.simulatedFiles[0]->getLocations().size(), 5);
  eos::mgm::DynamicEC UUT3(str, 11556926, 1000000, 95, 92, false);
  UUT3.fillSingleSmallFile(time(0) - 21556926, 5000000, 6);
  UUT3.kQrainReduction(UUT3.simulatedFiles[0]);
  ASSERT_EQ(UUT3.simulatedFiles[0]->getLocations().size(), 4);
  eos::mgm::DynamicEC UUT4(str, 11556926, 1000000, 95, 92, false);
  UUT4.fillSingleSmallFile(time(0) - 21556926, 5000000, 5);
  UUT4.kQrainReduction(UUT4.simulatedFiles[0]);
  ASSERT_EQ(UUT4.simulatedFiles[0]->getLocations().size(), 3);
  eos::mgm::DynamicEC UUT5(str, 11556926, 1000000, 95, 92, false);
  UUT5.fillSingleSmallFile(time(0) - 21556926, 5000000, 9);
  UUT5.kQrainReduction(UUT5.simulatedFiles[0]);
  ASSERT_EQ(UUT5.simulatedFiles[0]->getLocations().size(), 7);
  eos::mgm::DynamicEC UUT6(str, 11556926, 1000000, 95, 92, false);
  UUT6.fillSingleSmallFile(time(0) - 21556926, 5000000, 10);
  UUT6.kQrainReduction(UUT6.simulatedFiles[0]);
  ASSERT_EQ(UUT6.simulatedFiles[0]->getLocations().size(), 8);
  eos::mgm::DynamicEC UUT7(str, 11556926, 1000000, 95, 92, false);
  UUT7.fillSingleSmallFile(time(0) - 21556926, 5000000, 11);
  UUT7.kQrainReduction(UUT7.simulatedFiles[0]);
  ASSERT_EQ(UUT7.simulatedFiles[0]->getLocations().size(), 9);
}
*/


TEST(DynamicEC, TestForFillingInMoreFiles)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 1000000, 95, 92, false);
  UUT.fillFiles();
  ASSERT_EQ(UUT.simulatedFiles.size(), 100000);
  int a = UUT.simulatedFiles.size();
  //std::cerr << a << " seconds " << " nano seconds " << std::endl;
  UUT.fillFiles(100000);
  ASSERT_EQ(UUT.simulatedFiles.size(), 200000);
}

/*
TEST(DynamicEC, TestForMultiDeletion)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 1000000, 95, 92, false);
  UUT.fillFiles();
  eos::mgm::statusForSystem status;
  status = UUT.SpaceStatus();
  ASSERT_EQ(status.usedSize, UUT.createdFileSize);
  ASSERT_EQ(status.deletedSize, UUT.deletedFileSize);
  ASSERT_TRUE(status.undeletedSize > UUT.createdFileSize * 0.079);
  ASSERT_TRUE(status.undeletedSize < UUT.createdFileSize * 0.081);
  UUT.Cleanup();
  uint64_t before = status.undeletedSize;
  status = UUT.SpaceStatus();
  ASSERT_EQ(status.totalSize, UUT.createdFileSize);
  ASSERT_EQ(status.usedSize, UUT.createdFileSize - UUT.deletedFileSize);
  ASSERT_EQ(status.deletedSize, UUT.deletedFileSize);
  ASSERT_TRUE(status.deletedSize > before * 0.99);
  ASSERT_TRUE(status.deletedSize < before * 1.01);
  ASSERT_EQ(status.undeletedSize, 0);
  UUT.fillFiles(100000);
  status = UUT.SpaceStatus();
  ASSERT_EQ(status.usedSize, (UUT.createdFileSize - UUT.deletedFileSize));
  ASSERT_EQ(status.deletedSize, UUT.deletedFileSize);
  //ASSERT_EQ(UUT.createdFileSize*0.079, 2);
  ASSERT_TRUE(status.undeletedSize > UUT.createdFileSize * 0.039);
  ASSERT_TRUE(status.undeletedSize < UUT.createdFileSize * 0.041);
  //82914756080113 undeleted file size
  //663316964003874 used file size
}
*/

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
//  ASSERT_EQ(UUT.getTimeFromWhenToDelete(), before);
  //UUT.setTimeFromWhenToDelete(0);
  //ASSERT_EQ(UUT.getTimeFromWhenToDelete(), time(0));
  UUT.setMinForDeletion(12345678910);
  ASSERT_EQ(UUT.getMinForDeletion(), 12345678910);
  //Something to catch a negative number
  //UUT.setMinForDeletion(-12345678910);
  //ASSERT_EQ(UUT.getMinForDeletion(),12345678910);
}

TEST(DynamicEC, TestForWaitTime)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 95, 92, false);
  ASSERT_EQ(UUT.getWaitTime(), 10);
  UUT.setWaitTime(-2);
  ASSERT_EQ(UUT.getWaitTime(), 10);
  UUT.setWaitTime(2);
  ASSERT_EQ(UUT.getWaitTime(), 2);
}

/*
TEST(DynamicEC, TestForFailToDeleteAll)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 80, 40, false);
  UUT.fillFiles();
  UUT.Cleanup();
  eos::mgm::statusForSystem status;
  status = UUT.SpaceStatus();
  ASSERT_TRUE(status.undeletedSize > 10000);
}
*/

TEST(DynamicEC, TestForLayout)
{
  std::string url2 = "root://localhost//eos/testarea/dynec/rawfile";
  std::string url1 = url2 + "1" + ".xrdcl";
  std::string url = "root://localhost//eos/testarea/dynec/rawfile1.xrdcl" ;
  ASSERT_EQ(url, url1);
}

//Test for what did this come from

TEST(DynamicEC, TestForLayout2)
{
  ASSERT_EQ(eos::common::LayoutId::kRaid6, 1);
}


//43647012370960
//301830635816071
/*
TEST(DynamicEC, TestFor)
{
  eos::mgm::DynamicEC UUT(11556926,10000000,1,1);
  UUT.fillFiles();
  ASSERT_TRUE(UUT.createdFileSize > 250000000000000);
}
*/

/*
TEST(DynamicEC, TestForTime)
{
  time_t seconds;
  seconds = time(0);
  ASSERT_EQ(seconds,10);
}
*/

//Test for systems
/*
TEST(DynamicEC, TestForDeletionOfFileSystemWithTiming)
{
  eos::mgm::DynamicEC UUT(1,1,1,1);
  UUT.fillSingleSmallFile(100000000, 1000000000000, 8);
  ASSERT_TRUE(UUT.DeletionOfFileID(UUT.simulatedFiles[0]));
}
*/

///test for how to make the currect time in the DynamicEC.cc function
/*
TEST(DynamicEC, StuffToTest)
{
  eos::IFileMD::ctime_t time;
  auto file = std::make_shared<DynamicECFile>(1);
  file->setCTimeNow();
  file->getCTime(time);
  uint64_t time1 = time.tv_sec;
  time.tv_sec -= 1000000;
  file->setCTime(time);

  eos::IFileMD::ctime_t time2;
  //Get the new time and see if eq
  file->getCTime(time2);

  ASSERT_EQ(time2.tv_sec,time1);
}
*/

/* this is test on how to test
TEST(DynamicEC, Setting)
{
  eos::mgm::DynamicEC UUT;
  ASSERT_EQ(UUT.DummyFunction(4), 4);
}

TEST(DynamicEC, FailTry)
{
  mgm::DynamicEC UUT;
  ASSERT_EQ(UUT.DummyFunction(4),5);
}

TEST(DynamicEC, ReturnOfTrue)
{
  mgm::DynamicEC UUT;
  ASSERT_TRUE(UUT.TrueForAllRequest());
}

TEST(DynamicEC, ReturnOfTrueFail)
{
  mgm::DynamicEC UUT;
  ASSERT_FALSE(UUT.TrueForAllRequest());
}
*/

