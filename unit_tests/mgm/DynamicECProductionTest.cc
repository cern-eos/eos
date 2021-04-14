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
}

TEST(DynamicECProduction, TestForSetterAndGetter2)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 1, 1, false);
}

TEST(DynamicECProduction, TestForFillingInFiles)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 1, 1, false);
  UUT.testFilesBeignFilled(8, 2, 2, 10);
  EXPECT_EQ(UUT.GetMap().size(), 10);
}

TEST(DynamicECProduction, TestForFillingInFilesIfFail)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 11556926, 10000000, 1, 1, false);
  UUT.testFilesBeignFilled(6, 2, 2, 1);
  EXPECT_EQ(UUT.mStatusFilesMD.size(), 1);

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()), 5);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 2);
  }
}

TEST(DynamicECProduction, TestForThis)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 3600, 1024 * 1024, 98, 95, false, 30, 1);
  UUT.testFilesBeignFilled(6, 2, 2, 1);
  EXPECT_EQ(UUT.mStatusFilesMD.size(), 1);

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    fprintf(stderr, "This is for locations: %lu \n",
            it->second->getLocations().size());
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              6);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(it->second->getLocations().size(), 8);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    UUT.kReduceMD(it->second);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    fprintf(stderr, "This is for locations: %lu \n",
            it->second->getLocations().size());
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              6);
    EXPECT_EQ(eos::common::LayoutId::GetRedundancy(it->second->getLayoutId(),
              it->second->getLocations().size()) + 1, 4);
    EXPECT_EQ(it->second->getLocations().size(), 6);
  }
}

TEST(DynamicECProduction, TestForCleanup)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 3600, 1024 * 1024, 98, 95, false, 30, 1);
  UUT.testFilesBeignFilled(6, 2, 2, 1);
  EXPECT_EQ(UUT.mStatusFilesMD.size(), 1);

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              6);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetRedundancy(it->second->getLayoutId(),
              it->second->getLocations().size()) + 1, 6);
    EXPECT_EQ(it->second->getLocations().size(), 8);
  }

  UUT.CleanupMD();

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              6);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 0);
    EXPECT_EQ(eos::common::LayoutId::GetRedundancy(it->second->getLayoutId(),
              it->second->getLocations().size()) + 1, 2);
    EXPECT_EQ(it->second->getLocations().size(), 6);
  }
}

TEST(DynamicECProduction, TestForSmallFileDeletion)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 3600, 1024 * 1024, 98, 95, false, 30, 1);
  UUT.testForSingleFile(6, 2, 2, 1000000);

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              6);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetRedundancy(it->second->getLayoutId(),
              it->second->getLocations().size()) + 1, 6);
    EXPECT_EQ(it->second->getLocations().size(), 8);
  }

  UUT.CleanupMD();

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              6);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(it->second->getLocations().size(), 8);
  }
}

TEST(DynamicECProduction, TestForSmallFileDeletionComeTrue)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 3600, 1024 * 1024, 98, 95, false, 30, 1);
  UUT.testForSingleFile(6, 2, 2, 1050000);

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              6);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetRedundancy(it->second->getLayoutId(),
              it->second->getLocations().size()) + 1, 6);
    EXPECT_EQ(it->second->getLocations().size(), 8);
  }

  UUT.CleanupMD();

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              6);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetRedundancy(it->second->getLayoutId(),
              it->second->getLocations().size()) + 1, 2);
    EXPECT_EQ(it->second->getLocations().size(), 6);
  }
}


TEST(DynamicECProduction, TestForMultiDeletionComeTrue)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 3600, 500000, 98, 95, false, 30, 1);
  UUT.testFilesBeignFilledCompiledSize(6, 2, 2, 100, 1000000);
  EXPECT_EQ(UUT.mStatusFilesMD.size(), 100);
  UUT.CleanupMD();
  EXPECT_EQ(UUT.mStatusFilesMD.size(), 90);
}





TEST(DynamicECProduction, TestForThisWithThekReduceMD)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 3600, 1024 * 1024, 98, 95, false, 30, 1);
  UUT.testFilesBeignFilled(6, 2, 2, 100);
  EXPECT_EQ(UUT.mStatusFilesMD.size(), 100);

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              6);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(it->second->getLocations().size(), 8);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    UUT.kReduceMD(it->second);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              6);
    EXPECT_EQ(eos::common::LayoutId::GetRedundancy(it->second->getLayoutId(),
              it->second->getLocations().size()) + 1, 4);
    EXPECT_EQ(it->second->getLocations().size(), 6);
  }
}

TEST(DynamicECProduction, TestForCleanUp)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 3600, 1024 * 1024, 98, 95, false, 30, 1);
  UUT.testForSingleFile(6, 2, 2, 10000000);

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    UUT.kReduceMD(it->second);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(it->second->getLocations().size(), 6);
  }
}

TEST(DynamicECProduction, TestForOtherLayoutskQrain)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 3600, 1024 * 1024, 98, 95, false, 30, 1);
  UUT.testForSingleFileWithkQrain(6, 4, 2, 1500000);

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 4);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              6);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(it->second->getLocations().size(), 8);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    UUT.kReduceMD(it->second);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 4);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              6);
    EXPECT_EQ(it->second->getLocations().size(), 6);
  }
}

TEST(DynamicECProduction, TestForOtherLayoutskQrainNoExcessstripes)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 3600, 1024 * 1024, 98, 95, false, 30, 1);
  UUT.testForSingleFileWithkQrain(6, 4, 0, 1500000);

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 4);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              6);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 0);
    EXPECT_EQ(it->second->getLocations().size(), 6);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    UUT.kReduceMD(it->second);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 4);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              6);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 0);
    EXPECT_EQ(it->second->getLocations().size(), 6);
  }
}

TEST(DynamicECProduction, TestForOtherLayoutkPlain)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 3600, 1024 * 1024, 98, 95, false, 30, 1);
  UUT.testForSingleFileWithkPlain(1, 0, 0, 1500000);

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 0);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              1);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 0);
    EXPECT_EQ(it->second->getLocations().size(), 1);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    UUT.kReduceMD(it->second);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 0);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              1);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 0);
    EXPECT_EQ(it->second->getLocations().size(), 1);
  }
}

TEST(DynamicECProduction, TestForOtherLayoutkReplica)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 3600, 1024 * 1024, 98, 95, false, 30, 1);
  UUT.testForSingleFileWithkReplica(4, 3, 2, 1500000);

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 3);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              4);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(it->second->getLocations().size(), 6);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    UUT.kReduceMD(it->second);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 3);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              4);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(it->second->getLocations().size(), 4);
  }
}

TEST(DynamicECProduction, TestForOtherLayoutkArchive)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 3600, 1024 * 1024, 98, 95, false, 30, 1);
  UUT.testForSingleFileWithkArchive(4, 3, 2, 1500000);

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 3);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              4);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(it->second->getLocations().size(), 6);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    UUT.kReduceMD(it->second);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 3);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              4);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(it->second->getLocations().size(), 4);
  }
}

TEST(DynamicECProduction, TestForOtherLayoutkRaidDP)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 3600, 1024 * 1024, 98, 95, false, 30, 1);
  UUT.testForSingleFileWithkRaidDP(8, 2, 2, 1500000);

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              8);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(it->second->getLocations().size(), 10);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    UUT.kReduceMD(it->second);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              8);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(it->second->getLocations().size(), 8);
  }
}

TEST(DynamicECProduction, TestForOtherLayoutkRaid5)
{
  const char* str = "DynamicTest";
  eos::mgm::DynamicEC UUT(str, 3600, 1024 * 1024, 98, 95, false, 30, 1);
  UUT.testForSingleFileWithkRaid5(8, 2, 2, 1500000);

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              8);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(it->second->getLocations().size(), 10);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    UUT.kReduceMD(it->second);
  }

  for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
         UUT.mStatusFilesMD.begin(); it != UUT.mStatusFilesMD.end(); ++it) {
    EXPECT_EQ(eos::common::LayoutId::GetRedundancyStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(eos::common::LayoutId::GetStripeNumber(it->second->getLayoutId()) + 1,
              8);
    EXPECT_EQ(eos::common::LayoutId::GetExcessStripeNumber(
                it->second->getLayoutId()), 2);
    EXPECT_EQ(it->second->getLocations().size(), 8);
  }
}
