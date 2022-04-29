//------------------------------------------------------------------------------
// File: DrainProgressTrackerTests.cc
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#include "mgm/groupdrainer/DrainProgressTracker.hh"
#include "gtest/gtest.h"

using eos::mgm::DrainProgressTracker;

TEST(DrainProgressTracker, SetTotalFiles)
{
  DrainProgressTracker tracker;
  int fsid = 1;
  tracker.setTotalFiles(fsid, 100);
  EXPECT_EQ(100, tracker.getTotalFiles(fsid));
  EXPECT_EQ(0, tracker.getFileCounter(fsid));

  tracker.increment(fsid);
  EXPECT_FLOAT_EQ(1, tracker.getDrainStatus(fsid));
  EXPECT_EQ(1, tracker.getFileCounter(fsid));

  // set to a lower value, will be ignored, this happens as the drain progresses
  tracker.setTotalFiles(fsid, 50);
  EXPECT_EQ(100, tracker.getTotalFiles(fsid));
  EXPECT_FLOAT_EQ(1, tracker.getDrainStatus(fsid));

  // Set to a higher value, will be set
  tracker.setTotalFiles(fsid, 200);
  EXPECT_EQ(200, tracker.getTotalFiles(fsid));
  EXPECT_FLOAT_EQ(0.5, tracker.getDrainStatus(fsid));

  tracker.increment(fsid); // Now 2 files out of 200!
  EXPECT_EQ(2, tracker.getFileCounter(fsid));
  EXPECT_FLOAT_EQ(1, tracker.getDrainStatus(fsid));
}

TEST(DrainProgressTracker, Deletions)
{
  DrainProgressTracker tracker;
  int fsid = 1;
  tracker.setTotalFiles(fsid, 100);
  // No file entry yet, should return 0;
  EXPECT_FLOAT_EQ(0, tracker.getDrainStatus(fsid));

  tracker.increment(fsid);
  EXPECT_FLOAT_EQ(1, tracker.getDrainStatus(fsid));
  // set to a lower value, will be ignored, this happens as the drain progresses
  tracker.setTotalFiles(fsid, 50);
  tracker.dropFsid(fsid);
  EXPECT_FLOAT_EQ(0, tracker.getDrainStatus(fsid));
}