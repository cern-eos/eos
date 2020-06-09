//------------------------------------------------------------------------------
//! @file IdTrackerWithValidityTests.cc
//! @author Elvin-Alin Sindrilaru <esindril at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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
#include "mgm/IdTrackerWithValidity.hh"

//------------------------------------------------------------------------------
// Test tracker basic functionality
//------------------------------------------------------------------------------
TEST(IdTrackerWithValidity, BasicFunctionality)
{
  using namespace eos::mgm;
  IdTrackerWithValidity<uint64_t> tracker(std::chrono::seconds(10),
                                          std::chrono::seconds(60), true);
  auto& clock = tracker.GetClock();

  for (uint64_t i = 11; i < 100; i += 10) {
    tracker.AddEntry(i, TrackerType::Drain);
    clock.advance(std::chrono::seconds(5));
  }

  for (uint64_t i = 11; i < 100; i += 10) {
    ASSERT_TRUE(tracker.HasEntry(i));
  }

  for (uint64_t i = 12; i < 100; i += 10) {
    ASSERT_FALSE(tracker.HasEntry(i));
  }

  clock.advance(std::chrono::seconds(16)); // Should expire the first entry
  tracker.DoCleanup(TrackerType::Drain);
  ASSERT_FALSE(tracker.HasEntry(11));
  ASSERT_TRUE(tracker.HasEntry(21));
  clock.advance(std::chrono::seconds(100)); // Should expire all entries
  tracker.DoCleanup(TrackerType::Drain);

  for (uint64_t i = 11; i < 100; i += 10) {
    ASSERT_FALSE(tracker.HasEntry(i));
  }

  tracker.AddEntry(121, TrackerType::Drain);
  ASSERT_TRUE(tracker.HasEntry(121));
  tracker.RemoveEntry(121);
  ASSERT_FALSE(tracker.HasEntry(121));

  // Add enties with custom expiration time
  for (uint64_t i = 13; i < 100; i += 10) {
    tracker.AddEntry(i, TrackerType::Drain, std::chrono::seconds(i));
  }

  clock.advance(std::chrono::seconds(90));
  tracker.DoCleanup(TrackerType::Drain);
  // All but the last entry should be expired
  ASSERT_TRUE(tracker.HasEntry(93));

  for (uint64_t i = 13; i < 90; i += 10) {
    ASSERT_FALSE(tracker.HasEntry(i));
  }

  tracker.Clear(TrackerType::All);
  ASSERT_FALSE(tracker.HasEntry(93));
}
