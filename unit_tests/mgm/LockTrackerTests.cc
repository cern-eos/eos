//------------------------------------------------------------------------------
// File: LockTrackerTests.cc
// Author: Georgios Bitzes <georgios.bitzes@cern.ch>
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

#include "gtest/gtest.h"
#include "mgm/fuse-locks/LockTracker.hh"
#include <fcntl.h>

using namespace eos::mgm;

#define ASSERT_OVERLAP(RANGE1, RANGE2) { ASSERT_TRUE(RANGE1.overlap(RANGE2)); ASSERT_TRUE(RANGE2.overlap(RANGE1)); }
#define ASSERT_NOT_OVERLAP(RANGE1, RANGE2) { ASSERT_FALSE(RANGE1.overlap(RANGE2)); ASSERT_FALSE(RANGE2.overlap(RANGE1)); }

std::vector<ByteRange> vec(const ByteRange &b1) { return {b1}; }
std::vector<ByteRange> vec(const ByteRange &b1, const ByteRange &b2) { return {b1, b2}; }
std::vector<ByteRange> vec() { return {}; }

TEST(ByteRange, overlap) {
  ByteRange b1(4, 3); // [4, 6]
  ByteRange b2(5, 1); // [5, 5)
  ASSERT_OVERLAP(b1, b2);

  b1 = ByteRange(5, -1); // [5, inf)
  b2 = ByteRange(1, 3);  // [1, 3]
  ASSERT_NOT_OVERLAP(b1, b2);

  b2 = ByteRange(1, 4);  // [1, 4]
  ASSERT_NOT_OVERLAP(b1, b2);

  b2 = ByteRange(1, 5);  // [1, 5]
  ASSERT_OVERLAP(b1, b2);

  b1 = ByteRange(10, 3); // [10, 12]
  ASSERT_NOT_OVERLAP(b1, b2);

  b2 = ByteRange(14, -1); // [14, inf)
  ASSERT_NOT_OVERLAP(b1, b2);

  b1 = ByteRange(10, 4); // [10, 13]
  ASSERT_NOT_OVERLAP(b1, b2);

  b1 = ByteRange(10, 5); // [10, 14]
  ASSERT_OVERLAP(b1, b2);
}

TEST(ByteRange, overlapOrTouch) {
  ByteRange b1(4, 3); // [4, 6]
  ByteRange b2(1, 3); // [1, 3]

  ASSERT_TRUE(b1.overlapOrTouch(b2));
  ASSERT_FALSE(b1.overlap(b2));

  ASSERT_TRUE(b2.overlapOrTouch(b1));
  ASSERT_FALSE(b2.overlap(b1));

  ByteRange b3(7, 2); // [7, 8]

  ASSERT_TRUE(b1.overlapOrTouch(b3));
  ASSERT_FALSE(b1.overlap(b3));

  ASSERT_TRUE(b3.overlapOrTouch(b1));
  ASSERT_FALSE(b3.overlap(b1));
}

TEST(ByteRange, absorb) {
  ByteRange b1(4, 3); // [4, 6]
  ByteRange b2(5, 1); // [5, 5)

  ASSERT_TRUE(b2.absorb(b1));
  ASSERT_EQ(b2.start(), 4);
  ASSERT_EQ(b2.end(), 7);

  ByteRange b3(9, -1);
  ASSERT_FALSE(b2.absorb(b3));

  b3 = ByteRange(5, -1);
  ASSERT_TRUE(b2.absorb(b3));
  ASSERT_EQ(b2.start(), 4);
  ASSERT_EQ(b2.len(), -1);
}

TEST(ByteRange, absorb2) {
  ByteRange b1(10, 5); // [10, 14]
  ByteRange b2(15, 6); // [15, 20]

  ASSERT_TRUE(b1.absorb(b2));
  ASSERT_EQ(b1.start(), 10);
  ASSERT_EQ(b1.end(), 21);
}

TEST(ByteRange, absorb3) {
  ByteRange b1(10, 5); // [10, 14]
  ByteRange b2(15, 6); // [15, 20]

  ASSERT_TRUE(b2.absorb(b1));
  ASSERT_EQ(b2.start(), 10);
  ASSERT_EQ(b2.end(), 21);
}

TEST(ByteRange, contains) {
  ByteRange b1(4, 3); // [4, 6]
  ByteRange b2(4, 4); // [4, 7]

  ASSERT_FALSE(b1.contains(b2));
  ASSERT_TRUE(b2.contains(b1));

  ASSERT_TRUE(b1.contains(b1));
  ASSERT_TRUE(b2.contains(b2));

  b1 = ByteRange(4, -1); // [4, inf)
  b2 = ByteRange(3, 3);  // [3, 5]

  ASSERT_TRUE(b1.contains(b1));
  ASSERT_TRUE(b2.contains(b2));

  ASSERT_FALSE(b1.contains(b2));
  ASSERT_FALSE(b2.contains(b1));

  b2 = ByteRange(3, -1); // [3, inf)
  ASSERT_TRUE(b2.contains(b2));
  ASSERT_FALSE(b1.contains(b2));
  ASSERT_TRUE(b2.contains(b1));
}

TEST(ByteRange, minus_all_cases) {
  ByteRange b1(4, 3); // [4, 6]
  ByteRange b2(1, 2); // [1, 3]
  ByteRange b3(5, 4); // [5, 8]
  ByteRange b4(3, 3); // [3, 5]
  ByteRange b5(6, 3); // [6, 8]
  ByteRange b6(6, 4); // [6, 9]
  ByteRange b7(6, 1); // [6, 6]

  ASSERT_EQ(b1.minus(b2), vec(b1)); // b2 fully to the right
  ASSERT_EQ(b2.minus(b1), vec(b2)); // b1 fully to the left

  ASSERT_EQ(b3.minus(b4), vec(ByteRange(6, 3))); // b4 eats the start

  ASSERT_EQ(b3.minus(b5), vec(ByteRange(5, 1))); // b5 eats the end
  ASSERT_EQ(b3.minus(b6), vec(ByteRange(5, 1))); // b6 eats the end

  ASSERT_EQ(b3.minus(b7), vec(ByteRange(5, 1), ByteRange(7, 2))); // b7 eats the middle
}

TEST(ByteRange, minus_case_eat_whole) {
  ByteRange b1(100, 50); // [100, 149]
  ByteRange b2(99, 51);  // [99, 149]
  ByteRange b3(0, 200);  // [0, 199]
  ByteRange b4(100, 51); // [100, 150]
  ByteRange b5(99, 52);  // [99, 150]
  ByteRange b6(50, 300); // [50, 349]

  ASSERT_EQ(b1.minus(b2), vec());
  ASSERT_EQ(b1.minus(b3), vec());
  ASSERT_EQ(b1.minus(b4), vec());
  ASSERT_EQ(b1.minus(b5), vec());
  ASSERT_EQ(b1.minus(b6), vec());
}

TEST(ByteRange, minus_case_to_the_left) {
  ByteRange b1(100, 50); // [100, 149]
  ByteRange b2(50, 10);  // [50, 59]
  ByteRange b3(90, 10);  // [90, 99]
  ByteRange b4(0, 100);  // [0, 99]
  ByteRange b5(99, 1);   // [99, 99]

  ASSERT_EQ(b1.minus(b2), vec(b1));
  ASSERT_EQ(b1.minus(b3), vec(b1));
  ASSERT_EQ(b1.minus(b4), vec(b1));
  ASSERT_EQ(b1.minus(b5), vec(b1));
}

TEST(ByteRange, minus_case_to_the_right) {
  ByteRange b1(100, 50); // [100, 149]
  ByteRange b2(200, 10); // [200, 209]
  ByteRange b3(150, 10); // [150, 159]
  ByteRange b4(150, 20); // [150, 169]
  ByteRange b5(150, 1);  // [150, 150]
  ByteRange b6(300, 1);  // [300, 300]

  ASSERT_EQ(b1.minus(b2), vec(b1));
  ASSERT_EQ(b1.minus(b3), vec(b1));
  ASSERT_EQ(b1.minus(b4), vec(b1));
  ASSERT_EQ(b1.minus(b5), vec(b1));
  ASSERT_EQ(b1.minus(b6), vec(b1));
}

TEST(ByteRange, minus_case_eat_middle) {
  ByteRange b1(100, 50); // [100, 149]
  ByteRange b2(120, 10); // [120, 129]
  ByteRange b3(101, 48); // [101, 148]
  ByteRange b4(101, 1);  // [101, 101]
  ByteRange b5(148, 1);  // [148, 148]
  ByteRange b6(110, 10); // [110, 119]

  ASSERT_EQ(b1.minus(b2), vec(ByteRange(100, 20), ByteRange(130, 20)));
  ASSERT_EQ(b1.minus(b3), vec(ByteRange(100, 1), ByteRange(149, 1)));
  ASSERT_EQ(b1.minus(b4), vec(ByteRange(100, 1), ByteRange(102, 48)));
  ASSERT_EQ(b1.minus(b5), vec(ByteRange(100, 48), ByteRange(149, 1)));
  ASSERT_EQ(b1.minus(b6), vec(ByteRange(100, 10), ByteRange(120, 30)));
}

TEST(ByteRange, minus_case_eat_start) {
  ByteRange b1(100, 50); // [100, 149]
  ByteRange b2(100, 1);  // [100, 100]
  ByteRange b3(99, 2);   // [99, 100]
  ByteRange b4(99, 3);   // [99, 101]
  ByteRange b5(100, 10); // [100, 109]
  ByteRange b6(90, 30);  // [90, 119]

  ASSERT_EQ(b1.minus(b2), vec(ByteRange(101, 49)));
  ASSERT_EQ(b1.minus(b3), vec(ByteRange(101, 49)));
  ASSERT_EQ(b1.minus(b4), vec(ByteRange(102, 48)));
  ASSERT_EQ(b1.minus(b5), vec(ByteRange(110, 40)));
  ASSERT_EQ(b1.minus(b6), vec(ByteRange(120, 30)));
}

TEST(ByteRange, minus_case_eat_end) {
  ByteRange b1(100, 50); // [100, 149]
  ByteRange b2(149, 1);  // [149, 149]
  ByteRange b3(149, 2);  // [149, 150]
  ByteRange b4(148, 2);  // [148, 149]
  ByteRange b5(148, 10); // [148, 157]
  ByteRange b6(120, 50); // [120, 169]

  ASSERT_EQ(b1.minus(b2), vec(ByteRange(100, 49)));
  ASSERT_EQ(b1.minus(b3), vec(ByteRange(100, 49)));
  ASSERT_EQ(b1.minus(b4), vec(ByteRange(100, 48)));
  ASSERT_EQ(b1.minus(b5), vec(ByteRange(100, 48)));
  ASSERT_EQ(b1.minus(b6), vec(ByteRange(100, 20)));
}

TEST(Lock, absorb) {
  Lock l1(ByteRange(2, 2), 1);
  Lock l2(ByteRange(3, 2), 2);
  Lock l3(ByteRange(3, 2), 1);

  ASSERT_FALSE(l1.absorb(l2)); // pids don't match

  ASSERT_TRUE(l1.absorb(l3)); // pids match and there's overlap
  ASSERT_EQ(l1.range().start(), 2);
  ASSERT_EQ(l1.range().end(), 5);

  Lock l4(ByteRange(1, 2), 3);
  Lock l5(ByteRange(3, 2), 3);
  ASSERT_TRUE(l4.absorb(l5));
  ASSERT_EQ(l4, Lock(ByteRange(1, 4), 3));
}

TEST(LockSet, various) {
  LockSet set;

  set.add(Lock(ByteRange(10, 5), 1)); // [10, 14]
  set.add(Lock(ByteRange(14, 3), 2)); // [14, 16]
  set.add(Lock(ByteRange(15, 6), 1)); // [15, 20]

  ASSERT_EQ(set.nlocks(2), 1u);
  ASSERT_EQ(set.nlocks(1), 1u);

  ASSERT_TRUE(set.overlap(Lock(ByteRange(10, 1), 1)));
  ASSERT_TRUE(set.overlap(Lock(ByteRange(10, 4), 1)));
  ASSERT_TRUE(set.overlap(Lock(ByteRange(10, 100), 1)));

  ASSERT_TRUE(set.overlap(Lock(ByteRange(10, 100), 2)));
  ASSERT_FALSE(set.overlap(Lock(ByteRange(10, 100), 3)));

  ASSERT_TRUE(set.overlap(ByteRange(20, 1)));
  ASSERT_FALSE(set.overlap(ByteRange(21, 1)));

  ASSERT_FALSE(set.overlap(ByteRange(9, 1)));
  ASSERT_TRUE(set.overlap(ByteRange(9, 2)));
  ASSERT_TRUE(set.overlap(ByteRange(10, 1)));

  set.remove(Lock(ByteRange(13, 3), 1)); // split range for pid "1" into two
  ASSERT_EQ(set.nlocks(2), 1u);

  // Now, for pid 1 we have
  // [10, 12]
  // [16, 20]

  ASSERT_TRUE(set.overlap(Lock(ByteRange(11, 2), 1)));
  ASSERT_TRUE(set.overlap(Lock(ByteRange(12, 2), 1)));
  ASSERT_FALSE(set.overlap(Lock(ByteRange(13, 2), 1)));
  ASSERT_FALSE(set.overlap(Lock(ByteRange(14, 2), 1)));
  ASSERT_TRUE(set.overlap(Lock(ByteRange(15, 2), 1)));
  ASSERT_TRUE(set.overlap(Lock(ByteRange(20, 1), 1)));
  ASSERT_FALSE(set.overlap(Lock(ByteRange(21, 1), 1)));
  ASSERT_TRUE(set.overlap(Lock(ByteRange(19, 3), 1)));

  ASSERT_TRUE(set.conflict(Lock(ByteRange(19, 3), 2)));
  ASSERT_FALSE(set.conflict(Lock(ByteRange(19, 3), 1)));
}

TEST(LockTracker, various) {
  LockTracker tracker;

  // Write lock [1, 100] by PID 1
  struct flock lock;
  lock.l_start = 1;
  lock.l_len = 100;
  lock.l_type = F_WRLCK;

  ASSERT_TRUE(tracker.setlk(2, &lock, 0, "owner"));
  ASSERT_FALSE(tracker.setlk(3, &lock, 0, "owner"));
  ASSERT_TRUE(tracker.setlk(2, &lock, 0, "owner")); // lock again by same pid, should be no-op

  // Release [5, 10]
  lock.l_start = 5;
  lock.l_len = 6;
  lock.l_type = F_UNLCK;

  ASSERT_TRUE(tracker.setlk(2, &lock, 0, "owner"));

  // Lock [5, 10], by pid 3
  lock.l_type = F_WRLCK;
  ASSERT_TRUE(tracker.setlk(3, &lock, 0, "owner"));
  ASSERT_FALSE(tracker.setlk(2, &lock, 0, "owner")); // pid 2 should not be able to reclaim it

  // Convert [5, 6] into read lock
  lock.l_start = 5;
  lock.l_len = 2;
  lock.l_type = F_RDLCK;
  ASSERT_TRUE(tracker.setlk(3, &lock, 0, "owner"));

  // Add read lock from a different process
  ASSERT_TRUE(tracker.setlk(4, &lock, 0, "owner"));

  // Make sure no write locks are allowed
  lock.l_type = F_WRLCK;
  ASSERT_FALSE(tracker.setlk(5, &lock, 0, "owner"));

  // Even if coming from a process which has a read lock already, in case there
  // are other readers
  ASSERT_FALSE(tracker.setlk(4, &lock, 0, "owner"));

  // Remove read lock from pid 3
  lock.l_type = F_UNLCK;
  ASSERT_TRUE(tracker.setlk(3, &lock, 0, "owner"));

  // Now it should be possible to convert it into a write lock, since pid 4 is
  // the only reader
  lock.l_type = F_WRLCK;
  ASSERT_TRUE(tracker.setlk(4, &lock, 0, "owner"));
}
