//------------------------------------------------------------------------------
//! @file BufferManagerTests.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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
#define IN_TEST_HARNESS
#include "common/BufferManager.hh"
#undef IN_TEST_HARNESS
#include "common/StringConversion.hh"
#include <random>
#include <thread>
#include <chrono>


TEST(BufferManager, PowerCeil)
{
  ASSERT_EQ(1024, eos::common::GetPowerCeil(1));
  ASSERT_EQ(1024, eos::common::GetPowerCeil(1000));
  ASSERT_EQ(1024, eos::common::GetPowerCeil(1024));
  ASSERT_EQ(2048, eos::common::GetPowerCeil(1025));
  ASSERT_EQ(4096, eos::common::GetPowerCeil(2049));
  ASSERT_EQ(8192, eos::common::GetPowerCeil(5000));
  ASSERT_EQ(16384, eos::common::GetPowerCeil(9001));
  ASSERT_EQ(2048, eos::common::GetPowerCeil(1, 2048));
  ASSERT_EQ(4096, eos::common::GetPowerCeil(2049, 2048));
  ASSERT_EQ(16384, eos::common::GetPowerCeil(1, 16384));
  ASSERT_EQ(32768, eos::common::GetPowerCeil(16385, 16384));
}

TEST(BufferManager, MatchingSizes)
{
  using namespace eos::common;
  eos::common::BufferManager buff_mgr(20 * MB);
  uint64_t buff_sz = 512 * KB;
  auto buffer = buff_mgr.GetBuffer(buff_sz);
  ASSERT_NE(buffer, nullptr);
  ASSERT_EQ(buffer->mCapacity, 1 * MB);
  buff_sz = 1 * MB;
  buffer = buff_mgr.GetBuffer(buff_sz);
  ASSERT_NE(buffer, nullptr);
  ASSERT_EQ(buffer->mCapacity, 1 * MB);
  buff_sz = 1;
  buffer = buff_mgr.GetBuffer(buff_sz);
  ASSERT_NE(buffer, nullptr);
  ASSERT_EQ(buffer->mCapacity, 1 * MB);
  buff_sz = 1 * MB + 22 * KB;
  buffer = buff_mgr.GetBuffer(buff_sz);
  ASSERT_NE(buffer, nullptr);
  ASSERT_EQ(buffer->mCapacity, 2 * MB);
  buff_sz = 1 * MB + 44 * KB;
  buffer = buff_mgr.GetBuffer(buff_sz);
  ASSERT_NE(buffer, nullptr);
  ASSERT_EQ(buffer->mCapacity, 2 * MB);
  buff_sz = 3 * MB + 11 * KB;
  buffer = buff_mgr.GetBuffer(buff_sz);
  ASSERT_NE(buffer, nullptr);
  ASSERT_EQ(buffer->mCapacity, 4 * MB);
  buff_sz = 512 * MB + 33 * KB;
  buffer = buff_mgr.GetBuffer(buff_sz);
  ASSERT_EQ(buffer, nullptr);
  uint64_t total_size {0ull};
  auto slot_sizes = buff_mgr.GetSortedSlotSizes(total_size);
  ASSERT_EQ(11 * MB, total_size);
  // By default there are 6 slots populated like this
  // index: 0 slot: 3 size: 0
  // index: 1 slot: 4 size: 0
  // index: 2 slot: 5 size: 0
  // index: 3 slot: 6 size: 0
  // index: 4 slot: 0 size: 3145728
  // index: 5 slot: 1 size: 4194304
  // index: 6 slot: 2 size: 4194304
  ASSERT_EQ(3, slot_sizes[0].first);
  ASSERT_EQ(0, slot_sizes[0].second);
  ASSERT_EQ(4, slot_sizes[1].first);
  ASSERT_EQ(0, slot_sizes[1].second);
  ASSERT_EQ(5, slot_sizes[2].first);
  ASSERT_EQ(0, slot_sizes[2].second);
  ASSERT_EQ(6, slot_sizes[3].first);
  ASSERT_EQ(0, slot_sizes[3].second);
  ASSERT_EQ(0, slot_sizes[4].first);
  ASSERT_EQ(3145728, slot_sizes[4].second);
  ASSERT_EQ(1,       slot_sizes[5].first);
  ASSERT_EQ(4194304, slot_sizes[5].second);
  ASSERT_EQ(2,       slot_sizes[6].first);
  ASSERT_EQ(4194304, slot_sizes[6].second);
}

TEST(BufferManager, RecycleSingleBuffer)
{
  using namespace eos::common;
  eos::common::BufferManager buff_mgr(20 * MB);

  for (int i = 0; i < 100; ++i) {
    auto buffer = buff_mgr.GetBuffer(1 * MB);
    buff_mgr.Recycle(buffer);
  }

  uint64_t total_size {0ull};
  auto sorted_slots = buff_mgr.GetSortedSlotSizes(total_size);
  ASSERT_EQ(total_size, 1 * MB);
}

TEST(BufferManager, AdjustCachedSizes)
{
  using namespace eos::common;
  eos::common::BufferManager buff_mgr(20 * MB);
  std::list<std::shared_ptr<eos::common::Buffer>> lst_buffs;

  // Recycle a 1MB blocks in a loop
  for (int i = 0; i < 20; ++i) {
    lst_buffs.push_back(buff_mgr.GetBuffer(1 * MB));
    // do some work with the buffer
    buff_mgr.Recycle(lst_buffs.back());
    lst_buffs.pop_back();
  }

  uint64_t total_size {0ull};
  auto sorted_slots = buff_mgr.GetSortedSlotSizes(total_size);
  ASSERT_EQ(total_size, 1 * MB);

  // Fill cache with 1MB blocks
  for (int i = 0; i < 20; ++i) {
    lst_buffs.push_back(buff_mgr.GetBuffer(1 * MB));
  }

  for (int i = 0; i < 20; ++i) {
    buff_mgr.Recycle(lst_buffs.back());
    lst_buffs.pop_back();
  }

  sorted_slots = buff_mgr.GetSortedSlotSizes(total_size);
  ASSERT_EQ(total_size, 20 * MB);
  auto buffer = buff_mgr.GetBuffer(3 * MB);
  ASSERT_NE(buffer, nullptr);
  ASSERT_EQ(buffer->mCapacity, 4 * MB);
  buff_mgr.Recycle(buffer);
  sorted_slots = buff_mgr.GetSortedSlotSizes(total_size);
  ASSERT_EQ(total_size, 16 * MB);
}

TEST(BufferManager, MultipleThreads)
{
  using namespace eos::common;
  auto work = [](eos::common::BufferManager & buff_mgr, int num_blocks,
  float mean, float stddev) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::normal_distribution<> dis(mean, stddev);

    for (int i = 0; i < num_blocks; ++i) {
      uint64_t max_buff_sz = (1 << buff_mgr.GetNumSlots()) * MB;
      uint64_t value = std::round(std::abs(dis(gen)));

      // Make sure the generated value is within limits
      if (value <= 0) {
        value = 1;
      } else if (value > max_buff_sz) {
        value = max_buff_sz;
      }

      auto buffer = buff_mgr.GetBuffer(value);
      ASSERT_NE(buffer, nullptr);
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      buff_mgr.Recycle(buffer);
    }
  };
  int num_threads = 32;
  int num_blocks = 24;
  std::list<std::thread> lst_threads;
  std::list<std::pair<float, float>> uniform_dist_params;
  uniform_dist_params.emplace_back(500 * KB, 200 * KB);
  uniform_dist_params.emplace_back(1500 * KB, 200 * KB);
  uniform_dist_params.emplace_back(3500 * KB, 400 * KB);

  for (auto& dis_params : uniform_dist_params) {
    eos::common::BufferManager buff_mgr(100 * MB);

    for (int i = 0; i < num_threads; ++i) {
      lst_threads.emplace_back(work, std::ref(buff_mgr), num_blocks,
                               dis_params.first, dis_params.second);
    }

    for (auto& job : lst_threads) {
      job.join();
    }

    lst_threads.clear();
    uint64_t total_size {0ull};
    auto sorted_slots = buff_mgr.GetSortedSlotSizes(total_size);
    ASSERT_LE(total_size, buff_mgr.GetMaxSize());
    // Get the most used slot according to the distribution mean
    uint32_t slot {UINT32_MAX};

    for (auto i = 0ull; i <= buff_mgr.GetNumSlots(); ++i) {
      if (dis_params.first <= (1 << (i + 20))) {
        slot = i;
        break;
      }
    }

    // The slot with the largest allocated size should be "slot"
    // for (auto& elem: sorted_slots) {
    //   std::cerr << "Slot: " << elem.first << " size: " << elem.second << std::endl;
    // }
    ASSERT_EQ(sorted_slots.rbegin()->first, slot);
  }
}
