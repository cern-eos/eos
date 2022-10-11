// ----------------------------------------------------------------------
// File: FollyExecutorFixture.hh
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                           *
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


#pragma once

#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gtest/gtest.h>

class FollyExecutor_F : public ::testing::Test
{
protected:
  void SetUp() override
  {
    folly_io_executor = std::make_shared<folly::IOThreadPoolExecutor>(kNumThreads);
    folly_cpu_executor = std::make_shared<folly::CPUThreadPoolExecutor>(kNumThreads);
  }

  void TearDown() override
  {
    folly_io_executor.reset();
    folly_cpu_executor.reset();
  }

  std::shared_ptr<folly::IOThreadPoolExecutor> folly_io_executor;
  std::shared_ptr<folly::CPUThreadPoolExecutor> folly_cpu_executor;
  constexpr static size_t kNumThreads = 4;
};
