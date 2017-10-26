//------------------------------------------------------------------------------
// File: ThreadPoolTest.cc
// Author: root - CERN
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

#include "common/ThreadPool.hh"
#include <chrono>

using namespace eos::common;

int main(int argc, char* argv[]) {
  ThreadPool pool(2, 8, 5, 5);

  std::vector<std::future<int>> futures;
  for(int i = 0; i < 200000; i++) {
    auto future = pool.PushTask<int>(
      [i] {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        cout << i << " from " << std::this_thread::get_id() << endl;
        return i;
      }
    );

    futures.emplace_back(std::move(future));
  }

  for(auto&& future : futures) {
    cout << future.get() << endl;
  }
  futures.clear();

  std::this_thread::sleep_for(std::chrono::seconds(25));
  for(int i = 60; i < 100; i++) {
    auto future = pool.PushTask<int>(
      [i] {
        std::this_thread::sleep_for(std::chrono::seconds(3));
        cout << i << " from " << std::this_thread::get_id() << endl;
        return i;
      }
    );

    futures.emplace_back(std::move(future));
  }

  for(auto&& future : futures) {
    cout << future.get() << endl;
  }

  pool.Stop();

  return 0;
}

