//------------------------------------------------------------------------------
// File: ScanRate.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "fst/utils/ScanRate.hh"
#include "fst/Load.hh"
#include <thread>

EOSFSTNAMESPACE_BEGIN

namespace utils
{

//------------------------------------------------------------------------------
// Enforce and adjust scan rate logic
//------------------------------------------------------------------------------
void EnforceAndAdjustScanRate(const off_t offset,
                              const std::chrono::time_point
                              <std::chrono::system_clock> open_ts,
                              int& scan_rate,
                              Load* fst_load,
                              const char* dir_path,
                              const int max_rate)
{
  using namespace std::chrono;

  if (scan_rate) {
    const auto now_ts = std::chrono::system_clock::now();
    uint64_t scan_duration_msec =
      duration_cast<milliseconds>(now_ts - open_ts).count();
    uint64_t expect_duration_msec =
      (uint64_t)((1000.0 * offset) / (scan_rate * 1024 * 1024));

    if (expect_duration_msec > scan_duration_msec) {
      std::this_thread::sleep_for(milliseconds(expect_duration_msec -
                                  scan_duration_msec));
    }

    if (fst_load && dir_path) {
      // Adjust the rate according to the load information
      double load = fst_load->GetDiskRate(dir_path, "millisIO") / 1000.0;

      if (load > 0.7) {
        // Adjust the scan_rate which is in MB/s but no lower then 5 MB/s
        if (scan_rate > 5) {
          scan_rate = 0.9 * scan_rate;
        }
      } else if (max_rate) {
        scan_rate = max_rate;
      }
    }
  }
}

} // namespace utils

EOSFSTNAMESPACE_END
