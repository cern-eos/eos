//------------------------------------------------------------------------------
// File: ScanRate.hh
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

#pragma once
#include "fst/Namespace.hh"
#include <chrono>
#include <sys/types.h>

EOSFSTNAMESPACE_BEGIN

class Load;

namespace utils
{

//------------------------------------------------------------------------------
//! Enforce the scan rate by throttling the current thread and also adjust it
//! depending on the IO load on the mountpoint
//!
//! @param offset current offset in file
//! @param open_ts time point when file was opened
//! @param scan_rate current scan rate, if 0 then then rate limiting is
//!        disabled
//! @param fst_load load object
//! @param dir_path path to the directory being scanned
//! @param max_rate maximum allowed scan rate
//------------------------------------------------------------------------------
void EnforceAndAdjustScanRate(const off_t offset,
                              const std::chrono::time_point
                              <std::chrono::system_clock> open_ts,
                              int& scan_rate,
                              Load* fst_load = nullptr,
                              const char* dir_path = nullptr,
                              int max_rate = 0);

} // namespace utils

EOSFSTNAMESPACE_END
