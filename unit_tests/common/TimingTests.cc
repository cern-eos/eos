//------------------------------------------------------------------------------
// File: TimingTest.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
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
#include "Namespace.hh"
#include "common/Timing.hh"

EOSCOMMONTESTING_BEGIN

TEST(Timing, LsFormat)
{
  using namespace eos::common;
  time_t now = time(0);
  struct tm utc;
  struct tm* tm = gmtime_r(&now, &utc);
  std::string output;
  output = Timing::ToLsFormat(tm);
  // Shoud contain the hour:minute
  ASSERT_TRUE(output.find(':') != std::string::npos);
  // 1 year ago
  tm->tm_year--;
  output = Timing::ToLsFormat(tm);
  // Should contain only the year at the end
  ASSERT_TRUE(output.find(':') == std::string::npos);
}

EOSCOMMONTESTING_END
