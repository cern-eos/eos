//------------------------------------------------------------------------------
// File: StatsTest.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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

#include "TestEnv.hh"
#include "fst/stat/Stat.hh"
#include "gtest/gtest.h"
#include "common/Timing.hh"

TEST(FstStats, BasicSanity)
{
  eos::fst::Stat fstStat;
  std::string out;
  fstStat.Start();
  sleep(5);

  eos::common::Timing tm("Test");
  COMMONTIMING("START",&tm);
  for (size_t u=0; u < 10; ++u) {
    for (size_t i= 0; i< 100000; ++i) {
      fstStat.Add("/eos/file1", u, 0, "cms", "rbytes", i);
      fstStat.Add("/eos/file2", u, 0, "cms", "rbytes", i);
      fstStat.AddExec("/eos/file1",u, 0, "cms", "rbytes", i);
      fstStat.AddExec("/eos/file2",u, 0, "cms", "rbytes", i);
    }
  }
  COMMONTIMING("STOP",&tm);
  fprintf(stderr,"realtime = %.02f rate=%.02f\n", tm.RealTime(), 10*100000*2/tm.RealTime()*1000.0);
  fstStat.PrintOutTotal(out, false);std::cout << out << std::endl;
  std::cout << fstStat.PrintOutTotalJson() << std::endl;
  
  sleep(1);
  fstStat.PrintOutTotal(out, false);std::cout << out << std::endl;
  sleep(1);
  fstStat.PrintOutTotal(out, true);std::cout << out << std::endl;
  sleep(1);
  fstStat.PrintOutTotal(out, false);std::cout << out << std::endl;
  sleep(1);
  fstStat.PrintOutTotal(out, false);std::cout << out << std::endl;
  sleep(1);
  fstStat.PrintOutTotal(out, false);std::cout << out << std::endl;
  sleep(1);
  fstStat.PrintOutTotal(out, false);std::cout << out << std::endl;
  sleep(1);
  fstStat.PrintOutTotal(out, false);std::cout << out << std::endl;
  sleep(1);
  fstStat.PrintOutTotal(out, false);std::cout << out << std::endl;
  sleep(1);
  fstStat.PrintOutTotal(out, false);std::cout << out << std::endl;
  fstStat.Stop();
}

