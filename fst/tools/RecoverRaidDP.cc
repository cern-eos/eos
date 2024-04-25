//------------------------------------------------------------------------------
// File: RecoverRaidDP.cc
// Author: Elvin-Alin Sindrilaru - CERN
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

#include <cstdio>
#include <cstdlib>
#include <fst/RaidDPScan.hh>
#include <XrdOuc/XrdOucString.hh>

int
main(int argc, char* argv[])
{
  if (argc != 2) {
    fprintf(stderr, "usage: eos-raiddp-scan <file_name>\n");
    exit(-1);
  }

  XrdOucString fileName = argv[1];
  eos::fst::RaidDPScan* rds = new eos::fst::RaidDPScan(fileName.c_str(), false);

  if (rds) {
    eos::fst::RaidDPScan::StaticThreadProc((void*) rds);
    delete rds;
  }
}
