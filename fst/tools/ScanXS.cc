// ----------------------------------------------------------------------
// File: ScanXS.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#include "fst/ScanDir.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "fst/filemd/FmdDbMap.hh"
#include "fst/Config.hh"
#include "common/LayoutId.hh"
#include "common/AssistedThread.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int
main(int argc, char* argv[])
{
  bool setxs = false;
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  g_logging.SetLogPriority(LOG_INFO);
  g_logging.SetUnit("Scandir");

  if ((argc < 2) || (argc > 3)) {
    fprintf(stderr, "usage: eos-scan-fs <directory> [--setxs]\n");
    exit(-1);
  }

  if (argc == 3) {
    XrdOucString set = argv[2];

    if (set != "--setxs") {
      fprintf(stderr, "usage: eos-scan-fs <directory> [--setxs]\n");
      exit(-1);
    }

    setxs = true;
  }

  srand((unsigned int) time(NULL));
  eos::fst::Load fstLoad(1);
  fstLoad.Monitor();
  usleep(100000);
  XrdOucString dirName = argv[1];
  eos::fst::ScanDir* sd =
    new eos::fst::ScanDir(dirName.c_str(), 0, &fstLoad, false, 10, 100, setxs);
  AssistedThread thread;
  thread.reset(&eos::fst::ScanDir::RunDiskScan, sd);
  thread.blockUntilThreadJoins();
  delete sd;
}
