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
#define IN_TEST_HARNESS
#include "fst/Health.hh"
#undef IN_TEST_HARNESS
#include <vector>

using eos::fst::DiskHealth;

std::vector<std::string> demo_raid {
  "Personalities : [raid1] [raid6] [raid5] [raid4] [raid0] \n\
\n\
md125 : active raid6 sdx[3] sdae[7] sdw[2] sdy[4] sdz[6] sdv[1]\n\
      15627549952 blocks super 1.2 level 6, 32k chunk, algorithm 2 [6/6] [UUUUUU]\n\
      bitmap: 0/30 pages [0KB], 65536KB chunk\n\
      \n\
md1 : active raid1 sdb2[1] sda2[0]\n\
      1952333824 blocks super 1.2 [2/2] [UU]\n\
      bitmap: 6/15 pages [24KB], 65536KB chunk\n\
\n\
md0 : active raid1 sda1[0] sdb1[1]\n\
      1048512 blocks super 1.0 [2/2] [UU]\n\
      bitmap: 0/1 pages [0KB], 65536KB chunk\n\
\n\
unused devices: <none>",
  "Personalities : [raid1] [raid0] \n\
md96 : active raid0 md109[0] md105[2] md121[1]\n\
      17580781056 blocks super 1.2 128k chunks\n\
      \n\
md97 : active raid0 md108[0] md123[1] md126[2]\n\
      17580781056 blocks super 1.2 128k chunks\n\
      \n\
md99 : active raid0 md104[1] md118[2] md115[0]\n\
      17580781056 blocks super 1.2 128k chunks\n\
\n\
md106 : active raid1 sdaf[1] sdae[0]\n\
      5860391488 blocks super 1.2 [2/2] [UU]\n\
      bitmap: 0/44 pages [0KB], 65536KB chunk\n"
};

TEST(HealthTest, ParseRaidStatus)
{
  for (size_t i = 0; i < demo_raid.size(); ++i) {
    std::string tmp_path = "/tmp/eos.health.XXXXXX";
    int fd = mkstemp((char*)tmp_path.c_str());
    ASSERT_EQ(demo_raid[i].length(),
              write(fd, demo_raid[i].data(), demo_raid[i].length()));
    ASSERT_EQ(0, close(fd));
    DiskHealth dh;
    std::vector<std::string> devices {"md1", "dummy_md0", "md125", "md96"};

    for (const auto& dev : devices) {
      auto mstatus = dh.parse_mdstat(dev, tmp_path);

      if (i == 0) {
        if (dev == "md1") {
          ASSERT_EQ("0", mstatus["drives_failed"]);
          ASSERT_EQ("2", mstatus["drives_healthy"]);
          ASSERT_EQ("2", mstatus["drives_total"]);
          ASSERT_EQ("0", mstatus["indicator"]);
          ASSERT_EQ("1", mstatus["redundancy_factor"]);
          ASSERT_EQ("2/2 (+1)", mstatus["summary"]);
        } else if (dev == "dummy_md0") {
          ASSERT_EQ("no mdstat", mstatus["summary"]);
        } else if (dev == "md125") {
          ASSERT_EQ("0", mstatus["drives_failed"]);
          ASSERT_EQ("6", mstatus["drives_healthy"]);
          ASSERT_EQ("6", mstatus["drives_total"]);
          ASSERT_EQ("0", mstatus["indicator"]);
          ASSERT_EQ("2", mstatus["redundancy_factor"]);
          ASSERT_EQ("6/6 (+2)", mstatus["summary"]);
        }
      } else if (i == 1) {
        if (dev == "md96") {
          ASSERT_EQ("no mdstat", mstatus["summary"]);
        }
      }
    }

    unlink(tmp_path.c_str());
  }
}
