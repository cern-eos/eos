//------------------------------------------------------------------------------
//! @file lru-test.cc
//! @author Andreas-Joachim Peters CERN
//! @brief tests for lru functionality in md class
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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
#include "eosfuse.hh"
#include "md/md.hh"
#include <random>

TEST(LRU, BasicSanity)
{
  ASSERT_EQ(system("rm -rf /tmp/eos-fusex-tests"), 0);
  metad::pmap tmap;
  std::map<uint64_t, metad::shared_md> lut;
  std::map<uint64_t, bool> blut;
  std::mt19937 gen(0);
  std::uniform_int_distribution<> distrib(1, 1000);
  std::uniform_int_distribution<> op(1, 3);

  for (auto i = 1; i <= 1000; i++) {
    metad::shared_md md = std::make_shared<metad::mdx>(i);
    lut[i] = md;
    tmap[i] = md;
    tmap.lru_add(i, md);
    blut[i] = true;

    if (i == 1) {
      ASSERT_EQ(tmap.lru_newest(), 0);
      ASSERT_EQ(tmap.lru_oldest(), 0);
    } else {
      ASSERT_EQ(tmap.lru_newest(), i);
      ASSERT_EQ(tmap.lru_oldest(), 2);
    }
  }

  for (auto i = 1; i < 10000000; i++) {
    auto k = distrib(gen);
    auto kop = op(gen);

    if (k == 1) {
      continue;
    }

    switch (kop) {
    case 1:
      if (tmap.count(k) && tmap[k]) {
        //  std::cout << "LRU-UPDATE: " << k << std::endl;
        tmap.lru_update(k, lut[k]);
        ASSERT_EQ(tmap.lru_newest(), k);
      }

      break;

    case 2:
      if (!tmap.count(k) || !tmap[k]) {
        //  std::cout << "LRU-ADD: " << k << std::endl;
        tmap[k] = lut[k];
        tmap.lru_add(k, lut[k]);
        ASSERT_EQ(tmap.lru_newest(), k);
      }

      blut[k] = true;
      break;

    case 3:
      if (tmap.count(k) || !tmap[k]) {
        //  std::cout << "LRU-REMOVE: " << k << std::endl;
        tmap.lru_remove(k);
        tmap[k] = 0;
      }

      blut[k] = false;
      break;

    default:
      break;
    }

    //    std::cout << "oldest: " << tmap.lru_oldest() << " " << blut[tmap.lru_oldest()] << " op:" << kop << " ino:" << k <<std::endl;
    ASSERT_EQ(tmap[tmap.lru_oldest()]->lru_prev(), 0);
    ASSERT_EQ(blut[tmap.lru_oldest()], true);
    //    std::cout << k << " : " << kop << " blut: " << blut[k] << std::endl;
  }

  auto cnt = 0;

  for (auto i = 1; i <= 1000; ++i) {
    if (blut[i]) {
      cnt++;
    }
  }

  auto old = tmap.lru_oldest();

  for (auto i = 2; i <= 1000; ++i) {
    if (!blut[i]) {
      ASSERT_EQ(tmap[i], nullptr);
    }
  }

  // check sanity of the left-over LRU list
  auto c = 1;

  for (c = 1; c <= 1000; c++) {
    auto n = tmap[old]->lru_next();

    if (!n) {
      break;
    }

    ASSERT_EQ(tmap[n]->lru_prev(), old);
    old = n;
  }

  ASSERT_EQ(c + 1, cnt);
}
