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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Main.cc, test initialization
//------------------------------------------------------------------------------

#include <gtest/gtest.h>
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"

int main(int argc, char **argv) {
  const std::string testpath("/tmp/eos-ns-tests/");

  std::string rmrf = "rm -rf " + testpath;
  system(rmrf.c_str());
  mkdir(testpath.c_str(), 0755);
  eos::MetadataFlusherFactory::setQueuePath(testpath);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
