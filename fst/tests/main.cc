//------------------------------------------------------------------------------
// File: main.cc
// Author: Mihai Patrascoiu <mihai.patrascoiu@cern.ch>
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
#include "FstTestsUtils.hh"

std::string eos::fst::test::FstTestsEnv::instanceName = "dev";
bool eos::fst::test::FstTestsEnv::verbose = false;

//------------------------------------------------------------------------------
// Main function for the eos-fst-test executable.
// Performs write and read operations of a file on a plain and raiddp setup.
// Also tests the partition monitoring functionality.
//
// Note: a running EOS instance is required for this test to run successfully
//
// For setup, please read the info written in eos::fst::tests::TestEnv
//------------------------------------------------------------------------------
int main(int argc, char* argv[]) {
  using namespace eos::fst::test;

  int c;
  bool verbose = false;
  std::string instance("dev");

  std::string usage = "Usage: eos-fst-test [-v] [-h] [-n <instance>]\
                           \nTests the writing and downloading of a file on a plain and raiddp setup.\
                           \nTests the partition monitoring functionality.\
                           \nNote: a running EOS instance is required for this test to run successfully\
                           \n\t\t            -v : verbose mode\
                           \n\t\t            -h : display help\
                           \n\t\t -n <instance> : the EOS instance name (default is dev)\
                           \n";

  // Parse remaining arguments
  while ((c = getopt(argc, argv, "n:vh")) != -1) {
    switch (c) {
      case 'n': { // Register instance name
        instance = optarg;
        break;
      }

      case 'v': { // Enable verbose mode
        verbose = true;
        break;
      }

      case 'h': { // Display help text
        std::cout << usage << std::endl;
        exit(1);
      }

      case ':': {
        std::cout << usage << std::endl;
        exit(1);
      }
    }
  }

  // Trim starting and trailing '/'
  instance.erase(instance.find_last_not_of("/") + 1);
  instance.erase(0, instance.find_first_not_of("/"));

  if (instance.empty()) {
    std::cerr << "Invalid instance name!" << std::endl;
    exit(1);
  }

  // Prepare global environment
  FstTestsEnv::instanceName = instance;
  FstTestsEnv::verbose = verbose;

  // Initialize GTest
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
