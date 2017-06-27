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

//------------------------------------------------------------------------------
// author: Branko Blagojević <branko.blagojevic@comtrade.com>
// desc:   ZSTD dictionary training utility
//------------------------------------------------------------------------------

#include <iostream>
#include <string>
#include "namespace/utils/DataHelper.hh"
#include "namespace/ns_in_memory/persistency/TrainDictionary.hh"

//------------------------------------------------------------------------------
// Here we go
//------------------------------------------------------------------------------
int main(int argc, char** argv)
{
  //----------------------------------------------------------------------------
  // Check the commandline parameters
  //----------------------------------------------------------------------------
  if (argc != 3) {
    std::cerr << "Usage:" << std::endl;
    std::cerr << "  " << argv[0] << " log_file output_file";
    std::cerr << std::endl;
    return 1;
  }

  //----------------------------------------------------------------------------
  // Train ZSTD dictionary
  //----------------------------------------------------------------------------
  try {
    eos::TrainDictionary::train(std::string(argv[1]), std::string(argv[2]));
    eos::DataHelper::copyOwnership(std::string(argv[2]), std::string(argv[1]));
  } catch (eos::MDException& e) {
    std::cerr << std::endl;
    std::cerr << "Error: " << e.what() << std::endl;
    return 2;
  }

  return 0;
}
