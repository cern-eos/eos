//------------------------------------------------------------------------------
//! @file EosLayoutPrint.cc
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Tool print in human readable format the layout information
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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

#include "common/LayoutId.hh"
#include <ostream>

int main(int argc, char* argv[])
{
  using eos::common::LayoutId;

  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <lid_value>" << std::endl;
    return -1;
  }

  uint64_t lid {0ull};
  std::string slid = argv[1];

  try {
    lid = std::stoull(slid, 0, 16);
  } catch (...) {
    std::cerr << "error: failed to convert give layout id" << std::endl;
    return -1;
  }

  std::cout << "Layout type:        " << LayoutId::GetLayoutTypeString(
              lid) << std::endl
            << "Checksum type:      " << LayoutId::GetChecksumString(LayoutId::GetChecksum(
                  lid)) << std::endl
            << "Block checksum:     " << LayoutId::GetBlockChecksumString(lid) << std::endl
            << "Block size:         " << LayoutId::GetBlockSizeString(lid) << std::endl
            << "Total stripes:      " << LayoutId::GetStripeNumberString(lid) << std::endl
            << "Redundancy stripes: " << LayoutId::GetRedundancyStripeNumber(lid)
            << std::endl;
  return 0;
}
