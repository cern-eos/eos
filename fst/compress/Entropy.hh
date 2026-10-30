//------------------------------------------------------------------------------
// File: Entropy.hh
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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
//! Shannon entropy of a byte histogram, expressed as how much of the file a
//! compressor could hope to remove. 100% is a single repeated byte. 0% is a
//! uniform mix of all 256 byte values. An empty histogram is 0%.
//------------------------------------------------------------------------------

#pragma once

#include <cmath>
#include <cstdint>

namespace eos::fst::compress {

inline unsigned
CompressionPotential(const std::uint64_t counts[256], std::uint64_t total)
{
  if (total == 0) {
    return 0;
  }

  double entropy = 0;

  for (int i = 0; i < 256; ++i) {
    if (counts[i] == 0) {
      continue;
    }

    const double probability = static_cast<double>(counts[i]) /
                               static_cast<double>(total);
    entropy -= probability * std::log2(probability);
  }

  double potential = (1.0 - entropy / 8.0) * 100.0;

  if (potential < 0) {
    potential = 0;
  } else if (potential > 100) {
    potential = 100;
  }

  return static_cast<unsigned>(std::lround(potential));
}

} // namespace eos::fst::compress
