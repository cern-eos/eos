// ----------------------------------------------------------------------
// File: Statistics.hh
// Author: Andreas Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                 *
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

#ifndef EOSCOMMON_STATISTICS_HH
#define EOSCOMMON_STATISTICS_HH

#include "common/Namespace.hh"
#include <set>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Base Statistics Functions
//------------------------------------------------------------------------------
class Statistics
{
public:
  static double max(std::multiset<float>& s)
  {
    double lmax = 0;

    for (auto it : s) {
      if (it > lmax) {
        lmax = it;
      }
    }

    return lmax;
  }

  static double avg(std::multiset<float>& s)
  {
    double sum = 0;

    if (!s.size()) {
      return 0;
    }

    for (auto it : s) {
      sum += it;
    }

    return sum / s.size();
  }

  static double sig(std::multiset<float>& s)
  {
    double average = avg(s);
    double sum = 0;

    for (auto it : s) {
      sum += (it - average) * (it - average);
    }

    return sqrt(sum / s.size());
  }

  static double nperc(std::multiset<float>& s, double perc = 99.0)
  {
    size_t n = s.size();
    size_t n_perc = (size_t)(n * perc / 100.0);
    size_t i = 0;
    double nperc = 0;

    for (auto it : s) {
      i++;

      if (i >= n_perc) {
        nperc = it;
        break;
      }
    }

    return nperc;
  }
};

EOSCOMMONNAMESPACE_END

#endif
