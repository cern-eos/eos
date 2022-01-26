//------------------------------------------------------------------------------
// File: BalancerEngineUtils.hh
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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
#pragma once

#include <string>
#include <numeric>
#include <random>
#include "mgm/groupbalancer/BalancerEngine.hh"

namespace {
  std::random_device rd;
  std::mt19937 generator(rd());
}

namespace eos::mgm::group_balancer {

inline uint32_t getRandom(uint32_t max) {
  std::uniform_int_distribution<> distribution(0,max);
  return distribution(generator);
}


inline double calculateAvg(const group_size_map& m)
{
  if (!m.size())
    return 0.0;

  return std::accumulate(m.begin(),m.end(),0.0,
                         [](double s,const auto& kv) -> double
                         { return s + kv.second.filled(); })/m.size();
}

template <typename map_type, typename key_type>
double extract_value(const map_type& m, const key_type& k,
                     double default_val = 0.0,
                     std::string* err_str=nullptr)
{
  auto kv = m.find(k);
  if (kv == m.end()) {
    return default_val;
  }

  double result;

  try {
    result = std::stod(kv->second);
  } catch (std::exception& e) {
    if (err_str)
      *err_str = e.what();
    return default_val;
  }
  return result;
}

template <typename... Args>
double extract_percent_value(Args&&... args)
{
  double value = extract_value(std::forward<Args>(args)...);
  return value/100.0;
}

inline bool is_valid_threshold(const std::string& threshold_str)
{
  double d;
  try {
    d = std::stod(threshold_str);
  } catch (std::exception& e) {
    return false;
  }

  return d > 0;
}

template <typename... Args>
inline bool is_valid_threshold(const std::string& threshold_str, Args&&... args)
{
  return is_valid_threshold(threshold_str) && is_valid_threshold(args...);
}

} // eos::mgm::group_balancer
