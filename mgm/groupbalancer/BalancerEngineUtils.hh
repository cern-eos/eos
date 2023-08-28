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
#include <functional>
#include "common/StringUtils.hh"
#include "common/StringSplit.hh"
#include "mgm/groupbalancer/BalancerEngine.hh"

namespace
{
std::random_device rd;
std::mt19937 generator(rd());
}

namespace eos::mgm::group_balancer
{

namespace detail
{
// uses CTAD
template <typename Fn>
using ret_type_t = typename decltype(std::function{std::declval<Fn>()})::result_type;
} // detail

inline uint32_t getRandom(uint32_t max)
{
  std::uniform_int_distribution<> distribution(0, max);
  return distribution(generator);
}


inline double calculateAvg(const group_size_map& m)
{
  if (!m.size()) {
    return 0.0;
  }

  return std::accumulate(m.begin(), m.end(), 0.0,
                         [](double s, const auto & kv) -> double
  { return s + kv.second.filled(); }) / m.size();
}


template <typename map_type, typename key_type, typename Fn,
          typename value_type = detail::ret_type_t<Fn>>
value_type
extract_value(const map_type& m, const key_type& k,
              Fn extractor_fn)
{
  auto kv = m.find(k);
  if (kv != m.end()) {
    return extractor_fn(kv->second);
  }

  return extractor_fn("");
}

template <typename map_type, typename key_type>
double extract_double_value(const map_type& m, const key_type& k,
                            double default_val = 0.0,
                            std::string* err_str = nullptr)
{
  auto double_extractor = [&default_val, err_str](const std::string& str) {
    double value;
    common::StringToNumeric(str, value, default_val, err_str);
    return value;
  };

  return extract_value(m, k, double_extractor);
}

template <typename... Args>
double extract_percent_value(Args&& ... args)
{
  double value = extract_double_value(std::forward<Args>(args)...);
  return value / 100.0;
}

template <typename map_type, typename key_type>
std::unordered_set<std::string>
extract_commalist_value(const map_type& m, const key_type& k)
{
  using namespace std::string_view_literals;
  auto cl_extractor_fn = [](std::string_view value) {
    return common::StringSplit<std::unordered_set<std::string>>(value, ", ");
  };
  return extract_value(m, k, cl_extractor_fn);
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
inline bool is_valid_threshold(const std::string& threshold_str,
                               Args&& ... args)
{
  return is_valid_threshold(threshold_str) && is_valid_threshold(args...);
}

} // eos::mgm::group_balancer
