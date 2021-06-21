//------------------------------------------------------------------------------
// File: concurrent_map.hh
// Author: Abhishek Lekshmanan <abhishek.lekshmanan@cern.ch>
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
#pragma once

#include "concurrent_map_adapter.hh"

// Aliases to make declaring concurrent_map_adapter types a bit more easier

#include <unordered_map>
#include <google/dense_hash_map>

namespace eos::common {


// A simple adapter using std::mutex and fair locking
template <class K, class V, class Mtx=std::mutex, class... Args>
using std_concurrent_map = concurrent_map_adapter<std::unordered_map<K,V,Args...>, Mtx>;

template <class K, class V, class Mtx=std::mutex, class... Args>
using dense_concurrent_map = concurrent_map_adapter<google::dense_hash_map<K,V,Args...>, Mtx>;

}
