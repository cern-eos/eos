//------------------------------------------------------------------------------
// File: XrdConnPool.cc
// Author: Elvin-Alin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "common/XrdConnPool.hh"
#include <limits>
#include <sstream>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdConnPool::XrdConnPool(bool is_enabled, uint32_t max_size):
  mIsEnabled(is_enabled), mMaxSize(max_size)
{
  if (!mIsEnabled && getenv("EOS_XRD_USE_CONNECTION_POOL")) {
    mIsEnabled = true;

    if (getenv("EOS_XRD_CONNECTION_POOL_SIZE")) {
      max_size = strtoul(getenv("EOS_XRD_CONNECTION_POOL_SIZE"), 0, 10);
    }

    if (max_size < 1) {
      eos_warning("%s", "msg=\"wrong EOS_XRD_CONNECTION_POOL_SIZE, forcing "
                  "max size to 1\"");
      max_size = 1;
    }

    if (max_size > 1024) {
      eos_warning("%s", "msg=\"too big EOS_XRD_CONNECTION_POOL_SIZE, forcing "
                  "max size to 1024\"");
      max_size = 1024;
    }

    mMaxSize = max_size;
  }
}

//------------------------------------------------------------------------------
// Assign new connection from the pool to the given URL. What this actually
// means is updating the username used in the URL when connecting to the
// XRootD server.
//------------------------------------------------------------------------------
uint32_t
XrdConnPool::AssignConnection(XrdCl::URL& url)
{
  uint32_t conn_id {0ull};

  if (!mIsEnabled) {
    return conn_id;
  }

  bool found {false};
  uint32_t best_conn_id {1};
  uint32_t best_conn_val {std::numeric_limits<uint32_t>::max()};
  std::string target_host = url.GetHostName();
  std::unique_lock<std::mutex> scope_lock(mPoolMutex);
  // Map of connection id and score for current host
  auto& map_id_score = mConnPool[target_host];

  for (auto& elem : map_id_score) {
    if (elem.second < best_conn_val) {
      best_conn_id = elem.first;
      best_conn_val = elem.second;
    }

    if (elem.second == 0) {
      ++elem.second;
      conn_id = elem.first;
      found = true;
      break;
    }
  }

  if (!found) {
    if (map_id_score.size() >= mMaxSize) {
      // Share the least busy connection
      ++map_id_score[best_conn_id];
      conn_id = best_conn_id;
      eos_static_debug("msg=\"connection pool limit reached - using %u/%u "
                       "connections\"", map_id_score.size(), mMaxSize);
    } else {
      conn_id = map_id_score.size() + 1;
      map_id_score[conn_id] = 1;
    }
  }

  if (conn_id) {
    url.SetUserName(std::to_string(conn_id));
  }

  return conn_id;
}

//------------------------------------------------------------------------------
// Release a connection and update the status of the pool
//------------------------------------------------------------------------------
void
XrdConnPool::ReleaseConnection(const XrdCl::URL& url)
{
  if (!mIsEnabled) {
    return;
  }

  uint32_t conn_id {0ull};

  try {
    conn_id = std::stoul(url.GetUserName());
  } catch (const std::exception& e) {
    // ignore
  }

  if (conn_id) {
    std::unique_lock<std::mutex> scope_lock(mPoolMutex);
    auto it = mConnPool.find(url.GetHostName());

    if (it != mConnPool.end()) {
      auto& map_id_score = it->second;

      if (map_id_score[conn_id] >= 1) {
        --map_id_score[conn_id];
      }
    }
  }
}

//------------------------------------------------------------------------------
// Dump the status of the connection pool to the given string
//------------------------------------------------------------------------------
void
XrdConnPool::Dump(std::string& out) const
{
  std::ostringstream oss;
  oss << "[connection-pool-dump]" << std::endl;

  for (auto it = mConnPool.begin(); it != mConnPool.end(); ++it) {
    for (auto fit = it->second.begin(); fit != it->second.end(); ++fit) {
      oss << "[connection-pool] host=" << it->first << " id="
          << fit->first << " usage=" << fit->second << std::endl;
    }
  }

  out = oss.str();
}

EOSCOMMONNAMESPACE_END
