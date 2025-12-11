//------------------------------------------------------------------------------
// File: LocalHash.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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

#include "common/mq/LocalHash.hh"
#include "qclient/shared/UpdateBatch.hh"

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
LocalHash::LocalHash(const std::string& key):
  qclient::SharedHash(nullptr, key),
  mKey(key)
{
  // empty
}

//------------------------------------------------------------------------------
// Set value
//------------------------------------------------------------------------------
std::future<qclient::redisReplyPtr>
LocalHash::set(const qclient::UpdateBatch& batch)
{
  std::promise<qclient::redisReplyPtr> promise;
  auto future = promise.get_future();
  promise.set_value(qclient::redisReplyPtr());
  std::unique_lock<std::mutex> lock(mMutex);

  for (const auto& batch_map : {
         batch.getLocal(),
         batch.getTransient(),
         batch.getPersistent()
       }) {
    for (auto it = batch_map.cbegin(); it != batch_map.cend(); ++it) {
      // This is actually a delete key
      if (it->second.empty()) {
        auto it_main = mMap.find(it->first);

        if (it_main != mMap.end()) {
          mMap.erase(it_main);
        }
      } else {
        // This is a real set key=val
        mMap[it->first] = it->second;
      }
    }
  }
  return future;
}

//------------------------------------------------------------------------------
// Get value
//------------------------------------------------------------------------------
bool
LocalHash::get(const std::string& key, std::string& value) const
{
  std::unique_lock<std::mutex> lock(mMutex);
  auto it = mMap.find(key);

  if (it != mMap.end()) {
    value = it->second;
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Get a list of values, returns a map of kv pairs of found values, expects
// empty map as the out param, returns true if all the values have been found
//------------------------------------------------------------------------------
bool
LocalHash::get(const std::vector<std::string>& keys,
               std::map<std::string, std::string>& out) const
{
  if (!out.empty()) {
    return false;
  }

  std::unique_lock<std::mutex> lock(mMutex);

  for (const auto& key : keys) {
    auto it = mMap.find(key);

    if (it != mMap.end()) {
      out.emplace(it->first, it->second);
    }
  }

  return (keys.size() == out.size());
}

//------------------------------------------------------------------------------
// Get the set of keys in the current hash
//------------------------------------------------------------------------------
std::vector<std::string>
LocalHash::getKeys() const
{
  std::vector<std::string> keys;
  std::unique_lock<std::mutex> lock(mMutex);

  for (const auto& elem : mMap) {
    keys.push_back(elem.first);
  }

  return keys;
}

//------------------------------------------------------------------------------
// Get contents of the hash
//------------------------------------------------------------------------------
std::map<std::string, std::string>
LocalHash::getContents() const
{
  std::unique_lock<std::mutex> lock(mMutex);
  return mMap;
}

EOSMQNAMESPACE_END
