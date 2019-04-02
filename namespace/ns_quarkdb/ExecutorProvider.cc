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

#include "namespace/ns_quarkdb/ExecutorProvider.hh"

#include <folly/executors/IOThreadPoolExecutor.h>


EOSNSNAMESPACE_BEGIN

// Static variables
std::map<std::string, std::unique_ptr<folly::Executor>> ExecutorProvider::executorMap;
std::mutex ExecutorProvider::mtx;

//------------------------------------------------------------------------------
// Get an executor object based on the given tag - no ownership
//------------------------------------------------------------------------------
folly::Executor* ExecutorProvider::getIOThreadPool(const std::string &tag) {
  std::lock_guard<std::mutex> lock(mtx);

  auto it = executorMap.find(tag);
  if(it != executorMap.end()) {
    return it->second.get();
  }

  folly::Executor *pool = new folly::IOThreadPoolExecutor(32);
  executorMap[tag] = std::unique_ptr<folly::Executor>(pool);
  return pool;
}

EOSNSNAMESPACE_END
