// ----------------------------------------------------------------------
// File: Config.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

/*----------------------------------------------------------------------------*/
#include "fst/Config.hh"
#include <vector>
#include "common/Logging.hh"
#include "common/InstanceName.hh"
#include "common/StringTokenizer.hh"
#include <thread>
#include <chrono>

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
Config Config::gConfig;
/*----------------------------------------------------------------------------*/

XrdOucString Config::getFstNodeConfigQueue(const std::string& location,
    bool blocking)
{
  while (!configQueueInitialized && blocking) {
    eos_static_info("msg=\"waiting for config queue in %s ...\"",
                    location.c_str());
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  std::unique_lock<std::mutex> lock(mConfigQueueMtx);
  return FstNodeConfigQueue;
}

void Config::setFstNodeConfigQueue(const std::string &value)
{
  std::unique_lock<std::mutex> lock(mConfigQueueMtx);
  if(configQueueInitialized) {
    return;
  }

  FstNodeConfigQueue = value.c_str();
  std::vector<std::string> parts =
    common::StringTokenizer::split<std::vector<std::string>>(value.c_str(), '/');
  common::InstanceName::set(parts[1]);
  mNodeHashLocator = common::SharedHashLocator(parts[1],
                     common::SharedHashLocator::Type::kNode, parts[3]);
  configQueueInitialized = true;
}

common::SharedHashLocator Config::getNodeHashLocator(const std::string&
    location,
    bool blocking)
{
  while (!configQueueInitialized && blocking) {
    std::this_thread::sleep_for(std::chrono::seconds(2));
    eos_static_info("Waiting for config queue in %s ... ", location.c_str());
  }

  if (configQueueInitialized) {
    return mNodeHashLocator;
  }

  return {};
}

std::chrono::seconds Config::getPublishInterval()
{
  XrdSysMutexHelper lock(Mutex);
  int localInterval = PublishInterval;
  lock.UnLock();

  if (localInterval < 2 || localInterval > 3600) {
    // Strange value, default to 10
    return std::chrono::seconds(10);
  }

  return std::chrono::seconds(localInterval);
}

std::chrono::milliseconds Config::getRandomizedPublishInterval()
{
  std::chrono::seconds interval = getPublishInterval();
  std::lock_guard<std::mutex> lock(mutex);
  std::uniform_int_distribution<> dist(interval.count() * 500,
                                       interval.count() * 1500);
  return std::chrono::milliseconds(dist(generator));
}

EOSFSTNAMESPACE_END
