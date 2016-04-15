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

#include "namespace/ns_on_redis/RedisClient.hh"

EOSNSNAMESPACE_BEGIN

// Static variables
std::atomic<redox::Redox*> RedisClient::sRedoxClient {nullptr};
std::string RedisClient::sRedisHost {"localhost"};
int RedisClient::sRedisPort {6382};
std::map<std::string, redox::Redox*> RedisClient::pMapClients;
std::mutex RedisClient::pMutexMap;

//------------------------------------------------------------------------------
// Get instance
//------------------------------------------------------------------------------
redox::Redox*
RedisClient::getInstance(const std::string& host, uint32_t port)
{
  bool is_default {false};
  std::string host_tmp {host};
  redox::Redox* instance {nullptr};

  if (host_tmp.empty() || !port)
  {
    // Try to be as efficient as possible in the default case
    instance = sRedoxClient.load();

    if (instance)
      return instance;

    host_tmp = sRedisHost;
    port = sRedisPort;
    is_default = true;
  }

  std::string redis_id = host_tmp + ":" + std::to_string(port);
  std::lock_guard<std::mutex> lock(pMutexMap);

  if (pMapClients.find(redis_id) == pMapClients.end())
  {
    instance = new redox::Redox();
    instance->logger_.level(redox::log::Error);

    // TODO: consider enabling the noWait option which keeps one CPU at 100%
    // but improves the performance of the event loop

    try
    {
      instance->connect(host_tmp, port);
    }
    catch (...)
    {
      std::cerr << "ERROR: Failed to connect to Redis instance" << std::endl;
      throw;
    }

    pMapClients.insert(std::make_pair(redis_id, instance));

    if (is_default)
    {
      sRedoxClient.store(instance);
    }
  }
  else
  {
    instance = pMapClients[redis_id];
  }

  return instance;
}

EOSNSNAMESPACE_END
