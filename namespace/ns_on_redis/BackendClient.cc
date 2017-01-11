/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "namespace/ns_on_redis/BackendClient.hh"

EOSNSNAMESPACE_BEGIN

// Static variables
std::atomic<qclient::QClient*> BackendClient::sQdbClient(nullptr);
std::string BackendClient::sQdbHost("localhost");
int BackendClient::sQdbPort(6382);
std::map<std::string, qclient::QClient*> BackendClient::pMapClients;
std::mutex BackendClient::pMutexMap;

//------------------------------------------------------------------------------
// Initialize
//------------------------------------------------------------------------------
void
BackendClient::Initialize() noexcept
{
  // empty
}

//------------------------------------------------------------------------------
// Finalize
//------------------------------------------------------------------------------
void
BackendClient::Finalize()
{
  std::lock_guard<std::mutex> lock(pMutexMap);

  for (auto& elem : pMapClients) {
    delete elem.second;
  }

  pMapClients.clear();
}

//------------------------------------------------------------------------------
// Get instance
//------------------------------------------------------------------------------
qclient::QClient*
BackendClient::getInstance(const std::string& host, uint32_t port)
{
  bool is_default{false};
  std::string host_tmp{host};
  qclient::QClient* instance{nullptr};

  if (host_tmp.empty() || (port == 0u)) {
    // Try to be as efficient as possible in the default case
    instance = sQdbClient.load();

    if (instance != nullptr) {
      return instance;
    }

    host_tmp = sQdbHost;
    port = sQdbPort;
    is_default = true;
  }

  std::string qdb_id = host_tmp + ":" + std::to_string(port);
  std::lock_guard<std::mutex> lock(pMutexMap);

  if (pMapClients.find(qdb_id) == pMapClients.end()) {
    instance = new qclient::QClient(host_tmp, port);
    pMapClients.insert(std::make_pair(qdb_id, instance));

    if (is_default) {
      sQdbClient.store(instance);
    }
  } else {
    instance = pMapClients[qdb_id];
  }

  return instance;
}

//------------------------------------------------------------------------------
// Initialization and finalization
//------------------------------------------------------------------------------
namespace
{
struct RedisInitializer {
  // Initializer
  RedisInitializer() noexcept
  {
    BackendClient::Initialize();
  }

  // Finalizer
  ~RedisInitializer()
  {
    BackendClient::Finalize();
  }
} finalizer;
} // namespace

EOSNSNAMESPACE_END
