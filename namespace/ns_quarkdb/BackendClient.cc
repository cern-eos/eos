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

#include "namespace/ns_quarkdb/BackendClient.hh"

EOSNSNAMESPACE_BEGIN

// Static variables
std::atomic<qclient::QClient*> BackendClient::sQdbClient(nullptr);
std::string BackendClient::sQdbHost("localhost");
int BackendClient::sQdbPort(7777);
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
// Get client for a particular quarkdb instance specified as a list of cluster
// members
//------------------------------------------------------------------------------
qclient::QClient*
BackendClient::getInstance(const qclient::Members& qdb_members,
                           const std::string& tag)
{
  std::ostringstream oss;
  oss << tag << ":";

  for (const auto& elem : qdb_members.getEndpoints()) {
    oss << elem.toString() << " ";
  }

  std::string qdb_id = oss.str();
  qdb_id.pop_back();
  qclient::QClient* instance{nullptr};
  std::lock_guard<std::mutex> lock(pMutexMap);

  if (pMapClients.find(qdb_id) == pMapClients.end()) {
    qclient::Options opts;
    opts.transparentRedirects = true;
    opts.retryStrategy = qclient::RetryStrategy::WithTimeout(std::chrono::minutes(2));
    instance = new qclient::QClient(qdb_members, std::move(opts));
    pMapClients.insert(std::make_pair(qdb_id, instance));
  } else {
    instance = pMapClients[qdb_id];
  }

  return instance;
}

//------------------------------------------------------------------------------
// Get client for a particular quarkdb instance specified as a list of cluster
// members. The cluster specification is a list of space separated host:port.
//------------------------------------------------------------------------------
qclient::QClient*
BackendClient::getInstance(const std::string& qdb_cluster,
                           const std::string& tag)
{
  qclient::Members qdb_members;

  if (!qdb_members.parse(qdb_cluster)) {
    return nullptr;
  }

  return getInstance(qdb_members, tag);
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
