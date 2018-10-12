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
#include "namespace/ns_quarkdb/QdbContactDetails.hh"

EOSNSNAMESPACE_BEGIN

// Static variables
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
BackendClient::getInstance(const QdbContactDetails& contactDetails,
                           const std::string& tag)
{
  std::ostringstream oss;
  oss << tag << ":";

  for (const auto& elem : contactDetails.members.getEndpoints()) {
    oss << elem.toString() << " ";
  }

  std::string qdb_id = oss.str();
  qdb_id.pop_back();
  qclient::QClient* instance{nullptr};
  std::lock_guard<std::mutex> lock(pMutexMap);

  if (pMapClients.find(qdb_id) == pMapClients.end()) {
    instance = new qclient::QClient(contactDetails.members,
                                    contactDetails.constructOptions());
    pMapClients.insert(std::make_pair(qdb_id, instance));
  } else {
    instance = pMapClients[qdb_id];
  }

  return instance;
}

//------------------------------------------------------------------------------
//! Initialization and finalization
//------------------------------------------------------------------------------
static struct Initializer {
  std::atomic<int> mCounter;

  //----------------------------------------------------------------------------
  //! Constructor will be invoked in every translation unit that includes the
  //! current header file, but the BackendClient will be initialized only once
  //----------------------------------------------------------------------------
  Initializer() noexcept
  {
    if (mCounter++ == 0) {
      eos::BackendClient::Initialize();
    }
  }

  //----------------------------------------------------------------------------
  //! Destructor will be invoked in every translation unit that includes the
  //! current header file, but the BackendClient will be finalized only once
  //----------------------------------------------------------------------------
  ~Initializer()
  {
    if (--mCounter == 0) {
      eos::BackendClient::Finalize();
    }
  }
} finalizer;

EOSNSNAMESPACE_END
