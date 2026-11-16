//------------------------------------------------------------------------------
//! @file QdrantEngine.hh
//! @brief Index documents into an external Qdrant endpoint and search them
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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
#include "common/AssistedThread.hh"
#include "common/Mapping.hh"
#include "common/VirtualIdentity.hh"
#include "common/qdrant/QdrantClient.hh"
#include "mgm/Namespace.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace qclient {
class QClient;
}

namespace eos::common {
class ProtocolHandler;
}

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! MGM-side client of one external Qdrant endpoint.
//!
//! The FST indexers embed and upsert documents. This engine publishes the
//! Qdrant settings they read, deletes points for removed files, and answers
//! search. Search asks Qdrant for neighbors and keeps a hit only after the
//! caller's VirtualIdentity passes the namespace ACL check.
//------------------------------------------------------------------------------
class QdrantEngine {
public:
  struct Config {
    std::string endpoint;
    std::string apiKey;
    std::string collection = "eos";
    std::string embedEndpoint;
    std::string embedModel = "text-embedding";
    int overfetch = 5;
    uint64_t maxBytes = 32ull * 1024ull * 1024ull;
  };

  struct SearchResult {
    int retc = 0;
    std::string error;
    std::string text;
    std::string json;
  };

  QdrantEngine(Config config, const eos::QdbContactDetails& qdb);
  ~QdrantEngine();

  void Start();
  void Stop();

  //! Remember a file id whose points should be removed. Safe to call while
  //! the caller holds a namespace lock: it only queues the id.
  void NoteFileRemoved(uint64_t fid);

  bool HasEndpoint() const { return !mConfig.endpoint.empty(); }
  bool HasEmbedder() const { return !mConfig.embedEndpoint.empty(); }
  uint64_t MaxBytes() const { return mConfig.maxBytes; }

  //----------------------------------------------------------------------------
  //! Embed `query`, search Qdrant, and drop every hit the caller cannot read.
  //----------------------------------------------------------------------------
  SearchResult Search(const eos::common::VirtualIdentity& vid,
                      const std::string& query, uint32_t limit, bool asJson);

private:
  void Run(ThreadAssistant& assistant);
  bool IndexingSwitchOn() const;
  void PublishEnabled(bool enabled);
  void PersistDeletions();
  void DrainTombstones();

  Config mConfig;
  eos::common::QdrantClient mClient;
  eos::QdbContactDetails mQdb;
  std::shared_ptr<qclient::QClient> mQcl;
  AssistedThread mThread;
  std::mutex mDeleteMutex;
  std::vector<uint64_t> mDeletes;
};

//------------------------------------------------------------------------------
//! Protocol handler for POST /v1/search. The MGM HTTP front authenticates the
//! caller before constructing it.
//------------------------------------------------------------------------------
std::unique_ptr<eos::common::ProtocolHandler>
CreateQdrantSearchHandler(eos::common::VirtualIdentity* vid);

EOSMGMNAMESPACE_END
