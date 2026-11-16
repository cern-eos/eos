//------------------------------------------------------------------------------
//! @file QdrantIndexer.hh
//! @brief FST-local document indexer, fed by the disk scan
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
#include "common/FileSystem.hh"
#include "fst/Namespace.hh"
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace qclient {
class QClient;
}

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Re-reads replicas queued by a finished write or by the disk scan, and
//! upserts their chunks to Qdrant. One worker runs per FST.
//------------------------------------------------------------------------------
class QdrantIndexer {
public:
  explicit QdrantIndexer(std::shared_ptr<qclient::QClient> qcl);
  ~QdrantIndexer();

  void Start();
  void Stop();

  //! Queue a local replica. Returns immediately. A full queue leaves a scan
  //! job for a later scan. An immediate job, from a finished write, is placed
  //! first and replaces a queued scan job when the queue is full.
  bool Enqueue(uint64_t fid, eos::common::FileSystem::fsid_t fsid,
               std::string path, std::string lfn, std::string checksum,
               uint64_t size, bool immediate = false);

private:
  struct Job {
    uint64_t fid = 0;
    eos::common::FileSystem::fsid_t fsid = 0;
    std::string path;
    std::string lfn;
    std::string checksum;
    uint64_t size = 0;
    int retries = 0;
    int64_t notBefore = 0;
  };

  void Run(ThreadAssistant& assistant);
  bool Refresh();
  bool Claim(uint64_t fid, const std::string& checksum);
  void ReleaseClaim(uint64_t fid);
  bool ReadPayload(const std::string& path, uint64_t maxBytes, std::string& data,
                   std::string& error);

  std::shared_ptr<qclient::QClient> mQcl;
  AssistedThread mThread;
  std::mutex mMutex;
  std::vector<Job> mQueue;
  bool mEnabled = false;
  uint64_t mMaxBytes = 32ull * 1024ull * 1024ull;
  std::string mEndpoint;
  std::string mApiKey;
  std::string mCollection;
  std::string mEmbedEndpoint;
  std::string mEmbedModel;
};

EOSFSTNAMESPACE_END
