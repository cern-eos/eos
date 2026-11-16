//------------------------------------------------------------------------------
//! @file QdrantClient.hh
//! @brief HTTP client for one external Qdrant collection and its embedder
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
#include "common/Namespace.hh"
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Talks to one Qdrant collection and one OpenAI-compatible embedder.
//! The MGM uses it for search and deletion. Each FST indexer uses its own
//! instance to embed and upsert the replicas it scanned.
//------------------------------------------------------------------------------
class QdrantClient {
public:
  struct Settings {
    std::string endpoint;
    std::string apiKey;
    std::string collection = "eos";
    std::string embedEndpoint;
    std::string embedModel = "text-embedding";
  };

  QdrantClient() = default;

  void Update(Settings settings);

  std::vector<float> Embed(const std::string& text, std::string& error);
  bool DeleteFile(uint64_t fid, std::string& error);
  bool Upsert(uint64_t fid, const std::string& checksum,
              const std::vector<std::string>& chunks,
              const std::vector<std::vector<float>>& vectors, std::string& error);
  bool Search(const std::vector<float>& vector, int limit, long& status,
              std::string& body, std::string& error);

private:
  Settings CopySettings();
  bool EnsureCollection(const Settings& settings, const std::vector<float>& probe,
                        std::string& error);

  std::mutex mMutex;
  Settings mSettings;
  bool mReady = false;
  int mDim = 0;
};

EOSCOMMONNAMESPACE_END
