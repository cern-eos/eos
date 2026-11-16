//------------------------------------------------------------------------------
//! @file QdrantIndexer.cc
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

#include "fst/qdrant/QdrantIndexer.hh"
#include "common/Logging.hh"
#include "common/qdrant/DocumentIndex.hh"
#include "common/qdrant/QdrantClient.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/io/local/FsIo.hh"
#include "fst/layout/HeaderCRC.hh"
#include "qclient/Reply.hh"
#include "qclient/structures/QHash.hh"
#include <XrdSfs/XrdSfsInterface.hh>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <ctime>

EOSFSTNAMESPACE_BEGIN

namespace {

std::string
RedisString(const qclient::redisReplyPtr& reply)
{
  if (!reply || reply->type != REDIS_REPLY_STRING || reply->str == nullptr) {
    return {};
  }

  return std::string(reply->str, reply->len);
}

}  // namespace

QdrantIndexer::QdrantIndexer(std::shared_ptr<qclient::QClient> qcl)
  : mQcl(std::move(qcl))
{
}

QdrantIndexer::~QdrantIndexer()
{
  Stop();
}

void
QdrantIndexer::Start()
{
  Stop();
  mThread.reset(&QdrantIndexer::Run, this);
}

void
QdrantIndexer::Stop()
{
  mThread.join();
}

bool
QdrantIndexer::Enqueue(uint64_t fid, eos::common::FileSystem::fsid_t fsid,
                       std::string path, std::string lfn, std::string checksum,
                       uint64_t size, bool immediate)
{
  if (fid == 0 || path.empty() || lfn.empty() || checksum.empty() || size == 0) {
    return false;
  }

  const std::lock_guard<std::mutex> lock(mMutex);

  // A write close must not queue while indexing is off. The scan has its own
  // gate and remains the backstop once indexing is turned on.
  if (immediate && !mEnabled) {
    return false;
  }

  for (auto it = mQueue.begin(); it != mQueue.end(); ++it) {
    if (it->fid != fid) {
      continue;
    }

    if (it->checksum == checksum) {
      if (!immediate || it == mQueue.begin()) {
        return false;
      }

      Job existing = std::move(*it);
      mQueue.erase(it);
      mQueue.insert(mQueue.begin(), std::move(existing));
      return true;
    }

    mQueue.erase(it);
    break;
  }

  if (mQueue.size() >= 128) {
    if (!immediate) {
      return false;
    }

    mQueue.pop_back();
  }

  Job job;
  job.fid = fid;
  job.fsid = fsid;
  job.path = std::move(path);
  job.lfn = std::move(lfn);
  job.checksum = std::move(checksum);
  job.size = size;

  if (immediate) {
    mQueue.insert(mQueue.begin(), std::move(job));
  } else {
    mQueue.push_back(std::move(job));
  }

  return true;
}

bool
QdrantIndexer::Refresh()
{
  if (mQcl == nullptr) {
    const std::lock_guard<std::mutex> lock(mMutex);
    mEnabled = false;
    return false;
  }

  try {
    const bool enabled =
      RedisString(mQcl->exec("GET", eos::common::kQdrantEnabledKey).get()) == "1";
    {
      const std::lock_guard<std::mutex> lock(mMutex);
      mEnabled = enabled;
    }
    const std::string bytes = RedisString(mQcl->exec("GET", eos::common::kQdrantMaxBytesKey).get());

    if (!bytes.empty()) {
      char* end = nullptr;
      errno = 0;
      const auto parsed = std::strtoull(bytes.c_str(), &end, 10);

      if (errno == 0 && end != bytes.c_str() && parsed > 0) {
        mMaxBytes = parsed;
      }
    }

    mEndpoint = RedisString(mQcl->exec("GET", eos::common::kQdrantEndpointKey).get());
    mApiKey = RedisString(mQcl->exec("GET", eos::common::kQdrantApiKeyKey).get());
    mCollection = RedisString(mQcl->exec("GET", eos::common::kQdrantCollectionKey).get());
    mEmbedEndpoint = RedisString(mQcl->exec("GET", eos::common::kQdrantEmbedEndpointKey).get());
    mEmbedModel = RedisString(mQcl->exec("GET", eos::common::kQdrantEmbedModelKey).get());
  } catch (const std::exception& e) {
    eos_static_debug("msg=\"qdrant indexer config lookup failed\" emsg=\"%s\"", e.what());
    const std::lock_guard<std::mutex> lock(mMutex);
    mEnabled = false;
    return false;
  }

  const std::lock_guard<std::mutex> lock(mMutex);
  return mEnabled && !mEndpoint.empty() && !mEmbedEndpoint.empty();
}

bool
QdrantIndexer::Claim(uint64_t fid, const std::string& checksum)
{
  if (mQcl == nullptr) {
    return false;
  }

  qclient::QHash claims(*mQcl, eos::common::kQdrantClaimKey);
  const std::string field = std::to_string(fid);
  const int64_t now = static_cast<int64_t>(time(nullptr));
  const std::string mine = checksum + "\t" + std::to_string(now + 900);

  if (claims.hsetnx(field, mine)) {
    return true;
  }

  const std::string current = claims.hget(field);
  const auto tab = current.find('\t');
  const std::string held = tab == std::string::npos ? current : current.substr(0, tab);
  int64_t expiry = 0;

  if (tab != std::string::npos) {
    expiry = std::atoll(current.c_str() + tab + 1);
  }

  if (held == checksum && expiry > now) {
    return false;
  }

  claims.hdel(field);
  return claims.hsetnx(field, mine);
}

void
QdrantIndexer::ReleaseClaim(uint64_t fid)
{
  if (mQcl == nullptr) {
    return;
  }

  qclient::QHash claims(*mQcl, eos::common::kQdrantClaimKey);
  claims.hdel(std::to_string(fid));
}

bool
QdrantIndexer::ReadPayload(const std::string& path, uint64_t maxBytes,
                           std::string& data, std::string& error)
{
  eos::fst::FsIo file(path);

  if (file.fileOpen(SFS_O_RDONLY)) {
    error = "failed to open local replica";
    return false;
  }

  eos::fst::HeaderCRC header(0, 0);
  const bool rain = header.ReadFromFile(&file, 0) && header.IsValid();
  uint64_t logical = 0;
  int headerSize = 0;

  if (rain) {
    logical = static_cast<uint64_t>(header.GetSizeFile());
    headerSize = header.GetSize();
  } else {
    struct stat info;

    if (file.fileStat(&info) || info.st_size <= 0) {
      error = "replica payload is empty or over the size cap";
      file.fileClose();
      return false;
    }

    logical = static_cast<uint64_t>(info.st_size);
  }

  if (logical == 0 || logical > maxBytes) {
    error = "replica payload is empty or over the size cap";
    file.fileClose();
    return false;
  }

  data.assign(static_cast<size_t>(logical), '\0');
  uint64_t offset = 0;

  while (offset < logical) {
    const uint32_t chunk = static_cast<uint32_t>(
        std::min<uint64_t>(1024ull * 1024ull, logical - offset));
    const int64_t nread = file.fileRead(static_cast<XrdSfsFileOffset>(headerSize + offset),
                                        &data[offset], static_cast<XrdSfsXferSize>(chunk));

    if (nread <= 0) {
      error = "short read of local replica";
      file.fileClose();
      return false;
    }

    offset += static_cast<uint64_t>(nread);
  }

  file.fileClose();
  return true;
}

void
QdrantIndexer::Run(ThreadAssistant& assistant)
{
  eos_static_info("%s", "msg=\"qdrant indexer started\"");
  auto refreshed = std::chrono::steady_clock::time_point {};
  eos::common::QdrantClient client;
  bool ready = false;

  while (!assistant.terminationRequested()) {
    const auto now = std::chrono::steady_clock::now();

    if (now - refreshed > std::chrono::seconds(30)) {
      refreshed = now;
      ready = Refresh();
      eos::common::QdrantClient::Settings settings;
      settings.endpoint = mEndpoint;
      settings.apiKey = mApiKey;
      settings.collection = mCollection;
      settings.embedEndpoint = mEmbedEndpoint;
      settings.embedModel = mEmbedModel;
      client.Update(std::move(settings));
    }

    Job job;
    {
      const std::lock_guard<std::mutex> lock(mMutex);
      auto it = mQueue.begin();

      for (; it != mQueue.end(); ++it) {
        if (it->notBefore <= static_cast<int64_t>(time(nullptr))) {
          break;
        }
      }

      if (it == mQueue.end()) {
        job.fid = 0;
      } else {
        job = std::move(*it);
        mQueue.erase(it);
      }
    }

    if (job.fid == 0) {
      assistant.wait_for(std::chrono::seconds(1));
      continue;
    }

    auto retry = [&](const std::string& error) {
      eos_static_warning("msg=\"qdrant index failed\" fxid=%08llx emsg=\"%s\"",
                         static_cast<unsigned long long>(job.fid), error.c_str());
      ReleaseClaim(job.fid);
      ++job.retries;

      if (job.retries > 5) {
        return;
      }

      job.notBefore = static_cast<int64_t>(time(nullptr)) +
                      std::min<int64_t>(3600, 30ll << std::min(job.retries, 10));
      const std::lock_guard<std::mutex> lock(mMutex);

      if (mQueue.size() < 128) {
        mQueue.push_back(std::move(job));
      }
    };

    if (!ready) {
      retry("qdrant indexing is not configured");
      continue;
    }

    if (job.size > mMaxBytes) {
      continue;
    }

    try {
      if (gOFS.openedForWriting.isOpen(job.fsid, job.fid)) {
        retry("file is open for writing");
        continue;
      }

      qclient::QHash catalog(*mQcl, eos::common::kQdrantCatalogKey);
      const std::string field = std::to_string(job.fid);

      if (catalog.hget(field) == job.checksum) {
        continue;
      }

      if (!Claim(job.fid, job.checksum)) {
        continue;
      }

      std::string bytes;
      std::string error;

      if (!ReadPayload(job.path, mMaxBytes, bytes, error)) {
        retry(error);
        continue;
      }

      const std::string text = eos::common::ExtractDocumentText(job.lfn, bytes);
      const auto chunks = eos::common::ChunkDocumentText(text);

      if (!client.DeleteFile(job.fid, error)) {
        retry(error);
        continue;
      }

      if (chunks.empty()) {
        catalog.hset(field, job.checksum);
        ReleaseClaim(job.fid);
        continue;
      }

      std::vector<std::vector<float>> vectors;
      vectors.reserve(chunks.size());
      bool embedded = true;

      for (const auto& chunk : chunks) {
        auto vector = client.Embed(chunk, error);

        if (vector.empty()) {
          embedded = false;
          break;
        }

        vectors.push_back(std::move(vector));
      }

      if (!embedded || !client.Upsert(job.fid, job.checksum, chunks, vectors, error)) {
        retry(error.empty() ? "empty embedding" : error);
        continue;
      }

      catalog.hset(field, job.checksum);
      ReleaseClaim(job.fid);
      eos_static_info("msg=\"indexed document\" fxid=%08llx chunks=%llu",
                      static_cast<unsigned long long>(job.fid),
                      static_cast<unsigned long long>(chunks.size()));
    } catch (const std::exception& e) {
      retry(e.what());
    }
  }
}

EOSFSTNAMESPACE_END
