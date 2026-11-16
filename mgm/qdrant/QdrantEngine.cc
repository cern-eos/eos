//------------------------------------------------------------------------------
//! @file QdrantEngine.cc
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

#include "mgm/qdrant/QdrantEngine.hh"
#include "common/Constants.hh"
#include "common/qdrant/QdrantClient.hh"
#include "common/FileId.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "common/http/HttpRequest.hh"
#include "common/http/PlainHttpResponse.hh"
#include "common/http/ProtocolHandler.hh"
#include "common/qdrant/DocumentIndex.hh"
#include "mgm/auth/AccessChecker.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IView.hh"
#include "qclient/QClient.hh"
#include "qclient/structures/QHash.hh"
#include <json/json.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <sstream>

EOSMGMNAMESPACE_BEGIN

namespace {

std::string
JsonString(const Json::Value& value)
{
  Json::StreamWriterBuilder builder;
  builder["indentation"] = "";
  return Json::writeString(builder, value);
}

std::string
Lower(std::string value)
{
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return value;
}

bool
SameChecksum(const std::string& left, const std::string& right)
{
  return !left.empty() && Lower(left) == Lower(right);
}

class SearchHandler : public eos::common::ProtocolHandler {
public:
  explicit SearchHandler(eos::common::VirtualIdentity* vid) : ProtocolHandler(vid) {}

  void HandleRequest(eos::common::HttpRequest* request) override
  {
    auto* response = new eos::common::PlainHttpResponse();
    mHttpResponse = response;
    response->AddHeader("Content-Type", "application/json; charset=utf-8");

    auto fail = [&](int http, int retc, const std::string& message) {
      response->SetResponseCode(http);
      Json::Value body;
      body["error"] = message;
      response->SetBody(JsonString(body));
      (void)retc;
    };

    if (request == nullptr || request->GetMethod() != "POST") {
      fail(405, EINVAL, "POST /v1/search expects a JSON body");
      return;
    }

    if (gOFS == nullptr || gOFS->mQdrantEngine == nullptr) {
      fail(503, ENOTSUP, "qdrant endpoint is not configured");
      return;
    }

    Json::Value root;
    Json::CharReaderBuilder builder;
    std::string parseError;
    std::istringstream in(request->GetBody());

    if (!Json::parseFromStream(builder, in, &root, &parseError) || !root.isObject() ||
        !root.isMember("query") || !root["query"].isString()) {
      fail(400, EINVAL, "request body must be a JSON object with a string query");
      return;
    }

    const std::string query = root["query"].asString();
    uint32_t limit = 8;

    if (root.isMember("limit")) {
      if (!root["limit"].isIntegral()) {
        fail(400, EINVAL, "limit must be an integer");
        return;
      }

      const auto parsed = root["limit"].asInt64();

      if (parsed < 1 || parsed > 50) {
        fail(400, EINVAL, "limit must be between 1 and 50");
        return;
      }

      limit = static_cast<uint32_t>(parsed);
    }

    QdrantEngine::SearchResult result;

    try {
      result = gOFS->mQdrantEngine->Search(*mVirtualIdentity, query, limit, true);
    } catch (const std::exception&) {
      fail(503, EAGAIN, "search failed");
      return;
    }

    if (result.retc == EINVAL) {
      fail(400, result.retc, result.error);
      return;
    }

    if (result.retc != 0) {
      fail(503, result.retc, result.error);
      return;
    }

    response->SetResponseCode(200);
    response->SetBody(result.json);
  }
};

}  // namespace

//------------------------------------------------------------------------------
// Construct the engine. Qdrant calls wait until Start().
//------------------------------------------------------------------------------
QdrantEngine::QdrantEngine(Config config, const eos::QdbContactDetails& qdb)
  : mConfig(std::move(config)), mQdb(qdb)
{
  if (mConfig.collection.empty()) {
    mConfig.collection = "eos";
  }

  if (mConfig.embedModel.empty()) {
    mConfig.embedModel = "text-embedding";
  }

  if (mConfig.overfetch < 1) {
    mConfig.overfetch = 1;
  }

  if (mConfig.maxBytes == 0) {
    mConfig.maxBytes = eos::common::kQdrantDefaultMaxBytes;
  }

  auto trimSlash = [](std::string value) {
    while (!value.empty() && value.back() == '/') {
      value.pop_back();
    }
    return value;
  };

  if (!mConfig.endpoint.empty() && mConfig.endpoint.find("://") == std::string::npos) {
    mConfig.endpoint.insert(0, "http://");
  }

  mConfig.endpoint = trimSlash(mConfig.endpoint);

  if (!mConfig.embedEndpoint.empty() &&
      mConfig.embedEndpoint.find("://") == std::string::npos) {
    mConfig.embedEndpoint.insert(0, "http://");
  }

  mConfig.embedEndpoint = trimSlash(mConfig.embedEndpoint);

  if (!mConfig.embedEndpoint.empty() &&
      mConfig.embedEndpoint.find("embeddings") == std::string::npos) {
    mConfig.embedEndpoint += "/v1/embeddings";
  }

  if (!mQdb.empty()) {
    mQcl = std::make_shared<qclient::QClient>(mQdb.members, mQdb.constructOptions());
  }

  eos::common::QdrantClient::Settings settings;
  settings.endpoint = mConfig.endpoint;
  settings.apiKey = mConfig.apiKey;
  settings.collection = mConfig.collection;
  settings.embedEndpoint = mConfig.embedEndpoint;
  settings.embedModel = mConfig.embedModel;
  mClient.Update(std::move(settings));
}

QdrantEngine::~QdrantEngine()
{
  Stop();
}

void
QdrantEngine::Start()
{
  Stop();
  mThread.reset(&QdrantEngine::Run, this);
}

void
QdrantEngine::Stop()
{
  mThread.join();
}

void
QdrantEngine::NoteFileRemoved(uint64_t fid)
{
  if (fid == 0) {
    return;
  }

  const std::lock_guard<std::mutex> lock(mDeleteMutex);
  mDeletes.push_back(fid);
}

bool
QdrantEngine::IndexingSwitchOn() const
{
  try {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    const auto it = FsView::gFsView.mSpaceView.find("default");

    if (it == FsView::gFsView.mSpaceView.end() || it->second == nullptr) {
      return false;
    }

    const std::string value = it->second->GetConfigMember(eos::common::SPACE_QDRANT_INDEX);
    return value == "1" || value == "true";
  } catch (const std::exception& e) {
    eos_static_debug("msg=\"failed to read space.qdrant.index\" emsg=\"%s\"",
                     e.what());
    return false;
  }
}

void
QdrantEngine::PublishEnabled(bool enabled)
{
  if (mQcl == nullptr) {
    return;
  }

  try {
    mQcl->exec("SET", eos::common::kQdrantEnabledKey,
               std::string(enabled ? "1" : "0")).get();
    mQcl->exec("SET", eos::common::kQdrantMaxBytesKey,
               std::to_string(mConfig.maxBytes)).get();
    // FSTs read these to index locally. The api-key is not returned to clients.
    mQcl->exec("SET", eos::common::kQdrantEndpointKey, mConfig.endpoint).get();
    mQcl->exec("SET", eos::common::kQdrantApiKeyKey, mConfig.apiKey).get();
    mQcl->exec("SET", eos::common::kQdrantCollectionKey, mConfig.collection).get();
    mQcl->exec("SET", eos::common::kQdrantEmbedEndpointKey, mConfig.embedEndpoint).get();
    mQcl->exec("SET", eos::common::kQdrantEmbedModelKey, mConfig.embedModel).get();
  } catch (const std::exception& e) {
    eos_static_err("msg=\"failed to publish qdrant indexing flag\" emsg=\"%s\"",
                   e.what());
  }
}

namespace {

std::string
ChecksumHex(const eos::Buffer& checksum)
{
  if (checksum.empty()) {
    return {};
  }

  return eos::common::StringConversion::BinData2HexString(
           &checksum[0], checksum.size(), checksum.size(), 0);
}

bool
CallerCanRead(const eos::common::VirtualIdentity& vid, eos::IFileMD* file,
              std::string& path)
{
  path = gOFS->eosView->getUri(file);

  if (!AccessChecker::checkPublicAccess(path, vid)) {
    return false;
  }

  std::vector<std::shared_ptr<eos::IContainerMD>> chain;
  std::shared_ptr<eos::IContainerMD> current =
      gOFS->eosDirectoryService->getContainerMD(file->getContainerId());

  while (current) {
    chain.push_back(current);
    const auto parent = current->getParentId();

    if (parent == 0 || parent == current->getId()) {
      break;
    }

    current = gOFS->eosDirectoryService->getContainerMD(parent);
  }

  if (chain.empty()) {
    return false;
  }

  for (size_t i = chain.size(); i-- > 0;) {
    eos::IContainerMD::XAttrMap attrs;
    gOFS->listAttributes(gOFS->eosView, chain[i].get(), attrs, false);
    const int mode = (i == 0) ? (R_OK | X_OK) : X_OK;

    if (!AccessChecker::checkContainer(chain[i].get(), attrs, mode, vid)) {
      return false;
    }
  }

  return AccessChecker::checkFile(file, X_OK, static_cast<int>(chain.front()->getMode()),
                                  vid);
}

struct Candidate {
  uint64_t fid = 0;
  uint32_t chunk = 0;
  double score = 0;
  std::string checksum;
  std::string text;
};

std::vector<Candidate>
ParseSearchHits(const std::string& body, std::string& error)
{
  Json::Value root;
  Json::CharReaderBuilder builder;
  std::string parseError;
  std::istringstream in(body);

  if (!Json::parseFromStream(builder, in, &root, &parseError)) {
    error = "Qdrant returned invalid JSON";
    return {};
  }

  const Json::Value& result = root["result"];
  if (!result.isArray()) {
    error = "Qdrant response has no result array";
    return {};
  }

  std::vector<Candidate> hits;

  for (const auto& entry : result) {
    const Json::Value& payload = entry["payload"];
    if (!payload.isObject() || !payload.isMember("fileid")) {
      continue;
    }

    Candidate hit;
    try {
      hit.fid = std::stoull(payload["fileid"].asString());
    } catch (const std::exception&) {
      continue;
    }

    hit.chunk = payload.get("chunk", 0).asUInt();
    hit.score = entry.get("score", 0).asDouble();
    hit.checksum = payload.get("checksum", "").asString();
    hit.text = payload.get("text", "").asString();
    hits.push_back(std::move(hit));
  }

  return hits;
}

}  // namespace

void
QdrantEngine::PersistDeletions()
{
  std::vector<uint64_t> pending;
  {
    const std::lock_guard<std::mutex> lock(mDeleteMutex);
    pending.swap(mDeletes);
  }

  if (pending.empty() || mQcl == nullptr) {
    if (mQcl == nullptr) {
      const std::lock_guard<std::mutex> lock(mDeleteMutex);
      mDeletes.insert(mDeletes.end(), pending.begin(), pending.end());
    }
    return;
  }

  try {
    qclient::QHash tombstones(*mQcl, eos::common::kQdrantTombstoneKey);
    qclient::QHash queue(*mQcl, eos::common::kQdrantQueueKey);
    qclient::QHash catalog(*mQcl, eos::common::kQdrantCatalogKey);

    for (uint64_t fid : pending) {
      const std::string field = std::to_string(fid);
      tombstones.hset(field, "1");
      queue.hdel(field);
      catalog.hdel(field);
    }
  } catch (const std::exception& e) {
    eos_static_err("msg=\"failed to persist qdrant tombstones\" emsg=\"%s\"",
                   e.what());
    const std::lock_guard<std::mutex> lock(mDeleteMutex);
    mDeletes.insert(mDeletes.end(), pending.begin(), pending.end());
  }
}

void
QdrantEngine::DrainTombstones()
{
  if (mQcl == nullptr || mConfig.endpoint.empty()) {
    return;
  }

  try {
    qclient::QHash tombstones(*mQcl, eos::common::kQdrantTombstoneKey);
    const auto page = tombstones.hscan("0", 32);

    for (const auto& entry : page.second) {
      uint64_t fid = 0;
      try {
        fid = std::stoull(entry.first);
      } catch (const std::exception&) {
        tombstones.hdel(entry.first);
        continue;
      }

      std::string error;
      if (!mClient.DeleteFile(fid, error)) {
        eos_static_warning("msg=\"qdrant delete failed\" fxid=%08llx emsg=\"%s\"",
                           static_cast<unsigned long long>(fid), error.c_str());
        continue;
      }

      tombstones.hdel(entry.first);
    }
  } catch (const std::exception& e) {
    eos_static_err("msg=\"failed to drain qdrant tombstones\" emsg=\"%s\"", e.what());
  }
}

void
QdrantEngine::Run(ThreadAssistant& assistant)
{
  eos_static_info("msg=\"qdrant engine started\" endpoint=\"%s\" collection=\"%s\"",
                  mConfig.endpoint.c_str(), mConfig.collection.c_str());

  while (!assistant.terminationRequested()) {
    const bool master = gOFS != nullptr && gOFS->mMaster != nullptr &&
                        gOFS->mMaster->IsMaster();

    if (master) {
      const bool enabled = HasEndpoint() && IndexingSwitchOn();
      PublishEnabled(enabled);
      PersistDeletions();
      DrainTombstones();

    }

    assistant.wait_for(std::chrono::seconds(2));
  }
}

QdrantEngine::SearchResult
QdrantEngine::Search(const eos::common::VirtualIdentity& vid,
                     const std::string& query, uint32_t limit, bool asJson)
{
  SearchResult result;
  auto finish = [&](int retc, const std::string& error) {
    result.retc = retc;
    result.error = error;
    Json::Value body;
    body["error"] = error;
    result.json = JsonString(body);
    result.text = error;
    return result;
  };

  if (query.empty() ||
      std::all_of(query.begin(), query.end(), [](unsigned char c) {
        return std::isspace(c) != 0;
      })) {
    return finish(EINVAL, "query is empty");
  }

  if (query.size() > 4096) {
    return finish(EINVAL, "query is too long");
  }

  if (!HasEndpoint()) {
    return finish(ENOTSUP, "qdrant endpoint is not configured");
  }

  if (!HasEmbedder()) {
    return finish(ENOTSUP, "embed endpoint is not configured");
  }

  if (limit == 0) {
    limit = 8;
  }

  if (limit > 50) {
    limit = 50;
  }

  std::string error;
  const auto vector = mClient.Embed(query, error);

  if (vector.empty()) {
    return finish(EAGAIN, error.empty() ? "embedding failed" : error);
  }

  const int ask = std::min(200, static_cast<int>(limit) * mConfig.overfetch);
  long status = 0;
  std::string response;

  if (!mClient.Search(vector, ask, status, response, error)) {
    return finish(EAGAIN, error);
  }

  if (status == 404) {
    Json::Value body;
    body["results"] = Json::Value(Json::arrayValue);
    result.json = JsonString(body);
    result.text = asJson ? result.json : "no matching documents\n";
    return result;
  }

  if (status < 200 || status >= 300) {
    return finish(EAGAIN, "Qdrant returned HTTP " + std::to_string(status));
  }

  auto candidates = ParseSearchHits(response, error);

  if (!error.empty()) {
    return finish(EAGAIN, error);
  }

  Json::Value hits(Json::arrayValue);
  std::ostringstream text;
  std::vector<uint64_t> seen;

  for (const auto& candidate : candidates) {
    if (hits.size() >= limit) {
      break;
    }

    if (std::find(seen.begin(), seen.end(), candidate.fid) != seen.end()) {
      continue;
    }

    seen.push_back(candidate.fid);
    std::string path;
    std::string nsChecksum;

    try {
      auto file = gOFS->eosFileService->getFileMD(candidate.fid);
      nsChecksum = ChecksumHex(file->getChecksum());

      if (!CallerCanRead(vid, file.get(), path)) {
        continue;
      }
    } catch (const eos::MDException&) {
      continue;
    }

    if (!candidate.checksum.empty() && !nsChecksum.empty() &&
        !SameChecksum(candidate.checksum, nsChecksum)) {
      continue;
    }

    Json::Value hit;
    hit["fileid"] = std::to_string(candidate.fid);
    hit["path"] = path;
    hit["chunk"] = candidate.chunk;
    hit["score"] = candidate.score;
    hit["text"] = candidate.text;
    hits.append(hit);
    text << "score=" << candidate.score
         << " fxid=" << eos::common::FileId::Fid2Hex(candidate.fid)
         << " chunk=" << candidate.chunk << " path=" << path << "\n"
         << candidate.text << "\n";
  }

  Json::Value body;
  body["results"] = hits;
  result.json = JsonString(body);
  result.text = asJson ? result.json : text.str();

  if (!asJson && result.text.empty()) {
    result.text = "no matching documents\n";
  }

  // A JSON caller still wants the empty result object, not the console line.
  if (asJson) {
    result.text = result.json;
  }

  return result;
}

std::unique_ptr<eos::common::ProtocolHandler>
CreateQdrantSearchHandler(eos::common::VirtualIdentity* vid)
{
  return std::unique_ptr<eos::common::ProtocolHandler>(new SearchHandler(vid));
}

EOSMGMNAMESPACE_END
