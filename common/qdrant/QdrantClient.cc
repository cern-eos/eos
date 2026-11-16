//------------------------------------------------------------------------------
//! @file QdrantClient.cc
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

#include "common/qdrant/QdrantClient.hh"
#include <curl/curl.h>
#include <json/json.h>
#include <cstdio>
#include <sstream>

EOSCOMMONNAMESPACE_BEGIN

namespace {

struct CurlSession {
  CURL* easy = nullptr;
  curl_slist* headers = nullptr;
  std::string body;

  CurlSession() : easy(curl_easy_init()) {}
  ~CurlSession()
  {
    curl_slist_free_all(headers);
    curl_easy_cleanup(easy);
  }

  CurlSession(const CurlSession&) = delete;
  CurlSession& operator=(const CurlSession&) = delete;
};

size_t
CurlWrite(char* ptr, size_t size, size_t nmemb, void* userdata)
{
  auto* out = static_cast<std::string*>(userdata);
  out->append(ptr, size * nmemb);
  return size * nmemb;
}

std::string
JsonString(const Json::Value& value)
{
  Json::StreamWriterBuilder builder;
  builder["indentation"] = "";
  return Json::writeString(builder, value);
}

bool
HttpCall(const std::string& method, const std::string& url,
         const std::string& apiKey, const std::string& request, long timeoutSec,
         long& status, std::string& response, std::string& error)
{
  CurlSession curl;

  if (curl.easy == nullptr) {
    error = "failed to initialise HTTP client";
    return false;
  }

  curl.headers = curl_slist_append(curl.headers, "Content-Type: application/json");
  curl.headers = curl_slist_append(curl.headers, "Accept: application/json");
  std::string keyHeader;

  if (!apiKey.empty()) {
    keyHeader = "api-key: " + apiKey;
    curl.headers = curl_slist_append(curl.headers, keyHeader.c_str());
  }

  curl_easy_setopt(curl.easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl.easy, CURLOPT_CUSTOMREQUEST, method.c_str());
  curl_easy_setopt(curl.easy, CURLOPT_HTTPHEADER, curl.headers);
  curl_easy_setopt(curl.easy, CURLOPT_POSTFIELDS, request.c_str());
  curl_easy_setopt(curl.easy, CURLOPT_POSTFIELDSIZE, static_cast<long>(request.size()));
  curl_easy_setopt(curl.easy, CURLOPT_WRITEFUNCTION, CurlWrite);
  curl_easy_setopt(curl.easy, CURLOPT_WRITEDATA, &curl.body);
  curl_easy_setopt(curl.easy, CURLOPT_TIMEOUT, timeoutSec);
  curl_easy_setopt(curl.easy, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(curl.easy, CURLOPT_FOLLOWLOCATION, 1L);

  const CURLcode rc = curl_easy_perform(curl.easy);

  if (rc != CURLE_OK) {
    error = curl_easy_strerror(rc);
    return false;
  }

  curl_easy_getinfo(curl.easy, CURLINFO_RESPONSE_CODE, &status);
  response = std::move(curl.body);
  return true;
}

std::string
JoinUrl(const std::string& endpoint, const std::string& path)
{
  if (!endpoint.empty() && endpoint.back() == '/') {
    return endpoint.substr(0, endpoint.size() - 1) + path;
  }

  return endpoint + path;
}

std::string
PointId(uint64_t fid, uint32_t chunk)
{
  char buf[40];
  snprintf(buf, sizeof(buf), "%08x-%04x-%04x-%04x-%012x",
           static_cast<unsigned>((fid >> 32) & 0xffffffffu),
           static_cast<unsigned>((fid >> 16) & 0xffffu),
           static_cast<unsigned>(fid & 0xffffu),
           static_cast<unsigned>((chunk >> 16) & 0xffffu),
           static_cast<unsigned>(chunk & 0xffffffffu));
  return buf;
}

std::vector<float>
ParseEmbedding(const std::string& body, std::string& error)
{
  Json::Value root;
  Json::CharReaderBuilder builder;
  std::string parseError;
  std::istringstream in(body);

  if (!Json::parseFromStream(builder, in, &root, &parseError)) {
    error = "embedder returned invalid JSON";
    return {};
  }

  const Json::Value* vector = nullptr;

  if (root.isMember("data") && root["data"].isArray() && !root["data"].empty() &&
      root["data"][0].isMember("embedding")) {
    vector = &root["data"][0]["embedding"];
  } else if (root.isMember("embedding")) {
    vector = &root["embedding"];
  } else if (root.isMember("vector")) {
    vector = &root["vector"];
  }

  if (vector == nullptr || !vector->isArray() || vector->empty()) {
    error = "embedder response has no vector";
    return {};
  }

  std::vector<float> values;
  values.reserve(vector->size());

  for (const auto& entry : *vector) {
    if (!entry.isNumeric()) {
      error = "embedder vector is not numeric";
      return {};
    }

    values.push_back(entry.asFloat());
  }

  return values;
}

}  // namespace

void
QdrantClient::Update(Settings settings)
{
  if (settings.collection.empty()) {
    settings.collection = "eos";
  }

  if (settings.embedModel.empty()) {
    settings.embedModel = "text-embedding";
  }

  const std::lock_guard<std::mutex> lock(mMutex);

  if (settings.endpoint != mSettings.endpoint ||
      settings.collection != mSettings.collection ||
      settings.embedModel != mSettings.embedModel) {
    mReady = false;
    mDim = 0;
  }

  mSettings = std::move(settings);
}

QdrantClient::Settings
QdrantClient::CopySettings()
{
  const std::lock_guard<std::mutex> lock(mMutex);
  return mSettings;
}

bool
QdrantClient::EnsureCollection(const Settings& settings,
                               const std::vector<float>& probe, std::string& error)
{
  const std::lock_guard<std::mutex> lock(mMutex);

  if (mReady && mDim == static_cast<int>(probe.size())) {
    return true;
  }

  if (mReady && mDim != static_cast<int>(probe.size())) {
    error = "embedding dimension does not match the Qdrant collection";
    return false;
  }

  Json::Value vectors;
  vectors["size"] = static_cast<int>(probe.size());
  vectors["distance"] = "Cosine";
  Json::Value body;
  body["vectors"] = vectors;
  long status = 0;
  std::string response;
  const std::string url = JoinUrl(settings.endpoint,
                                  "/collections/" + settings.collection);

  if (!HttpCall("PUT", url, settings.apiKey, JsonString(body), 30, status, response,
                error)) {
    return false;
  }

  if (status != 200 && status != 201 && status != 409) {
    error = "failed to create Qdrant collection: HTTP " + std::to_string(status);
    return false;
  }

  Json::Value index;
  index["field_name"] = "fileid";
  index["field_schema"] = "keyword";
  std::string indexError;
  HttpCall("PUT", url + "/index", settings.apiKey, JsonString(index), 30, status,
           response, indexError);
  mReady = true;
  mDim = static_cast<int>(probe.size());
  return true;
}

std::vector<float>
QdrantClient::Embed(const std::string& text, std::string& error)
{
  const Settings settings = CopySettings();
  Json::Value body;
  body["model"] = settings.embedModel;
  body["input"] = text;
  long status = 0;
  std::string response;

  // The Qdrant api-key is not sent to the embedder.
  if (!HttpCall("POST", settings.embedEndpoint, "", JsonString(body), 60, status,
                response, error)) {
    return {};
  }

  if (status < 200 || status >= 300) {
    error = "embedder returned HTTP " + std::to_string(status);
    return {};
  }

  return ParseEmbedding(response, error);
}

bool
QdrantClient::DeleteFile(uint64_t fid, std::string& error)
{
  const Settings settings = CopySettings();
  Json::Value match;
  match["value"] = std::to_string(fid);
  Json::Value clause;
  clause["key"] = "fileid";
  clause["match"] = match;
  Json::Value must(Json::arrayValue);
  must.append(clause);
  Json::Value filter;
  filter["must"] = must;
  Json::Value body;
  body["filter"] = filter;
  long status = 0;
  std::string response;
  const std::string url = JoinUrl(settings.endpoint, "/collections/" +
                                    settings.collection + "/points/delete?wait=true");

  if (!HttpCall("POST", url, settings.apiKey, JsonString(body), 60, status, response,
                error)) {
    return false;
  }

  return status == 200 || status == 404;
}

bool
QdrantClient::Upsert(uint64_t fid, const std::string& checksum,
                     const std::vector<std::string>& chunks,
                     const std::vector<std::vector<float>>& vectors,
                     std::string& error)
{
  if (chunks.empty() || chunks.size() != vectors.size()) {
    error = "chunk and vector counts differ";
    return false;
  }

  const Settings settings = CopySettings();

  if (!EnsureCollection(settings, vectors.front(), error)) {
    return false;
  }

  Json::Value points(Json::arrayValue);

  for (size_t i = 0; i < chunks.size(); ++i) {
    Json::Value payload;
    payload["fileid"] = std::to_string(fid);
    payload["chunk"] = static_cast<Json::UInt>(i);
    payload["checksum"] = checksum;
    payload["model"] = settings.embedModel;
    payload["text"] = chunks[i];
    Json::Value vector(Json::arrayValue);

    for (float value : vectors[i]) {
      vector.append(value);
    }

    Json::Value point;
    point["id"] = PointId(fid, static_cast<uint32_t>(i));
    point["vector"] = vector;
    point["payload"] = payload;
    points.append(point);
  }

  Json::Value body;
  body["points"] = points;
  long status = 0;
  std::string response;
  const std::string url = JoinUrl(settings.endpoint, "/collections/" +
                                    settings.collection + "/points?wait=true");

  if (!HttpCall("PUT", url, settings.apiKey, JsonString(body), 120, status, response,
                error)) {
    return false;
  }

  if (status < 200 || status >= 300) {
    error = "Qdrant upsert returned HTTP " + std::to_string(status);
    return false;
  }

  return true;
}

bool
QdrantClient::Search(const std::vector<float>& vector, int limit, long& status,
                     std::string& body, std::string& error)
{
  const Settings settings = CopySettings();
  Json::Value requestVector(Json::arrayValue);

  for (float value : vector) {
    requestVector.append(value);
  }

  Json::Value request;
  request["vector"] = requestVector;
  request["limit"] = limit;
  request["with_payload"] = true;
  const std::string url = JoinUrl(settings.endpoint, "/collections/" +
                                    settings.collection + "/points/search");
  return HttpCall("POST", url, settings.apiKey, JsonString(request), 30, status, body,
                  error);
}

EOSCOMMONNAMESPACE_END
