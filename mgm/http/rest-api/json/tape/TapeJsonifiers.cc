// ----------------------------------------------------------------------
// File: TapeJsonifiers.cc
// Author: Consolidated tape REST API jsonifiers
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 ************************************************************************/

#include "TapeJsonifiers.hh"
#include "mgm/http/rest-api/wellknown/tape/TapeWellKnownInfos.hh"
#include <json/json.h>

EOSMGMRESTNAMESPACE_BEGIN

namespace tape_json {

void writeValue(const Json::Value& value, std::stringstream& ss)
{
  Json::StreamWriterBuilder builder;
  builder["indentation"] = "";
  ss << Json::writeString(builder, value);
}

} // namespace tape_json

void ErrorModelJsonifier::jsonify(const ErrorModel* obj, std::stringstream& ss)
{
  Json::Value root;
  root["title"] = obj->getTitle();
  root["status"] = obj->getStatus();

  if (obj->getDetail()) {
    root["detail"] = *obj->getDetail();
  }

  if (obj->getType()) {
    root["type"] = *obj->getType();
  }

  tape_json::writeValue(root, ss);
}

void CreatedStageBulkRequestJsonifier::jsonify(
  const CreatedStageBulkRequestResponseModel* obj, std::stringstream& ss)
{
  Json::Value root;
  root["requestId"] = obj->getRequestId();
  tape_json::writeValue(root, ss);
}

void GetStageBulkRequestJsonifier::jsonify(
  const GetStageBulkRequestResponseModel* obj, std::stringstream& ss)
{
  Json::Value root;
  root["id"] = obj->getId();
  root["createdAt"] = static_cast<Json::Int64>(obj->getCreatedAt());
  root["startedAt"] = static_cast<Json::Int64>(obj->getStartedAt());

  Json::Value files(Json::arrayValue);
  for (const auto& file : obj->getFiles()) {
    Json::Value fileJson;
    fileJson["path"] = file->mPath;
    fileJson["onDisk"] = file->mOnDisk;

    if (file->mError && !file->mError->empty()) {
      fileJson["error"] = *file->mError;
    }

    files.append(fileJson);
  }

  root["files"] = files;
  tape_json::writeValue(root, ss);
}

void GetArchiveInfoResponseJsonifier::jsonify(
  const GetArchiveInfoResponseModel* obj, std::stringstream& ss)
{
  Json::Value root(Json::arrayValue);

  for (const auto& entry : obj->getEntries()) {
    Json::Value item;
    item["path"] = entry.path;

    if (entry.locality) {
      item["locality"] = *entry.locality;
    }

    if (entry.error) {
      item["error"] = *entry.error;
    }

    root.append(item);
  }

  tape_json::writeValue(root, ss);
}

void GetTapeWellKnownModelJsonifier::jsonify(const GetTapeWellKnownModel* obj,
    std::stringstream& ss)
{
  const TapeWellKnownInfos* infos = obj->getTapeWellKnownInfos();
  Json::Value root;
  root["sitename"] = infos->getSiteName();

  if (!infos->getDescription().empty()) {
    root["description"] = infos->getDescription();
  }

  Json::Value endpoints(Json::arrayValue);
  for (const auto& endpoint : infos->getEndpoints()) {
    Json::Value endpointJson;
    endpointJson["uri"] = endpoint->getUri();
    endpointJson["version"] = endpoint->getVersion();
    endpoints.append(endpointJson);
  }

  root["endpoints"] = endpoints;
  tape_json::writeValue(root, ss);
}

EOSMGMRESTNAMESPACE_END
