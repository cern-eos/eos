// ----------------------------------------------------------------------
// File: TapeJsonifiers.hh
// Author: Consolidated tape REST API jsonifiers
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 ************************************************************************/

#ifndef EOS_TAPE_JSONIFIERS_HH
#define EOS_TAPE_JSONIFIERS_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/json/tape/TapeRestApiJsonifier.hh"
#include "mgm/http/rest-api/model/tape/archiveinfo/GetArchiveInfoResponseModel.hh"
#include "mgm/http/rest-api/model/tape/common/ErrorModel.hh"
#include "mgm/http/rest-api/model/tape/stage/CreatedStageBulkRequestResponseModel.hh"
#include "mgm/http/rest-api/model/tape/stage/GetStageBulkRequestResponseModel.hh"
#include "mgm/http/rest-api/model/wellknown/tape/GetTapeWellKnownModel.hh"
#include <json/json.h>

EOSMGMRESTNAMESPACE_BEGIN

// ErrorModel jsonifier
class ErrorModelJsonifier : public TapeRestApiJsonifier<ErrorModel>
{
public:
  void jsonify(const ErrorModel* obj, std::stringstream& ss) override
  {
    Json::Value root;

    if (obj->getType()) {
      root["type"] = obj->getType().value();
    }

    root["title"] = obj->getTitle();
    root["status"] = obj->getStatus();

    if (obj->getDetail()) {
      root["detail"] = obj->getDetail().value();
    }

    ss << root;
  }
};

// CreatedStageBulkRequest jsonifier
class CreatedStageBulkRequestJsonifier : public TapeRestApiJsonifier<CreatedStageBulkRequestResponseModel>
{
public:
  void jsonify(const CreatedStageBulkRequestResponseModel* obj, std::stringstream& ss) override
  {
    Json::Value root;
    root["requestId"] = obj->getRequestId();
    ss << root;
  }
};

// GetStageBulkRequest jsonifier
class GetStageBulkRequestJsonifier : public TapeRestApiJsonifier<GetStageBulkRequestResponseModel>
{
public:
  void jsonify(const GetStageBulkRequestResponseModel* obj, std::stringstream& ss) override
  {
    Json::Value root;
    root["createdAt"] = Json::UInt64(obj->getCreationTime());
    root["startedAt"] = Json::UInt64(obj->getCreationTime());
    root["id"] = obj->getId();
    root["files"] = Json::Value(Json::arrayValue);
    Json::Value& files = root["files"];

    for (auto& file : obj->getFiles()) {
      Json::Value fileObj;
      fileObj["path"] = file->mPath;

      if (!file->mError.empty()) {
        fileObj["error"] = file->mError;
      }

      fileObj["onDisk"] = file->mOnDisk;
      files.append(fileObj);
    }

    ss << root;
  }
};

// GetArchiveInfoResponse jsonifier
class GetArchiveInfoResponseJsonifier : public TapeRestApiJsonifier<GetArchiveInfoResponseModel>
{
public:
  void jsonify(const GetArchiveInfoResponseModel* obj, std::stringstream& ss) override
  {
    Json::Value root(Json::arrayValue);

    if (auto queryPrepareResponse = obj->getQueryPrepareResponse()) {
      for (const auto& queryPrepareFileResponse : queryPrepareResponse->responses) {
        Json::Value fileResponse;
        fileResponse["path"] = queryPrepareFileResponse.path;
        std::string locality;

        if (queryPrepareFileResponse.is_online && queryPrepareFileResponse.is_on_tape) {
          locality = "DISK_AND_TAPE";
        } else if (queryPrepareFileResponse.is_online) {
          locality = "DISK";
        } else if (queryPrepareFileResponse.is_on_tape) {
          locality = "TAPE";
        }

        if (!locality.empty()) {
          fileResponse["locality"] = locality;
        }

        if (!queryPrepareFileResponse.error_text.empty()) {
          fileResponse["error"] = queryPrepareFileResponse.error_text;
        }

        root.append(fileResponse);
      }
    }

    ss << root;
  }
};

// GetTapeWellKnownModel jsonifier
class GetTapeWellKnownModelJsonifier : public TapeRestApiJsonifier<GetTapeWellKnownModel>
{
public:
  void jsonify(const GetTapeWellKnownModel* obj, std::stringstream& ss) override
  {
    Json::Value root;
    const TapeWellKnownInfos* infos = obj->getTapeWellKnownInfos();
    root["sitename"] = infos->getSiteName();
    root["endpoints"] = Json::Value(Json::arrayValue);

    for (auto& endpoint : infos->getEndpoints()) {
      Json::Value endpointJson;
      endpointJson["uri"] = endpoint->getUri();
      endpointJson["version"] = endpoint->getVersion();
      root["endpoints"].append(endpointJson);
    }

    ss << root;
  }
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPE_JSONIFIERS_HH
