// ----------------------------------------------------------------------
// File: TapeModelBuilders.hh
// Author: Consolidated tape REST API model builders and validators
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 ************************************************************************/

#ifndef EOS_TAPE_MODEL_BUILDERS_HH
#define EOS_TAPE_MODEL_BUILDERS_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/json/builder/JsonModelBuilder.hh"
#include "mgm/http/rest-api/json/builder/jsoncpp/JsonCppModelBuilder.hh"
#include "mgm/http/rest-api/json/builder/jsoncpp/JsonCppValidator.hh"
#include "mgm/http/rest-api/json/tape/model-builders/validators/TapeJsonCppValidator.hh"
#include "mgm/http/rest-api/model/tape/stage/PathsModel.hh"
#include "mgm/http/rest-api/model/tape/stage/CreateStageBulkRequestModel.hh"
#include <string>

EOSMGMRESTNAMESPACE_BEGIN

class PathsModelBuilder : public JsonCppModelBuilder<PathsModel>
{
public:
  std::unique_ptr<PathsModel> buildFromJson(const std::string& json) override
  {
    Json::Value root;
    parseJson(json, root);

    // Accept either {"files":[{"path":"..."}, ...]} or {"paths":["...", ...]}
    const char* filesKey = "files";
    const char* pathKey = "path";
    const char* pathsKey = "paths";

    auto model = std::make_unique<PathsModel>();

    if (root.isMember(filesKey)) {
      const Json::Value& files = root[filesKey];
      if (!files.isArray() || files.empty()) {
        throw JsonValidationException("'files' must be a non-empty array");
      }
      for (const auto& entry : files) {
        if (!entry.isObject() || !entry.isMember(pathKey) || !entry[pathKey].isString()) {
          throw JsonValidationException("Each file entry must be an object with a string 'path'");
        }
        model->addFile(entry[pathKey].asString());
      }
      return model;
    }

    if (root.isMember(pathsKey)) {
      const Json::Value& paths = root[pathsKey];
      if (!paths.isArray() || paths.empty()) {
        throw JsonValidationException("'paths' must be a non-empty array");
      }
      for (const auto& p : paths) {
        if (!p.isString()) {
          throw JsonValidationException("Each path must be a string");
        }
        model->addFile(p.asString());
      }
      return model;
    }

    throw JsonValidationException("Expected 'files' or 'paths' field in request body");
  }
};

class CreateStageRequestModelBuilder : public JsonCppModelBuilder<CreateStageBulkRequestModel>
{
public:
  // JSON field keys used by tests and builder
  static inline const std::string FILES_KEY_NAME = "files";
  static inline const std::string PATH_KEY_NAME = "path";
  static inline const std::string TARGETED_METADATA_KEY_NAME = "targeted_metadata";
  explicit CreateStageRequestModelBuilder(const std::string& restApiEndpointId)
    : mRestApiEndpointId(restApiEndpointId) {}
  std::unique_ptr<CreateStageBulkRequestModel> buildFromJson(const std::string& json) override
  {
    Json::Value root;
    parseJson(json, root);

    if (!root.isMember(FILES_KEY_NAME) || !root[FILES_KEY_NAME].isArray()) {
      throw JsonValidationException("Missing or invalid 'files' array");
    }
    const Json::Value& files = root[FILES_KEY_NAME];
    if (files.empty()) {
      throw JsonValidationException("'files' must be a non-empty array");
    }

    auto model = std::make_unique<CreateStageBulkRequestModel>();
    for (const auto& f : files) {
      if (!f.isObject()) {
        throw JsonValidationException("file entry must be an object");
      }
      if (!f.isMember(PATH_KEY_NAME) || !f[PATH_KEY_NAME].isString()) {
        throw JsonValidationException("file entry must contain a string 'path'");
      }

      std::string opaque;
      if (f.isMember(TARGETED_METADATA_KEY_NAME) && f[TARGETED_METADATA_KEY_NAME].isObject()) {
        const Json::Value& tmd = f[TARGETED_METADATA_KEY_NAME];
        std::string activity;
        // prefer endpoint-specific over default
        if (tmd.isMember(mRestApiEndpointId) && tmd[mRestApiEndpointId].isObject()) {
          const Json::Value& ep = tmd[mRestApiEndpointId];
          if (ep.isMember("activity") && ep["activity"].isString()) {
            activity = ep["activity"].asString();
          }
        }
        if (activity.empty() && tmd.isMember("default") && tmd["default"].isObject()) {
          const Json::Value& def = tmd["default"];
          if (def.isMember("activity") && def["activity"].isString()) {
            activity = def["activity"].asString();
          }
        }
        if (!activity.empty()) {
          opaque = std::string("activity=") + activity;
        }
      }

      model->addFile(f[PATH_KEY_NAME].asString(), opaque);
    }

    return model;
  }
private:
  std::string mRestApiEndpointId;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPE_MODEL_BUILDERS_HH


