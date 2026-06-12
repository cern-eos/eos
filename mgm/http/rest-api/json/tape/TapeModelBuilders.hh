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
#include <sstream>
#include <string>

EOSMGMRESTNAMESPACE_BEGIN

class PathsModelBuilder : public JsonCppModelBuilder<PathsModel>
{
public:
  std::unique_ptr<PathsModel> buildFromJson(const std::string& json) override
  {
    Json::Value root;
    parseJson(json, root);

    // WLCG v1 uses {"paths":["...", ...]} for cancel/release/archiveinfo.
    const char* filesKey = "files";
    const char* pathKey = "path";
    const char* pathsKey = "paths";

    auto model = std::make_unique<PathsModel>();

    if (root.isMember(pathsKey)) {
      const Json::Value& paths = root[pathsKey];
      if (!paths.isArray() || paths.empty()) {
        throw JsonValidationException(
          "paths – Field does not exist or is not a valid non-empty array");
      }
      for (const auto& p : paths) {
        if (!p.isString()) {
          throw JsonValidationException("Each path must be a string");
        }
        model->addFile(p.asString());
      }
      return model;
    }

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

    throw JsonValidationException(
      "paths – Field does not exist or is not a valid non-empty array");
  }
};

class CreateStageRequestModelBuilder : public JsonCppModelBuilder<CreateStageBulkRequestModel>
{
public:
  static inline const std::string FILES_KEY_NAME = "files";
  static inline const std::string PATH_KEY_NAME = "path";
  static inline const std::string TARGETED_METADATA_KEY_NAME = "targetedMetadata";
  static inline const std::string LEGACY_TARGETED_METADATA_KEY_NAME = "targeted_metadata";

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

      std::string opaque = buildOpaqueInfo(f);
      model->addFile(f[PATH_KEY_NAME].asString(), opaque);
    }

    return model;
  }

private:
  const Json::Value* getTargetedMetadata(const Json::Value& file) const
  {
    if (file.isMember(TARGETED_METADATA_KEY_NAME) &&
        file[TARGETED_METADATA_KEY_NAME].isObject()) {
      return &file[TARGETED_METADATA_KEY_NAME];
    }
    if (file.isMember(LEGACY_TARGETED_METADATA_KEY_NAME) &&
        file[LEGACY_TARGETED_METADATA_KEY_NAME].isObject()) {
      return &file[LEGACY_TARGETED_METADATA_KEY_NAME];
    }
    return nullptr;
  }

  std::string buildOpaqueInfo(const Json::Value& file) const
  {
    std::ostringstream opaque;
    const Json::Value* targetedMetadata = getTargetedMetadata(file);

    if (targetedMetadata != nullptr) {
      std::string activity;
      if (targetedMetadata->isMember(mRestApiEndpointId) &&
          (*targetedMetadata)[mRestApiEndpointId].isObject()) {
        const Json::Value& ep = (*targetedMetadata)[mRestApiEndpointId];
        if (ep.isMember("activity") && ep["activity"].isString()) {
          activity = ep["activity"].asString();
        }
      }
      if (activity.empty() && targetedMetadata->isMember("default") &&
          (*targetedMetadata)["default"].isObject()) {
        const Json::Value& def = (*targetedMetadata)["default"];
        if (def.isMember("activity") && def["activity"].isString()) {
          activity = def["activity"].asString();
        }
      }
      if (!activity.empty()) {
        opaque << "activity=" << activity;
      }
    }

    return opaque.str();
  }

  std::string mRestApiEndpointId;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPE_MODEL_BUILDERS_HH

