// ----------------------------------------------------------------------
// File: CreateStageRequestModelBuilder.hh
// Author: Cedric Caffy - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#include "CreateStageRequestModelBuilder.hh"
#include "mgm/http/rest-api/json/builder/ValidationError.hh"

EOSMGMRESTNAMESPACE_BEGIN

CreateStageRequestModelBuilder::CreateStageRequestModelBuilder(
  const std::string& restApiEndpointId) : mRestApiEndpointId(restApiEndpointId) {}

std::unique_ptr<CreateStageBulkRequestModel>
CreateStageRequestModelBuilder::buildFromJson(const std::string& json)
{
  std::unique_ptr<CreateStageBulkRequestModel> model(new
      CreateStageBulkRequestModel());
  std::unique_ptr<ValidationErrors> validationErrors(new ValidationErrors());
  Json::Value root;
  parseJson(json, root);

  try {
    mValidatorFactory.getObjectValidator()->validate(root);
  } catch (const ValidatorException& ex) {
    throw JsonValidationException("The root object of the input JSON must be an object");
  }

  Json::Value& files = root[FILES_KEY_NAME];

  try {
    mValidatorFactory.getNonEmptyArrayValidator()->validate(files);
  } catch (const ValidatorException& ex) {
    validationErrors->addError(FILES_KEY_NAME, ex.what());
    throw JsonValidationException(std::move(validationErrors));
  }

  for (auto& file : files) {
    try {
      mValidatorFactory.getObjectValidator()->validate(file);
    } catch (const ValidatorException& ex) {
      std::stringstream ss;
      ss << "The field must be an array of objects.";
      validationErrors->addError(FILES_KEY_NAME, ss.str());
      throw JsonValidationException(std::move(validationErrors));
    }

    Json::Value& path = file[PATH_KEY_NAME];

    try {
      mValidatorFactory.getPathValidator()->validate(path);
    } catch (const ValidatorException& ex) {
      std::stringstream ss;
      ss << "The field has not been provided or does not contain a valid string.";
      validationErrors->addError(PATH_KEY_NAME, ss.str());
      throw JsonValidationException(std::move(validationErrors));
    }

    Json::Value& targetedMetadata = file[TARGETED_METADATA_KEY_NAME];
    std::string opaqueInfos = "";

    if (!targetedMetadata.empty()) {
      // The targeted_metadata object is set, check the correctness of the targetedMetadata
      // provided, should just be an object
      try {
        mValidatorFactory.getObjectValidator()->validate(targetedMetadata);
      } catch (const ValidatorException& ex) {
        validationErrors->addError(TARGETED_METADATA_KEY_NAME, ex.what());
        throw JsonValidationException(std::move(validationErrors));
      }

      // let's see if there are metadata targeted to us
      Json::Value& myTargetedMetadata =
          targetedMetadata[mRestApiEndpointId].empty() ? targetedMetadata[TARGETED_METADATA_DEFAULT_ENDPOINT_KEY] : targetedMetadata[mRestApiEndpointId];

      if (!myTargetedMetadata.empty()) {
        //There are metadata for us
        //Check the correctness of the targeted metadata, should be an object
        try {
          mValidatorFactory.getObjectValidator()->validate(myTargetedMetadata);
        } catch (const ValidatorException& ex) {
          validationErrors->addError(mRestApiEndpointId, ex.what());
          throw JsonValidationException(std::move(validationErrors));
        }

        //Each metadata will be converted into an opaque info
        const auto metadataKeys = myTargetedMetadata.getMemberNames();

        //Get all the keys
        for (const auto& metadataKey : metadataKeys) {
          Json::Value& metadataValue = myTargetedMetadata[metadataKey];

          //Get the value, if it is convertible to a string then we add it to
          //the opaque infos of the file
          if (metadataValue.isConvertibleTo(Json::ValueType::stringValue)) {
            std::string opaqueInfoValue = metadataValue.asString();
            opaqueInfos += metadataKey + "=" + opaqueInfoValue + "&";
          } else {
            validationErrors->addError(metadataKey,
                                       "The value must be convertible into a string");
            throw JsonValidationException(std::move(validationErrors));
          }
        }

        //Remove the trailing "&" from the opaque infos
        if (opaqueInfos.size()) {
          opaqueInfos.pop_back();
        }
      }
    }

    model->addFile(path.asString(), opaqueInfos);
  }
  //copy-ellision here
  return model;
}

EOSMGMRESTNAMESPACE_END
