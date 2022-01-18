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
#include "common/Logging.hh"

EOSMGMRESTNAMESPACE_BEGIN

std::unique_ptr<CreateStageBulkRequestModel> CreateStageRequestModelBuilder::buildFromJson(const std::string& json) {
  std::unique_ptr<CreateStageBulkRequestModel> model(new CreateStageBulkRequestModel());
  std::unique_ptr<ValidationErrors> validationErrors(new ValidationErrors());
  Json::Value root;
  parseJson(json, root);
  Json::Value & paths = root[PATHS_KEY_NAME];
  try {
    mValidatorFactory.getNonEmptyArrayValidator()->validate(paths);
  } catch(const ValidatorException &ex) {
    validationErrors->addError(PATHS_KEY_NAME,ex.what());
    throw JsonValidationException(std::move(validationErrors));
  }
  for(auto path = paths.begin(); path != paths.end(); path++) {
    try {
      mValidatorFactory.getPathValidator()->validate(*path);
      model->addFile(path->asString(),"");
    } catch(const ValidatorException & ex) {
      std::stringstream ss;
      ss << "The value " << *path << " is not a correct path.";
      validationErrors->addError(PATHS_KEY_NAME,ss.str());
    }
  }
  if(validationErrors->hasAnyError()){
    throw JsonValidationException(std::move(validationErrors));
  }
  return std::move(model);
}

std::unique_ptr<CreateStageBulkRequestModel> CreateStageRequestWithFilesModelBuilder::buildFromJson(const std::string& json) {
  std::unique_ptr<CreateStageBulkRequestModel> model(new CreateStageBulkRequestModel());
  std::unique_ptr<ValidationErrors> validationErrors(new ValidationErrors());
  Json::Value root;
  parseJson(json, root);
  Json::Value & files = root[FILES_KEY_NAME];
  try {
    mValidatorFactory.getNonEmptyArrayValidator()->validate(files);
  } catch(const ValidatorException &ex) {
    validationErrors->addError(FILES_KEY_NAME,ex.what());
    throw JsonValidationException(std::move(validationErrors));
  }
  for(auto & file: files) {
    Json::Value & path = file[PATH_KEY_NAME];
    try {
      mValidatorFactory.getPathValidator()->validate(path);
    } catch(const ValidatorException & ex) {
      std::stringstream ss;
      ss << "The value " << path << " is not a correct path.";
      validationErrors->addError(PATH_KEY_NAME,ss.str());
    }
    Json::Value & targetedMetadata = file[TARGETED_METADATA_KEY_NAME];
    std::string opaqueInfos = "";
    if(!targetedMetadata.empty()) {
      // The targeted_metadata object is set, let's see if there are metadata
      // targeted to us
      //TODO: HARDCODED FOR TESTING, THE UNIQUE ID OF THE ENDPOINT MUST BE PASSED
      //VIA THE CONSTRUCTOR OF THIS CLASS
      Json::Value & myTargetedMetadata = targetedMetadata["localhost"];
      if(!myTargetedMetadata.empty()) {
        //There are metadata for us
        //Each metadata will be converted into an opaque info
        const auto metadataKeys = myTargetedMetadata.getMemberNames();
        //Get all the keys
        for(const auto & metadataKey: metadataKeys){
          Json::Value & metadataValue = myTargetedMetadata[metadataKey];
          //Get the value, if it is convertible to a string then we add it to
          //the opaque infos of the file
          if(metadataValue.isConvertibleTo(Json::ValueType::stringValue)){
            std::string opaqueInfoValue = metadataValue.asString();
            opaqueInfos += metadataKey + "=" + opaqueInfoValue + "&";
          } else {
            validationErrors->addError(metadataKey,"The value must be convertible into a string");
            continue;
          }
        }
        //Remove the trailing "&" from the opaque infos
        if(opaqueInfos.size())
          opaqueInfos.pop_back();
      }
    }
    model->addFile(path.asString(),opaqueInfos);
  }
  return std::move(model);
}

EOSMGMRESTNAMESPACE_END