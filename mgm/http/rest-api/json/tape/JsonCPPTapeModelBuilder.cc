// ----------------------------------------------------------------------
// File: JsonCPPTapeModelBuilder.cc
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

#include "JsonCPPTapeModelBuilder.hh"
#include "mgm/http/rest-api/exception/InvalidJSONException.hh"
#include "mgm/http/rest-api/exception/JsonObjectModelMalformedException.hh"
#include <map>
#include <sstream>
#include <iostream>

EOSMGMRESTNAMESPACE_BEGIN

std::unique_ptr<CreateStageBulkRequestModel> JsonCPPTapeModelBuilder::buildCreateStageBulkRequestModel(const std::string & json) {
  std::unique_ptr<CreateStageBulkRequestModel> createStageBulkReq(new CreateStageBulkRequestModel());
  std::map<std::string,std::string> errors;
  Json::Value root;
  parseJson(json, root);
  Json::Value paths = root[CreateStageBulkRequestModel::PATHS_KEY_NAME];
  checkFieldNotNull(paths,CreateStageBulkRequestModel::PATHS_KEY_NAME);
  checkIsNotAnEmptyArray(paths,CreateStageBulkRequestModel::PATHS_KEY_NAME);
  for(auto path = paths.begin(); path != paths.end(); path++){
    std::ostringstream oss;
    oss << "The " << CreateStageBulkRequestModel::PATHS_KEY_NAME << " object should contain only strings";
    checkIsString(*path,oss.str());
    createStageBulkReq->addFile(path->asString(),"");
    //TODO in the future: metadata
  }
  return std::move(createStageBulkReq);
}

std::unique_ptr<CancelStageBulkRequestModel> JsonCPPTapeModelBuilder::buildCancelStageBulkRequestModel(const std::string& json) {
  std::unique_ptr<CancelStageBulkRequestModel> cancelStageBulkRequestModel(new CancelStageBulkRequestModel());
  Json::Value root;
  parseJson(json, root);
  Json::Value paths = root[CancelStageBulkRequestModel::PATHS_KEY_NAME];
  checkFieldNotNull(paths,CreateStageBulkRequestModel::PATHS_KEY_NAME);
  checkIsNotAnEmptyArray(paths,CreateStageBulkRequestModel::PATHS_KEY_NAME);
  for(auto path = paths.begin(); path != paths.end(); path++){
    std::ostringstream oss;
    oss << "The " << CreateStageBulkRequestModel::PATHS_KEY_NAME << " object should contain only strings";
    checkIsString(*path,oss.str());
    cancelStageBulkRequestModel->addFile(path->asString());
    //TODO in the future: metadata
  }
  return std::move(cancelStageBulkRequestModel);
}

void JsonCPPTapeModelBuilder::parseJson(const std::string& json, Json::Value& root) const{
  Json::Reader reader;
  bool parsingSuccessful = reader.parse(json,root);
  if(!parsingSuccessful){
    std::ostringstream oss;
    oss << "Unable to create a JSON object from the json string provided. json=" << json;
    throw InvalidJSONException(oss.str());
  }
}

void JsonCPPTapeModelBuilder::checkNotNull(const Json::Value& value, const std::string& errorMsg) const{
  if(value.isNull()) {
    throw JsonObjectModelMalformedException(errorMsg);
  }
}

void JsonCPPTapeModelBuilder::checkFieldNotNull(const Json::Value& value, const std::string& fieldName) const {
  std::ostringstream oss;
  oss << "No " << fieldName << " attribute provided";
  checkNotNull(value,oss.str());
}

void JsonCPPTapeModelBuilder::checkFieldIsNotAnEmptyArray(const Json::Value& value, const std::string& fieldName) const {
  std::ostringstream oss;
  oss << "The " << fieldName << " attribute should be a non-empty array";
  checkIsNotAnEmptyArray(value,oss.str());
}

void JsonCPPTapeModelBuilder::checkIsNotAnEmptyArray(const Json::Value& value, const std::string& errorMsg) const {
  if(!value.isArray() || value.empty()) {
    throw JsonObjectModelMalformedException(errorMsg);
  }
}

void JsonCPPTapeModelBuilder::checkIsString(const Json::Value& value, const std::string& errorMsg) const {
  if(!value.isString()){
    throw JsonObjectModelMalformedException(errorMsg);
  }
}

EOSMGMRESTNAMESPACE_END