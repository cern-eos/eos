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

std::shared_ptr<CreateStageBulkRequestModel> JsonCPPTapeModelBuilder::buildCreateStageBulkRequestModel(const std::string & json){
  std::shared_ptr<CreateStageBulkRequestModel> createStageBulkReq(new CreateStageBulkRequestModel());
  std::map<std::string,std::string> errors;
  Json::Value root;
  parseJson(json, root);
  Json::Value paths = root[CreateStageBulkRequestModel::PATHS_KEY_NAME];
  if(paths.isNull()){
    std::ostringstream oss;
    oss << "No " << CreateStageBulkRequestModel::PATHS_KEY_NAME << " attribute has been provided" ;
    throw JsonObjectModelMalformedException(oss.str());
  }
  if(!paths.isArray() || paths.empty()) {
    std::ostringstream oss;
    oss << "The " << CreateStageBulkRequestModel::PATHS_KEY_NAME << " attribute should be a non-empty array of file paths";
    throw JsonObjectModelMalformedException(oss.str());
  }
  for(auto path = paths.begin(); path != paths.end(); path++){
    if(!path->isString()){
      std::ostringstream oss;
      oss << "The " << CreateStageBulkRequestModel::PATHS_KEY_NAME << " object should contain only string";
      throw JsonObjectModelMalformedException(oss.str());
    }
    createStageBulkReq->addPath(path->asString());
  }
  //TODO in the future: metadata
  return createStageBulkReq;
}

void JsonCPPTapeModelBuilder::parseJson(const std::string& json, Json::Value& root){
  Json::Reader reader;
  bool parsingSuccessful = reader.parse(json,root);
  if(!parsingSuccessful){
    std::ostringstream oss;
    oss << "Unable to create a JSON object from the json string provided. json=" << json;
    throw InvalidJSONException(oss.str());
  }
}

EOSMGMRESTNAMESPACE_END