// ----------------------------------------------------------------------
// File: JsonCPPTapeModelJsonifier.hh
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

#include "JsonCPPTapeModelJsonifier.hh"
#include "mgm/http/rest-api/model/tape/ErrorModel.hh"
#include "mgm/http/rest-api/model/tape/stage/CreatedStageBulkRequestResponseModel.hh"

EOSMGMRESTNAMESPACE_BEGIN

void JsonCPPTapeModelJsonifier::jsonify(const ErrorModel& errorModel, std::stringstream& oss){
  Json::Value root;
  root["type"] = errorModel.getType();
  root["title"] = errorModel.getTitle();
  root["status"] = errorModel.getStatus();
  root["detail"] = errorModel.getDetail() ? errorModel.getDetail().value() : "";
  oss << root;
}

void JsonCPPTapeModelJsonifier::jsonify(const CreatedStageBulkRequestResponseModel& createdStageBulkRequestModel, std::stringstream& oss) {
  Json::Value root;
  root["accessURL"] = createdStageBulkRequestModel.getAccessURL();
  Json::Reader reader;
  reader.parse(createdStageBulkRequestModel.getJsonRequest(),root["request"]);
  oss << root;
}

EOSMGMRESTNAMESPACE_END