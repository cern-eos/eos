// ----------------------------------------------------------------------
// File: TapeRestApiV1JsonObject.hh
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
#ifndef EOS_TAPERESTAPIV1JSONOBJECT_HH
#define EOS_TAPERESTAPIV1JSONOBJECT_HH

#include "mgm/Namespace.hh"
#include "TapeRestApiJsonObject.hh"
#include "mgm/http/rest-api/model/tape/stage/CreatedStageBulkRequestResponseModel.hh"
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"

EOSMGMRESTNAMESPACE_BEGIN

template<typename Obj>
class TapeRestApiV1JsonObject : public TapeRestApiJsonObject<Obj>{
public:
  template<class... Args>
  TapeRestApiV1JsonObject(Args... args):TapeRestApiJsonObject<Obj>(args...){}
  virtual void jsonify(std::stringstream & ss) {
    TapeRestApiJsonObject<Obj>::jsonify(ss); };
};

template<>
inline void
TapeRestApiV1JsonObject<CreatedStageBulkRequestResponseModel>::jsonify(std::stringstream& ss) {
  Json::Value root;
  root["accessURL"] = mObject->getAccessURL();
  Json::Reader reader;
  reader.parse(mObject->getJsonRequest(),root["request"]);
  ss << root;
}

template<>
inline void
TapeRestApiV1JsonObject<bulk::QueryPrepareResponse>::jsonify(std::stringstream& ss) {
  Json::Value root;
  root = Json::Value(Json::arrayValue);
  for(auto response: mObject->responses) {
    Json::Value fileObj;
    fileObj["path"] = response.path;
    fileObj["error"] = response.error_text;
    fileObj["onDisk"] = response.is_online;
    fileObj["onTape"] = response.is_on_tape;
    root.append(fileObj);
  }
  ss << root;
}

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPERESTAPIV1JSONOBJECT_HH
