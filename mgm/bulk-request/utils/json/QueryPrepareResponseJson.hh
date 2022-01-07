// ----------------------------------------------------------------------
// File: BulkJsonCppObject.hh
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

#ifndef EOS_QUERYPREPARERESPONSEJSON_HH
#define EOS_QUERYPREPARERESPONSEJSON_HH

#include "mgm/Namespace.hh"
#include "common/json/JsonCppJsonifier.hh"
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"

EOSBULKNAMESPACE_BEGIN

class QueryPrepareResponseJson : public common::JsonCppJsonifier<QueryPrepareResponse> {
public:
  virtual void jsonify(const QueryPrepareResponse * obj, std::stringstream & ss) override;
private:
  void jsonify(const QueryPrepareFileResponse & fileResponse,Json::Value & value);
};


void QueryPrepareResponseJson::jsonify(const QueryPrepareResponse * obj,std::stringstream &ss) {
  Json::Value root;
  root["request_id"] = obj->request_id;
  root["responses"] = Json::arrayValue;
  for(const auto & fileResponse: obj->responses){
    Json::Value response;
    jsonify(fileResponse,response);
    root["responses"].append(response);
  }
  ss << root;
}

void QueryPrepareResponseJson::jsonify(const QueryPrepareFileResponse & fileResponse, Json::Value& value) {
  value["path"] = fileResponse.path;
  value["path_exists"] = fileResponse.is_exists;
  value["on_tape"] = fileResponse.is_on_tape;
  value["online"] = fileResponse.is_online;
  value["requested"] = fileResponse.is_requested;
  value["has_reqid"] = fileResponse.is_reqid_present;
  value["req_time"] = fileResponse.request_time;
  value["error_text"] = fileResponse.error_text;
}


EOSBULKNAMESPACE_END

#endif // EOS_QUERYPREPARERESPONSEJSON_HH
