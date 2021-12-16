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

#ifndef EOS_BULKJSONCPPOBJECT_HH
#define EOS_BULKJSONCPPOBJECT_HH

#include "mgm/Namespace.hh"
#include "common/json/JsonCppObject.hh"
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"

EOSBULKNAMESPACE_BEGIN

template<typename Obj>
class BulkJsonCppObject : public common::JsonCppObject<Obj> {
public:
  template<class... Args>
  BulkJsonCppObject(Args... args):common::JsonCppObject<Obj>(args...){}
  virtual void jsonify(std::stringstream & ss) override { common::JsonCppObject<Obj>::jsonify(ss); }
protected:
  template<typename SubObj>
  void jsonify(const SubObj & subObject, Json::Value & value) {}
};

template<>
template<>
inline void
BulkJsonCppObject<QueryPrepareResponse>::jsonify(const QueryPrepareFileResponse& fileResponse, Json::Value& value) {
  value["path"] = fileResponse.path;
  value["path_exists"] = fileResponse.is_exists;
  value["on_tape"] = fileResponse.is_on_tape;
  value["online"] = fileResponse.is_online;
  value["requested"] = fileResponse.is_requested;
  value["has_reqid"] = fileResponse.is_reqid_present;
  value["req_time"] = fileResponse.request_time;
  value["error_text"] = fileResponse.error_text;
}

template<>
inline void
BulkJsonCppObject<QueryPrepareResponse>::jsonify(std::stringstream &ss) {
  Json::Value root;
  root["request_id"] = mObject->request_id;
  root["responses"] = Json::arrayValue;
  for(const auto & fileResponse: mObject->responses){
    Json::Value response;
    jsonify(fileResponse,response);
    root["responses"].append(response);
  }
  ss << root;
}



EOSBULKNAMESPACE_END

#endif // EOS_BULKJSONCPPOBJECT_HH
