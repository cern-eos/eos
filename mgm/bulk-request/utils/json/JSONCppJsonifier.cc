//------------------------------------------------------------------------------
//! @file JSONCppJsonifier.cc
//! @author Cedric Caffy - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "JSONCppJsonifier.hh"
#include <json/json.h>
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"

EOSBULKNAMESPACE_BEGIN

void JSONCppJsonifier::jsonify(const QueryPrepareResponse & response, std::stringstream & oss) {
  Json::Value root;
  root["request_id"] = response.request_id;
  for(const auto & fileResponse: response.responses){
    Json::Value fileResponseJson;
    fileResponseJson["path"] = fileResponse.path;
    fileResponseJson["path_exists"] = fileResponse.is_exists;
    fileResponseJson["on_tape"] = fileResponse.is_on_tape;
    fileResponseJson["online"] = fileResponse.is_online;
    fileResponseJson["requested"] = fileResponse.is_requested;
    fileResponseJson["has_reqid"] = fileResponse.is_reqid_present;
    fileResponseJson["req_time"] = fileResponse.request_time;
    fileResponseJson["error_text"] = fileResponse.error_text;
    root["responses"].append(fileResponseJson);
  }
  oss << root;
}

EOSBULKNAMESPACE_END
