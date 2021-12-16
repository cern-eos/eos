/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

/************************************************************************
 * @file  QueryPrepareResponse.hh                                       *
 * @brief Struct to store "xrdfs query prepare" responses and           *
 *        serialize to JSON                                             *
 ************************************************************************/

#pragma once

#include "mgm/Namespace.hh"
#include <sstream>
#include <string>
#include <vector>

EOSBULKNAMESPACE_BEGIN

class QueryPrepareFileResponse {
public:

  QueryPrepareFileResponse() :
      is_exists(false), is_on_tape(false), is_online(false), is_requested(false), is_reqid_present(false) {}

  QueryPrepareFileResponse(const std::string _path) :
      path(_path), is_exists(false), is_on_tape(false), is_online(false), is_requested(false), is_reqid_present(false) {}

  // @ccaffy TODO: to be removed at the end of the bulk-request implementation
  friend std::ostream& operator<<(std::ostream& json, QueryPrepareFileResponse &qpr) {
    json << "{"
         << "\"path\":\""       << qpr.path << "\","
         << "\"path_exists\":"  << (qpr.is_exists        ? "true," : "false,")
         << "\"on_tape\":"      << (qpr.is_on_tape       ? "true," : "false,")
         << "\"online\":"       << (qpr.is_online        ? "true," : "false,")
         << "\"requested\":"    << (qpr.is_requested     ? "true," : "false,")
         << "\"has_reqid\":"    << (qpr.is_reqid_present ? "true," : "false,")
         << "\"req_time\":\""   << qpr.request_time << "\","
         << "\"error_text\":\"" << qpr.error_text << "\""
         << "}";
    return json;
  }

  //Path of the file
  std::string path;
  //Does it exist?
  bool is_exists;
  //Is it on tape?
  bool is_on_tape;
  //Is it on disk?
  bool is_online;
  //Is it currently requested?
  bool is_requested;
  //Is this file has a request id?
  bool is_reqid_present;
  //The time this file was requested
  std::string request_time;
  //The eventual error that the file encountered while being staged or archived
  std::string error_text;
};

/**
 * Class holding the information contained in the response
 * of a QueryPrepare query. This is the class that will be
 * returned to the user in json format
 */
class QueryPrepareResponse {
public:
  std::string request_id;
  std::vector<QueryPrepareFileResponse> responses;
};

EOSBULKNAMESPACE_END
