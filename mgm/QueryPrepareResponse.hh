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

EOSMGMNAMESPACE_BEGIN

struct QueryPrepareResponse {
  QueryPrepareResponse() :
    is_exists(false), is_on_tape(false), is_online(false), is_requested(false), is_reqid_present(false) {}

  QueryPrepareResponse(const std::string _path) :
    path(_path), is_exists(false), is_on_tape(false), is_online(false), is_requested(false), is_reqid_present(false) {}

  friend std::ostream& operator<<(std::ostream& json, QueryPrepareResponse &qpr) {
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

  std::string path;
  bool is_exists;
  bool is_on_tape;
  bool is_online;
  bool is_requested;
  bool is_reqid_present;
  std::string request_time;
  std::string error_text;
};

EOSMGMNAMESPACE_END
