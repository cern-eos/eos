// ----------------------------------------------------------------------
// File: GetFileInfoResponseJsonifier.cc
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

#include "GetFileInfoResponseJsonifier.hh"

EOSMGMRESTNAMESPACE_BEGIN

void GetFileInfoResponseJsonifier::jsonify(const GetFileInfoResponseModel* obj, std::stringstream& ss) {
  Json::Value root;
  initializeArray(root);
  auto queryPrepareResponse = obj->getQueryPrepareResponse();
  for(const auto & queryPrepareFileResponse: queryPrepareResponse->responses) {
    Json::Value fileResponse;
    fileResponse["path"] = queryPrepareFileResponse.path;
    fileResponse["exists"] = queryPrepareFileResponse.is_exists;
    fileResponse["error"] = queryPrepareFileResponse.error_text;
    fileResponse["onDisk"] = queryPrepareFileResponse.is_online;
    fileResponse["onTape"] = queryPrepareFileResponse.is_on_tape;
    root.append(fileResponse);
  }
  ss << root;
}

EOSMGMRESTNAMESPACE_END