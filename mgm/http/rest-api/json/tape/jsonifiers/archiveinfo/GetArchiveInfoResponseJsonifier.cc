// ----------------------------------------------------------------------
// File: GetArchiveInfoResponseJsonifier.cc
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

#include "GetArchiveInfoResponseJsonifier.hh"

EOSMGMRESTNAMESPACE_BEGIN

void
GetArchiveInfoResponseJsonifier::jsonify(const GetArchiveInfoResponseModel* obj, std::stringstream& ss) {
  Json::Value root;
  initializeArray(root);
  auto queryPrepareResponse = obj->getQueryPrepareResponse();
  for(const auto & queryPrepareFileResponse: queryPrepareResponse->responses) {
    Json::Value fileResponse;
    fileResponse["path"] = queryPrepareFileResponse.path;
    std::string locality = "";
    if(queryPrepareFileResponse.is_online && queryPrepareFileResponse.is_on_tape) {
      locality = "DISK_AND_TAPE";
    } else if(queryPrepareFileResponse.is_online){
      locality = "DISK";
    } else if(queryPrepareFileResponse.is_on_tape){
      locality = "TAPE";
    }
    if(!locality.empty()) {
      fileResponse["locality"] = locality;
    }
    if(!queryPrepareFileResponse.error_text.empty()) {
      fileResponse["error"] = queryPrepareFileResponse.error_text;
    }
    root.append(fileResponse);
  }
  ss << root;
}

EOSMGMRESTNAMESPACE_END