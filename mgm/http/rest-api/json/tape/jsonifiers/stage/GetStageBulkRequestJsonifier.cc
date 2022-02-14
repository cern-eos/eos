// ----------------------------------------------------------------------
// File: GetStageBulkRequestJsonifier.hh
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

#include "GetStageBulkRequestJsonifier.hh"

EOSMGMRESTNAMESPACE_BEGIN

void GetStageBulkRequestJsonifier::jsonify(const GetStageBulkRequestResponseModel* obj, std::stringstream& ss) {
  Json::Value root;
  root["createdAt"] = Json::UInt64(obj->getCreationTime());
  //Started time is the same as the creation time
  root["startedAt"] = Json::UInt64(obj->getCreationTime());
  initializeArray(root["files"]);
  Json::Value & files = root["files"];
  for(auto & file: obj->getFiles()) {
    Json::Value fileObj;
    fileObj["path"] = file->mPath;
    fileObj["error"] = file->mError;
    fileObj["onDisk"] = file->mOnDisk;
    files.append(fileObj);
  }
  ss << root;
}

EOSMGMRESTNAMESPACE_END