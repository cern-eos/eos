// ----------------------------------------------------------------------
// File: PathsModelBuilder.hh
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

#include "PathsModelBuilder.hh"

EOSMGMRESTNAMESPACE_BEGIN

std::unique_ptr<PathsModel>
PathsModelBuilder::buildFromJson(const std::string& json) const {
  std::unique_ptr<PathsModel> cancelStageBulkRequestModel(new PathsModel());
  Json::Value root;
  parseJson(json, root);
  Json::Value paths = root[PathsModel::PATHS_KEY_NAME];
  checkFieldNotNull(paths, PathsModel::PATHS_KEY_NAME);
  checkIsNotAnEmptyArray(paths, PathsModel::PATHS_KEY_NAME);
  for(auto path = paths.begin(); path != paths.end(); path++){
    std::ostringstream oss;
    oss << "The " << PathsModel::PATHS_KEY_NAME << " object should contain only strings";
    checkIsString(*path,oss.str());
    cancelStageBulkRequestModel->addFile(path->asString());
    //TODO in the future: metadata
  }
  return std::move(cancelStageBulkRequestModel);
}

EOSMGMRESTNAMESPACE_END