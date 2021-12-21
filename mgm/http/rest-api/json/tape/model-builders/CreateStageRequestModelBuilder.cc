// ----------------------------------------------------------------------
// File: CreateStageRequestModelBuilder.hh
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

#include "CreateStageRequestModelBuilder.hh"

EOSMGMRESTNAMESPACE_BEGIN

std::unique_ptr<CreateStageBulkRequestModel> CreateStageRequestModelBuilder::buildFromJson(const std::string& json) const {
  std::unique_ptr<CreateStageBulkRequestModel> createStageBulkReq(new CreateStageBulkRequestModel());
  std::map<std::string,std::string> errors;
  Json::Value root;
  parseJson(json, root);
  Json::Value paths = root[CreateStageBulkRequestModel::PATHS_KEY_NAME];
  checkFieldNotNull(paths,CreateStageBulkRequestModel::PATHS_KEY_NAME);
  checkIsNotAnEmptyArray(paths,CreateStageBulkRequestModel::PATHS_KEY_NAME);
  for(auto path = paths.begin(); path != paths.end(); path++){
    std::ostringstream oss;
    oss << "The " << CreateStageBulkRequestModel::PATHS_KEY_NAME << " object should contain only strings";
    checkIsString(*path,oss.str());
    createStageBulkReq->addFile(path->asString(),"");
    //TODO in the future: metadata
  }
  return std::move(createStageBulkReq);
}

EOSMGMRESTNAMESPACE_END