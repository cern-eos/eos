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

std::unique_ptr<PathsModel> PathsModelBuilder::buildFromJson(const std::string& json) {
  std::unique_ptr<PathsModel> model(new PathsModel());
  std::unique_ptr<ValidationErrors> validationErrors(new ValidationErrors());
  Json::Value root;
  parseJson(json, root);
  try {
    mValidatorFactory.getObjectValidator()->validate(root);
  } catch(const ValidatorException &ex) {
    throw JsonValidationException("The root object of the input JSON must be an object");
  }
  Json::Value & paths = root[PATHS_KEY_NAME];
  try {
    mValidatorFactory.getNonEmptyArrayValidator()->validate(paths);
  } catch(const ValidatorException &ex) {
    validationErrors->addError(PATHS_KEY_NAME, ex.what());
    throw JsonValidationException(std::move(validationErrors));
  }
  for(auto path = paths.begin(); path != paths.end(); path++){
    try {
      mValidatorFactory.getPathValidator()->validate(*path);
      model->addFile(path->asString());
    } catch(const ValidatorException & ex) {
      std::stringstream ss;
      ss << "The value " << *path << " is not a correct path.";
      validationErrors->addError(PATHS_KEY_NAME,ss.str());
    }
  }
  if(validationErrors->hasAnyError()) {
    throw JsonValidationException(std::move(validationErrors));
  }
  return std::move(model);
}

EOSMGMRESTNAMESPACE_END