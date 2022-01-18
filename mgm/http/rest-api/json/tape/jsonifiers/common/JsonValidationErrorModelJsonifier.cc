// ----------------------------------------------------------------------
// File: JsonValidationErrorModelJsonifier.cc
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

#include "JsonValidationErrorModelJsonifier.hh"

EOSMGMRESTNAMESPACE_BEGIN

void JsonValidationErrorModelJsonifier::jsonify(const JsonValidationErrorModel * model, std::stringstream & ss) {
  Json::Value root;
  ErrorModelJsonifier::jsonify(model,root);
  const ValidationErrors * errors = model->getValidationErrors();
  if(errors != nullptr) {
    root[VALIDATION_ERRORS_KEY] = Json::Value(Json::arrayValue);
    for(const auto & validationError: *(errors->getErrors())) {
      Json::Value errorItem;
      errorItem["name"] = validationError->getFieldName();
      errorItem["reason"] = validationError->getReason();
      root[VALIDATION_ERRORS_KEY].append(errorItem);
    }
  }
  ss << root;
}
EOSMGMRESTNAMESPACE_END
