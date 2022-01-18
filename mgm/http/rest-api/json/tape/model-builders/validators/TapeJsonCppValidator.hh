// ----------------------------------------------------------------------
// File: TapeJsonCppValidator.hh
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


#ifndef EOS_TAPEJSONCPPVALIDATOR_HH
#define EOS_TAPEJSONCPPVALIDATOR_HH

#include "common/Path.hh"
#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/json/builder/jsoncpp/JsonCppValidator.hh"
#include "common/StringUtils.hh"

EOSMGMRESTNAMESPACE_BEGIN

class PathValidator : public JsonCppValidator {
public:
  void validate(const Json::Value & value) override {
    if(value.empty() || !value.isString() || value.asString().empty()) {
      throw ValidatorException("The value must be a valid non-empty string");
    }
  }
};

class TapeJsonCppValidatorFactory : public JsonCppValidatorFactory {
public:
  std::unique_ptr<JsonCppValidator> getPathValidator() {
    return std::make_unique<PathValidator>();
  }
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPEJSONCPPVALIDATOR_HH
