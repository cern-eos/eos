// ----------------------------------------------------------------------
// File: JsonValidationErrorModelJsonifier.hh
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

#ifndef EOS_JSONVALIDATIONERRORMODELJSONIFIER_HH
#define EOS_JSONVALIDATIONERRORMODELJSONIFIER_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/json/tape/jsonifiers/common/ErrorModelJsonifier.hh"
#include "mgm/http/rest-api/model/tape/common/JsonValidationErrorModel.hh"

EOSMGMRESTNAMESPACE_BEGIN

class JsonValidationErrorModelJsonifier : public ErrorModelJsonifier, public TapeRestApiJsonifier<JsonValidationErrorModel>, public common::JsonCppJsonifier<JsonValidationErrorModel> {
public:
  void jsonify(const JsonValidationErrorModel * model, std::stringstream & ss) override;
protected:
  inline static const std::string VALIDATION_ERRORS_KEY = "validationErrors";
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_JSONVALIDATIONERRORMODELJSONIFIER_HH
