// ----------------------------------------------------------------------
// File: ObjectModelMalformedException.hh
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

#ifndef EOS_JSONVALIDATIONEXCEPTION_HH
#define EOS_JSONVALIDATIONEXCEPTION_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/exception/RestException.hh"
#include "mgm/http/rest-api/json/builder/ValidationError.hh"

EOSMGMRESTNAMESPACE_BEGIN

/**
 * Exception class to use when a json string cannot allow to instanciate
 * a Model object (wrong field names, invalid JSON format...)
 */
class JsonValidationException : public RestException {
public:
  JsonValidationException(const std::string & exceptionMsg);
  JsonValidationException(std::unique_ptr<ValidationErrors> && validationErrors);
  inline const ValidationErrors * getValidationErrors() const { return mValidationErrors.get(); }
  inline std::unique_ptr<ValidationErrors> getValidationErrors() { return std::move(mValidationErrors); }
protected:
  std::unique_ptr<ValidationErrors> mValidationErrors;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_JSONVALIDATIONEXCEPTION_HH
