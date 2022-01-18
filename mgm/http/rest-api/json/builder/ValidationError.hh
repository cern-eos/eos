// ----------------------------------------------------------------------
// File: ValidationError.hh
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

#ifndef EOS_VALIDATIONERROR_HH
#define EOS_VALIDATIONERROR_HH

#include "mgm/Namespace.hh"
#include <string>
#include <vector>
#include <memory>

EOSMGMRESTNAMESPACE_BEGIN

class ValidationError {
public:
  ValidationError(const std::string & fieldName, const std::string & reason):mFieldName(fieldName),mReason(reason){}
  typedef std::vector<std::unique_ptr<ValidationError>> List;
  inline const std::string & getFieldName() const { return mFieldName; }
  inline const std::string & getReason() const {return mReason;}
private:
  std::string mFieldName;
  std::string mReason;
};

class ValidationErrors {
public:
  typedef std::vector<std::unique_ptr<ValidationError>> ErrorVector;
  ValidationErrors(){ mErrors.reset(new ErrorVector()); }
  inline void addError(const std::string & fieldName, const std::string & reason) {
    mErrors->push_back(std::make_unique<ValidationError>(fieldName,reason));
  }
  inline const ErrorVector * getErrors() const {
    return mErrors.get();
  }
  inline bool hasAnyError() const { return !mErrors->empty(); }
private:
  std::unique_ptr<ErrorVector> mErrors;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_VALIDATIONERROR_HH
