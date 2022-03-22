// ----------------------------------------------------------------------
// File: JsonCppValidator.hh
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

#ifndef EOS_JSONCPPVALIDATOR_HH
#define EOS_JSONCPPVALIDATOR_HH

#include "mgm/Namespace.hh"
#include <json/json.h>
#include <memory>
#include "common/exception/Exception.hh"

EOSMGMRESTNAMESPACE_BEGIN

/**
 * Global exception class for JSON validation exception
 */
class ValidatorException : public common::Exception
{
public:
  ValidatorException(const std::string& msg): common::Exception(msg) {}
};

/**
 * This abstract class should be inherited in order
 * to validate a specific JSON CPP Value
 */
class JsonCppValidator
{
public:
  virtual void validate(const Json::Value& value) = 0;
  virtual ~JsonCppValidator() {}
};

class NonEmptyArrayValidator : public JsonCppValidator
{
public:
  virtual void validate(const Json::Value& value) override
  {
    if (value.empty() || !value.isArray()) {
      throw ValidatorException("Field does not exist or is not a valid non-empty array.");
    }
  }
};

class StringValidator : public JsonCppValidator
{
public:
  virtual void validate(const Json::Value& value) override
  {
    if (!value.isString()) {
      throw ValidatorException("Field is not a valid string.");
    }
  }
};

class ObjectValidator : public JsonCppValidator
{
public:
  virtual void validate(const Json::Value& value) override
  {
    if (!value.isObject()) {
      throw ValidatorException("Field is not an object.");
    }
  }
};

/**
 * Factory of validators
 */
class JsonCppValidatorFactory
{
public:
  std::unique_ptr<JsonCppValidator> getNonEmptyArrayValidator()
  {
    return std::make_unique<NonEmptyArrayValidator>();
  }
  std::unique_ptr<JsonCppValidator> getStringValidator()
  {
    return std::make_unique<StringValidator>();
  }
  std::unique_ptr<JsonCppValidator> getNotNullValidator()
  {
    return std::make_unique<StringValidator>();
  }
  std::unique_ptr<JsonCppValidator> getObjectValidator()
  {
    return std::make_unique<ObjectValidator>();
  }
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_JSONCPPVALIDATOR_HH
