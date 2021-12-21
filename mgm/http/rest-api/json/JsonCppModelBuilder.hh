// ----------------------------------------------------------------------
// File: JsonCppModelBuilder.hh
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

#ifndef EOS_JSONCPPMODELBUILDER_HH
#define EOS_JSONCPPMODELBUILDER_HH

#include "mgm/Namespace.hh"
#include "ModelBuilder.hh"
#include <json/json.h>
#include <sstream>
#include "mgm/http/rest-api/exception/InvalidJSONException.hh"
#include "mgm/http/rest-api/exception/JsonObjectModelMalformedException.hh"

EOSMGMRESTNAMESPACE_BEGIN

template<typename Model>
class JsonCppModelBuilder : public ModelBuilder<Model> {
public:
  virtual std::unique_ptr<Model> buildFromJson(const std::string & json) const = 0;
protected:
  /**
  * Parses the json string passed in parameter and
  * create JsonCpp-related object out of it
  * @param json the string representing the object
  * @param root the JsonCpp root object
  * @throws InvalidJsonException if the parsing could not be done
   */
  void parseJson(const std::string& json, Json::Value& root) const{
    Json::Reader reader;
    bool parsingSuccessful = reader.parse(json,root);
    if(!parsingSuccessful){
      std::ostringstream oss;
      oss << "Unable to create a JSON object from the json string provided. json=" << json;
      throw InvalidJSONException(oss.str());
    }
  }

  void checkNotNull(const Json::Value& value, const std::string& errorMsg) const{
    if(value.isNull()) {
      throw JsonObjectModelMalformedException(errorMsg);
    }
  }

  void checkFieldNotNull(const Json::Value& value, const std::string& fieldName) const {
    std::ostringstream oss;
    oss << "No " << fieldName << " attribute provided";
    checkNotNull(value,oss.str());
  }

  void checkFieldIsNotAnEmptyArray(const Json::Value& value, const std::string& fieldName) const {
    std::ostringstream oss;
    oss << "The " << fieldName << " attribute should be a non-empty array";
    checkIsNotAnEmptyArray(value,oss.str());
  }

  void checkIsNotAnEmptyArray(const Json::Value& value, const std::string& errorMsg) const {
    if(!value.isArray() || value.empty()) {
      throw JsonObjectModelMalformedException(errorMsg);
    }
  }

  void checkIsString(const Json::Value& value, const std::string& errorMsg) const {
    if(!value.isString()){
      throw JsonObjectModelMalformedException(errorMsg);
    }
  }
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_JSONCPPMODELBUILDER_HH
