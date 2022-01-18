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
#include "mgm/http/rest-api/json/builder/JsonModelBuilder.hh"
#include <json/json.h>
#include <sstream>
#include "mgm/http/rest-api/exception/JsonValidationException.hh"
#include "mgm/http/rest-api/json/builder/ValidationError.hh"

EOSMGMRESTNAMESPACE_BEGIN

template<typename Model>
class JsonCppModelBuilder : public JsonModelBuilder<Model> {
public:
  virtual std::unique_ptr<Model> buildFromJson(const std::string & json) = 0;
protected:
  /**
  * Parses the json string passed in parameter and
  * create JsonCpp-related object out of it
  * @param json the string representing the object
  * @param root the JsonCpp root object
  * @throws JsonValidationException if the parsing could not be done
   */
  void parseJson(const std::string& json, Json::Value& root) const{
    Json::Reader reader;
    bool parsingSuccessful = reader.parse(json,root);
    if(!parsingSuccessful){
      std::ostringstream oss;
      oss << "Unable to create a JSON object from the json string provided. json=" << json;
      throw JsonValidationException(oss.str());
    }
  }
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_JSONCPPMODELBUILDER_HH
