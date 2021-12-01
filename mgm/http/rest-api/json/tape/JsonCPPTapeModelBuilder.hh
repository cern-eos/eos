// ----------------------------------------------------------------------
// File: JsonCPPTapeModelBuilder.hh
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

#ifndef EOS_JSONCPPTAPEMODELBUILDER_HH
#define EOS_JSONCPPTAPEMODELBUILDER_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/json/tape/JsonTapeModelBuilder.hh"

EOSMGMRESTNAMESPACE_BEGIN

/**
 * JsonCPP-specific tape-rest-api model object builder
 */
class JsonCPPTapeModelBuilder : public JsonTapeModelBuilder {
public:
  virtual std::unique_ptr<CreateStageBulkRequestModel> buildCreateStageBulkRequestModel(const std::string & json) override;
  virtual std::unique_ptr<CancelStageBulkRequestModel> buildCancelStageBulkRequestModel(const std::string &json) override;
private:
  /**
   * Parses the json string passed in parameter and
   * create JsonCpp-related object out of it
   * @param json the string representing the object
   * @param root the JsonCpp root object
   * @throws InvalidJsonException if the parsing could not be done
   */
  void parseJson(const std::string & json,Json::Value & root) const;

  void checkFieldNotNull(const Json::Value & value, const std::string & fieldName) const;

  void checkNotNull(const Json::Value & value, const std::string & errorMsg) const;

  void checkFieldIsNotAnEmptyArray(const Json::Value & value, const std::string & fieldName) const;

  void checkIsNotAnEmptyArray(const Json::Value & value, const std::string & errorMsg) const;

  void checkIsString(const Json::Value & value, const std::string & errorMsg) const;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_JSONCPPTAPEMODELBUILDER_HH
