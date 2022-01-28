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

class JsonCPPTapeModelBuilder : public JsonTapeModelBuilder {
public:
  virtual std::shared_ptr<CreateStageBulkRequestModel> buildCreateStageBulkRequestModel(const std::string & json) override;
private:
  void parseJson(const std::string & json,Json::Value & root);
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_JSONCPPTAPEMODELBUILDER_HH
