//------------------------------------------------------------------------------
//! @file JsonCPPTapeModelBuilderTest.cc
//! @author Cedric Caffy - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "JsonCPPTapeModelBuilderTest.hh"
#include "mgm/http/rest-api/exception/InvalidJSONException.hh"
#include "mgm/http/rest-api/exception/JsonObjectModelMalformedException.hh"
#include "mgm/http/rest-api/json/tape/model-builders/CreateStageRequestModelBuilder.hh"

using namespace eos::mgm::rest;

TEST_F(JsonCPPTapeModelBuilderTest,createStageRequestModelBuilderTest){
  CreateStageRequestModelBuilder builder;
  std::string json = "jsonNotValid";
  ASSERT_THROW(builder.buildFromJson(json),InvalidJSONException);
  json = "{}";
  ASSERT_THROW(builder.buildFromJson(json),JsonObjectModelMalformedException);
  json = "{\"" + CreateStageBulkRequestModel::PATHS_KEY_NAME + "\":12345}";
  ASSERT_THROW(builder.buildFromJson(json),JsonObjectModelMalformedException);
  json = "{\"" + CreateStageBulkRequestModel::PATHS_KEY_NAME + "\":[]}";
  ASSERT_THROW(builder.buildFromJson(json),JsonObjectModelMalformedException);
  json = "{\"" + CreateStageBulkRequestModel::PATHS_KEY_NAME + "\":[1,2,3]}";
  ASSERT_THROW(builder.buildFromJson(json),JsonObjectModelMalformedException);
  json = "{\"" + CreateStageBulkRequestModel::PATHS_KEY_NAME + "\":[\"/path/to/file.txt\",\"/path/to/file2.txt\"]}";
  ASSERT_NO_THROW(builder.buildFromJson(json));
}
