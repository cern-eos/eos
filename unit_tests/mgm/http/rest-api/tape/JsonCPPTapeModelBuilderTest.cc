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
#include "mgm/http/rest-api/exception/JsonValidationException.hh"
#include "mgm/http/rest-api/json/tape/model-builders/CreateStageRequestModelBuilder.hh"

using namespace eos::mgm::rest;

TEST_F(JsonCPPTapeModelBuilderTest,createStageRequestModelBuilderTest){
  std::string restApiEndpointID = "REST_API_ENDPOINT_ID";
  CreateStageRequestModelBuilder builder(restApiEndpointID);
  std::string json = "jsonNotValid";
  ASSERT_THROW(builder.buildFromJson(json),JsonValidationException);
  json = "{}";
  ASSERT_THROW(builder.buildFromJson(json), JsonValidationException);
  json = "{\"wrong_field\":[]}";
  ASSERT_THROW(builder.buildFromJson(json), JsonValidationException);
  json = "{\"" + CreateStageRequestModelBuilder::FILES_KEY_NAME + "\":12345}";
  ASSERT_THROW(builder.buildFromJson(json), JsonValidationException);
  json = "{\"" + CreateStageRequestModelBuilder::FILES_KEY_NAME + "\":[]}";
  ASSERT_THROW(builder.buildFromJson(json), JsonValidationException);
  json = "{\"" + CreateStageRequestModelBuilder::FILES_KEY_NAME + "\":[1,2,3]}";
  ASSERT_THROW(builder.buildFromJson(json), JsonValidationException);
  json = "{\"" + CreateStageRequestModelBuilder::FILES_KEY_NAME + "\":[{\"" + CreateStageRequestModelBuilder::PATH_KEY_NAME +"\":1234}]";
  ASSERT_THROW(builder.buildFromJson(json), JsonValidationException);
  json = "{\"" + CreateStageRequestModelBuilder::FILES_KEY_NAME + "\":[{\"" + CreateStageRequestModelBuilder::PATH_KEY_NAME + "\":\"/path/to/file.txt\"},{\"" + CreateStageRequestModelBuilder::PATH_KEY_NAME + "\":\"/path/to/file2.txt\"}]}";
  ASSERT_NO_THROW(builder.buildFromJson(json));
  std::string activityValue = "activityTest";
  std::ostringstream oss;
  oss << "{\""
      <<    CreateStageRequestModelBuilder::FILES_KEY_NAME << "\": ["
      <<      "{"
      <<          "\"" << CreateStageRequestModelBuilder::PATH_KEY_NAME << "\": \"/path/to/file.txt\","
      <<          "\"" << CreateStageRequestModelBuilder::TARGETED_METADATA_KEY_NAME <<  "\": {"
      <<             "\"" << restApiEndpointID << "\" : {"
      <<               "\"activity\":\"" <<  activityValue << "\""
      <<             "}"
      <<          "}"
      <<      "}"
      <<    "]"
      << "}";
  json = oss.str();
  auto createStageRequestModel = builder.buildFromJson(json);
  std::string expectedActivityOpaque = "activity=" + activityValue;
  ASSERT_EQ(expectedActivityOpaque,createStageRequestModel->getFiles().getOpaqueInfos().at(0));
}
