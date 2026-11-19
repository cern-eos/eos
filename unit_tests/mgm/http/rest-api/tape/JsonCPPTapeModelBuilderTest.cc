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
#include "mgm/http/rest-api/json/tape/TapeModelBuilders.hh"

using namespace eos::mgm::rest;

std::string JsonCPPTapeModelBuilderTest::restApiEndpointID = "REST_API_ENDPOINT_ID";

namespace {
// Paths with a "." or ".." component
const std::vector<std::string> kPathsWithDotComponents = {"/eos/in/../out/f",
                                                          "/eos/in/./f",
                                                          "/eos/in/..",
                                                          "/eos/in/.",
                                                          "/eos/in/../",
                                                          "/eos/in/./",
                                                          "/eos/in//../f",
                                                          "../eos/f",
                                                          "./eos/f",
                                                          "..",
                                                          "."};

// Paths whose components contain dots but are not "." or ".."
const std::vector<std::string> kPathsWithDotsInNames = {
    "/eos/in/.hidden", "/eos/in/a..b", "/eos/in/...",  "/eos/in/f.",
    "/eos/in/..f",     "/eos/in/.d/f", "/eos/in/d../f"};

std::string
toJson(const Json::Value& root)
{
  Json::StreamWriterBuilder builder;
  builder["indentation"] = "";
  return Json::writeString(builder, root);
}

// {"paths":["<path>"]}
std::string
pathsJson(const std::string& path)
{
  Json::Value root;
  root["paths"].append(path);
  return toJson(root);
}

// {"files":[{"path":"<path>"}]}
std::string
filesJson(const std::string& path)
{
  Json::Value file;
  file["path"] = path;
  Json::Value root;
  root["files"].append(file);
  return toJson(root);
}
} // namespace

TEST_F(JsonCPPTapeModelBuilderTest, createStageRequestModelBuilderTest_jsonNotValid) {
  CreateStageRequestModelBuilder builder(restApiEndpointID);
  std::string json = "jsonNotValid";
  ASSERT_THROW(builder.buildFromJson(json), JsonValidationException);
}

TEST_F(JsonCPPTapeModelBuilderTest, createStageRequestModelBuilderTest_emptyJson) {
  CreateStageRequestModelBuilder builder(restApiEndpointID);
  std::string json = "{}";
  ASSERT_THROW(builder.buildFromJson(json), JsonValidationException);
}

TEST_F(JsonCPPTapeModelBuilderTest, createStageRequestModelBuilderTest_wrongField)
{
  CreateStageRequestModelBuilder builder(restApiEndpointID);
  std::string json = "{\"wrong_field\":[]}";
  ASSERT_THROW(builder.buildFromJson(json), JsonValidationException);
}

TEST_F(JsonCPPTapeModelBuilderTest, createStageRequestModelBuilderTest_wrongFormat1)
{
  CreateStageRequestModelBuilder builder(restApiEndpointID);
  std::string json = "{\"" + CreateStageRequestModelBuilder::FILES_KEY_NAME + "\":12345}";
  ASSERT_THROW(builder.buildFromJson(json), JsonValidationException);
}

TEST_F(JsonCPPTapeModelBuilderTest, createStageRequestModelBuilderTest_wrongFormat2)
{
  CreateStageRequestModelBuilder builder(restApiEndpointID);
  std::string json = "{\"" + CreateStageRequestModelBuilder::FILES_KEY_NAME + "\":[]}";
  ASSERT_THROW(builder.buildFromJson(json), JsonValidationException);
}

TEST_F(JsonCPPTapeModelBuilderTest, createStageRequestModelBuilderTest_wrongFormat3)
{
  CreateStageRequestModelBuilder builder(restApiEndpointID);
  std::string json = "{\"" + CreateStageRequestModelBuilder::FILES_KEY_NAME + "\":[1,2,3]}";
  ASSERT_THROW(builder.buildFromJson(json), JsonValidationException);
}

TEST_F(JsonCPPTapeModelBuilderTest, createStageRequestModelBuilderTest_wrongFormat4)
{
  CreateStageRequestModelBuilder builder(restApiEndpointID);
  std::string json = "{\"" + CreateStageRequestModelBuilder::FILES_KEY_NAME + "\":[{\"" +
         CreateStageRequestModelBuilder::PATH_KEY_NAME + "\":1234}]";
  ASSERT_THROW(builder.buildFromJson(json), JsonValidationException);
}

TEST_F(JsonCPPTapeModelBuilderTest, createStageRequestModelBuilderTest_correctFormat)
{
  CreateStageRequestModelBuilder builder(restApiEndpointID);
  std::string json = "{\"" + CreateStageRequestModelBuilder::FILES_KEY_NAME + "\":[{\"" +
         CreateStageRequestModelBuilder::PATH_KEY_NAME +
         "\":\"/path/to/file.txt\"},{\"" +
         CreateStageRequestModelBuilder::PATH_KEY_NAME +
         "\":\"/path/to/file2.txt\"}]}";
  ASSERT_NO_THROW(builder.buildFromJson(json));
}

TEST_F(JsonCPPTapeModelBuilderTest, createStageRequestModelBuilderTest_activity_defaultEndpoint)
{
  CreateStageRequestModelBuilder builder(restApiEndpointID);
  std::string activityValue_default = "activityTest_default";
  std::ostringstream oss;
  oss << "{\"" << CreateStageRequestModelBuilder::FILES_KEY_NAME << "\": ["
      << "{"
      << "\"" << CreateStageRequestModelBuilder::PATH_KEY_NAME
      << "\": \"/path/to/file.txt\","
      << "\"" << CreateStageRequestModelBuilder::TARGETED_METADATA_KEY_NAME
      << "\": {"
      << "\"default\" : {"
      << "\"activity\":\"" << activityValue_default << "\""
      << "}"
      << "}"
      << "}"
      << "]}";
  std::string json = oss.str();
  auto createStageRequestModel = builder.buildFromJson(json);
  std::string expectedActivityOpaque = "activity=" + activityValue_default;
  ASSERT_EQ(expectedActivityOpaque,
            createStageRequestModel->getFiles().getOpaqueInfos().at(0));
}

TEST_F(JsonCPPTapeModelBuilderTest, createStageRequestModelBuilderTest_ignoresDiskLifetime)
{
  CreateStageRequestModelBuilder builder(restApiEndpointID);
  std::ostringstream oss;
  oss << "{\"" << CreateStageRequestModelBuilder::FILES_KEY_NAME << "\": ["
      << "{"
      << "\"" << CreateStageRequestModelBuilder::PATH_KEY_NAME
      << "\": \"/path/to/file.txt\","
      << "\"diskLifetime\": \"PT1H\""
      << "}"
      << "]}";
  auto createStageRequestModel = builder.buildFromJson(oss.str());
  ASSERT_EQ("", createStageRequestModel->getFiles().getOpaqueInfos().at(0));
}

TEST_F(JsonCPPTapeModelBuilderTest, pathsModelBuilderTest_pathsField)
{
  PathsModelBuilder builder;
  std::string json = R"({"paths":["/path/example.txt","/path/example2.txt"]})";
  auto model = builder.buildFromJson(json);
  ASSERT_EQ(2u, model->getFiles().getPaths().size());
  ASSERT_EQ("/path/example.txt", model->getFiles().getPaths().at(0));
  ASSERT_EQ("/path/example2.txt", model->getFiles().getPaths().at(1));
}

TEST_F(JsonCPPTapeModelBuilderTest, pathsModelBuilderTest_rejectsEmptyPaths)
{
  PathsModelBuilder builder;
  std::string json = R"({"paths":[]})";
  ASSERT_THROW(builder.buildFromJson(json), JsonValidationException);
}

TEST_F(JsonCPPTapeModelBuilderTest, createStageRequestModelBuilderTest_activity_normalEndpoint)
{
  CreateStageRequestModelBuilder builder(restApiEndpointID);
  std::string activityValue = "activityTest";
  std::string activityValue_default = "activityTest_default";
  std::ostringstream oss;
  oss << "{\"" << CreateStageRequestModelBuilder::FILES_KEY_NAME << "\": ["
      << "{"
      << "\"" << CreateStageRequestModelBuilder::PATH_KEY_NAME
      << "\": \"/path/to/file.txt\","
      << "\"" << CreateStageRequestModelBuilder::TARGETED_METADATA_KEY_NAME
      << "\": {"
      << "\"default\" : {"
      << "\"activity\":\"" << activityValue_default << "\""
      << "},"
      << "\"" << restApiEndpointID << "\" : {"
      << "\"activity\":\"" << activityValue << "\""
      << "}"
      << "}"
      << "}"
      << "]}";
  std::string json = oss.str();
  auto createStageRequestModel = builder.buildFromJson(json);
  std::string expectedActivityOpaque = "activity=" + activityValue;
  ASSERT_EQ(expectedActivityOpaque,
            createStageRequestModel->getFiles().getOpaqueInfos().at(0));
}

TEST_F(JsonCPPTapeModelBuilderTest, pathsModelBuilderTest_rejectsDotComponents)
{
  PathsModelBuilder builder;

  for (const auto& path : kPathsWithDotComponents) {
    EXPECT_THROW(builder.buildFromJson(pathsJson(path)), JsonValidationException)
        << "paths: " << path;
    EXPECT_THROW(builder.buildFromJson(filesJson(path)), JsonValidationException)
        << "files: " << path;
  }
}

TEST_F(JsonCPPTapeModelBuilderTest,
       pathsModelBuilderTest_rejectsDotComponentAmongValidPaths)
{
  PathsModelBuilder builder;
  const std::string json = R"({"paths":["/eos/in/f","/eos/in/../out/f","/eos/in/g"]})";
  ASSERT_THROW(builder.buildFromJson(json), JsonValidationException);
}

TEST_F(JsonCPPTapeModelBuilderTest, pathsModelBuilderTest_acceptsDotsInNames)
{
  PathsModelBuilder builder;

  for (const auto& path : kPathsWithDotsInNames) {
    std::unique_ptr<PathsModel> model;
    EXPECT_NO_THROW(model = builder.buildFromJson(pathsJson(path))) << "paths: " << path;

    if (model) {
      EXPECT_EQ(std::vector<std::string>{path}, model->getFiles().getPaths());
    }

    EXPECT_NO_THROW(builder.buildFromJson(filesJson(path))) << "files: " << path;
  }
}

TEST_F(JsonCPPTapeModelBuilderTest,
       createStageRequestModelBuilderTest_rejectsDotComponents)
{
  CreateStageRequestModelBuilder builder(restApiEndpointID);

  for (const auto& path : kPathsWithDotComponents) {
    EXPECT_THROW(builder.buildFromJson(filesJson(path)), JsonValidationException) << path;
  }
}

TEST_F(JsonCPPTapeModelBuilderTest, createStageRequestModelBuilderTest_acceptsDotsInNames)
{
  CreateStageRequestModelBuilder builder(restApiEndpointID);

  for (const auto& path : kPathsWithDotsInNames) {
    std::unique_ptr<CreateStageBulkRequestModel> model;
    EXPECT_NO_THROW(model = builder.buildFromJson(filesJson(path))) << path;

    if (model) {
      EXPECT_EQ(std::vector<std::string>{path}, model->getFiles().getPaths());
    }
  }
}

TEST_F(JsonCPPTapeModelBuilderTest, pathsModelBuilderTest_rejectsEmptyPath)
{
  PathsModelBuilder builder;
  EXPECT_THROW(builder.buildFromJson(pathsJson("")), JsonValidationException);
  EXPECT_THROW(builder.buildFromJson(filesJson("")), JsonValidationException);
}

TEST_F(JsonCPPTapeModelBuilderTest, createStageRequestModelBuilderTest_rejectsEmptyPath)
{
  CreateStageRequestModelBuilder builder(restApiEndpointID);
  EXPECT_THROW(builder.buildFromJson(filesJson("")), JsonValidationException);
}

// The prepare code would only see the part of a path before a NUL character
TEST_F(JsonCPPTapeModelBuilderTest, pathsModelBuilderTest_rejectsNulInPath)
{
  PathsModelBuilder builder;
  EXPECT_THROW(builder.buildFromJson(R"({"paths":["/eos/in/..\u0000"]})"),
               JsonValidationException);
  EXPECT_THROW(builder.buildFromJson(R"({"files":[{"path":"/eos/in/f\u0000x"}]})"),
               JsonValidationException);
}

TEST_F(JsonCPPTapeModelBuilderTest, createStageRequestModelBuilderTest_rejectsNulInPath)
{
  CreateStageRequestModelBuilder builder(restApiEndpointID);
  EXPECT_THROW(builder.buildFromJson(R"({"files":[{"path":"/eos/in/..\u0000"}]})"),
               JsonValidationException);
}
