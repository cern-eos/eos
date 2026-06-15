//------------------------------------------------------------------------------
//! @file TapeJsonifierTest.cc
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

#include "TapeJsonifierTest.hh"
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"
#include "mgm/http/rest-api/json/tape/TapeJsonifiers.hh"
#include "mgm/http/rest-api/model/tape/archiveinfo/GetArchiveInfoResponseModel.hh"
#include "mgm/http/rest-api/model/tape/common/ErrorModel.hh"
#include "mgm/http/rest-api/model/tape/stage/CreatedStageBulkRequestResponseModel.hh"
#include "mgm/http/rest-api/model/tape/stage/GetStageBulkRequestResponseModel.hh"
#include "mgm/http/rest-api/model/wellknown/tape/GetTapeWellKnownModel.hh"
#include "mgm/http/rest-api/wellknown/tape/TapeWellKnownInfos.hh"

using namespace eos::mgm::rest;

TEST_F(TapeJsonifierTest, wellKnownJsonifierWithoutDescription)
{
  TapeWellKnownInfos infos("cern-prod-tape-atlas");
  infos.addEndpoint("https://tape-api.example.org:1234/api/v1", "v1");

  GetTapeWellKnownModel model(&infos);
  model.setJsonifier(std::make_shared<GetTapeWellKnownModelJsonifier>());

  const Json::Value root = parseJson(toJson(model));
  ASSERT_TRUE(root.isObject());
  ASSERT_EQ("cern-prod-tape-atlas", root["sitename"].asString());
  ASSERT_FALSE(root.isMember("description"));
  ASSERT_TRUE(root["endpoints"].isArray());
  ASSERT_EQ(1u, root["endpoints"].size());
  ASSERT_EQ("https://tape-api.example.org:1234/api/v1",
            root["endpoints"][0]["uri"].asString());
  ASSERT_EQ("v1", root["endpoints"][0]["version"].asString());
}

TEST_F(TapeJsonifierTest, wellKnownJsonifierWithDescription)
{
  TapeWellKnownInfos infos("cern-prod-tape-atlas",
                           "This is the CERN tape REST API endpoint for CTA ATLAS");
  infos.addEndpoint("https://tape-api.example.org:1234/api/v1", "v1");

  GetTapeWellKnownModel model(&infos);
  model.setJsonifier(std::make_shared<GetTapeWellKnownModelJsonifier>());

  const Json::Value root = parseJson(toJson(model));
  ASSERT_EQ("This is the CERN tape REST API endpoint for CTA ATLAS",
            root["description"].asString());
}

TEST_F(TapeJsonifierTest, wellKnownJsonifierMultipleEndpoints)
{
  TapeWellKnownInfos infos("test-site");
  infos.addEndpoint("https://tape-api.example.org:1234/api/v1", "v1");
  infos.addEndpoint("https://tape-api.example.org:1234/api/v0.1", "v0.1");

  GetTapeWellKnownModel model(&infos);
  model.setJsonifier(std::make_shared<GetTapeWellKnownModelJsonifier>());

  const Json::Value root = parseJson(toJson(model));
  ASSERT_EQ(2u, root["endpoints"].size());
  ASSERT_EQ("v1", root["endpoints"][0]["version"].asString());
  ASSERT_EQ("v0.1", root["endpoints"][1]["version"].asString());
}

TEST_F(TapeJsonifierTest, createdStageJsonifierUsesRequestId)
{
  CreatedStageBulkRequestResponseModel model("e2b8f2b0-9952-11ec-9c53-fa163e0f6dc7");
  model.setJsonifier(std::make_shared<CreatedStageBulkRequestJsonifier>());

  const Json::Value root = parseJson(toJson(model));
  ASSERT_EQ("e2b8f2b0-9952-11ec-9c53-fa163e0f6dc7", root["requestId"].asString());
  ASSERT_FALSE(root.isMember("request_id"));
}

TEST_F(TapeJsonifierTest, getStageJsonifierInProgressResponse)
{
  GetStageBulkRequestResponseModel model;
  model.setId("93be38df-435c-4322-801d-b95e77ac5bbc");
  model.setCreatedAt(1646305430);
  model.setStartedAt(1646305456);

  auto fileOnDisk = std::make_unique<GetStageBulkRequestResponseModel::File>();
  fileOnDisk->mPath = "/test/file.txt";
  fileOnDisk->mOnDisk = true;
  model.addFile(std::move(fileOnDisk));

  auto filePending = std::make_unique<GetStageBulkRequestResponseModel::File>();
  filePending->mPath = "/test/file2.txt";
  filePending->mOnDisk = false;
  filePending->mError = "Tape backend is unreachable";
  model.addFile(std::move(filePending));

  model.setJsonifier(std::make_shared<GetStageBulkRequestJsonifier>());

  const Json::Value root = parseJson(toJson(model));
  ASSERT_EQ("93be38df-435c-4322-801d-b95e77ac5bbc", root["id"].asString());
  ASSERT_EQ(1646305430, root["createdAt"].asInt());
  ASSERT_EQ(1646305456, root["startedAt"].asInt());
  ASSERT_FALSE(root.isMember("completedAt"));
  ASSERT_FALSE(root.isMember("creation_time"));

  ASSERT_EQ(2u, root["files"].size());
  ASSERT_TRUE(root["files"][0]["onDisk"].asBool());
  ASSERT_FALSE(root["files"][0].isMember("state"));
  ASSERT_FALSE(root["files"][1]["onDisk"].asBool());
  ASSERT_EQ("Tape backend is unreachable", root["files"][1]["error"].asString());
  ASSERT_FALSE(root["files"][1].isMember("state"));
}

TEST_F(TapeJsonifierTest, getStageJsonifierTerminalResponse)
{
  GetStageBulkRequestResponseModel model;
  model.setId("93be38df-435c-4322-801d-b95e77ac5bbc");
  model.setCreatedAt(1646305430);
  model.setStartedAt(1646305456);

  auto completedFile = std::make_unique<GetStageBulkRequestResponseModel::File>();
  completedFile->mPath = "/test/file.txt";
  completedFile->mState = "COMPLETED";
  completedFile->mStartedAt = 1646305470;
  completedFile->mFinishedAt = 1646306000;
  model.addFile(std::move(completedFile));

  auto failedFile = std::make_unique<GetStageBulkRequestResponseModel::File>();
  failedFile->mPath = "/test/file2.txt";
  failedFile->mState = "FAILED";
  failedFile->mStartedAt = 1646305456;
  failedFile->mFinishedAt = 1646305456;
  failedFile->mError = "Tape backend is unreachable";
  model.addFile(std::move(failedFile));

  model.setJsonifier(std::make_shared<GetStageBulkRequestJsonifier>());

  const Json::Value root = parseJson(toJson(model));
  ASSERT_FALSE(root.isMember("completedAt"));
  ASSERT_EQ("COMPLETED", root["files"][0]["state"].asString());
  ASSERT_EQ(1646305470, root["files"][0]["startedAt"].asInt());
  ASSERT_EQ(1646306000, root["files"][0]["finishedAt"].asInt());
  ASSERT_FALSE(root["files"][0].isMember("onDisk"));
  ASSERT_EQ("FAILED", root["files"][1]["state"].asString());
  ASSERT_EQ("Tape backend is unreachable", root["files"][1]["error"].asString());
  ASSERT_FALSE(root["files"][1].isMember("onDisk"));
}

TEST_F(TapeJsonifierTest, archiveInfoJsonifierReturnsArrayWithLocality)
{
  auto queryResponse = std::make_shared<eos::mgm::bulk::QueryPrepareResponse>();
  eos::mgm::bulk::QueryPrepareFileResponse onDiskAndTape("/eos/ccaffy/tape/test.txt");
  onDiskAndTape.is_exists = true;
  onDiskAndTape.is_online = true;
  onDiskAndTape.is_on_tape = true;
  queryResponse->responses.push_back(onDiskAndTape);

  eos::mgm::bulk::QueryPrepareFileResponse missingFile("/file/does/not/exist");
  missingFile.is_exists = false;
  queryResponse->responses.push_back(missingFile);

  GetArchiveInfoResponseModel model(queryResponse);
  model.setJsonifier(std::make_shared<GetArchiveInfoResponseJsonifier>());

  const Json::Value root = parseJson(toJson(model));
  ASSERT_TRUE(root.isArray());
  ASSERT_EQ(2u, root.size());
  ASSERT_EQ("/eos/ccaffy/tape/test.txt", root[0]["path"].asString());
  ASSERT_EQ("DISK_AND_TAPE", root[0]["locality"].asString());
  ASSERT_FALSE(root[0].isMember("error"));
  ASSERT_EQ("/file/does/not/exist", root[1]["path"].asString());
  ASSERT_FALSE(root[1].isMember("locality"));
  ASSERT_TRUE(root[1].isMember("error"));
}

TEST_F(TapeJsonifierTest, archiveInfoJsonifierMapsDiskTapeAndUnavailable)
{
  auto queryResponse = std::make_shared<eos::mgm::bulk::QueryPrepareResponse>();

  eos::mgm::bulk::QueryPrepareFileResponse diskOnly("/disk/only.txt");
  diskOnly.is_exists = true;
  diskOnly.is_online = true;
  queryResponse->responses.push_back(diskOnly);

  eos::mgm::bulk::QueryPrepareFileResponse tapeOnly("/tape/only.txt");
  tapeOnly.is_exists = true;
  tapeOnly.is_on_tape = true;
  queryResponse->responses.push_back(tapeOnly);

  eos::mgm::bulk::QueryPrepareFileResponse unavailable("/unavailable.txt");
  unavailable.is_exists = true;
  queryResponse->responses.push_back(unavailable);

  GetArchiveInfoResponseModel model(queryResponse);
  model.setJsonifier(std::make_shared<GetArchiveInfoResponseJsonifier>());

  const Json::Value root = parseJson(toJson(model));
  ASSERT_EQ("DISK", root[0]["locality"].asString());
  ASSERT_EQ("TAPE", root[1]["locality"].asString());
  ASSERT_EQ("UNAVAILABLE", root[2]["locality"].asString());
}

TEST_F(TapeJsonifierTest, errorModelJsonifierMatchesRfc7807Shape)
{
  ErrorModel model("JSON Validation error", 400, "paths – Field does not exist");
  model.setJsonifier(std::make_shared<ErrorModelJsonifier>());

  const Json::Value root = parseJson(toJson(model));
  ASSERT_EQ("JSON Validation error", root["title"].asString());
  ASSERT_EQ(400, root["status"].asInt());
  ASSERT_EQ("paths – Field does not exist", root["detail"].asString());
  ASSERT_FALSE(root.isMember("type"));
}

TEST_F(TapeJsonifierTest, errorModelJsonifierEscapesSpecialCharacters)
{
  ErrorModel model("File missing from stage request", 400,
                   "The file /path/with\"quotes does not belong");
  model.setJsonifier(std::make_shared<ErrorModelJsonifier>());

  const Json::Value root = parseJson(toJson(model));
  ASSERT_EQ("The file /path/with\"quotes does not belong", root["detail"].asString());
}
