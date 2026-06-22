//------------------------------------------------------------------------------
//! @file RestApiTest.hh
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

#include "RestApiTest.hh"
#include "common/http/HttpResponse.hh"
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"
#include "mgm/http/rest-api/exception/RestException.hh"
#include "mgm/http/rest-api/handler/tape/TapeRestHandler.hh"
#include "mgm/http/rest-api/json/tape/TapeJsonifiers.hh"
#include "mgm/http/rest-api/model/tape/archiveinfo/GetArchiveInfoResponseModel.hh"
#include "mgm/http/rest-api/model/tape/common/ErrorModel.hh"
#include "mgm/http/rest-api/model/tape/stage/CreatedStageBulkRequestResponseModel.hh"
#include "mgm/http/rest-api/model/tape/stage/GetStageBulkRequestResponseModel.hh"
#include "mgm/http/rest-api/model/wellknown/tape/GetTapeWellKnownModel.hh"
#include "mgm/http/rest-api/utils/URLParser.hh"
#include "mgm/http/rest-api/wellknown/tape/TapeWellKnownInfos.hh"
#include <json/json.h>
#include <memory>
#include <optional>
#include <sstream>
#include <string>

namespace {

Json::Value
ParseJson(const std::string& json)
{
  Json::Value root;
  Json::CharReaderBuilder builder;
  std::string errors;
  std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
  EXPECT_TRUE(reader->parse(json.data(), json.data() + json.size(), &root, &errors))
      << errors << "\n"
      << json;
  return root;
}

template <typename Model, typename Jsonifier>
Json::Value
Jsonify(const Model& model, Jsonifier& jsonifier)
{
  std::stringstream ss;
  jsonifier.jsonify(&model, ss);
  return ParseJson(ss.str());
}

} // namespace

TEST_F(RestApiTest, RestHandlerConstructorShouldThrowIfProgrammerGaveWrongURL)
{
  std::unique_ptr<TapeRestHandler> restHandler;
  ASSERT_THROW(restHandler.reset(new TapeRestHandler(
                                   createConfig("WRONG_URL").get())), RestException);
  ASSERT_THROW(restHandler.reset(new TapeRestHandler(
                                   createConfig("//test.fr").get())),
               RestException);
  ASSERT_THROW(restHandler.reset(new TapeRestHandler(
                                   createConfig("/api/v1/").get())),
               RestException);
  ASSERT_THROW(restHandler.reset(new TapeRestHandler(createConfig("//").get())),
               RestException);
  ASSERT_THROW(restHandler.reset(new TapeRestHandler(createConfig("/ /").get())),
               RestException);
  ASSERT_NO_THROW(restHandler.reset(new TapeRestHandler(
                                      createConfig("/rest-api-entry-point/").get())));
}

TEST_F(RestApiTest, RestHandlerHandleRequestNoResource)
{
  eos::common::VirtualIdentity vid;
  std::unique_ptr<TapeRestHandler> restHandler;
  restHandler.reset(new TapeRestHandler(
                      createConfig("/rest-api-entry-point/").get()));
  std::unique_ptr<eos::common::HttpRequest> request(
    createHttpRequestWithEmptyBody("/rest-api-entry-point/"));
  std::unique_ptr<eos::common::HttpResponse> response(restHandler->handleRequest(
        request.get(), &vid));
  ASSERT_EQ(eos::common::HttpResponse::ResponseCodes::NOT_FOUND,
            response->GetResponseCode());
  request = createHttpRequestWithEmptyBody("/rest-api-entry-point/v1");
  response.reset(restHandler->handleRequest(request.get(), &vid));
  ASSERT_EQ(eos::common::HttpResponse::ResponseCodes::NOT_FOUND,
            response->GetResponseCode());
}

TEST_F(RestApiTest, RestHandlerHandleRequestResourceButNoVersion)
{
  eos::common::VirtualIdentity vid;
  std::unique_ptr<TapeRestHandler> restHandler;
  restHandler.reset(new TapeRestHandler(
                      createConfig("/rest-api-entry-point/").get()));
  std::unique_ptr<eos::common::HttpRequest> request(
    createHttpRequestWithEmptyBody("/rest-api-entry-point/tape/"));
  std::unique_ptr<eos::common::HttpResponse> response(restHandler->handleRequest(
        request.get(), &vid));
  ASSERT_EQ(eos::common::HttpResponse::ResponseCodes::NOT_FOUND,
            response->GetResponseCode());
}

TEST_F(RestApiTest, RestHandlerHandleRequestResourceDoesNotExist)
{
  eos::common::VirtualIdentity vid;
  std::unique_ptr<TapeRestHandler> restHandler;
  restHandler.reset(new TapeRestHandler(
                      createConfig("/rest-api-entry-point/").get()));
  std::unique_ptr<eos::common::HttpRequest> request(
    createHttpRequestWithEmptyBody("/rest-api-entry-point/v1/NOT_EXIST_RESOURCE"));
  std::unique_ptr<eos::common::HttpResponse> response(restHandler->handleRequest(
        request.get(), &vid));
  ASSERT_EQ(eos::common::HttpResponse::ResponseCodes::NOT_FOUND,
            response->GetResponseCode());
}

TEST_F(RestApiTest, RestHandlerHandleRequestResourceAndVersionExist)
{
  eos::common::VirtualIdentity vid;
  std::unique_ptr<TapeRestHandler> restHandler;
  restHandler.reset(new TapeRestHandler(
                      createConfig("/rest-api-entry-point/").get()));
  std::unique_ptr<eos::common::HttpRequest> request(
    createHttpRequestWithEmptyBody("/rest-api-entry-point/v1/stage/"));
  std::unique_ptr<eos::common::HttpResponse> response(restHandler->handleRequest(
        request.get(), &vid));
  // Posting to stage without a valid body should yield a bad request
  ASSERT_EQ(eos::common::HttpResponse::ResponseCodes::BAD_REQUEST,
            response->GetResponseCode());
}

TEST_F(RestApiTest, URLParserTestMatchesBegin)
{
  std::string urlClient = "/api/v1/stage/";
  std::unique_ptr<URLParser> urlParser(new URLParser(urlClient));
  ASSERT_TRUE(urlParser->startsBy("/api/v1/stage/"));
  ASSERT_TRUE(urlParser->startsBy("/api/v1/stage"));
  urlClient = "/api/v1/";
  urlParser.reset(new URLParser(urlClient));
  ASSERT_FALSE(urlParser->startsBy("/api/v1/stage/"));
  urlClient = "/api/v1/stage/request-id/cancel";
  urlParser.reset(new URLParser(urlClient));
  ASSERT_TRUE(urlParser->startsBy("/api/v1/stage/"));
  ASSERT_TRUE(urlParser->startsBy("/api/v1/stage"));
}

TEST_F(RestApiTest, URLParserExtractsStageRequestIdFromGetUrl)
{
  const std::string requestId = "93be38df-435c-4322-801d-b95e77ac5bbc";
  std::unique_ptr<URLParser> urlParser(
    new URLParser("/api/v1/stage/" + requestId));
  std::map<std::string, std::string> params;
  ASSERT_TRUE(urlParser->matchesAndExtractParameters("/api/v1/stage/{id}", params));
  ASSERT_EQ(requestId, params.at("{id}"));
}

TEST_F(RestApiTest, URLParserExtractsStageRequestIdFromDeleteUrl)
{
  const std::string requestId = "93be38df-435c-4322-801d-b95e77ac5bbc";
  std::unique_ptr<URLParser> urlParser(
    new URLParser("/api/v1/stage/" + requestId));
  std::map<std::string, std::string> params;
  ASSERT_TRUE(urlParser->matchesAndExtractParameters("/api/v1/stage/{id}", params));
  ASSERT_EQ(requestId, params.at("{id}"));
}

TEST_F(RestApiTest, URLParserExtractsReleaseRequestIdFromUrl)
{
  const std::string requestId = "93be38df-435c-4322-801d-b95e77ac5bbc";
  std::unique_ptr<URLParser> urlParser(
    new URLParser("/api/v1/release/" + requestId));
  std::map<std::string, std::string> params;
  ASSERT_TRUE(urlParser->matchesAndExtractParameters("/api/v1/release/{id}", params));
  ASSERT_EQ(requestId, params.at("{id}"));
}

TEST_F(RestApiTest, URLParserTestMatchesAndExtractParameters)
{
  std::string urlClient = "/api/v1/stage/request-id/cancel";
  std::unique_ptr<URLParser> urlParser(new URLParser(urlClient));
  std::map<std::string, std::string> params;
  ASSERT_TRUE(urlParser->matchesAndExtractParameters("/api/v1/stage/{id}/cancel",
              params));
  ASSERT_EQ("request-id", params.at("{id}"));
  ASSERT_FALSE(urlParser->matchesAndExtractParameters("/api/v1/stage/", params));
  ASSERT_EQ(0, params.size());
  ASSERT_FALSE(urlParser->matchesAndExtractParameters("/api/v1/stage/id/cancel",
               params));
  ASSERT_EQ(0, params.size());
  urlClient = "/api/v1/{id}/stage/";
  urlParser.reset(new URLParser(urlClient));
  ASSERT_FALSE(urlParser->matchesAndExtractParameters("/api/v1/id/stage",
               params));
  ASSERT_EQ(0, params.size());
}

TEST_F(RestApiTest, URLBuilderTest)
{
  auto urlBuilder = URLBuilder::getInstance();
  std::string hostname = "hostname.cern.ch";
  uint16_t port = 1234;
  std::string url1 = urlBuilder->setHttpsProtocol()->setHostname(
                       hostname)->setPort(port)->build();
  ASSERT_EQ(std::string("https://")  + hostname + ":" + std::to_string(port),
            url1);
  auto urlBuilder2 = URLBuilder::getInstance();
  std::string urlstage = urlBuilder2->setHttpsProtocol()->setHostname(
                           hostname)->setPort(port)->add("/api/")->add("v1")->add("stage")->build();
  ASSERT_EQ(std::string("https://")  + hostname + ":" + std::to_string(
              port) + "/api/v1/stage", urlstage);
}

TEST_F(RestApiTest, TapeJsonifiersPreserveErrorResponseShape)
{
  ErrorModel model("Bad request", 400, std::optional<std::string>("invalid \"json\""));
  model.setType("https://example.org/problem");
  ErrorModelJsonifier jsonifier;

  Json::Value root = Jsonify(model, jsonifier);

  EXPECT_EQ("https://example.org/problem", root["type"].asString());
  EXPECT_EQ("Bad request", root["title"].asString());
  EXPECT_EQ(400, root["status"].asInt());
  EXPECT_EQ("invalid \"json\"", root["detail"].asString());
}

TEST_F(RestApiTest, TapeJsonifiersPreserveCreatedStageResponseShape)
{
  CreatedStageBulkRequestResponseModel model("request-id");
  CreatedStageBulkRequestJsonifier jsonifier;

  Json::Value root = Jsonify(model, jsonifier);

  EXPECT_EQ("request-id", root["requestId"].asString());
}

TEST_F(RestApiTest, TapeJsonifiersPreserveGetStageResponseShape)
{
  GetStageBulkRequestResponseModel model;
  model.setId("request-id");
  model.setCreatedAt(12345);
  model.setStartedAt(12345);
  auto file = std::make_unique<GetStageBulkRequestResponseModel::File>();
  file->mPath = "/path/to/file";
  file->mError = "failed";
  file->mOnDisk = true;
  model.addFile(std::move(file));
  GetStageBulkRequestJsonifier jsonifier;

  Json::Value root = Jsonify(model, jsonifier);

  EXPECT_EQ("request-id", root["id"].asString());
  EXPECT_EQ(Json::UInt64(12345), root["createdAt"].asUInt64());
  EXPECT_EQ(Json::UInt64(12345), root["startedAt"].asUInt64());
  ASSERT_TRUE(root["files"].isArray());
  ASSERT_EQ(1u, root["files"].size());
  EXPECT_EQ("/path/to/file", root["files"][0]["path"].asString());
  EXPECT_EQ("failed", root["files"][0]["error"].asString());
  EXPECT_TRUE(root["files"][0]["onDisk"].asBool());
}

TEST_F(RestApiTest, TapeJsonifiersPreserveGetStageResponseShape_PollNotAllowed)
{
  GetStageBulkRequestResponseModel model;
  model.setId("request-id");
  model.setCreatedAt(12345);
  model.setStartedAt(12345);
  auto file = std::make_unique<GetStageBulkRequestResponseModel::File>();
  file->mPath = "/path/to/file";
  file->mError = "failed";
  file->mOnDisk = std::nullopt;
  model.addFile(std::move(file));
  GetStageBulkRequestJsonifier jsonifier;

  Json::Value root = Jsonify(model, jsonifier);

  EXPECT_EQ("request-id", root["id"].asString());
  EXPECT_EQ(Json::UInt64(12345), root["createdAt"].asUInt64());
  EXPECT_EQ(Json::UInt64(12345), root["startedAt"].asUInt64());
  ASSERT_TRUE(root["files"].isArray());
  ASSERT_EQ(1u, root["files"].size());
  EXPECT_EQ("/path/to/file", root["files"][0]["path"].asString());
  EXPECT_EQ("failed", root["files"][0]["error"].asString());
  EXPECT_FALSE(root["files"][0].isMember("onDisk"));
}

TEST_F(RestApiTest, TapeJsonifiersPreserveArchiveInfoResponseShape)
{
  auto response = std::make_shared<eos::mgm::bulk::QueryPrepareResponse>();
  eos::mgm::bulk::QueryPrepareFileResponse diskAndTape("/path/to/file/on-both");
  diskAndTape.is_online = true;
  diskAndTape.is_on_tape = true;
  diskAndTape.can_show_locality = true;
  response->responses.emplace_back(diskAndTape);
  eos::mgm::bulk::QueryPrepareFileResponse tapeOnly("/path/to/file/on-tape");
  tapeOnly.is_on_tape = true;
  tapeOnly.error_text = "queued";
  tapeOnly.can_show_locality = true;
  response->responses.emplace_back(tapeOnly);
  GetArchiveInfoResponseModel model(response);
  GetArchiveInfoResponseJsonifier jsonifier;

  Json::Value root = Jsonify(model, jsonifier);

  ASSERT_TRUE(root.isArray());
  ASSERT_EQ(2u, root.size());
  EXPECT_EQ("/path/to/file/on-both", root[0]["path"].asString());
  EXPECT_EQ("DISK_AND_TAPE", root[0]["locality"].asString());
  EXPECT_EQ("/path/to/file/on-tape", root[1]["path"].asString());
  EXPECT_EQ("TAPE", root[1]["locality"].asString());
  EXPECT_EQ("queued", root[1]["error"].asString());
}

TEST_F(RestApiTest, TapeJsonifiersPreserveWellKnownResponseShape)
{
  TapeWellKnownInfos infos("tape-api-sitename");
  infos.addEndpoint("https://tape-api.example.org:1234/api/v1", "v1");
  GetTapeWellKnownModel model(&infos);
  GetTapeWellKnownModelJsonifier jsonifier;

  Json::Value root = Jsonify(model, jsonifier);

  EXPECT_EQ("tape-api-sitename", root["sitename"].asString());
  ASSERT_TRUE(root["endpoints"].isArray());
  ASSERT_EQ(1u, root["endpoints"].size());
  EXPECT_EQ("https://tape-api.example.org:1234/api/v1",
            root["endpoints"][0]["uri"].asString());
  EXPECT_EQ("v1", root["endpoints"][0]["version"].asString());
}
