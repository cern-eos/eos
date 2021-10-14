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
#include "mgm/http/rest-api/exception/RestException.hh"
#include "mgm/http/rest-api/handler/tape/TapeRestHandler.hh"
#include "mgm/http/rest-api/utils/URLParser.hh"
#include "common/http/HttpResponse.hh"

TEST_F(RestApiTest,RestHandlerConstructorShouldThrowIfProgrammerGaveWrongURL){
  std::unique_ptr<TapeRestHandler> restHandler;
  ASSERT_THROW(restHandler.reset(new TapeRestHandler("WRONG_URL")),
               RestException);
  ASSERT_THROW(restHandler.reset(new TapeRestHandler("//test.fr")),
               RestException);
  ASSERT_THROW(restHandler.reset(new TapeRestHandler("/api/v1/")),
               RestException);
  ASSERT_THROW(restHandler.reset(new TapeRestHandler("//")), RestException);
  ASSERT_THROW(restHandler.reset(new TapeRestHandler("/ /")), RestException);
  ASSERT_NO_THROW(restHandler.reset(new TapeRestHandler("/rest-api-entry-point/")));
}

TEST_F(RestApiTest,RestHandlerHandleRequestNoResource){
  eos::common::VirtualIdentity vid;
  std::unique_ptr<TapeRestHandler> restHandler;
  restHandler.reset(new TapeRestHandler("/rest-api-entry-point/"));
  std::unique_ptr<eos::common::HttpRequest> request(createHttpRequestWithEmptyBody("/rest-api-entry-point/"));
  std::unique_ptr<eos::common::HttpResponse> response(restHandler->handleRequest(request.get(),&vid));
  ASSERT_EQ(eos::common::HttpResponse::ResponseCodes::NOT_FOUND,response->GetResponseCode());
  request = std::move(createHttpRequestWithEmptyBody("/rest-api-entry-point/v1"));
  response.reset(restHandler->handleRequest(request.get(),&vid));
  ASSERT_EQ(eos::common::HttpResponse::ResponseCodes::NOT_FOUND,response->GetResponseCode());
}

TEST_F(RestApiTest,RestHandlerHandleRequestResourceButNoVersion){
  eos::common::VirtualIdentity vid;
  std::unique_ptr<TapeRestHandler> restHandler;
  restHandler.reset(new TapeRestHandler("/rest-api-entry-point/"));
  std::unique_ptr<eos::common::HttpRequest> request(createHttpRequestWithEmptyBody("/rest-api-entry-point/tape/"));
  std::unique_ptr<eos::common::HttpResponse> response(restHandler->handleRequest(request.get(),&vid));
  ASSERT_EQ(eos::common::HttpResponse::ResponseCodes::NOT_FOUND,response->GetResponseCode());
}

TEST_F(RestApiTest,RestHandlerHandleRequestResourceDoesNotExist){
  eos::common::VirtualIdentity vid;
  std::unique_ptr<TapeRestHandler> restHandler;
  restHandler.reset(new TapeRestHandler("/rest-api-entry-point/"));
  std::unique_ptr<eos::common::HttpRequest> request(createHttpRequestWithEmptyBody("/rest-api-entry-point/v1/NOT_EXIST_RESOURCE"));
  std::unique_ptr<eos::common::HttpResponse> response(restHandler->handleRequest(request.get(),&vid));
  ASSERT_EQ(eos::common::HttpResponse::ResponseCodes::NOT_FOUND,response->GetResponseCode());
}

TEST_F(RestApiTest,RestHandlerHandleRequestResourceAndVersionExist){
  eos::common::VirtualIdentity vid;
  std::unique_ptr<TapeRestHandler> restHandler;
  restHandler.reset(new TapeRestHandler("/rest-api-entry-point/"));
  std::unique_ptr<eos::common::HttpRequest> request(createHttpRequestWithEmptyBody("/rest-api-entry-point/v1/stage/"));
  std::unique_ptr<eos::common::HttpResponse> response(restHandler->handleRequest(request.get(),&vid));
  //ASSERT_EQ(response->)
}

TEST_F(RestApiTest,URLParserTestMatchesBegin){
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

TEST_F(RestApiTest,URLParserTestMatchesAndExtractParameters) {
  std::string urlClient = "/api/v1/stage/request-id/cancel";
  std::unique_ptr<URLParser> urlParser(new URLParser(urlClient));

  std::map<std::string, std::string> params;
  ASSERT_TRUE(urlParser->matchesAndExtractParameters("/api/v1/stage/{id}/cancel", params));
  ASSERT_EQ("request-id", params.at("{id}"));
  ASSERT_FALSE(urlParser->matchesAndExtractParameters("/api/v1/stage/", params));
  ASSERT_EQ(0, params.size());
  ASSERT_FALSE(urlParser->matchesAndExtractParameters("/api/v1/stage/id/cancel", params));
  ASSERT_EQ(0, params.size());

  urlClient = "/api/v1/{id}/stage/";
  urlParser.reset(new URLParser(urlClient));
  ASSERT_FALSE(urlParser->matchesAndExtractParameters("/api/v1/id/stage",params));
  ASSERT_EQ(0, params.size());
}