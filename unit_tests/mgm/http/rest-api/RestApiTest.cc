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
#include "common/http/HttpResponse.hh"
#include <common/http/PlainHttpResponse.hh>

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
  std::unique_ptr<TapeRestHandler> restHandler;
  restHandler.reset(new TapeRestHandler("/rest-api-entry-point/"));
  std::unique_ptr<eos::common::HttpRequest> request(createHttpRequestWithEmptyBody("/rest-api-entry-point/"));
  std::unique_ptr<eos::common::HttpResponse> response(restHandler->handleRequest(request.get()));
  ASSERT_EQ(eos::common::HttpResponse::ResponseCodes::NOT_FOUND,response->GetResponseCode());
  request = std::move(createHttpRequestWithEmptyBody("/rest-api-entry-point/v1"));
  response.reset(restHandler->handleRequest(request.get()));
  ASSERT_EQ(eos::common::HttpResponse::ResponseCodes::NOT_FOUND,response->GetResponseCode());
}

TEST_F(RestApiTest,RestHandlerHandleRequestResourceButNoVersion){
  std::unique_ptr<TapeRestHandler> restHandler;
  restHandler.reset(new TapeRestHandler("/rest-api-entry-point/"));
  std::unique_ptr<eos::common::HttpRequest> request(createHttpRequestWithEmptyBody("/rest-api-entry-point/tape/"));
  std::unique_ptr<eos::common::HttpResponse> response(restHandler->handleRequest(request.get()));
  ASSERT_EQ(eos::common::HttpResponse::ResponseCodes::NOT_FOUND,response->GetResponseCode());
}