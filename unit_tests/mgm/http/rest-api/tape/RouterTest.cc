//------------------------------------------------------------------------------
//! @file RouterTest.cc
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

#include "RestApiTestSupport.hh"
#include "mgm/http/rest-api/exception/Exceptions.hh"
#include "mgm/http/rest-api/router/Router.hh"
#include "common/http/PlainHttpResponse.hh"

using namespace eos::mgm::rest;
using eos::mgm::rest::test::createHttpRequest;

TEST(RouterTest, throwsActionNotFoundWhenNoRouteMatches)
{
  Router router;
  eos::common::VirtualIdentity vid;
  router.add("/api/v1/stage/", common::HttpHandler::Methods::POST,
             [](common::HttpRequest*, const common::VirtualIdentity*)
  -> common::HttpResponse* {
    return new common::PlainHttpResponse(common::HttpResponse::OK);
  });
  auto request = createHttpRequest("POST", "/api/v1/archiveinfo/");
  ASSERT_THROW(router.dispatch(request.get(), &vid), ActionNotFoundException);
}

TEST(RouterTest, throwsMethodNotAllowedWhenPatternMatchesButMethodDiffers)
{
  Router router;
  eos::common::VirtualIdentity vid;
  router.add("/api/v1/stage/", common::HttpHandler::Methods::POST,
             [](common::HttpRequest*, const common::VirtualIdentity*)
  -> common::HttpResponse* {
    return new common::PlainHttpResponse(common::HttpResponse::OK);
  });
  auto request = createHttpRequest("GET", "/api/v1/stage/");
  ASSERT_THROW(router.dispatch(request.get(), &vid), MethodNotAllowedException);
}

TEST(RouterTest, dispatchesWhenPatternAndMethodMatch)
{
  Router router;
  eos::common::VirtualIdentity vid;
  router.add("/api/v1/stage/", common::HttpHandler::Methods::POST,
             [](common::HttpRequest*, const common::VirtualIdentity*)
  -> common::HttpResponse* {
    return new common::PlainHttpResponse(common::HttpResponse::CREATED);
  });
  auto request = createHttpRequest("POST", "/api/v1/stage/");
  std::unique_ptr<common::HttpResponse> response(router.dispatch(request.get(), &vid));
  ASSERT_EQ(common::HttpResponse::CREATED, response->GetResponseCode());
}
