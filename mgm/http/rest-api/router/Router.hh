// ----------------------------------------------------------------------
// File: Router.hh
// Author: Refactor - Simplified REST routing
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) CERN/Switzerland                                       *
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

#ifndef EOS_REST_ROUTER_HH
#define EOS_REST_ROUTER_HH

#include "mgm/Namespace.hh"
#include "common/http/HttpHandler.hh"
#include "common/http/HttpRequest.hh"
#include "common/http/HttpResponse.hh"
#include "mgm/http/rest-api/utils/URLParser.hh"
#include "mgm/http/rest-api/exception/Exceptions.hh"
#include <type_traits>

#include <functional>
#include <string>
#include <vector>
#include <sstream>

EOSMGMRESTNAMESPACE_BEGIN

struct Route
{
  std::string pattern;
  common::HttpHandler::Methods method;
  std::function<common::HttpResponse*(common::HttpRequest*, const common::VirtualIdentity*)> handler;
};

class Router
{
public:
  void add(const std::string & pattern,
           common::HttpHandler::Methods method,
           std::function<common::HttpResponse*(common::HttpRequest*, const common::VirtualIdentity*)> handler)
  {
    Route r{pattern, method, std::move(handler)};
    mRoutes.emplace_back(std::move(r));
  }

  common::HttpResponse* dispatch(common::HttpRequest* request,
                                 const common::VirtualIdentity* vid)
  {
    const std::string url = request->GetUrl();
    const std::string methodStr = request->GetMethod();
    common::HttpHandler::Methods method = (common::HttpHandler::Methods)
                                          common::HttpHandler::ParseMethodString(methodStr);
    URLParser parser(url);

    bool patternMatched = false;
    for (auto & route : mRoutes) {
      if (parser.matches(route.pattern)) {
        patternMatched = true;
        if (route.method == method) {
          return route.handler(request, vid);
        }
      }
    }

    if (patternMatched) {
      std::ostringstream oss;
      oss << "The method " << methodStr << " is not allowed for this resource.";
      throw MethodNotAllowedException(oss.str());
    }

    std::ostringstream oss;
    oss << "The url provided (" << url << ") does not allow to identify a resource";
    throw ActionNotFoundException(oss.str());
  }

private:
  std::vector<Route> mRoutes;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_REST_ROUTER_HH


