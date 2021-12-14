// ----------------------------------------------------------------------
// File: ControllerActionDispatcher.hh
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

#include "ControllerActionDispatcher.hh"
#include "mgm/http/rest-api/exception/MethodNotAllowedException.hh"
#include "mgm/http/rest-api/exception/ControllerNotFoundException.hh"
#include "mgm/http/rest-api/utils/URLParser.hh"
#include <sstream>
EOSMGMRESTNAMESPACE_BEGIN

void ControllerActionDispatcher::addAction(std::unique_ptr<Action> && action){
  mURLMapMethodFunctionMap[action->getURLPattern()][action->getMethod()] = std::move(action);
}

Action * ControllerActionDispatcher::getAction(common::HttpRequest* request) {
  std::string methodStr = request->GetMethod();
  std::string url = request->GetUrl();
  URLParser requestUrlParser(url);
  HttpHandler::Methods method = (HttpHandler::Methods)HttpHandler::ParseMethodString(methodStr);
  //First we look if the URL is known by the dispatcher.
  //If it is known, the map<Method,Function> will be looked at
  auto urlMapMethodFunctionItor = std::find_if(
      mURLMapMethodFunctionMap.begin(), mURLMapMethodFunctionMap.end(),[&requestUrlParser](const auto & urlMethodFunctionItem){
    return requestUrlParser.matches(urlMethodFunctionItem.first);
  });
  if(urlMapMethodFunctionItor != mURLMapMethodFunctionMap.end()){
    //The URL allowed to identify a map<Method,Function>.
    //If the method exists, it will return the function (Action) to run
    auto methodFunctionItor = std::find_if(urlMapMethodFunctionItor->second.begin(), urlMapMethodFunctionItor->second.end(), [&method](const auto & methodFunctionItem){
      return method == methodFunctionItem.first;
    });
    if(methodFunctionItor != urlMapMethodFunctionItor->second.end()){
      return methodFunctionItor->second.get();
    } else {
      //Method not found
      std::ostringstream oss;
      oss << "The method " << methodStr << " is not allowed for this resource.";
      throw MethodNotAllowedException(oss.str());
    }
  } else {
    //URL not found
    std::ostringstream oss;
    oss << "The url provided (" << request->GetUrl() << ") does not allow to identify a controller";
    throw ControllerNotFoundException(oss.str());
  }
}

EOSMGMRESTNAMESPACE_END