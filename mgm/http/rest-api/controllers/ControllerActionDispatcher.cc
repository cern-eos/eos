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

ControllerActionDispatcher::ControllerActionDispatcher(){}

void ControllerActionDispatcher::addAction(const std::string& urlPattern, const common::HttpHandler::Methods method, const ControllerHandler& controllerHandler){
  mMethodFunctionMap[urlPattern][method] = controllerHandler;
}

ControllerActionDispatcher::ControllerHandler ControllerActionDispatcher::getAction(common::HttpRequest* request) {
  std::string methodStr = request->GetMethod();
  std::string url = request->GetUrl();
  URLParser requestUrlParser(url);
  HttpHandler::Methods method = (HttpHandler::Methods)HttpHandler::ParseMethodString(methodStr);
  auto methodFunctionItor = std::find_if(mMethodFunctionMap.begin(),mMethodFunctionMap.end(),[&requestUrlParser](const std::pair<std::string,std::map<common::HttpHandler::Methods,ControllerHandler>> & item){
    return requestUrlParser.matches(item.first);
  });
  if(methodFunctionItor != mMethodFunctionMap.end()){
    try {
      return methodFunctionItor->second.at(method);
    } catch (const std::out_of_range & ex){
      std::ostringstream oss;
      oss << "The method " << methodStr << " is not allowed for this resource.";
      throw MethodNotAllowedException(oss.str());
    }
  } else {
    std::ostringstream oss;
    oss << "The url provided (" << request->GetUrl() << ") does not allow to identify a controller";
    throw ControllerNotFoundException(oss.str());
  }
}

EOSMGMRESTNAMESPACE_END