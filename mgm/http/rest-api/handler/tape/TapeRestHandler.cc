// ----------------------------------------------------------------------
// File: TapeRestHandler.cc
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

#include "TapeRestHandler.hh"
#include <cstring>
#include <regex>
#include "common/StringConversion.hh"
#include "mgm/http/rest-api/exception/ResourceNotFoundException.hh"
#include "mgm/http/rest-api/resources/Resource.hh"
#include "mgm/http/rest-api/resources/ResourceFactory.hh"
#include "common/http/HttpServer.hh"
#include <regex>
#include <vector>
#include "mgm/http/rest-api/resources/tape/TapeResourceFactory.hh"
#include "mgm/http/rest-api/controllers/Controller.hh"

EOSMGMRESTNAMESPACE_BEGIN

TapeRestHandler::TapeRestHandler(const std::string& restApiUrl){
  verifyRestApiEntryPoint(restApiUrl);
  mRestAPIUrl = restApiUrl;
  //Instanciate the resource factory that will allow to access the resources related to the tape REST API
  mResourceFactory.reset(new TapeResourceFactory());
}

bool TapeRestHandler::isRestRequest(const std::string& requestUrl){
  //The URL should start with the API entry URL
  return ::strncmp(mRestAPIUrl.c_str(),requestUrl.c_str(),mRestAPIUrl.length()) == 0;
}

common::HttpResponse* TapeRestHandler::handleRequest(common::HttpRequest* request) {
  //URL = /entrypoint/version/resource-name
  std::string url = request->GetUrl();
  if(isRestRequest(url)) {
    std::vector<std::string> urlTokens;
    common::StringConversion::Tokenize(request->GetUrl(), urlTokens,"/");
    if(urlTokens.size() < 3) {
      // Return 400 bad request error
      common::HttpServer::HttpError("Bad request",400);
    }
    std::unique_ptr<Resource> resource;
    try {
      resource.reset(mResourceFactory->createResource(urlTokens.at(3)));
      resource->setVersion(urlTokens.at(2));
      return resource->handleRequest(request);
    } catch(const ResourceNotFoundException &ex) {
      //Todo: create Error object and return JSON
      return common::HttpServer::HttpError("Not found",404);
    }
  }
  return nullptr;
}

void TapeRestHandler::verifyRestApiEntryPoint(const std::string& restApiUrl){
  std::regex entryPointRegex(cEntryPointRegex);
  if(!std::regex_match(restApiUrl,entryPointRegex)){
    std::stringstream ss;
    ss << "The REST API entrypoint provided (" << restApiUrl << ") is malformed. It should be under the format: /apientrypoint/.";
    throw RestHandlerException(ss.str());
  }
}

EOSMGMRESTNAMESPACE_END