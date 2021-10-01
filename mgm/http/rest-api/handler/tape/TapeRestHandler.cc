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
#include "mgm/http/rest-api/exception/ControllerNotFoundException.hh"
#include "mgm/http/rest-api/resources/Resource.hh"
#include "mgm/http/rest-api/resources/ResourceFactory.hh"
#include "common/http/HttpServer.hh"
#include "mgm/http/rest-api/resources/tape/TapeResourceFactory.hh"
#include "mgm/http/rest-api/response/tape/TapeRestApiResponseFactory.hh"

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

common::HttpResponse* TapeRestHandler::handleRequest(common::HttpRequest* request, const common::VirtualIdentity * vid) {
  //URL = /entrypoint/version/resource-name
  std::string url = request->GetUrl();
  if(isRestRequest(url)) {
    std::vector<std::string> urlTokens;
    common::StringConversion::Tokenize(request->GetUrl(), urlTokens,"/");
    if(urlTokens.size() < 3) {
      // Return 404 not found error
      std::ostringstream oss;
      oss << "URL provided (" << url << ") does not allow to identify an API version or a resource";
      eos_static_info(oss.str().c_str());
      return TapeRestApiResponseFactory::createNotFoundError().getHttpResponse();
    }
    std::unique_ptr<Resource> resource;
    try {
      resource.reset(mResourceFactory->createResource(urlTokens.at(2)));
      resource->setVersion(urlTokens.at(1));
      return resource->handleRequest(request,vid);
    } catch(const ResourceNotFoundException &ex) {
      eos_static_info(ex.what());
      return TapeRestApiResponseFactory::createNotFoundError().getHttpResponse();
    } catch (const ControllerNotFoundException &ex) {
      eos_static_info(ex.what());
      return TapeRestApiResponseFactory::createNotFoundError().getHttpResponse();
    }
  }
  return nullptr;
}

void TapeRestHandler::verifyRestApiEntryPoint(const std::string& restApiUrl){
  std::regex entryPointRegex(cEntryPointRegex);
  if(!std::regex_match(restApiUrl,entryPointRegex)){
    std::stringstream ss;
    ss << "The REST API entrypoint provided (" << restApiUrl << ") is malformed. It should be under the format: /apientrypoint/.";
    throw RestException(ss.str());
  }
}

EOSMGMRESTNAMESPACE_END