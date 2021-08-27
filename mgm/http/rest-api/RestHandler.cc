// ----------------------------------------------------------------------
// File: RestHandler.cc
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

#include "RestHandler.hh"
#include "common/StringConversion.hh"
EOSMGMNAMESPACE_BEGIN

RestHandler::RestHandler(const std::string & restApiUrl):mRestAPIUrl(restApiUrl){

}

common::HttpResponse* RestHandler::handleRequest(common::HttpRequest* request){
  //Tokenize the URL given
  //Instanciate the Resource
  //Get the controller
  //Perform the controller action depending on the method (GET, POST,...)
  return nullptr;
}

bool RestHandler::isRestRequest(const std::string& requestUrl){
  //The URL should start with the API entry URL
  return ::strncmp(mRestAPIUrl.c_str(),requestUrl.c_str(),mRestAPIUrl.length()) == 0;
}

const std::string RestHandler::extractResourceFromUrl(const std::string& url) {
  return "";
}

const std::string RestHandler::extractVersionFromUrl(const std::string& url) {
  return "";
}

EOSMGMNAMESPACE_END
