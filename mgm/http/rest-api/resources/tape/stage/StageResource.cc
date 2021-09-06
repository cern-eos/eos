// ----------------------------------------------------------------------
// File: StageResource.cc
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

#include "StageResource.hh"
#include "mgm/http/rest-api/controllers/ControllerFactory.hh"

EOSMGMRESTNAMESPACE_BEGIN

const std::map<std::string,std::function<Controller *()>> StageResource::mVersionToControllerFactoryMethod = {
    {"v1",&ControllerFactory::getStageControllerV1}
};

StageResource::StageResource(){

}

common::HttpResponse* StageResource::handleRequest(common::HttpRequest* request){
  //Authorized ?
  //Which controller to instanciate ?

  return nullptr;
}

Controller* StageResource::getController(const std::string& version){
  return nullptr;
}

EOSMGMRESTNAMESPACE_END