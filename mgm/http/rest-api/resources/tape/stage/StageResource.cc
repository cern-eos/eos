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
#include <memory>
#include "mgm/http/rest-api/exception/ControllerNotFoundException.hh"
#include <sstream>

EOSMGMRESTNAMESPACE_BEGIN

const std::map<std::string,std::function<Controller *()>> StageResource::cVersionToControllerFactoryMethod = {
    {"v1",&ControllerFactory::getStageControllerV1}
};

StageResource::StageResource(){

}

common::HttpResponse* StageResource::handleRequest(common::HttpRequest* request){
  //Authorized ?
  //Which controller to instanciate ?
  std::unique_ptr<Controller> controller;
  try {
    controller.reset(getController());
  } catch(const ControllerNotFoundException &ex){
    //Return an error to the user
  }
  return controller->handleRequest(request);
}

Controller* StageResource::getController() {
  try {
    return cVersionToControllerFactoryMethod.at(mVersion)();
  } catch (const std::out_of_range &ex) {
    std::ostringstream ss;
    ss << "No controller version " << mVersion << " found for the stage resource";
    throw ControllerNotFoundException(ss.str());
  }
}

EOSMGMRESTNAMESPACE_END