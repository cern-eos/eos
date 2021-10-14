// ----------------------------------------------------------------------
// File: ControllerManager.cc
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
#include "ControllerManager.hh"
#include "mgm/http/rest-api/exception/ControllerNotFoundException.hh"
#include <sstream>
#include "mgm/http/rest-api/utils/URLParser.hh"

EOSMGMRESTNAMESPACE_BEGIN

ControllerManager::ControllerManager()
{}

void ControllerManager::addController(std::shared_ptr<Controller> controller) {
  mControllers[controller->getAccessURL()] = controller;
}

std::shared_ptr<Controller> ControllerManager::getController(const std::string & urlFromClient) const {
  URLParser urlFromClientParser(urlFromClient);
  const auto controllerItor = std::find_if(mControllers.begin(),mControllers.end(),[&urlFromClientParser](const std::pair<std::string,std::shared_ptr<Controller>> & keyValue){
    return urlFromClientParser.startsBy(keyValue.first);
  });
  if(controllerItor == mControllers.end()){
    std::ostringstream ss;
    ss << "The URL provided (" << urlFromClient << ") does not allow to identify an existing resource and its version";
    throw ControllerNotFoundException(ss.str());
  }
  return controllerItor->second;
}


EOSMGMRESTNAMESPACE_END