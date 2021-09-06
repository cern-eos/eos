// ----------------------------------------------------------------------
// File: TapeResourceFactory.cc
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

#include "TapeResourceFactory.hh"
#include "mgm/http/rest-api/resources/tape/stage/StageResource.hh"
#include "mgm/http/rest-api/exception/ResourceNotFoundException.hh"
#include <sstream>

EOSMGMRESTNAMESPACE_BEGIN

const std::map<std::string, ResourceFactory::resource_factory_method_t> TapeResourceFactory::cResourceStrToFactoryMethod = {
    {cStageResourceName,&TapeResourceFactory::createStageResource}
};

Resource * TapeResourceFactory::createStageResource(){
  return new StageResource();
}

Resource * TapeResourceFactory::createResource(const std::string & resourceName){
  try {
    return cResourceStrToFactoryMethod.at(resourceName)();
  } catch (const std::out_of_range &ex){
    std::stringstream ss;
    ss << "The resource " << resourceName << " has not been found";
    throw ResourceNotFoundException(ss.str());
  }
}


EOSMGMRESTNAMESPACE_END
