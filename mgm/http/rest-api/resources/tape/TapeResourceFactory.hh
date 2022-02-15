// ----------------------------------------------------------------------
// File: TapeResourceFactory.hh
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

#ifndef EOS_TAPERESOURCEFACTORY_HH
#define EOS_TAPERESOURCEFACTORY_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/resources/ResourceFactory.hh"
#include "mgm/http/rest-api/resources/tape/stage/StageResource.hh"

EOSMGMRESTNAMESPACE_BEGIN

class TapeResourceFactory : public ResourceFactory {
public:
  Resource * createResource(const std::string & resourceName) override;
private:
  static Resource * createStageResource();

  const static std::map<std::string,ResourceFactory::resource_factory_method_t> cResourceStrToFactoryMethod;
  inline static const std::string cStageResourceName = "stage";
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPERESOURCEFACTORY_HH
