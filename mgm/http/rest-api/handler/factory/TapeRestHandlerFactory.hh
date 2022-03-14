// ----------------------------------------------------------------------
// File: TapeRestHandlerFactory.hh
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

#ifndef EOS_TAPERESTHANDLERFACTORY_HH
#define EOS_TAPERESTHANDLERFACTORY_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/handler/factory/RestHandlerFactory.hh"
#include "mgm/http/rest-api/config/tape/TapeRestApiConfig.hh"

EOSMGMRESTNAMESPACE_BEGIN

/**
 * This Factory allows to create a tape REST API handler
 */
class TapeRestHandlerFactory : public RestHandlerFactory {
public:
  /**
   * Constructor of this factory, the tape REST API handler
   * will be instanciated depending to the config passed in parameter
   * @param config the object containing the configuration of the Tape REST API handler
   * to instanciate
   */
  TapeRestHandlerFactory(const TapeRestApiConfig * config);
  std::unique_ptr<RestHandler> createRestHandler() override;
  virtual ~TapeRestHandlerFactory() = default;
private:
  const TapeRestApiConfig * mConfig;
};

EOSMGMRESTNAMESPACE_END
#endif // EOS_TAPERESTHANDLERFACTORY_HH
