// ----------------------------------------------------------------------
// File: TapeWellKnownInfos.hh
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

#ifndef EOS_TAPEWELLKNOWNINFOS_HH
#define EOS_TAPEWELLKNOWNINFOS_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/wellknown/tape/TapeRestApiEndpoint.hh"
#include <string>
#include <vector>
#include <memory>

EOSMGMRESTNAMESPACE_BEGIN

/**
 * This class contains the information that can be used
 * by the tape REST API .well-known endpoint to display them
 * for the user
 */
class TapeWellKnownInfos
{
public:
  typedef std::vector<std::unique_ptr<TapeRestApiEndpoint>> Endpoints;
  TapeWellKnownInfos(const std::string& siteName);
  void addEndpoint(const std::string& uri, const std::string& version);
  const Endpoints& getEndpoints() const;
  const std::string getSiteName() const;
private:
  //The sitename that has to be used for targeted metadata on stage bulk-request submission
  std::string mSiteName;
  //The endpoints allowing the clients to reach a specific version of the tape REST API
  std::vector<std::unique_ptr<TapeRestApiEndpoint>> mEndpoints;
  //If metadata are needed, add a list in this class
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPEWELLKNOWNINFOS_HH
