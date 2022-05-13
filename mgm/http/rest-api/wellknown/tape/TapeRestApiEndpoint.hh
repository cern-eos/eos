// ----------------------------------------------------------------------
// File: TapeRestApiEndpoint.hh
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
#ifndef EOS_TAPERESTAPIENDPOINT_HH
#define EOS_TAPERESTAPIENDPOINT_HH

#include "mgm/Namespace.hh"
#include <string>

EOSMGMRESTNAMESPACE_BEGIN

/**
 * This class represent a tape REST API endpoint
 * It gathers information like the URI to access the tape REST API endpoint depending
 * on the version
 */
class TapeRestApiEndpoint {
public:
  /**
   * Constructor of the endpoint
   * @param uri the full URI to access a specific version of the tape REST API
   * @param version the version ass
   */
  TapeRestApiEndpoint(const std::string & uri, const std::string & version);
  /**
   * The full URI to access the tape REST API
   */
  const std::string getUri() const;
  /**
   * The version of the tape REST API associated to this endpoint
   * @return v0,v1...
   */
  const std::string getVersion() const;
private:
  std::string mUri;
  std::string mVersion;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPERESTAPIENDPOINT_HH
