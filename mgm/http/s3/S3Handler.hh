// ----------------------------------------------------------------------
// File: S3Handler.hh
// Author: Andreas-Joachim Peters & Justin Lewis Salmon - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

/**
 * @file   S3Handler.hh
 *
 * @brief  Dealing with all S3 goodies
 */

#ifndef __EOSMGM_S3_HANDLER__HH__
#define __EOSMGM_S3_HANDLER__HH__

/*----------------------------------------------------------------------------*/
#include "common/http/s3/S3Handler.hh"
#include "mgm/http/s3/S3Store.hh"
#include "mgm/Namespace.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <string>
#include <map>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class S3Store;

class S3Handler : public eos::common::S3Handler
{

protected:
  static S3Store *mS3Store; //!< S3Handler store object

public:

  /**
   * Constructor
   */
  S3Handler (eos::common::Mapping::VirtualIdentity *vid);

  /**
   * Destructor
   */
  virtual ~S3Handler () {};

  /**
   * Check whether the given method and headers are a match for this protocol.
   *
   * @param method  the request verb used by the client (GET, PUT, etc)
   * @param headers the map of request headers
   *
   * @return true if the protocol matches, false otherwise
   */
  static bool
  Matches (const std::string &method, HeaderMap &headers);

//  /**
//   * Analyze the header map, searching for HTTP and Amazon headers
//   *
//   * @param headers  the map of request headers
//   */
//  virtual void
//  ParseHeader (HeaderMap &headers);

  /**
   * Build a response to the given S3 request.
   *
   * @param request  the map of request headers sent by the client
   * @param method   the request verb used by the client (GET, PUT, etc)
   * @param url      the URL requested by the client
   * @param query    the GET request query string (if any)
   * @param body     the request body data sent by the client
   * @param bodysize the size of the request body
   * @param cookies  the map of cookie headers
   */
  void
  HandleRequest (eos::common::HttpRequest *request);

};

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END

#endif

