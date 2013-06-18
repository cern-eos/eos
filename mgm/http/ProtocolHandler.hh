// ----------------------------------------------------------------------
// File: ProtocolHandler.hh
// Author: Justin Lewis Salmon - CERN
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

/**
 * @file   ProtocolHandler.hh
 *
 * @brief  Abstract base class representing an interface which a concrete
 *         protocol must implement, e.g. HTTP, WebDAV, S3.
 */

#ifndef __EOSMGM_PROTOCOLHANDLER__HH__
#define __EOSMGM_PROTOCOLHANDLER__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <string>
#include <map>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class ProtocolHandler
{

public:
  typedef std::map<std::string, std::string> HeaderMap;

public:

  /**
   * Destructor
   */
  virtual ~ProtocolHandler () = 0;

  /**
   * Factory function to create an appropriate object which will handle this
   * request based on the method and headers.
   *
   * @param method  the request verb used by the client (GET, PUT, etc)
   * @param headers the map of request headers
   *
   * @return a concrete ProtocolHandler, or NULL if no matching protocol found
   */
  static ProtocolHandler*
  CreateProtocolHandler (const std::string &method, HeaderMap &headers);

  /**
   * Concrete implementations must use this function to check whether the given
   * method and headers are a match for their protocol.
   *
   * @param method  the request verb used by the client (GET, PUT, etc)
   * @param headers the map of request headers
   *
   * @return true if the protocol matches, false otherwise
   */
  static bool
  Matches (const std::string &method, HeaderMap &headers);

  /**
   * Concrete implementations must use this function to build a response to the
   * given request.
   *
   * @param method   [in]  the request verb used by the client (GET, PUT, etc)
   * @param url      [in]  the URL requested by the client
   * @param query    [in]  the GET request query string (if any)
   * @param body     [in]  the request body data sent by the client
   * @param bodysize [in]  the size of the request body
   * @param request  [in]  the map of request headers sent by the client
   * @param cookies  [in]  the map of cookie headers
   * @param response [out] the map of response headers to be built and returned
   *                       by the server
   * @param respcode [out] the HTTP response code to be set as appropriate
   *
   * @return the HTML body response
   */
  virtual std::string
  HandleRequest (const std::string &method,
                 const std::string &url,
                 const std::string &query,
                 const std::string &body,
                 size_t            *bodysize,
                 HeaderMap         &request,
                 HeaderMap         &cookies,
                 HeaderMap         &response,
                 int               &respcode) = 0;
};

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END

#endif /* __EOSMGM_PROTOCOLHANDLER__HH__ */
