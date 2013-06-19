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

protected:
  HeaderMap   mResponseHeaders; //!< the response headers
  std::string mResponseBody;    //!< the response body string
  int         mResponseCode;    //!< the HTTP response code

public:

  /**
   * Constructor
   */
  ProtocolHandler ();

  /**
   * Destructor
   */
  virtual ~ProtocolHandler () {};

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
   * @param request  the map of request headers sent by the client
   * @param method   the request verb used by the client (GET, PUT, etc)
   * @param url      the URL requested by the client
   * @param query    the GET request query string (if any)
   * @param body     the request body data sent by the client
   * @param bodysize the size of the request body
   * @param cookies  the map of cookie headers
   */
  virtual void
  HandleRequest (HeaderMap         &request,
                 const std::string &method,
                 const std::string &url,
                 const std::string &query,
                 const std::string &body,
                 size_t            *bodysize,
                 HeaderMap         &cookies) = 0;

  /**
   * @return the response headers that were built.
   */
  inline HeaderMap
  GetResponseHeaders () { return mResponseHeaders; };

  /**
   * @return the response body that was built.
   */
  inline std::string
  GetResponseBody () { return mResponseBody; };

  /**
   * @return the HTTP response code that was decided upon.
   */
  inline int
  GetResponseCode () { return mResponseCode; };

  /**
   * Dump all parts of the request to the log
   */
  void
  PrintResponse ();
};

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END

#endif /* __EOSMGM_PROTOCOLHANDLER__HH__ */
