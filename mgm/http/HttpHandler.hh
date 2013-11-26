// ----------------------------------------------------------------------
// File: HttpHandler.hh
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
 * @file   HttpHandler.hh
 *
 * @brief  Class to handle plain HTTP requests and build responses.
 */

#ifndef __EOSMGM_HTTP_HANDLER__HH__
#define __EOSMGM_HTTP_HANDLER__HH__

/*----------------------------------------------------------------------------*/
#include "common/http/HttpHandler.hh"
#include "mgm/Namespace.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <map>
#include <string>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class HttpHandler : public eos::common::HttpHandler
{

public:

  /**
   * Constructor
   */
  HttpHandler (eos::common::Mapping::VirtualIdentity *vid) :
    eos::common::ProtocolHandler(vid) {};

  /**
   * Destructor
   */
  virtual ~HttpHandler () {};

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

  /**
   * Build a response to the given plain HTTP request.
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

  /**
   * Handle an HTTP GET request.
   *
   * @param request  the client request object
   * @param isHEAD indicates that this is a HEAD request
   *
   * @return an HTTP response object
   */
  eos::common::HttpResponse*
  Get (eos::common::HttpRequest *request, bool isHEAD=false);

  /**
   * Handle an HTTP HEAD request.
   *
   * @param request  the client request object
   *
   * @return an HTTP response object
   */
  eos::common::HttpResponse*
  Head (eos::common::HttpRequest *request);

  /**
   * Handle an HTTP POST request.
   *
   * @param request  the client request object
   *
   * @return an HTTP response object
   */
  eos::common::HttpResponse*
  Post (eos::common::HttpRequest *request);

  /**
   * Handle an HTTP PUT request.
   *
   * @param request  the client request object
   *
   * @return an HTTP response object
   */
  eos::common::HttpResponse*
  Put (eos::common::HttpRequest *request);

  /**
   * Handle an HTTP DELETE request.
   *
   * @param request  the client request object
   *
   * @return an HTTP response object
   */
  eos::common::HttpResponse*
  Delete (eos::common::HttpRequest *request);

  /**
   * Handle an HTTP TRACE request.
   *
   * @param request  the client request object
   *
   * @return an HTTP response object
   */
  eos::common::HttpResponse*
  Trace (eos::common::HttpRequest *request);

  /**
   * Handle an HTTP OPTIONS request.
   *
   * @param request  the client request object
   *
   * @return an HTTP response object
   */
  eos::common::HttpResponse*
  Options (eos::common::HttpRequest *request);

  /**
   * Handle an HTTP CONNECT request.
   *
   * @param request  the client request object
   *
   * @return an HTTP response object
   */
  eos::common::HttpResponse*
  Connect (eos::common::HttpRequest *request);

  /**
   * Handle an HTTP PATCH request.
   *
   * @param request  the client request object
   *
   * @return an HTTP response object
   */
  eos::common::HttpResponse*
  Patch (eos::common::HttpRequest *request);

};

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END

#endif /* __EOSMGM_HTTP_HANDLER__HH__ */
