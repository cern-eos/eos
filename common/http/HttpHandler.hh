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

#ifndef __EOSCOMMON_HTTP_HANDLER__HH__
#define __EOSCOMMON_HTTP_HANDLER__HH__

/*----------------------------------------------------------------------------*/
#include "common/http/ProtocolHandler.hh"
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <map>
#include <string>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class HttpHandler : virtual public eos::common::ProtocolHandler
{

public:
  /**
   * Standard plain HTTP request methods
   */
  enum Methods
  {
    GET,     //!< Requests a representation of the specified resource. Requests
             //!< using GET should only retrieve data and have no other effect.
    HEAD,    //!< Asks for the response identical to the one that would
             //!< correspond to a GET request, but without the response body.
             //!< This is useful for retrieving meta-information written in
             //!< response headers, without having to transport the entire content.
    POST,    //!< Requests that the server accept the entity enclosed in the
             //!< request as a new subordinate of the web resource identified by
             //!< the URI.
    PUT,     //!< Requests that the enclosed entity be stored under the supplied
             //!< URI. If the URI refers to an already existing resource, it is
             //!< modified; if the URI does not point to an existing resource,
             //!< then the server can create the resource with that URI.
    DELETE,  //!< Deletes the specified resource.
    TRACE,   //!< Echoes back the received request so that a client can see what
             //!< (if any) changes or additions have been made by intermediate
             //!< servers.
    OPTIONS, //!< Returns the HTTP methods that the server supports for specified
             //!< URL. This can be used to check the functionality of a web
             //!< server by requesting '*' instead of a specific resource.
    CONNECT, //!< Converts the request connection to a transparent TCP/IP tunnel,
             //!< usually to facilitate SSL-encrypted communication (HTTPS)
             //!< through an unencrypted HTTP proxy.
    PATCH,   //!< Is used to apply partial modifications to a resource.
    CREATE,  //!< internal method used by Xrdhttp - creates a file without payload
  };

  /**
   * Constructor
   */
  HttpHandler () {};

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
  virtual void
  HandleRequest (eos::common::HttpRequest *request) = 0;

  /**
   * Convert the given request method string into its integer constant
   * representation.
   *
   * @param method  the method string to convert
   *
   * @return the converted method string as an integer
   */
  inline static int
  ParseMethodString (const std::string &method)
  {
    if      (method == "GET")     return Methods::GET;
    else if (method == "HEAD")    return Methods::HEAD;
    else if (method == "POST")    return Methods::POST;
    else if (method == "PUT")     return Methods::PUT;
    else if (method == "DELETE")  return Methods::DELETE;
    else if (method == "TRACE")   return Methods::TRACE;
    else if (method == "OPTIONS") return Methods::OPTIONS;
    else if (method == "CONNECT") return Methods::CONNECT;
    else if (method == "PATCH")   return Methods::PATCH;
    else if (method == "CREATE")   return Methods::CREATE;
    else return -1;
  }
};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif /* __EOSCOMMON_HTTP_HANDLER__HH__ */
