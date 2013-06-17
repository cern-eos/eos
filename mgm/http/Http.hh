// ----------------------------------------------------------------------
// File: Http.hh
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
 * @file   Http.hh
 *
 * @brief  TODO
 */

#ifndef __EOSMGM_HTTP__HH__
#define __EOSMGM_HTTP__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/http/ProtocolHandler.hh"
#include "mgm/Namespace.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <map>
#include <string>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class Http : public ProtocolHandler
{

private:
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
  };

public:
  Http ();
  virtual ~Http ();

  /**
   *
   */
  static bool
  Matches(std::string &method, HeaderMap &headers);

  /**
   *
   */
  virtual void
  ParseHeader(HeaderMap &headers);

  /**
   *
   */
  virtual std::string
  HandleRequest(HeaderMap request, HeaderMap response, int error);

  /**
   *
   */
  inline static int
  ParseMethodString(std::string &method)
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
    else return -1;
  }
};

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END

#endif /* __EOSMGM_HTTP__HH__ */
