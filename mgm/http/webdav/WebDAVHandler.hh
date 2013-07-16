// ----------------------------------------------------------------------
// File: WebDAVHandler.hh
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
 * @file   WebDAVHandler.hh
 *
 * @brief  Class to handle WebDAV requests and build responses.
 */

#ifndef __EOSMGM_WEBDAV_HANDLER__HH__
#define __EOSMGM_WEBDAV_HANDLER__HH__

/*----------------------------------------------------------------------------*/
#include "common/http/ProtocolHandler.hh"
#include "common/Mapping.hh"
#include "mgm/Namespace.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <map>
#include <string>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class WebDAVHandler : public eos::common::ProtocolHandler
{

private:
  /**
   * WebDAV HTTP extension methods
   */
  enum Methods
  {
    PROPFIND,  //!< Used to retrieve properties, stored as XML, from a web
               //!< resource. It is also overloaded to allow one to retrieve
               //!< the collection structure (a.k.a. directory hierarchy) of a
               //!< remote system.
    PROPPATCH, //!< Used to change and delete multiple properties on a resource
               //!< in a single atomic act
    MKCOL,     //!< Used to create collections (a.k.a. a directory)
    COPY,      //!< Used to copy a resource from one URI to another
    MOVE,      //!< Used to move a resource from one URI to another
    LOCK,      //!< Used to put a lock on a resource. WebDAV supports both
               //!< shared and exclusive locks.
    UNLOCK     //!< Used to remove a lock from a resource
  };

public:

  /**
   * Constructor
   */
  WebDAVHandler (eos::common::Mapping::VirtualIdentity *vid) :
    eos::common::ProtocolHandler(vid) {};

  /**
   * Destructor
   */
  virtual ~WebDAVHandler () {};

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
   * Build a response to the given WebDAV request.
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
   * Make a collection (create a directory). If any of the parent directories
   * do not exist, the response will be a failure, as WebDAV is not supposed
   * to create intermediate directories.
   *
   * @param request  the client request object
   *
   * @return the response object
   */
  eos::common::HttpResponse*
  MkCol (eos::common::HttpRequest *request);

  /**
   * Move a resource (file or directory). If the "Overwrite" header is set to
   * "T" and the target exists, the target will be overwritten.
   *
   * @param request  the client request object
   *
   * @return the response object
   */
  eos::common::HttpResponse*
  Move (eos::common::HttpRequest *request);

  /**
   * Copy a resource (file or directory).
   *
   * @param request  the client request object
   *
   * @return the response object
   */
  eos::common::HttpResponse*
  Copy (eos::common::HttpRequest *request);

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
    if      (method == "PROPFIND")  return Methods::PROPFIND;
    else if (method == "PROPPATCH") return Methods::PROPPATCH;
    else if (method == "MKCOL")     return Methods::MKCOL;
    else if (method == "COPY")      return Methods::COPY;
    else if (method == "MOVE")      return Methods::MOVE;
    else if (method == "LOCK")      return Methods::LOCK;
    else if (method == "UNLOCK")    return Methods::UNLOCK;
    else return -1;
  }
};

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END

#endif /* __EOSMGM_WEBDAV_HANDLER__HH__ */
