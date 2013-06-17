// ----------------------------------------------------------------------
// File: WebDAV.hh
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
 * @file   WebDAV.hh
 *
 * @brief  TODO
 */

#ifndef __EOSMGM_WEBDAV__HH__
#define __EOSMGM_WEBDAV__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/http/ProtocolHandler.hh"
#include "mgm/Namespace.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <map>
#include <string>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class WebDAV : public eos::mgm::ProtocolHandler
{

private:
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
   *
   */
  WebDAV () {};

  /**
   *
   */
  virtual ~WebDAV () {};

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

#endif /* __EOSMGM_WEBDAV__HH__ */
