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

#ifndef __EOSCOMMON_PROTOCOLHANDLER__HH__
#define __EOSCOMMON_PROTOCOLHANDLER__HH__

/*----------------------------------------------------------------------------*/
#include "common/http/HttpRequest.hh"
#include "common/http/HttpResponse.hh"
#include "common/Mapping.hh"
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <string>
#include <map>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class ProtocolHandler
{

public:
  typedef std::map<std::string, std::string> HeaderMap;

protected:
  HttpResponse                          *mHttpResponse;    //!< the HTTP response
  eos::common::Mapping::VirtualIdentity *mVirtualIdentity; //!< the virtual identity

public:

  /**
   * Constructor
   */
  ProtocolHandler () :
    mHttpResponse(0), mVirtualIdentity(0) {};

  /**
   * Constructor
   */
  ProtocolHandler (eos::common::Mapping::VirtualIdentity *vid) :
    mHttpResponse(0), mVirtualIdentity(vid) {};

  /**
   * Destructor
   */
  virtual ~ProtocolHandler () { delete mHttpResponse; };

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
   * @param request the client request object
   */
  virtual void
  HandleRequest (HttpRequest *request) = 0;

  /**
   * @return the HttpResponse object
   */
  inline HttpResponse*
  GetResponse() { return mHttpResponse; }

  /**
   * Delete the HttpResponse object
   */
  inline void
  DeleteResponse() { delete mHttpResponse; mHttpResponse = 0; }
};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif /* __EOSCOMMON_PROTOCOLHANDLER__HH__ */
