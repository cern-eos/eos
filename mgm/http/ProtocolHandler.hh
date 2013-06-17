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
   * Constructor
   */
  ProtocolHandler () {};

  /**
   * Destructor
   */
  virtual ~ProtocolHandler () {};

  /**
   * return NULL if no matching protocol found
   */
  static ProtocolHandler*
  CreateProtocolHandler(std::string &method, HeaderMap &headers);

  /**
   *
   */
  static bool
  Matches(std::string &method, HeaderMap &headers);

  /**
   *
   */
  virtual void
  ParseHeader(HeaderMap &headers) = 0;

  /**
   *
   */
  virtual std::string
  HandleRequest(HeaderMap request, HeaderMap response, int error) = 0;
};

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END

#endif /* __EOSMGM_PROTOCOLHANDLER__HH__ */
