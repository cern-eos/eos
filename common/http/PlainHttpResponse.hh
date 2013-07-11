// ----------------------------------------------------------------------
// File: PlainHttpResponse.hh
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
 * @file   PlainHttpResponse.hh
 *
 * @brief  The simplest possible HTTP response. Does no request processing
 *         whatsoever.
 */

#ifndef __EOSCOMMON_PLAIN_HTTP_RESPONSE__HH__
#define __EOSCOMMON_PLAIN_HTTP_RESPONSE__HH__

/*----------------------------------------------------------------------------*/
#include "common/http/HttpResponse.hh"
#include "common/Namespace.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <map>
#include <string>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class PlainHttpResponse : public HttpResponse
{

public:

  PlainHttpResponse () {};
  virtual ~PlainHttpResponse () {};

  /**
   * Build an appropriate response to the given request.
   *
   * @param request  the client request object
   *
   * @return the newly built response object (empty in this case)
   */
  HttpResponse*
  BuildResponse (eos::common::HttpRequest *request) { return this; };
};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif /* __EOSCOMMON_PLAIN_HTTP_RESPONSE__HH__ */
