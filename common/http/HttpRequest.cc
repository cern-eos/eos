// ----------------------------------------------------------------------
// File: HttpRequest.cc
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

/*----------------------------------------------------------------------------*/
#include "common/http/HttpRequest.hh"
#include "common/Namespace.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <sstream>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
HttpRequest::HttpRequest (HeaderMap          headers,
                          const std::string &method,
                          const std::string &url,
                          const std::string &query,
                          const std::string &body,
                          size_t            *bodySize,
                          HeaderMap          cookies) :
  mRequestHeaders(headers), mRequestMethod(method), mRequestUrl(url),
  mRequestQuery(query), mRequestBody(body), mRequestBodySize(bodySize),
  mRequestCookies(cookies) {}

/*----------------------------------------------------------------------------*/
std::string
HttpRequest::ToString()
{
  std::stringstream ss;
  ss << GetMethod() << " " << GetUrl() << (GetQuery().size() ? "?" : "")
     << GetQuery() << std::endl;
  for (auto it = GetHeaders().begin(); it != GetHeaders().end(); ++it)
  {
    ss << it->first << ": " << it->second.c_str() << std::endl;
  }
  return ss.str();
}

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END
