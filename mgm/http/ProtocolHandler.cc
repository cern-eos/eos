// ----------------------------------------------------------------------
// File: ProtocolHandler.cc
// Author: Justin Lewis Salmon - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
#include "mgm/http/ProtocolHandler.hh"
#include "mgm/http/Http.hh"
#include "mgm/http/S3.hh"
#include "mgm/http/WebDAV.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
ProtocolHandler::ProtocolHandler() : mResponseCode(200) {}

/*----------------------------------------------------------------------------*/
ProtocolHandler*
ProtocolHandler::CreateProtocolHandler (const std::string &method,
                                        HeaderMap         &headers)
{
  if (S3::Matches(method, headers))
  {
    return new S3();
  }
  else if (WebDAV::Matches(method, headers))
  {
    return new WebDAV();
  }
  else if (Http::Matches(method, headers))
  {
    return new Http();
  }

  else return NULL;
}

/*----------------------------------------------------------------------------*/
void
ProtocolHandler::PrintResponse()
{
  eos_static_info("response code=%d", mResponseCode);

  for (auto it = mResponseHeaders.begin(); it != mResponseHeaders.end(); ++it)
  {
    eos_static_info("response header:%s=%s", it->first.c_str(),
                                             it->second.c_str());
  }

  eos_static_info("response body=\n%s", mResponseBody.c_str());
}

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END
