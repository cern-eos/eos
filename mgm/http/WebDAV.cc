// ----------------------------------------------------------------------
// File: WebDAV.cc
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
#include "mgm/http/WebDAV.hh"
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
bool
WebDAV::Matches(std::string &meth, ProtocolHandler::HeaderMap &headers)
{
  int method =  ParseMethodString(meth);
  if (method == WebDAV::PROPFIND || method == WebDAV::PROPPATCH ||
      method == WebDAV::MKCOL    || method == WebDAV::COPY      ||
      method == WebDAV::MOVE     || method == WebDAV::LOCK      ||
      method == WebDAV::UNLOCK)
  {
    eos_static_info("info=Matched WebDAV protocol for request");
    return true;
  }
  else return false;
}

/*----------------------------------------------------------------------------*/
void
WebDAV::ParseHeader (ProtocolHandler::HeaderMap &headers)
{

}

/*----------------------------------------------------------------------------*/
std::string
WebDAV::HandleRequest(ProtocolHandler::HeaderMap request,
                      ProtocolHandler::HeaderMap response,
                      int                        error)
{
  error = 1;
  return "Not Implemented";
}

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END
