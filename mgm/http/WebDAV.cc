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
WebDAV::Matches (const std::string &meth, HeaderMap &headers)
{
  int method =  ParseMethodString(meth);
  if (method == PROPFIND || method == PROPPATCH || method == MKCOL ||
      method == COPY     || method == MOVE      || method == LOCK  ||
      method == UNLOCK)
  {
    eos_static_info("info=Matched WebDAV protocol for request");
    return true;
  }
  else return false;
}

/*----------------------------------------------------------------------------*/
std::string
WebDAV::HandleRequest (const std::string &method,
                       const std::string &url,
                       const std::string &query,
                       const std::string &body,
                       size_t            *bodysize,
                       HeaderMap         &request,
                       HeaderMap         &cookies,
                       HeaderMap         &response,
                       int               &respcode)
{
  std::string result;
  int meth = ParseMethodString(method);

  switch (meth)
  {
  case PROPFIND:
    result = PropFind(request, response, respcode);
    break;
  case PROPPATCH:
    break;
  case MKCOL:
    break;
  case COPY:
    break;
  case MOVE:
    break;
  case LOCK:
    break;
  case UNLOCK:
    break;
  default:
    respcode = 400;
    return "No such method";
  }

  return result;
}

/*----------------------------------------------------------------------------*/
std::string
WebDAV::PropFind (HeaderMap &request, HeaderMap &response, int &respcode)
{

}

/*----------------------------------------------------------------------------*/
std::string
WebDAV::PropPatch (HeaderMap &request, HeaderMap &response, int &respcode)
{

}

/*----------------------------------------------------------------------------*/
std::string
WebDAV::MkCol (HeaderMap &request, HeaderMap &response, int &respcode)
{

}

/*----------------------------------------------------------------------------*/
std::string
WebDAV::Copy (HeaderMap &request, HeaderMap &response, int &respcode)
{

}

/*----------------------------------------------------------------------------*/
std::string
WebDAV::Move (HeaderMap &request, HeaderMap &response, int &respcode)
{

}

/*----------------------------------------------------------------------------*/
std::string
WebDAV::Lock (HeaderMap &request, HeaderMap &response, int &respcode)
{

}

/*----------------------------------------------------------------------------*/
std::string
WebDAV::Unlock (HeaderMap &request, HeaderMap &response, int &respcode)
{

}

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END
