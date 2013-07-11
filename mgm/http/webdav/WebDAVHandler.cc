// ----------------------------------------------------------------------
// File: WebDAVHandler.cc
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
#include "mgm/http/webdav/WebDAVHandler.hh"
#include "mgm/http/webdav/PropFindResponse.hh"
#include "mgm/XrdMgmOfs.hh"
#include "common/http/PlainHttpResponse.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
bool
WebDAVHandler::Matches (const std::string &meth, HeaderMap &headers)
{
  int method =  ParseMethodString(meth);
  if (method == PROPFIND || method == PROPPATCH || method == MKCOL ||
      method == COPY     || method == MOVE      || method == LOCK  ||
      method == UNLOCK)
  {
    eos_static_info("Matched WebDAV protocol for request");
    return true;
  }
  else return false;
}

/*----------------------------------------------------------------------------*/
void
WebDAVHandler::HandleRequest (eos::common::HttpRequest *request)
{
  eos_static_info("handling webdav request");
  eos::common::HttpResponse *response = 0;

  int meth = ParseMethodString(request->GetMethod());
  switch (meth)
  {
  case PROPFIND:
    response = new PropFindResponse(request, mVirtualIdentity);
    break;
  case PROPPATCH:
    response = new eos::common::PlainHttpResponse();
    response->SetResponseCode(eos::common::HttpResponse::NOT_IMPLEMENTED);
    break;
  case MKCOL:
    response = MkCol(request);
    break;
  case COPY:
    response = new eos::common::PlainHttpResponse();
    response->SetResponseCode(eos::common::HttpResponse::NOT_IMPLEMENTED);
    break;
  case MOVE:
    response = new eos::common::PlainHttpResponse();
    response->SetResponseCode(eos::common::HttpResponse::NOT_IMPLEMENTED);
    break;
  case LOCK:
    response = new eos::common::PlainHttpResponse();
    response->SetResponseCode(eos::common::HttpResponse::NOT_IMPLEMENTED);
    break;
  case UNLOCK:
    response = new eos::common::PlainHttpResponse();
    response->SetResponseCode(eos::common::HttpResponse::NOT_IMPLEMENTED);
    break;
  default:
    response = new eos::common::PlainHttpResponse();
    response->SetResponseCode(eos::common::HttpResponse::BAD_REQUEST);
    break;
  }

  mHttpResponse = response->BuildResponse(request);
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
WebDAVHandler::MkCol (eos::common::HttpRequest *request)
{
  eos::common::HttpResponse *response = 0;

  if (!request->GetUrl().size())
  {
    response = HttpServer::HttpError("path name required", response->BAD_REQUEST);
  }
  else
  {
    XrdSfsMode mode = 0;
    XrdOucErrInfo error;

    if (gOFS->_mkdir(request->GetUrl().c_str(), mode, error,
                     *mVirtualIdentity, (const char*) 0))
    {
      response = HttpServer::HttpError("unable to create directory", errno);
    }
    else
    {
      response = new eos::common::PlainHttpResponse();
      response->SetResponseCode(response->CREATED);
    }
  }

  return response;
}

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END
