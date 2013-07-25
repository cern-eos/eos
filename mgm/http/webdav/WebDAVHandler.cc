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
    eos_static_debug("msg=\"matched webdav protocol for request\"");
    return true;
  }
  else return false;
}

/*----------------------------------------------------------------------------*/
void
WebDAVHandler::HandleRequest (eos::common::HttpRequest *request)
{
  eos_static_info("msg=\"handling webdav request\"");
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
    response = Copy(request);
    break;
  case MOVE:
    response = Move(request);
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

  XrdSecEntity client;
  client.name   = const_cast<char*>(mVirtualIdentity->name.c_str());
  client.host   = const_cast<char*>(mVirtualIdentity->host.c_str());
  client.tident = const_cast<char*>(mVirtualIdentity->tident.c_str());

  if (!request->GetUrl().size())
  {
    response = HttpServer::HttpError("path name required",
                                     response->BAD_REQUEST);
  }
  else if (*request->GetBodySize() != 0)
  {
    // we do not support request bodies with MKCOL requests
    response = HttpServer::HttpError("request body not supported",
                                     response->UNSUPPORTED_MEDIA_TYPE);
  }

  else
  {
    XrdSfsMode    mode = 0;
    int           rc   = 0;
    XrdOucErrInfo error;

    rc = gOFS->mkdir(request->GetUrl().c_str(), mode, error,
                     &client, (const char*) 0);
    if (rc != SFS_OK)
    {
      if (rc == SFS_ERROR)
      {
        if (error.getErrInfo() == EEXIST)
        {
          // directory exists
          response = HttpServer::HttpError(error.getErrText(),
                                           response->METHOD_NOT_ALLOWED);
        }
        else if (error.getErrInfo() == ENOENT)
        {
          // parent directory does not exist
          response = HttpServer::HttpError(error.getErrText(),
                                           response->CONFLICT);
        }
        else if (error.getErrInfo() == EPERM)
        {
          // not permitted
          response = HttpServer::HttpError(error.getErrText(),
                                           response->FORBIDDEN);
        }
        else if (error.getErrInfo() == ENOSPC)
        {
          // no space left
          response = HttpServer::HttpError(error.getErrText(),
                                           response->INSUFFICIENT_STORAGE);
        }
        else
        {
          // some other error
          response = HttpServer::HttpError(error.getErrText(),
                                           error.getErrInfo());
        }
      }
      else if (rc == SFS_REDIRECT)
      {
        // redirection
        response = HttpServer::HttpRedirect(request->GetUrl(),
                                            error.getErrText(),
                                            error.getErrInfo(), false);
      }
      else if (rc == SFS_STALL)
      {
        // stall
        response = HttpServer::HttpStall(error.getErrText(),
                                         error.getErrInfo());
      }
      else
      {
        // something unexpected
        response = HttpServer::HttpError(error.getErrText(),
                                         error.getErrInfo());
      }
    }
    else
    {
      // everything went well
      response = new eos::common::PlainHttpResponse();
      response->SetResponseCode(response->CREATED);
    }
  }

  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
WebDAVHandler::Move (eos::common::HttpRequest *request)
{
  eos::common::HttpResponse *response = 0;
  XrdOucString prot, port;
  const char *destination = eos::common::StringConversion::ParseUrl
      (request->GetHeaders()["Destination"].c_str(), prot, port);

  eos_static_info("action=\"move\" src=\"%s\", dest=\"%s\"",
                  request->GetUrl().c_str(), destination);

  XrdSecEntity client;
  client.name   = const_cast<char*>(mVirtualIdentity->name.c_str());
  client.host   = const_cast<char*>(mVirtualIdentity->host.c_str());
  client.tident = const_cast<char*>(mVirtualIdentity->tident.c_str());

  if (!request->GetUrl().size())
  {
    response = HttpServer::HttpError("source path required",
                                     response->BAD_REQUEST);
  }
  else if (!destination)
  {
    response = HttpServer::HttpError("destination required",
                                     response->BAD_REQUEST);
  }
  else
  {
    int           rc = 0;
    XrdOucErrInfo error;

    rc = gOFS->rename(request->GetUrl().c_str(),
                      destination,
                      error, &client, 0, 0);
    if (rc != SFS_OK)
    {
      if (rc == SFS_ERROR)
      {
        if (error.getErrInfo() == EEXIST)
        {
          // resource exists
          if (request->GetHeaders()["Overwrite"] == "T")
          {
            // force the rename
            struct stat buf;
            gOFS->_stat(request->GetUrl().c_str(), &buf, error,
                        *mVirtualIdentity, "");

            ProcCommand  cmd;
            XrdOucString info = "mgm.cmd=rm&mgm.path=";
            info += destination;
            if (S_ISDIR(buf.st_mode)) info += "&mgm.option=r";

            rc = cmd.open("/proc/user", info.c_str(), *mVirtualIdentity, &error);
            cmd.close();

            if (rc != SFS_OK)
            {
              // something went wrong while deleting the destination
              response = HttpServer::HttpError(error.getErrText(),
                                               error.getErrInfo());
            }
            else
            {
              // try the rename again
              rc = gOFS->rename(request->GetUrl().c_str(),
                                destination,
                                error, &client, 0, 0);

              if (rc != SFS_OK)
              {
                // something went wrong with the second rename
                response = HttpServer::HttpError(error.getErrText(),
                                                 error.getErrInfo());
              }
              else
              {
                // it worked!
                response = new eos::common::PlainHttpResponse();
                response->SetResponseCode(response->NO_CONTENT);
              }
            }

          }
          else
          {
            // directory exists but we are not overwriting
            response = HttpServer::HttpError(error.getErrText(),
                                             response->PRECONDITION_FAILED);
          }
        }
        else if (error.getErrInfo() == ENOENT)
        {
          // parent directory does not exist
          response = HttpServer::HttpError(error.getErrText(),
                                           response->CONFLICT);
        }
        else if (error.getErrInfo() == EPERM)
        {
          // not permitted
          response = HttpServer::HttpError(error.getErrText(),
                                           response->FORBIDDEN);
        }
        else
        {
          // some other error
          response = HttpServer::HttpError(error.getErrText(),
                                           error.getErrInfo());
        }
      }
      else if (rc == SFS_REDIRECT)
      {
        // redirection
        response = HttpServer::HttpRedirect(request->GetUrl(),
                                            error.getErrText(),
                                            error.getErrInfo(), false);
      }
      else if (rc == SFS_STALL)
      {
        // stall
        response = HttpServer::HttpStall(error.getErrText(),
                                         error.getErrInfo());
      }
      else
      {
        // something unexpected
        response = HttpServer::HttpError(error.getErrText(),
                                         error.getErrInfo());
      }
    }
    else
    {
      // everything went well
      response = new eos::common::PlainHttpResponse();
      response->SetResponseCode(response->CREATED);
    }
  }

  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
WebDAVHandler::Copy (eos::common::HttpRequest *request)
{
  eos::common::HttpResponse *response = 0;

  response = new eos::common::PlainHttpResponse();
  response->SetResponseCode(eos::common::HttpResponse::NOT_IMPLEMENTED);

  return response;
}

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END
