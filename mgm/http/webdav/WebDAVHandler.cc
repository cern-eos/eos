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
#include "mgm/http/webdav/PropPatchResponse.hh"
#include "mgm/http/webdav/LockResponse.hh"
#include "mgm/XrdMgmOfs.hh"
#include "common/http/PlainHttpResponse.hh"
#include "common/http/OwnCloud.hh"
#include "common/Logging.hh"
#ifdef __clang__
#pragma clang diagnostic ignored "-Wformat-security"
#endif
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
  eos_static_debug("msg=\"handling webdav request\"");
  eos::common::HttpResponse *response = 0;

  request->AddEosApp();

  int meth = ParseMethodString(request->GetMethod());
  switch (meth)
  {
  case PROPFIND:
    gOFS->MgmStats.Add("Http-PROPFIND", mVirtualIdentity->uid, mVirtualIdentity->gid, 1);
    response = new PropFindResponse(request, mVirtualIdentity);
    break;
  case PROPPATCH:
    gOFS->MgmStats.Add("Http-PROPPATCH", mVirtualIdentity->uid, mVirtualIdentity->gid, 1);
    response = new PropPatchResponse(request, mVirtualIdentity);
    break;
  case MKCOL:
    gOFS->MgmStats.Add("Http-MKCOL", mVirtualIdentity->uid, mVirtualIdentity->gid, 1);
    response = MkCol(request);
    break;
  case COPY:
    gOFS->MgmStats.Add("Http-COPY", mVirtualIdentity->uid, mVirtualIdentity->gid, 1);
    response = Copy(request);
    break;
  case MOVE:
    gOFS->MgmStats.Add("Http-MOVE", mVirtualIdentity->uid, mVirtualIdentity->gid, 1);
    response = Move(request);
    break;
  case LOCK:
    gOFS->MgmStats.Add("Http-LOCK", mVirtualIdentity->uid, mVirtualIdentity->gid, 1);
    response = new LockResponse(request, mVirtualIdentity);
    break;
  case UNLOCK:
    gOFS->MgmStats.Add("Http-UNLOCK", mVirtualIdentity->uid, mVirtualIdentity->gid, 1);
    response = new eos::common::PlainHttpResponse();
    response->SetResponseCode(eos::common::HttpResponse::NO_CONTENT);
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
  
  std::string url = request->GetUrl();
  eos_static_info("method=MKCOL path=%s", 
                          url.c_str());
  
  client.name   = const_cast<char*>(mVirtualIdentity->name.c_str());
  client.host   = const_cast<char*>(mVirtualIdentity->host.c_str());
  client.tident = const_cast<char*>(mVirtualIdentity->tident.c_str());
  snprintf(client.prot,sizeof(client.prot)-1,mVirtualIdentity->prot.c_str()); 

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
    XrdOucErrInfo error(mVirtualIdentity->tident.c_str());
    ino_t new_inode;
    rc = gOFS->mkdir(request->GetUrl().c_str(), mode, error,
                     &client, (const char*) 0, &new_inode);
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
      char sinode[16];
      snprintf(sinode, sizeof (sinode), "%llu", (unsigned long long) new_inode);
      response->AddHeader("OC-FileId", sinode);
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
  std::string destination = eos::common::StringConversion::ParseUrl
      (request->GetHeaders()["destination"].c_str(), prot, port);

  char encode_destination[1024];
  snprintf(encode_destination,sizeof(encode_destination),"%s",destination.c_str());
  char decode_destination[1024];
  decode_destination[0] = 0;

  if (destination.length() < sizeof(decode_destination)) {

    ::dav_uri_decode(encode_destination, decode_destination);
    destination = decode_destination;
  }

  // owncloud protocol patch
  XrdOucString spath = destination.c_str();

  eos::common::OwnCloud::OwnCloudRemapping(spath, request);
  eos::common::OwnCloud::ReplaceRemotePhp(spath);
  destination = spath.c_str();

  eos_static_info("method=MOVE src=\"%s\", dest=\"%s\"",
                  request->GetUrl().c_str(), destination.c_str());

  XrdSecEntity client;
  client.name   = const_cast<char*>(mVirtualIdentity->name.c_str());
  client.host   = const_cast<char*>(mVirtualIdentity->host.c_str());
  client.tident = const_cast<char*>(mVirtualIdentity->tident.c_str());
  snprintf(client.prot,sizeof(client.prot)-1,mVirtualIdentity->prot.c_str()); 

  if (!request->GetUrl().size())
  {
    response = HttpServer::HttpError("source path required",
                                     response->BAD_REQUEST);
  }
  else if (!destination.size())
  {
    response = HttpServer::HttpError("destination required",
                                     response->BAD_REQUEST);
  }
  else if (request->GetUrl() == destination)
  {
    response = HttpServer::HttpError("destination must be different from source",
                                     response->FORBIDDEN);
  }
  else
  {
    int           rc = 0;
    XrdOucErrInfo error(mVirtualIdentity->tident.c_str());

    rc = gOFS->rename(request->GetUrl().c_str(),
                      destination.c_str(),
                      error, &client, 0, 0);
    if (rc != SFS_OK)
    {
      if (rc == SFS_ERROR)
      {
        if (error.getErrInfo() == EEXIST)
        {
          // resource exists
	  // webdav specifies to overwrite by default if the special header is not set to F
          if ( (!request->GetHeaders().count("overwrite")) || (request->GetHeaders()["overwrite"] == "T") )
          {
            // force the rename
            struct stat buf;
            gOFS->_stat(request->GetUrl().c_str(), &buf, error,
                        *mVirtualIdentity, "");

            ProcCommand  cmd;
            XrdOucString info = "mgm.cmd=rm&mgm.path=";
            info += destination.c_str();
            if (S_ISDIR(buf.st_mode)) info += "&mgm.option=r";

            cmd.open("/proc/user", info.c_str(), *mVirtualIdentity, &error);
            cmd.close();
            rc = cmd.GetRetc();

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
                                destination.c_str(),
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
      if (!request->GetHeaders().count("cbox-skip-location-on-move"))
      {
        response->AddHeader("Location", request->GetHeaders()["destination"].c_str());
      }
    }
  }

  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
WebDAVHandler::Copy (eos::common::HttpRequest *request)
{
  eos::common::HttpResponse *response = 0;

  XrdOucString prot, port;
  std::string destination = eos::common::StringConversion::ParseUrl
      (request->GetHeaders()["destination"].c_str(), prot, port);

  eos_static_info("method=COPY src=\"%s\", dest=\"%s\"",
                  request->GetUrl().c_str(), destination.c_str());

  XrdSecEntity client;
  client.name   = const_cast<char*>(mVirtualIdentity->name.c_str());
  client.host   = const_cast<char*>(mVirtualIdentity->host.c_str());
  client.tident = const_cast<char*>(mVirtualIdentity->tident.c_str());
  snprintf(client.prot,sizeof(client.prot)-1,mVirtualIdentity->prot.c_str()); 

  if (!request->GetUrl().size())
  {
    response = HttpServer::HttpError("source path required",
                                     response->BAD_REQUEST);
  }
  else if (!destination.size())
  {
    response = HttpServer::HttpError("destination required",
                                     response->BAD_REQUEST);
  }
  else if (request->GetUrl() == destination)
  {
    response = HttpServer::HttpError("destination must be different from source",
                                     response->FORBIDDEN);
  }
  else
  {
    int           rc = 0;
    XrdOucErrInfo error;

    ProcCommand  cmd;
    XrdOucString info  = "mgm.cmd=file&mgm.subcmd=copy";
                 info += "&mgm.path=";
                 info += request->GetUrl().c_str();
                 info += "&mgm.file.target=";
                 info += destination.c_str();
                 info += "&eos.ruid=";
                 info += mVirtualIdentity->uid_string.c_str();
                 info += "&eos.rgid=";
                 info +=mVirtualIdentity->gid_string.c_str();

    eos_static_debug("cmd=%s", info.c_str());
    cmd.open("/proc/user", info.c_str(), *mVirtualIdentity, &error);
    cmd.close();
    rc = cmd.GetRetc();
    eos_static_debug("ret=%d", rc);

    if (rc != SFS_OK)
    {
      if (rc == EEXIST)
      {
        // resource exists
	if ( (!request->GetHeaders().count("overwrite")) || (request->GetHeaders()["overwrite"] == "T") )
        {
          // force overwrite
          info += "&mgm.file.option=f";
          eos_static_debug("overwriting: cmd=%s", info.c_str());
          cmd.open("/proc/user", info.c_str(), *mVirtualIdentity, &error);
          cmd.close();
          rc = cmd.GetRetc();
          eos_static_debug("ret=%d", rc);

          if (rc != 0)
          {
            // something went wrong with the overwrite
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
        else
        {
          // resource exists but we are not overwriting
          response = HttpServer::HttpError(error.getErrText(),
                                           response->PRECONDITION_FAILED);
        }
      }
      else if (rc == ENOENT)
      {
        // parent directory does not exist
        response = HttpServer::HttpError(error.getErrText(),
                                         response->CONFLICT);
      }
      else if (rc == EPERM)
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
EOSMGMNAMESPACE_END
