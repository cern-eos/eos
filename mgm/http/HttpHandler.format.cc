// ----------------------------------------------------------------------
// File: HttpHandler.cc
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
#include "mgm/http/HttpHandler.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "mgm/Namespace.hh"
#include "mgm/XrdMgmOfs.hh"
#include "common/http/PlainHttpResponse.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
bool
HttpHandler::Matches (const std::string &meth, HeaderMap &headers)
{
  int method = ParseMethodString(meth);
  if (method == GET || method == HEAD || method == POST ||
      method == PUT || method == DELETE || method == TRACE ||
      method == OPTIONS || method == CONNECT || method == PATCH)
  {
    eos_static_debug("Matched HTTP protocol for request");
    return true;
  }
  else return false;
}

/*----------------------------------------------------------------------------*/
void
HttpHandler::HandleRequest (eos::common::HttpRequest *request)
{
  eos_static_debug("handling http request");
  eos::common::HttpResponse *response = 0;

  int meth = ParseMethodString(request->GetMethod());
  switch (meth)
  {
  case GET:
    response = Get(request);
    break;
  case HEAD:
    response = Head(request);
    break;
  case POST:
    response = Post(request);
    break;
  case PUT:
    response = Put(request);
    break;
  case DELETE:
    response = Delete(request);
    break;
  case TRACE:
    response = Trace(request);
    break;
  case OPTIONS:
    response = Options(request);
    break;
  case CONNECT:
    response = Connect(request);
    break;
  case PATCH:
    response = Patch(request);
    break;
  default:
    response = new eos::common::PlainHttpResponse();
    response->SetResponseCode(eos::common::HttpResponse::BAD_REQUEST);
    response->SetBody("No such method");
  }

  mHttpResponse = response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
HttpHandler::Get (eos::common::HttpRequest *request, bool isHEAD)
{
  XrdSecEntity client(mVirtualIdentity->prot.c_str());
  client.name = const_cast<char*> (mVirtualIdentity->name.c_str());
  client.host = const_cast<char*> (mVirtualIdentity->host.c_str());
  client.tident = const_cast<char*> (mVirtualIdentity->tident.c_str());

  // Classify path to split between directory or file objects
  bool isfile = true;
  std::string url = request->GetUrl();
  std::string query = request->GetQuery();
  eos::common::HttpResponse *response = 0;
  struct stat buf;

  XrdOucString spath = request->GetUrl().c_str();
  if (!spath.beginswith("/proc/"))
  {
    if (spath.endswith("/"))
    {
      isfile = false;
    }
    else
    {
      
      XrdOucErrInfo error;
      // find out if it is a file or directory
      if (gOFS->stat(url.c_str(), &buf, error, &client, ""))
      {
        eos_static_info("method=GET error=ENOENT path=%s", 
                          url.c_str());
        response = HttpServer::HttpError("No such file or directory",
                                         response->NOT_FOUND);
        return response;
      }
      if (S_ISDIR(buf.st_mode))
        isfile = false;
      else 
      {
        if (isHEAD)
        {
          std::string basename = url.substr(url.rfind("/")+1);
          eos_static_info("cmd=GET(HEAD) size=%llu path=%s", 
                          buf.st_size, 
                          url.c_str());
          // HEAD requests on files can return from the MGM without redirection
          response = HttpServer::HttpHead(buf.st_size, basename);
          return response;
        }
      }
    }
  }
  else
  {
    isfile = true;
  }

  if (isfile)
  {
    eos_static_info("method=GET file=%s", 
                          url.c_str());
    XrdSfsFile* file = gOFS->newFile(client.name);
    if (file)
    {
      XrdSfsFileOpenMode open_mode = 0;
      mode_t create_mode = 0;

      int rc = file->open(url.c_str(), open_mode, create_mode, &client,
                          query.c_str());
      if ((rc != SFS_REDIRECT) && open_mode)
      {
        // retry as a file creation
        open_mode |= SFS_O_CREAT;
        rc = file->open(url.c_str(), open_mode, create_mode, &client,
                        query.c_str());
      }

      if (rc != SFS_OK)
      {
        if (rc == SFS_REDIRECT)
        {
          // the embedded server on FSTs is hardcoded to run on port 8001
          response = HttpServer::HttpRedirect(request->GetUrl(),
                                              file->error.getErrText(),
                                              8001, false);
        }
        else
          if (rc == SFS_ERROR)
        {
          if (file->error.getErrInfo() == ENODEV)
          {
            response = new eos::common::PlainHttpResponse();
          }
          else
          {
            response = HttpServer::HttpError(file->error.getErrText(),
                                             file->error.getErrInfo());
          }
        }
        else
          if (rc == SFS_DATA)
        {
          response = HttpServer::HttpData(file->error.getErrText(),
                                          file->error.getErrInfo());
        }
        else
          if (rc == SFS_STALL)
        {
          response = HttpServer::HttpStall(file->error.getErrText(),
                                           file->error.getErrInfo());
        }
        else
        {
          response = HttpServer::HttpError("Unexpected result from file open",
                                           EOPNOTSUPP);
        }
      }
      else
      {
        char buffer[65536];
        offset_t offset = 0;
        std::string result;
        do
        {
          size_t nread = file->read(offset, buffer, sizeof (buffer));
          if (nread > 0)
          {
            result.append(buffer, nread);
          }
          if (nread != sizeof (buffer))
          {
            break;
          }
        }
        while (1);
        file->close();
        response = new eos::common::PlainHttpResponse();
        response->SetBody(result);
      }
      // clean up the object
      delete file;
    }
  }
  else
  {
    eos_static_info("method=GET dir=%s", 
                          url.c_str());
    errno = 0;
    XrdMgmOfsDirectory directory;
    int listrc = directory.open(request->GetUrl().c_str(), *mVirtualIdentity,
                                (const char*) 0);

    if (!listrc)
    {
      std::string result;
      const char *val;
      result += "<!DOCTYPE html>\n";
      //result += "<head>\n<style type=\"text/css\">\n<!--\nbody "
      //  "{font-family:Arial, sans-serif; font-weight:lighter}\n-->\n</style>\n</head>";
      result += "<head>\n"
        " <title>EOS HTTP Browser</title>"
        "<link rel=\"stylesheet\" href=\"http://www.w3.org/StyleSheets/Core/Chocolate\" "
        "</head>\n";

      result += "<html>\n";
      result += "<body>\n";

      result += "<script type=\"text/javascript\">\n";
      result += "// Popup window code \n";
      result += "function newPopup(url) { \n";
      result += "popupWindow = window.open(\n";
      result += "url,'popUpWindow','height=200,width=500,left=10,top=10,resizable=no,scrollbars=no,toolbar=no,menubar=no,location=no,directories=no,status=no')\n";
      result += "}\n";
      result += "</script>\n";
      result += "<img src=\"data:image/jpeg;base64,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dE91dHB1dAAAAAQAAAAAUHN0U2Jvb2wBAAAAAEludGVlbnVtAAAAAEludGUAAAAAQ2xybQAAAA9wcmludFNpeHRlZW5CaXRib29sAAAAAAtwcmludGVyTmFtZVRFWFQAAAAfAEgAUAAgAEwAYQBzAGUAcgBKAGUAdAAgAFAAcgBvAGYAZQBzAHMAaQBvAG4AYQBsACAAUAAxADEAMAAyAAAAOEJJTQQ7AAAAAAGyAAAAEAAAAAEAAAAAABJwcmludE91dHB1dE9wdGlvbnMAAAASAAAAAENwdG5ib29sAAAAAABDbGJyYm9vbAAAAAAAUmdzTWJvb2wAAAAAAENybkNib29sAAAAAABDbnRDYm9vbAAAAAAATGJsc2Jvb2wAAAAAAE5ndHZib29sAAAAAABFbWxEYm9vbAAAAAAASW50cmJvb2wAAAAAAEJja2dPYmpjAAAAAQAAAAAAAFJHQkMAAAADAAAAAFJkICBkb3ViQG/gAAAAAAAAAAAAR3JuIGRvdWJAb+AAAAAAAAAAAABCbCAgZG91YkBv4AAAAAAAAAAAAEJyZFRVbnRGI1JsdAAAAAAAAAAAAAAAAEJsZCBVbnRGI1JsdAAAAAAAAAAAAAAAAFJzbHRVbnRGI1B4bEBh/64gAAAAAAAACnZlY3RvckRhdGFib29sAQAAAABQZ1BzZW51bQAAAABQZ1BzAAAAAFBnUEMAAAAATGVmdFVudEYjUmx0AAAAAAAAAAAAAAAAVG9wIFVudEYjUmx0AAAAAAAAAAAAAAAAU2NsIFVudEYjUHJjQFkAAAAAAAA4QklNA+0AAAAAABAAj/1xAAEAAgCP/XEAAQACOEJJTQQmAAAAAAAOAAAAAAAAAAAAAD+AAAA4QklNBA0AAAAAAAQAAAB4OEJJTQQZAAAAAAAEAAAAHjhCSU0D8wAAAAAACQAAAAAAAAAAAQA4QklNJxAAAAAAAAoAAQAAAAAAAAACOEJJTQP1AAAAAABIAC9mZgABAGxmZgAGAAAAAAABAC9mZgABAKGZmgAGAAAAAAABADIAAAABAFoAAAAGAAAAAAABADUAAAABAC0AAAAGAAAAAAABOEJJTQP4AAAAAABwAAD/////////////////////////////A+gAAAAA/////////////////////////////wPoAAAAAP//////////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YzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMC1jMDYwIDYxLjEzNDc3NywgMjAxMC8wMi8xMi0xNzozMjowMCAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvIiB4bWxuczp4bXBNTT0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wL21tLyIgeG1sbnM6c3RFdnQ9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9zVHlwZS9SZXNvdXJjZUV2ZW50IyIgeG1sbnM6ZGM9Imh0dHA6Ly9wdXJsLm9yZy9kYy9lbGVtZW50cy8xLjEvIiB4bWxuczpwaG90b3Nob3A9Imh0dHA6Ly9ucy5hZG9iZS5jb20vcGhvdG9zaG9wLzEuMC8iIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgUGhvdG9zaG9wIENTNSBNYWNpbnRvc2giIHhtcDpDcmVhdGVEYXRlPSIyMDEzLTEwLTA0VDE1OjA4OjI0KzAyOjAwIiB4bXA6TWV0YWRhdGFEYXRlPSIyMDEzLTEwLTA0VDE1OjA4OjI0KzAyOjAwIiB4bXA6TW9kaWZ5RGF0ZT0iMjAxMy0xMC0wNFQxNTowODoyNCswMjowMCIgeG1wTU06SW5zdGFuY2VJRD0ieG1wLmlpZDowQTgwMTE3NDA3MjA2ODExODhDNjk5OUZFMTkwRTUzMiIgeG1wTU06RG9jdW1lbnRJRD0ieG1wLmRpZDowOTgwMTE3NDA3MjA2ODExODhDNjk5OUZFMTkwRTUzMiIgeG1wTU06T3JpZ2luYWxEb2N1bWVudElEPSJ4bXAuZGlkOjA5ODAxMTc0MDcyMDY4MTE4OEM2OTk5RkUxOTBFNTMyIiBkYzpmb3JtYXQ9ImltYWdlL2pwZWciIHBob3Rvc2hvcDpDb2xvck1vZGU9IjMiIHBob3Rvc2hvcDpJQ0NQcm9maWxlPSJzUkdCIElFQzYxOTY2LTIuMSI+IDx4bXBNTTpIaXN0b3J5PiA8cmRmOlNlcT4gPHJkZjpsaSBzdEV2dDphY3Rpb249ImNyZWF0ZWQiIHN0RXZ0Omluc3RhbmNlSUQ9InhtcC5paWQ6MDk4MDExNzQwNzIwNjgxMTg4QzY5OTlGRTE5MEU1MzIiIHN0RXZ0OndoZW49IjIwMTMtMTAtMDRUMTU6MDg6MjQrMDI6MDAiIHN0RXZ0OnNvZnR3YXJlQWdlbnQ9IkFkb2JlIFBob3Rvc2hvcCBDUzUgTWFjaW50b3NoIi8+IDxyZGY6bGkgc3RFdnQ6YWN0aW9uPSJzYXZlZCIgc3RFdnQ6aW5zdGFuY2VJRD0ieG1wLmlpZDowQTgwMTE3NDA3MjA2ODExODhDNjk5OUZFMTkwRTUzMiIgc3RFdnQ6d2hlbj0iMjAxMy0xMC0wNFQxNTowODoyNCswMjowMCIgc3RFdnQ6c29mdHdhcmVBZ2VudD0iQWRvYmUgUGhvdG9zaG9wIENTNSBNYWNpbnRvc2giIHN0RXZ0OmNoYW5nZWQ9Ii8iLz4gPC9yZGY6U2VxPiA8L3htcE1NOkhpc3Rvcnk+IDwvcmRmOkRlc2NyaXB0aW9uPiA8L3JkZjpSREY+IDwveDp4bXBtZXRhPiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIDw/eHBhY2tldCBlbmQ9InciPz7/4gxYSUNDX1BST0ZJTEUAAQEAAAxITGlubwIQAABtbnRyUkdCIFhZWiAHzgACAAkABgAxAABhY3NwTVNGVAAAAABJRUMgc1JHQgAAAAAAAAAAAAAAAQAA9tYAAQAAAADTLUhQICAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA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mam5ydnp+So6SlpqeoqaqrrK2ur6/9oADAMBAAIRAxEAPwDgXNvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFXc28TirubeJxV3NvE4q7m3icVdzbxOKu5t4nFX/9DgGKuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2KuxV2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ADZG0TjSrHZDG2MYq6WePEyGNUFljxNoxt/UhkeJl4Tf1EY8S+E2bDbpg4l8JSbTq48S+Eh5NNOHiYeEpjRZJfsj/P8A4LDxL4Sw+XZhvxqPmP8AmrDxIOFBT6S8fUU+n/m7HiajipASQFDkmCiQRirWKuxV2KuxVMtH/b+j+OKplirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdiqPi0eSWFbgPGFauxO+x4+GC00g7pFthV3X6DiDaEPZzSXr+naQyzN4RoW9/2csEbRbPbL8odXuIRcSPDbg1+GYurbGn2fTyJFJUv+VValWnrW/8AwTf9U8Cub8qtTAr6tv8A8E//AFTxVY/5X6iv+7YP+Cb/AKp4LVZ/yrTUf9+Q/wDBN/zRhVcPyx1E/wC7YB/sm/6p4qpy/lvqMYJDROR+ypYn7uGGlSbS/Luoag/D0JIF7vMjKvf9rifDAdlWa5pSaP8A3l1bTN/LFIGP3fD45AStNJD+mYv5W+4f1yykJzo+lXmrEfV4JQh/bZG49/2lDfy5Lh2tWU/8qr1Pjz9W337cmr/ybyolNJPrXldtINLi6tSx7LJv2/ZZVP7WPEhizavCppRvnQU/XkwLVl+k+SL3U7dbqFo1R60Dlgdjx3+A4kUqM/5VrqHaSD/gm/5owK035a6iv7cNB3q3/VPFVjfl5fDrJD/wTf8ANGKoG/8AKd1YrykaM/In+K4qxm41OOBuDBifan9cVattUiuJBEoYE+NP64qzdPy31BxUSQ7/AOU3/NGGlaX8udQZ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sVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdir1LyY/HSYP9n/xN8iOqDyTibl2wslOSSnXqcVWiXYimKofUY+MNBuTSv35FXn/AOXZImul/wCMX6nywckIAn48irNvJ0g9Fgvt1+bYVfUn5Qgjy3DXanL/AJOyYq8BFQwI5d9hWnTbFXlGqfl9rWoXzh0IBp8biTj9n+bi38uSAV6j+Rvk228r+aLK5uJS92fV4qjAqf3cqv8ACyxt/dvkCd1ZP/zl9rdzoM2nPZlVa89fk24b939W4/ZK/wA+Kpf/AM47ecNF0bTJdS1FzdatLxNC0byJRp4/3Qdlnj5wleXxfF/q/akI9VV9O/MTzJfIbrULiSGV+scLyqq8Tx+GN3qvJfixKvTvyr86X9/d/ULxjKGAo7FmYUEj/ad/9j9nIqxT8zo00nXZHtD6P2acTxA/dx/y8f5sVeh/lrrtxqWh/WriUyv/ADFix/vHXqx5fZ4/tYUvmnzB+ZWo6sgjvZ2ZFOwd2I3Kt/uyRh+xkVejeUfzY8t+S/KxuoJoJNXWnJOURdqysB8KyRTNxil/axQ8F1f8+PNeoT+rFf3UVf2Ip5lXoP2Vm/yckAr6O/5xk/MDXfNltdWfmOJ5I4fS9KWRZDz5Gd5Ock7yK/D00ReC/wCTkqV5V5//ACm8s+X9UnW4mfmPT4wxNGAtVj+1GY46V5csgTSse/S9pplBpEEcZA2kKAP/AMHEV8WwblK2TzfqLE8Z5WY+DsT/AMSbtkSCrJvy88meaPNV6t1YSSQ8K0nkMqrusi/DKiP9riyN/wAi8RFX1kG0zVG+pXjWt1cxneOquy1+L7L8nXkm+WK+ZP8AnIj8yNbs9QbR75DHbCnDgrhJPhgmfZ34v6Tcf2PhbIkK8J1HVdSeMTPHIkLdHIYA02Px/ZP+VgpbSCW7kkJLMfvyVIUS1euFWsVdirsVdirsVdirsVdirsVdirsVRuk/70L9P6sJSOSf4EOxV2KuxV2Kv//Q4BirsVdirsVdiqUap/vQP9QfrOKomx7fPFU4/ZxQVFdpU/1h+vCf96zHR6Ta3FLWEf8AFaf8RzhJx9R/rF9bw/3cP+Fw/wBytNzjwuQIqT3WTEWYgpm6yXCzEWhcVBw8LLh2Y5d3/GZx/lH9ZzptNjjKFvmupzcOcxtTGpHxycMcXHGouRF97Z1I5Pg8mHjKcl6rbHDw+S+MouscnfJA+TWaPVSNiT9k/hkr8nHOAnkV8OmSsdzt9GQMr6NkMExzTW2snQZUfc7KGMhZqM7QpxXc0wxHkwzz4Uhb1HNTmQD5OolxSNuET4SfJBEimFhpUlywB6fRlMpV0c7DhlLmzDT9FW1RSdj/AJ/5WarLljI7O8hg8Pkf9imTERrzPbI+HZDfAGO7HdbvfX+HsP8Am7NjCFOs1GRjssQY5k8nUEWmOkacZJAfAgjK5lysGPdnui6azuiDdmIH38c1eXJw7vT4YULes6HpY020BP2nAY/SB/lf5Ged9o6s5510/H9FqlPikpuhlYkZhg05d8LjaGmPGviJZqBS3BaQ0A9sycdy5OTDdiOp6yHJWLp4/f8A5ObbFhrm5ASlpC3xHMwR6MmvtZn49KerjzyqqLmfUYOOZ2iI0pvgMwWHErqRlJjbAycLhSeNOnvgIYCSdWHBwOIzDyCt20G0fqNyLK1LDYmtP+BzH8EZF3iwe91sMeMu1em//NuWx0BiL/H+6cnFqASB3sev7gk7dDl2OLv8cKS6pbc5kOTa9BXAVJTOyt6/TmLOThTyUmIlCDgMx6vdx8cjdn6SlWp2oU+qnQ9f+Gzd6TOZekuk7R0vCeIckgvjTfN3AvK54pTIORzIdPIUhntgTthYKsUQTCEjm1K1N8jfqRjl6iEx0K05sZW6LUfSOLZrNfkrZ7H2a0glkOQ/5H95/pU9Z67d80lPoXHUuP8AgaU4SiRs2FSOSm+RIYq4uchwsa7mnvAoLdhucRC2vJIYvVP8f6VJrzXXclE2Hj/mubjT6MfUen473iu0O1PFNY/o/i/q/wCdFA/W2Y77/Rmz9LoJZB0KJtYnlbbJXTfiHEyrRtNhhX1peq7/AHcf5WzW6gmfJ3WPGI9U3n85tEnoxPRV2pSvT4f2lzEjojLc/j7WyWqjFJ7vzXJJsW/Af805kw03C489UChF19uoO/y/5tyZ03FswGqRtr5rkTZzUfR/zTmsz9lgbj8f7Jzces6fj7k7t9Z9Vean/P8A4HNHPBwmndQjxi0dBrZHU/5/8DlEsDBMLfWAe/8An/wOY8sLAphBqVcx5YmFI2LUMpONHBaY2l9Rga5WBwm2ieNkltfAxNv+yf1DM2eO6Lpc0Hyd53b1Navz/wAvM3/E2z1HSfRH+pH/AHLzOcbsNv03zJv72lZpIoX+j+OKphirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdir1DyaR+ioP9n/xNsidgSg8k3lcg+2FkouxJr1xVar0Fa4qoTvzBJrQUwUrzrydqMFheXH1h1QH06VIHQN/zVkgdkJTLrMBc0O3zH9cCs5/L2+W4jdowSRTrT/LyVK+tfyo/5R6Inr8X/J2TI2rwwypGKsByxCoK5uZ5urtx8Kn9VcSVTz8qfKt1febbDV6qbaz9bmKmo9SGSNP2eP2/5mwAKm//ADkV5Xg8y6paLduxjtPVoiEV+NIOqsrL+xkwVVfy4/J2zu4frXopBbDo4UKx+2uzenwbi4+LEyVlNxD5I0M8ZZ45yPF4HJ6fzcT+1kQqL8lfmD5a1XVI9M0O09KVuVZfSiUfYeT7cLsf2XxtLyn/AJyM1lbT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k06hxpVpqMaVrfGlpqhwI4lhYjFVNpThVdDIa4qm9gOVPnkerMSrZnvk3TmuaBRuSAPn8OYWqnwC/wCa9FoMdDie+aDbxeXtN9Q/3si8q/NR/Lz/AJM897Rn+YLnjH4kmAeadfNxIxr+H+t/k5LS6fhDuAOAUwu6vCzZuYQpqtCmfLeFWvXw8KbXxzVORMUjZZJMWeo7ZtdLGg4GolZRaXHopy7nMoCy1GdBjmta01So/wA/tf5OZUQ6LUZLYjd3RYkk5eHVGSWySknJMFlTiruWKqiSkYqjbaffBSQU7t5eSUwhlxNMu+QP1MgOIJ5oX7s8OzCn/Ec1XaEL3er7JyeGK70+hUD6M0Rd3kNqU01dhkoxagUHJLTLgHKxod5MsAc2JaZucZQ9wRhAopI8aMof1mM3CenKynsx/XnR4pWHyjUQ4JGH80rOWWtDq4QkNxfbX5jIVdpxij/nsmV/3SfIfqzmiNy+t45XAf1Y/c7njTLidywLbXPDS2uU4GUZ0VC/08SUm8OuZulz8Gzyna+k8WfGEPx4CiCnvmzl693QyO1LBalt2OWR2aJYOJY9uV2G+SM2g4KUJbYkdMPE1HGhZICO2BjwFR9MjthCkK0Iau2JTGKe6WVQgutfpyqYdhh2ZHDqSheKrSnvmFIO/wAWSgmFveu4ykxcyE7R0IY/EemEQbOOnT36QLuentlghbi5NSAl03m+3t9m6/I/805aMThHX1+P+OpbP+YkUYKqn4n/AJpyYxONPX/j8RSXUvzDnlUqnwqQR2P/ADLy6MKcGes4tvx9zELu/MjMx3LEnLnXSNpZLJyOLWjtG/b+j+OKplirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdir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ooTg8Li2ZcQCAuJwNlOVnQd34/2SeMJTd3BB3OUnCYc18QJXc3BOWxiwJSm6nO+ZUIuLOaUXMvLrmXEODkNJbMcyYusmbUDtljjGXCvicgge+Ec2Mo8QJRyHM0F0UhRRK9MBTJCkUY4odirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdirsVdiqQap/vQ30fqGKEGcUtYq7FXYq7FXYq7FV6vTFV8N1JCaxMyHxUkYquub+a53nkeSn8xJ/XiqiSDirVcVXxzNGaoSCPDCq1mqanFXBgKYqiH1CeVfTkkdkHYsSP8AhsVQxOKthhSmNq2XrirXEt0xVepaPcGh8R1wqqz3s9xQTO7gdAzE/wDEq40qhyptTBStjffGlfW3mn8jNA88eXRq/kwQpeH7Cr6YQ0kWJ/UFtFM/2I5W+1/zaFeHwf8AOPvm+W4+rfUpUoPttDME6cvt+j/sf9fAr6d8kaNY/kf5Ve41WVPrzU9VQ6nnSZlj9ES/VXbgl2vPlkgr4hu55LmQzS7s3Xr/ABw0qmBthVViv54lKRyOqnqFYgYqoM9eu5wKtOBWsCuxV2KuxV2FXY0rYwgIbyYCuGSVGaX/AL0L9P6sGRMU+ylXYq7FXYq7FX//1OAYq7FXYq7FXYqlGqf70D/UH6ziqJse3zxVOP2cSkIaXvkbQPqSeVviOUkO3idltTjSbdU4rbi2TAtBm2pOR5MulppEKqMvviDq8sgSiI1yMRTAq/2RkmKjJdAYqpG+xV31/FXfX8Vd9fxV31/FWvruKrXv8VQk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XYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FX//0eAYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FX//0uAYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FXYq7FX//0+Df8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVd/wGKu/4DFXf8Birv+AxV3/AYq7/AIDFXf8AAYq7/gMVf//Z\" " 
                " alt=\"EOS Browser\" width=\"1000\" height=\"120\" style=\"border: #00008B 0px solid;\""
        ">\n";
      result += "<hr style=\"border:solid #00ffff 3px;background-color:#0000ff;"
        "height:10px;width:400px;text-align:left;\">";
      result += "<h2> <font color=\"#2C3539\">[ ";
      // show [ name@instance ]
      result += client.name;
      result += "@";
      result += gOFS-> MgmOfsInstanceName.c_str();
      result += " ]:</font> ";
      result += url.c_str();
      result += "</h2>";
      result += "<div><table border:1px solid #aaa !important;\"\n";
      //      result += "<div><table>\n";

      // put the header
      result += "<tr>\n";
      result += "<th style=\"min-width:150px\">Path</th> <th style=\"min-width:20px\"></th>  <th style=\"min-width:150px\">Size</th> "
        "<th style=\"min-width:150px\">Created</th> <th style=\"min_width:100\">Mode</th> "
        "<th style=\"min-width:60px\">owner</th> <th style=\"min-width:60px\">group</th> "
        "<th style=\"min-width:150px\">Acl</th>\n";
      result += "</tr>\n";


      while ((val = directory.nextEntry()))
      {
        XrdOucString entryname = val;
        XrdOucString linkname = "";
	bool isFile = true;

        if ((spath == "/") &&
            ((entryname == ".") ||
             (entryname == "..")))
          continue;

        result += "<tr>\n";

        result += "  <td style=\"padding-right: 5px\">";
        result += "<a href=\"";
        if (entryname == ".")
        {
          linkname = spath.c_str();
        }
        else
        {
          if (entryname == "..")
          {
            if (spath != "/")
            {
              eos::common::Path cPath(spath.c_str());
              linkname = cPath.GetParentPath();
            }
            else
            {
              linkname = "/";
            }
          }
          else
          {
            linkname = spath.c_str();
            if (!spath.endswith("/") && (spath != "/"))
              linkname += "/";
            linkname += entryname.c_str();

          }
        }
        result += linkname.c_str();
        result += "\">";
        result += "<font size=\"2\">";
        result += entryname.c_str();
        result += "</font>";
        struct stat buf;
        buf.st_mode = 0;
        XrdOucErrInfo error;
        XrdOucString sizestring;
        XrdOucString entrypath = spath.c_str();
        entrypath += "/";
        entrypath += entryname.c_str();
        
	isFile = true;
        // find out if it is a file or directory
        if (!gOFS->stat(entrypath.c_str(), &buf, error, &client, "")) 
        {
          if (S_ISDIR(buf.st_mode)) {
	    isFile = false;
            result += "/";
	  }
	}
        result += "  </td>\n";
	
	// -----------------------------------------------------------------------------------------------------------
	// share link icon
	// -----------------------------------------------------------------------------------------------------------
	result += " <td> \n";
	if (isFile) 
	{
	  result += "<a href=\"JavaScript:newPopup('";
	  result += "/proc/user/?mgm.cmd=file&mgm.subcmd=share&mgm.option=s&mgm.file.expires=0&mgm.format=http&mgm.path=";
	  result += linkname.c_str();
	  result += "');\"> <img alt=\"\" src=\"data:image/gif;base64,R0lGODlhEAANAJEAAAJ6xv///wAAAAAAACH5BAkAAAEALAAAAAAQAA0AAAg0AAMIHEiwoMGDCBMqFAigIYCFDBsadPgwAMWJBB1axBix4kGPEhN6HDgyI8eTJBFSvEgwIAA7\" /> </a>\n";
	}
	result += " </td>\n";

	// -----------------------------------------------------------------------------------------------------------
	// file size
	// -----------------------------------------------------------------------------------------------------------
        result += "  <td style=\"padding-right: 5px\">";
        result += "<font size=\"2\">";
        if (S_ISDIR(buf.st_mode))
          result += "";
        else
          result += eos::common::StringConversion::GetReadableSizeString(sizestring, buf.st_size, "Bytes");
        result += "</font>";
        result += "</td>\n";


        char uidlimit[16];
        char gidlimit[16];
        // try to translate with password database
        int terrc = 0;
        std::string username = "";
        username = eos::common::Mapping::UidToUserName(buf.st_uid, terrc);
        if (!terrc)
        {
          snprintf(uidlimit, 12, "%-12s", username.c_str());
        }
        else
        {
          snprintf(uidlimit, 12, "%d", buf.st_uid);
        }
        // try to translate with password database
        std::string groupname = "";
        groupname = eos::common::Mapping::GidToGroupName(buf.st_gid, terrc);
        if (!terrc)
        {
          snprintf(gidlimit, 12, "%-12s", groupname.c_str());
        }
        else
        {
          snprintf(gidlimit, 12, "%d", buf.st_gid);
        }

        char t_creat[36];
        char modestr[11];

        {
          char ftype[8];
          unsigned int ftype_v[7];
          char fmode[10];
          int fmode_v[9];
          strcpy(ftype, "pcdb-ls");
          ftype_v[0] = S_IFIFO;
          ftype_v[1] = S_IFCHR;
          ftype_v[2] = S_IFDIR;
          ftype_v[3] = S_IFBLK;
          ftype_v[4] = S_IFREG;
          ftype_v[5] = S_IFLNK;
          ftype_v[6] = S_IFSOCK;
          strcpy(fmode, "rwxrwxrwx");
          fmode_v[0] = S_IRUSR;
          fmode_v[1] = S_IWUSR;
          fmode_v[2] = S_IXUSR;
          fmode_v[3] = S_IRGRP;
          fmode_v[4] = S_IWGRP;
          fmode_v[5] = S_IXGRP;
          fmode_v[6] = S_IROTH;
          fmode_v[7] = S_IWOTH;
          fmode_v[8] = S_IXOTH;
          struct tm *t_tm;
          struct tm t_tm_local;
          int i;

          t_tm = localtime_r(&buf.st_ctime, &t_tm_local);

          strcpy(modestr, "----------");
          for (i = 0; i < 6; i++) if (ftype_v[i] == (S_IFMT & buf.st_mode)) break;
          modestr[0] = ftype[i];
          for (i = 0; i < 9; i++) if (fmode_v[i] & buf.st_mode) modestr[i + 1] = fmode[i];
          if (S_ISUID & buf.st_mode) modestr[3] = 's';
          if (S_ISGID & buf.st_mode) modestr[6] = 's';
          if (S_ISVTX & buf.st_mode) modestr[9] = '+';

          strftime(t_creat, 36, "%b %d %Y %H:%M", t_tm);
        }

        // show creation date
        result += "<td style=\"padding-right: 5px\"><font size=\"2\" face=\"Courier New\" color=\"darkgrey\">";
        result += t_creat;
        // show permissions
        result += "<td style=\"padding-right: 5px\"><font size=\"2\" face=\"Courier New\" color=\"darkgrey\">";
        result += modestr;

        // show user name
        result += "<td style=\"padding-right: 5px\"><font color=\"darkgrey\">\n";
        result += uidlimit;
        result += "</font></td>\n";

        // show group name
        result += "<td style=\"padding-right: 5px\"><font color=\"grey\">\n";
        result += gidlimit;
        result += "</font></td>\n";
        // show acl's if there
        XrdOucString acl;
        result += "<td style=\"padding-right: 5px\"><font color=\"#81DAF5\">";
        if (S_ISDIR(buf.st_mode))
        {
          if (!gOFS->attr_get(linkname.c_str(),
                              error,
                              &client,
                              "",
                              "sys.acl",
                              acl))
          {
            result += acl.c_str();
          }
        }
        result += "</td>\n";
        result += "</tr>\n";
      }
      result += "</table></div>\n";
      result += "</body>\n";
      result += "</html>\n";
      response = new eos::common::PlainHttpResponse();
      response->SetBody(result);
    }
    else
    {
      response = HttpServer::HttpError("Unable to open directory",
                                       errno);
    }
  }

  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse *
HttpHandler::Head (eos::common::HttpRequest * request)
{
  eos::common::HttpResponse *response = Get(request, true);
  response->mUseFileReaderCallback = false;
  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse *
HttpHandler::Post (eos::common::HttpRequest * request)
{
  using namespace eos::common;
  std::string url = request->GetUrl();
  eos_static_info("method=POST error=NOTIMPLEMENTED path=%s", 
                          url.c_str());
  HttpResponse *response = new PlainHttpResponse();
  response->SetResponseCode(HttpResponse::ResponseCodes::NOT_IMPLEMENTED);
  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse *
HttpHandler::Put (eos::common::HttpRequest * request)
{
  XrdSecEntity client(mVirtualIdentity->prot.c_str());
  client.name = const_cast<char*> (mVirtualIdentity->name.c_str());
  client.host = const_cast<char*> (mVirtualIdentity->host.c_str());
  client.tident = const_cast<char*> (mVirtualIdentity->tident.c_str());

  std::string url = request->GetUrl();
  eos_static_info("method=PUT path=%s", 
                          url.c_str());
  // Classify path to split between directory or file objects
  bool isfile = true;
  eos::common::HttpResponse *response = 0;

  XrdOucString spath = request->GetUrl().c_str();
  if (!spath.beginswith("/proc/"))
  {
    if (spath.endswith("/"))
    {
      isfile = false;
    }
  }

  if (isfile)
  {
    XrdSfsFile* file = gOFS->newFile(client.name);
    if (file)
    {
      XrdSfsFileOpenMode open_mode = 0;
      mode_t create_mode = 0;

      // use the proper creation/open flags for PUT's
      open_mode |= SFS_O_TRUNC;
      open_mode |= SFS_O_RDWR;
      open_mode |= SFS_O_MKPTH;
      create_mode |= (SFS_O_MKPTH | S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

      std::string query;
      if (request->GetHeaders()["Content-Length"] == "0" ||
          *request->GetBodySize() == 0)
      {
        query += "eos.bookingsize=0";
      }

      int rc = file->open(url.c_str(), open_mode, create_mode, &client,
                          query.c_str());
      if (rc != SFS_OK)
      {
        if ((rc != SFS_REDIRECT) && open_mode)
        {
          // retry as a file creation
          open_mode |= SFS_O_CREAT;
          rc = file->open(url.c_str(), open_mode, create_mode, &client,
                          query.c_str());
        }
      }

      if (rc != SFS_OK)
      {
        if (rc == SFS_REDIRECT)
        {
          // the embedded server on FSTs is hardcoded to run on port 8001
          response = HttpServer::HttpRedirect(request->GetUrl(),
                                              file->error.getErrText(),
                                              8001, false);
        }
        else if (rc == SFS_ERROR)
        {
          response = HttpServer::HttpError(file->error.getErrText(),
                                           file->error.getErrInfo());
        }
        else if (rc == SFS_DATA)
        {
          response = HttpServer::HttpData(file->error.getErrText(),
                                          file->error.getErrInfo());
        }
        else if (rc == SFS_STALL)
        {
          response = HttpServer::HttpStall(file->error.getErrText(),
                                           file->error.getErrInfo());
        }
        else
        {
          response = HttpServer::HttpError("Unexpected result from file open",
                                           EOPNOTSUPP);
        }
      }
      else
      {
        response = new eos::common::PlainHttpResponse();
        response->SetResponseCode(response->CREATED);
      }
      // clean up the object
      delete file;
    }
  }
  else
  {
    // DIR requests
    response = HttpServer::HttpError("Not Implemented", EOPNOTSUPP);
  }

  return response;

}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse *
HttpHandler::Delete (eos::common::HttpRequest * request)
{
  eos::common::HttpResponse *response = 0;
  XrdOucErrInfo error;
  struct stat buf;
  ProcCommand cmd;
  
  std::string url = request->GetUrl();
  eos_static_info("method=DELETE path=%s", 
                          url.c_str());
  gOFS->_stat(request->GetUrl().c_str(), &buf, error, *mVirtualIdentity, "");

  XrdOucString info = "mgm.cmd=rm&mgm.path=";
  info += request->GetUrl().c_str();
  if (S_ISDIR(buf.st_mode)) info += "&mgm.option=r";

  cmd.open("/proc/user", info.c_str(), *mVirtualIdentity, &error);
  cmd.close();
  int rc = cmd.GetRetc();

  if (rc != SFS_OK)
  {
    if (error.getErrInfo() == EPERM)
    {
      response = HttpServer::HttpError(error.getErrText(), response->FORBIDDEN);
    }
    else if (error.getErrInfo() == ENOENT)
    {
      response = HttpServer::HttpError(error.getErrText(), response->NOT_FOUND);
    }
    else
    {
      response = HttpServer::HttpError(error.getErrText(), error.getErrInfo());
    }
  }
  else
  {
    response = new eos::common::PlainHttpResponse();
    response->SetResponseCode(response->NO_CONTENT);
  }

  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse *
HttpHandler::Trace (eos::common::HttpRequest * request)
{
  using namespace eos::common;
  std::string url = request->GetUrl();
  eos_static_info("method=TRACE error=NOTIMPLEMENTED path=%s", 
                          url.c_str());
  HttpResponse *response = new PlainHttpResponse();
  response->SetResponseCode(HttpResponse::ResponseCodes::NOT_IMPLEMENTED);
  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse *
HttpHandler::Options (eos::common::HttpRequest * request)
{
  eos::common::HttpResponse *response = new eos::common::PlainHttpResponse();
  response->AddHeader("DAV", "1,2");
  response->AddHeader("Allow", "OPTIONS,GET,HEAD,POST,DELETE,TRACE,"\
                               "PROPFIND,PROPPATCH,COPY,MOVE,LOCK,UNLOCK");
  response->AddHeader("Content-Length", "0");

  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse *
HttpHandler::Connect (eos::common::HttpRequest * request)
{
  using namespace eos::common;
  std::string url = request->GetUrl();
  eos_static_info("method=CONNECT error=NOTIMPLEMENTED path=%s", 
                          url.c_str());
  HttpResponse *response = new PlainHttpResponse();
  response->SetResponseCode(HttpResponse::ResponseCodes::NOT_IMPLEMENTED);
  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse *
HttpHandler::Patch (eos::common::HttpRequest * request)
{
  using namespace eos::common;
  std::string url = request->GetUrl();
  eos_static_info("method=PATCH error=NOTIMPLEMENTED path=%s", 
                          url.c_str());
  HttpResponse *response = new PlainHttpResponse();
  response->SetResponseCode(HttpResponse::ResponseCodes::NOT_IMPLEMENTED);
  return response;
}

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END
