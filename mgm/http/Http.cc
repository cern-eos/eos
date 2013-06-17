// ----------------------------------------------------------------------
// File: Http.cc
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
#include "mgm/http/Http.hh"
#include "mgm/Namespace.hh"
#include "mgm/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
Http::Http ()
{

}

/*----------------------------------------------------------------------------*/
Http::~Http ()
{

}

/*----------------------------------------------------------------------------*/
bool
Http::Matches(std::string &method, ProtocolHandler::HeaderMap &headers)
{
  return true;
}

/*----------------------------------------------------------------------------*/
void
Http::ParseHeader (ProtocolHandler::HeaderMap &headers)
{

}

/*----------------------------------------------------------------------------*/
std::string
Http::HandleRequest(ProtocolHandler::HeaderMap request,
                    ProtocolHandler::HeaderMap response,
                    int                        error)
{
//  error = 1;
//  return "Not Implemented";

  XrdSecEntity        client("unix");
  client.name       = strdup("nobody");
  client.host       = strdup("localhost");
  client.tident     = strdup("http");
  std::string path  = request["Path"];
  std::string query = request["Query"];
  std::string result;

  // Classify path to split between directory or file objects
  bool isfile = true;
  XrdOucString spath = path.c_str();
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
      mode_t             create_mode = 0;

      if (request["HttpMethod"] == "PUT")
      {
        // use the proper creation/open flags for PUT's
        open_mode   |= SFS_O_TRUNC;
        open_mode   |= SFS_O_RDWR;
        open_mode   |= SFS_O_MKPTH;
        create_mode |= (SFS_O_MKPTH | S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
      }

      int rc = file->open(path.c_str(), open_mode, create_mode, &client,
                          query.c_str());
      if ((rc != SFS_REDIRECT) && open_mode)
      {
        // retry as a file creation
        open_mode |= SFS_O_CREAT;
        rc = file->open(path.c_str(), open_mode, create_mode, &client,
                        query.c_str());
      }

      if (rc != SFS_OK)
      {
        if (rc == SFS_REDIRECT)
        {
          // the embedded server on FSTs is hardcoded to run on port 8001
          result = HttpServer::HttpRedirect(error, response, file->error.getErrText(),
                                            8001, path, query, false);
        }
        else
        if (rc == SFS_ERROR)
        {
          result = HttpServer::HttpError(error, response, file->error.getErrText(),
                                         file->error.getErrInfo());
        }
        else
        if (rc == SFS_DATA)
        {
          result = HttpServer::HttpData(error, response, file->error.getErrText(),
                                        file->error.getErrInfo());
        }
        else
        if (rc == SFS_STALL)
        {
          result = HttpServer::HttpStall(error, response, file->error.getErrText(),
                                         file->error.getErrInfo());
        }
        else
        {
          result = HttpServer::HttpError(error, response,
                                         "unexpected result from file open",
                                         EOPNOTSUPP);
        }
      }
      else
      {
        char buffer[65536];
        offset_t offset = 0;
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
      }
      // clean up the object
      delete file;
    }
  }
  else
  {
    // DIR requests
    result = HttpServer::HttpError(error, response, "not implemented", EOPNOTSUPP);
  }

  return result;
}

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END
