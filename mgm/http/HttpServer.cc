// ----------------------------------------------------------------------
// File: HttpServer.cc
// Author: Andreas-Joachim Peters & Justin Lewis Salmon - CERN
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
#include "mgm/http/HttpServer.hh"
#include "mgm/http/ProtocolHandlerFactory.hh"
#include "common/http/ProtocolHandler.hh"
#include "common/http/HttpRequest.hh"
#include "common/StringConversion.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/
#include <sstream>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

#define EOSMGM_HTTP_PAGE "<html><head><title>No such file or directory</title>\
                          </head><body>No such file or directory</body></html>"

#ifdef EOS_MICRO_HTTPD
/*----------------------------------------------------------------------------*/
int
HttpServer::Handler (void                  *cls,
                     struct MHD_Connection *connection,
                     const char            *url,
                     const char            *method,
                     const char            *version,
                     const char            *uploadData,
                     size_t                *uploadDataSize,
                     void                 **ptr)
{
  std::map<std::string, std::string> headers;

  // If this is the first call, create an appropriate protocol handler based
  // on the headers and store it in *ptr. We should only return MHD_YES here
  // (unless error)
  if (*ptr == 0)
  {
    // Get the headers
    MHD_get_connection_values(connection, MHD_HEADER_KIND,
                              &HttpServer::BuildHeaderMap, (void*) &headers);

    for (auto it = headers.begin(); it != headers.end(); ++it)
    {
      eos_static_info("%s: %s", (*it).first.c_str(), (*it).second.c_str());
    }

    // Authenticate the client
    eos::common::Mapping::VirtualIdentity *vid = Authenticate(headers);
    if (!vid)
    {
      eos::common::HttpResponse *response = HttpError("Forbidden",
                                                       response->FORBIDDEN);
      // TODO: move this to a function
      struct MHD_Response *mhdResponse;
      mhdResponse = MHD_create_response_from_buffer(response->GetBodySize(),
                                                    (void *) response->GetBody().c_str(),
                                                    MHD_RESPMEM_MUST_COPY);
      // Add all the response header tags
      headers = response->GetHeaders();
      for (auto it = headers.begin(); it != headers.end(); it++)
      {
        MHD_add_response_header(mhdResponse, it->first.c_str(), it->second.c_str());
      }

      int ret = MHD_queue_response(connection, response->GetResponseCode(),
                                       mhdResponse);
      eos_static_info("MHD_queue_response ret=%d", ret);
      return ret;
    }

    eos::common::ProtocolHandler *handler;
    ProtocolHandlerFactory factory = ProtocolHandlerFactory();
    handler = factory.CreateProtocolHandler(method, headers, vid);
    if (!handler)
    {
      eos_static_err("msg=no matching protocol for request");
      return MHD_NO;
    }

    *ptr = handler;
    return MHD_YES;
  }

  // Retrieve the protocol handler stored in *ptr
  eos::common::ProtocolHandler *protocolHandler = (eos::common::ProtocolHandler*) *ptr;

  // For requests which have a body (i.e. uploadDataSize != 0) we must handle
  // the body data on the second reentrant call to this function. We must
  // create the response and store it inside the protocol handler, but we must
  // NOT queue the response until the third call.
  if (!protocolHandler->GetResponse())
  {
    // Get the request headers again
    MHD_get_connection_values(connection, MHD_HEADER_KIND,
                              &HttpServer::BuildHeaderMap, (void*) &headers);

    // Get the request query string
    std::string query;
    MHD_get_connection_values(connection, MHD_GET_ARGUMENT_KIND,
                              &HttpServer::BuildQueryString, (void*) &query);

    // Get the cookies
    std::map<std::string, std::string> cookies;
    MHD_get_connection_values(connection, MHD_COOKIE_KIND,
                              &HttpServer::BuildHeaderMap, (void*) &cookies);

    // Make a request object
    std::string body(uploadData, *uploadDataSize);
    eos::common::HttpRequest *request = new eos::common::HttpRequest(
                                            headers, method, url,
                                            query.c_str() ? query : "",
                                            body, uploadDataSize, cookies);
    eos_static_info("\n\n%s", request->ToString().c_str());

    // Handle the request and build a response based on the specific protocol
    protocolHandler->HandleRequest(request);
    delete request;
  }

  // If we have a non-empty body, we must "process" it, set the body size to
  // zero, and return MHD_YES. We should not queue the response yet - we must
  // do that on the next (third) call.
  if (*uploadDataSize != 0)
  {
    *uploadDataSize = 0;
    return MHD_YES;
  }

  eos::common::HttpResponse *response = protocolHandler->GetResponse();
  if (!response)
  {
    eos_static_crit("msg=\"response creation failed\"");
    return MHD_NO;
  }
  eos_static_info("\n\n%s", response->ToString().c_str());

  // Create the response
  struct MHD_Response *mhdResponse;
  mhdResponse = MHD_create_response_from_buffer(response->GetBodySize(), (void*)
                                                response->GetBody().c_str(),
                                                MHD_RESPMEM_MUST_COPY);

  if (mhdResponse)
  {
    // Add all the response header tags
    headers = response->GetHeaders();
    for (auto it = headers.begin(); it != headers.end(); it++)
    {
      MHD_add_response_header(mhdResponse, it->first.c_str(), it->second.c_str());
    }

    // Queue the response
    int ret = MHD_queue_response(connection, response->GetResponseCode(),
                                 mhdResponse);
    eos_static_info("MHD_queue_response ret=%d", ret);
    return ret;
  }
  else
  {
    eos_static_crit("msg=\"response creation failed\"");
    return MHD_NO;
  }
}

#endif

/*----------------------------------------------------------------------------*/
eos::common::Mapping::VirtualIdentity*
HttpServer::Authenticate(std::map<std::string, std::string> &headers)
{
  eos::common::Mapping::VirtualIdentity *vid = 0;
  std::string clientDN   = headers["SSL_CLIENT_S_DN"];
  std::string remoteUser = headers["Remote-User"];
  std::string dn;
  std::string username;
  unsigned    pos;

  if (clientDN.empty() && remoteUser.empty())
  {
    eos_static_info("msg=client supplied neither SSL_CLIENT_S_DN nor "
                    "Remote-User headers");
    return NULL;
  }

  // Stat the gridmap file
  struct stat info;
  if (stat("/etc/grid-security/grid-mapfile", &info) == -1)
  {
    eos_static_err("error stat'ing gridmap file: %s", strerror(errno));
    return NULL;
  }

  // Initially load the file, or reload it if it was modified
  if (!mGridMapFileLastModTime.tv_sec ||
       mGridMapFileLastModTime.tv_sec != info.st_mtim.tv_sec)
  {
    eos_static_info("msg=reloading gridmap file");

    std::ifstream in("/etc/grid-security/grid-mapfile");
    std::stringstream buffer;
    buffer << in.rdbuf();
    mGridMapFile = buffer.str();
    mGridMapFileLastModTime = info.st_mtim;
    in.close();
  }

  // Process each mapping
  std::vector<std::string> mappings;
  eos::common::StringConversion::Tokenize(mGridMapFile, mappings, "\n");

  bool match = false;
  for (auto it = mappings.begin(); it != mappings.end(); ++it)
  {
    eos_static_debug("grid mapping: %s", (*it).c_str());

    // Split off the last whitespace-separated token (i.e. username)
    pos = (*it).find_last_of(" \t");
    if (pos == string::npos)
    {
      eos_static_err("msg=malformed gridmap file");
      return NULL;
    }

    dn       = (*it).substr(1, pos - 2); // Remove quotes around DN
    username = (*it).substr(pos + 1);
    eos_static_debug(" dn:       %s", dn.c_str());
    eos_static_debug(" username: %s", username.c_str());

    // Try to match with SSL header
    if (dn == clientDN)
    {
      eos_static_info("msg=mapped client certificate successfully dn=%s "
                      "username=%s", dn.c_str(), username.c_str());
      match = true;
      break;
    }

    // Try to match with kerberos username
    pos = remoteUser.find_last_of("@");
    std::string remoteUserName = remoteUser.substr(0, pos);

    std::vector<std::string> tokens;
    eos::common::StringConversion::Tokenize(dn, tokens, "/");

    for (auto it = tokens.begin(); it != tokens.end(); ++it)
    {
      eos_static_debug("dn token=%s", (*it).c_str());

      pos            = (*it).find_last_of("=");
      std::string cn = (*it).substr(pos + 1);
      eos_static_debug("cn=%s", cn.c_str());

      if (cn == remoteUserName)
      {
        username = (*it).substr(pos + 1);
        eos_static_info("msg=mapped client krb5 username successfully "
                        "username=%s", username.c_str());
        match = true;
        break;
      }
    }
  }

  // If we found a match, make a virtual identity
  if (match)
  {
    vid = new eos::common::Mapping::VirtualIdentity();
    eos::common::Mapping::getPhysicalIds(username.c_str(), *vid);

    vid->dn     = dn;
    vid->name   = XrdOucString(username.c_str());
    vid->host   = headers["Host"];
    vid->tident = "dummy.0:0@localhost";
    vid->prot   = "http";

    return vid;
  }
  else
  {
    eos_static_info("msg=client not authenticated with certificate or kerberos "
                    "SSL_CLIENT_S_DN=%s, Remote-User=%s", clientDN.c_str(),
                    remoteUser.c_str());
    return NULL;
  }
}

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END
