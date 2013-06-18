// ----------------------------------------------------------------------
// File: HttpServer.cc
// Author: Andreas-Joachim Peters - CERN
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
#include "mgm/http/ProtocolHandler.hh"
#include "mgm/http/Http.hh"
#include "mgm/http/S3.hh"
#include "mgm/http/WebDAV.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/
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
                     const char            *body,
                     size_t                *bodysize,
                     void                 **ptr)
{
  std::map<std::string, std::string> requestHeaders;

  // If this is the first call, create an appropriate protocol handler based
  // on the headers. We should only return MHD_YES here (unless error)
  if (*ptr == 0)
  {
    // get the headers
    MHD_get_connection_values(connection, MHD_HEADER_KIND,
                              &HttpServer::BuildHeaderMap, (void*) &requestHeaders);

    ProtocolHandler *handler;
    handler = ProtocolHandler::CreateProtocolHandler(method, requestHeaders);
    if (!handler)
    {
      eos_static_err("msg=No matching protocol for request");
      return MHD_NO;
    }

    *ptr = handler;
    return MHD_YES;
  }

  // If this is the second call, actually process the request
  // Use the existing protocol handler stored in ptr
  ProtocolHandler *protocolHandler = (ProtocolHandler*) *ptr;

  // Get the requestHeaders again
  MHD_get_connection_values(connection, MHD_HEADER_KIND,
                            &HttpServer::BuildHeaderMap, (void*) &requestHeaders);

  // Get the request query string
  std::string query;
  MHD_get_connection_values(connection, MHD_GET_ARGUMENT_KIND,
                            &HttpServer::BuildQueryString, (void*) &query);

  // Get the cookies
  std::map<std::string, std::string> cookies;
  MHD_get_connection_values(connection, MHD_COOKIE_KIND,
                            &HttpServer::BuildHeaderMap, (void*) &cookies);

  eos_static_info("path=%s query=%s", url ? url : "",
                                      query.c_str() ? query.c_str() : "");

  // Add query, path & method into the map
//  headers["Path"]       = std::string(url);
//  requestHeaders["Query"]      = query;
//  requestHeaders["HttpMethod"] = method;

  std::map<std::string, std::string> responseHeaders;
  struct MHD_Response               *response;
  std::string                        result;
  int                                respcode = MHD_HTTP_OK;

  // Handle the request and build a response based on the specific protocol
  result = protocolHandler->HandleRequest(method,
                                          url,
                                          query,
                                          body,
                                          bodysize,
                                          requestHeaders,
                                          cookies,
                                          responseHeaders,
                                          respcode);

  eos_static_info("result=%s", result.c_str());

  // Create the response
  response = MHD_create_response_from_buffer(result.length(),(void *) result.c_str(),
                                             MHD_RESPMEM_MUST_COPY);

  if (response)
  {
    for (auto it = responseHeaders.begin(); it != responseHeaders.end(); it++)
    {
      // Add all the response header tags
      MHD_add_response_header(response, it->first.c_str(), it->second.c_str());
    }

    int ret = MHD_queue_response(connection, respcode, response);
    eos_static_info("MHD_queue_response ret=%d respcode=%d", ret, respcode);
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
EOSMGMNAMESPACE_END
