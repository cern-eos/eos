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

/*----------------------------------------------------------------------------*/
//HttpServer::HttpServer (int port) : eos::common::HttpServer (port) {
//
//}

/*----------------------------------------------------------------------------*/
HttpServer::~HttpServer ()
{
//  if (mS3Store)
//  {
//    delete mS3Store;
//    mS3Store = 0;
//  }
}

#ifdef EOS_MICRO_HTTPD
/*----------------------------------------------------------------------------*/
int
HttpServer::Handler (void                  *cls,
                     struct MHD_Connection *connection,
                     const char            *url,
                     const char            *method,
                     const char            *version,
                     const char            *upload_data,
                     size_t                *upload_data_size,
                     void                 **ptr)
{
  std::map<std::string, std::string> headers;

  // If this is the first call, create an appropriate protocol handler based
  // on the headers. We should only return MHD_YES here (unless error)
  if (*ptr == 0)
  {
    // get the headers
    MHD_get_connection_values(connection, MHD_HEADER_KIND,
                              &HttpServer::BuildHeaderMap, (void*) &headers);


    std::string methodstr(method);
    ProtocolHandler *handler = ProtocolHandler::CreateProtocolHandler(methodstr,
                                                                      headers);
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

  // Get the headers again
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

  eos_static_info("path=%s query=%s", url ? url : "",
                                      query.c_str() ? query.c_str() : "");

  // Add query, path & method into the map
  headers["Path"]       = std::string(url);
  headers["Query"]      = query;
  headers["HttpMethod"] = method;

  std::map<std::string, std::string> responseHeaders;
  struct MHD_Response               *response;
  std::string                        result;
  int                                mhdResponse = MHD_HTTP_OK;

  // Handle the request and build a response based on the specific protocol
  result = protocolHandler->HandleRequest(headers, responseHeaders, mhdResponse);
  eos_static_info("result=%s", result.c_str());

  // Create the response
  response = MHD_create_response_from_buffer(result.length(),
                                             (void *) result.c_str(),
                                             MHD_RESPMEM_MUST_COPY);

  if (response)
  {
    for (auto it = responseHeaders.begin(); it != responseHeaders.end(); it++)
    {
      // Add all the response header tags
      MHD_add_response_header(response, it->first.c_str(), it->second.c_str());
    }

    int ret = MHD_queue_response(connection, mhdResponse, response);
    eos_static_info("MHD_queue_response ret=%d mhdRepsonse=%d",
                    ret, mhdResponse);
    return ret;
  }
  else
  {
    eos_static_crit("msg=\"response creation failed\"");
    return MHD_NO;
  }


//  static int aptr;
//  struct MHD_Response *response;
//
//  std::string query;
//
//  if (!mS3Store)
//  {
//    // create the store if it does not exist yet
//    mS3Store = new S3Store(gOFS->MgmProcPath.c_str());
//  }
//  // currently support only GET,HEAD,PUT methods
//  if ((0 != strcmp(method, MHD_HTTP_METHOD_GET))  &&
//      (0 != strcmp(method, MHD_HTTP_METHOD_HEAD)) &&
//      (0 != strcmp(method, MHD_HTTP_METHOD_PUT))  &&
//      (0 != strcmp(method, MHD_HTTP_METHOD_OPTIONS))  &&
//      (0 != strcmp(method, "PROPFIND")))
//    return MHD_NO; /* unexpected method */
//
//  if ( ( &aptr != *ptr) && (strcmp(method, MHD_HTTP_METHOD_PUT)) )
//  {
//    /* do never respond on first call for head/put */
//    *ptr = &aptr;
//    return MHD_YES;
//  }
//
//  MHD_get_connection_values(connection, MHD_GET_ARGUMENT_KIND, &HttpServer::BuildQueryString,
//                            (void*) &query);
//
//
//  *ptr = NULL; /* reset when done */
//
//  std::string path = url;
//
//  eos_static_info("path=%s query=%s", url ? url : "", path.c_str() ? query.c_str() : "");
//
//
//  // classify path to split between directory or file objects
//  bool isfile = true;
//
//  XrdOucString spath = path.c_str();
//  if (!spath.beginswith("/proc/"))
//  {
//    if (spath.endswith("/"))
//    {
//      isfile = false;
//    }
//  }
//  XrdSecEntity client("unix");
//
//  client.name = strdup("nobody");
//  client.host = strdup("localhost");
//  client.tident = strdup("http");
//
//  std::string result;
//
//  int mhd_response = MHD_HTTP_OK;
//
//  std::map<std::string, std::string> header;
//  std::map<std::string, std::string> cookies;
//
//  // get the header INFO
//  MHD_get_connection_values(connection, MHD_HEADER_KIND, &HttpServer::BuildHeaderMap,
//                            (void*) &header);
//
//  MHD_get_connection_values(connection, MHD_COOKIE_KIND, &HttpServer::BuildHeaderMap,
//                            (void*) &cookies);
//
//  // add query, path & method into the map
//  header["Path"] = path;
//  header["Query"] = query;
//  header["HttpMethod"] = method;
//
//  for (auto it = header.begin(); it != header.end(); it++)
//  {
//    eos_static_info("header:%s=%s", it->first.c_str(), it->second.c_str());
//  }
//
//  for (auto it = cookies.begin(); it != cookies.end(); it++)
//  {
//    eos_static_info("cookie:%s=%s", it->first.c_str(), it->second.c_str());
//  }
//
//  std::map<std::string, std::string> responseheader;
//
//  eos::common::S3* s3 = eos::common::S3::ParseS3(header);
//
//  if (s3)
//  {
//    eos_static_info("msg=\"handling s3 request\"");
//    //...........................................................................
//    // handle S3 request
//    //...........................................................................
//
//    mS3Store->Refresh();
//
//    if (!mS3Store->VerifySignature(*s3))
//    {
//      result = s3->RestErrorResponse(mhd_response, 403, "SignatureDoesNotMatch", "", s3->getBucket(), "");
//    }
//    else
//    {
//      if (header["HttpMethod"] == "GET")
//      {
//        if (s3->getBucket() == "")
//        {
//          // GET SERVICE REQUEST
//          result = mS3Store->ListBuckets(mhd_response, *s3, responseheader);
//        }
//        else
//        {
//          if (s3->getPath() == "/")
//          {
//            // GET BUCKET LISTING REQUEST
//            result = mS3Store->ListBucket(mhd_response, *s3, responseheader);
//          }
//          else
//          {
//            // GET OBJECT REQUEST
//            result = mS3Store->GetObject(mhd_response, *s3, responseheader);
//          }
//        }
//      }
//      else
//      {
//        if (header["HttpMethod"] == "HEAD")
//        {
//          if (s3->getPath() == "/")
//          {
//            // HEAD BUCKET REQUEST
//            result = mS3Store->HeadBucket(mhd_response, *s3, responseheader);
//          }
//          else
//          {
//            // HEAD OBJECT REQUEST
//            result = mS3Store->HeadObject(mhd_response, *s3, responseheader);
//          }
//        }
//        else
//        {
//          // PUT REQUEST ...
//          result = mS3Store->PutObject(mhd_response, *s3, responseheader);
//        }
//      }
//
//    }
//    delete s3;
//  }
//  else
//  {
//    //...........................................................................
//    // handle HTTP request
//    //...........................................................................
//
//    if (isfile)
//    {
//      // FILE requests
//      XrdSfsFile* file = gOFS->newFile(client.name);
//
//      if (file)
//      {
//        XrdSfsFileOpenMode open_mode = 0;
//        mode_t create_mode = 0;
//
//        if (header["HttpMethod"] == "PUT")
//        {
//          // use the proper creation/open flags for PUT's
//          open_mode |= SFS_O_TRUNC;
//          open_mode |= SFS_O_RDWR;
//          open_mode |= SFS_O_MKPTH;
//          create_mode |= (SFS_O_MKPTH | S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
//        }
//
//        int rc = file->open(path.c_str(), open_mode, create_mode, &client, query.c_str());
//        if ( (rc!= SFS_REDIRECT) && open_mode) {
//          // retry as a file creation
//          open_mode |= SFS_O_CREAT;
//          rc = file->open(path.c_str(), open_mode, create_mode, &client, query.c_str());
//        }
//
//        if (rc != SFS_OK)
//        {
//          if (rc == SFS_REDIRECT)
//          {
//            // the embedded server on FSTs is hardcoded to run on port 8001
//            result = HttpRedirect(mhd_response, responseheader, file->error.getErrText(), 8001, path, query, false);
//          }
//          else
//            if (rc == SFS_ERROR)
//          {
//            result = HttpError(mhd_response, responseheader, file->error.getErrText(), file->error.getErrInfo());
//          }
//          else
//            if (rc == SFS_DATA)
//          {
//            result = HttpData(mhd_response, responseheader, file->error.getErrText(), file->error.getErrInfo());
//          }
//          else
//            if (rc == SFS_STALL)
//          {
//            result = HttpStall(mhd_response, responseheader, file->error.getErrText(), file->error.getErrInfo());
//          }
//          else
//          {
//            result = HttpError(mhd_response, responseheader, "unexpected result from file open", EOPNOTSUPP);
//          }
//        }
//        else
//        {
//          char buffer[65536];
//          offset_t offset = 0;
//          do
//          {
//            size_t nread = file->read(offset, buffer, sizeof (buffer));
//            if (nread > 0)
//            {
//              result.append(buffer, nread);
//            }
//            if (nread != sizeof (buffer))
//            {
//              break;
//            }
//          }
//          while (1);
//          file->close();
//        }
//        // clean up the object
//        delete file;
//      }
//    }
//    else
//    {
//      eos::common::WebDAV *dav = eos::common::WebDAV::ParseWebDAV(header);
//      // DIR requests
//      // result = HttpError(mhd_response, responseheader, "not implemented", EOPNOTSUPP);
//      if (header["HttpMethod"] == "OPTIONS")
//      {
//        result = HttpData(mhd_response, responseheader, "lol\n", 4);
//      }
//
//      if (header["HttpMethod"] == "PROPFIND")
//      {
//        result = HttpData(mhd_response, responseheader, "lol\n", 4);
//      }
//    }
//  }
//
//  for (auto it = responseheader.begin(); it != responseheader.end(); it++)
//  {
//    eos_static_info("response_header:%s=%s", it->first.c_str(), it->second.c_str());
//  }
//
//  eos_static_info("result=%s", result.c_str());
//
//  response = MHD_create_response_from_buffer(result.length(),
//                                             (void *) result.c_str(),
//                                             MHD_RESPMEM_MUST_COPY);
//
//  if (response)
//  {
//    for (auto it = responseheader.begin(); it != responseheader.end(); it++)
//    {
//      // add all the response header tags
//      MHD_add_response_header(response, it->first.c_str(), it->second.c_str());
//    }
//    int ret = MHD_queue_response(connection, mhd_response, response);
//    eos_static_info("MHD_queue_response ret=%d mhd_repsonse=%d", ret, mhd_response);
//    return ret;
//  }
//  else
//  {
//    eos_static_crit("msg=\"response creation failed\"");
//    return 0;
//  }
}

#endif

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END
