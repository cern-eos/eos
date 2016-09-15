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
#include "fst/http/HttpServer.hh"
#include "fst/http/ProtocolHandlerFactory.hh"
#include "common/http/ProtocolHandler.hh"
#include "fst/XrdFstOfs.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

#ifdef EOS_MICRO_HTTPD
/*----------------------------------------------------------------------------*/

int
HttpServer::Handler(void* cls,
                    struct MHD_Connection* connection,
                    const char* url,
                    const char* method,
                    const char* version,
                    const char* uploadData,
                    size_t* uploadDataSize,
                    void** ptr)
{
  // The handler function is called in a 'stateless' fashion, so to keep state
  // the implementation stores a HttpHandler object using **ptr.
  // libmicrohttpd moreover deals with 100-continue responses used by PUT/POST
  // in the upper protocol level, so the handler has to return for GET requests
  // just MHD_YES if there is not yet an HttpHandler and for PUT requests
  // should only create a response object if the open for the PUT fails for
  // whatever reason.
  // So when the HTTP header have arrived Handler is called the first time
  // and in following Handler calls we should not decode the headers again and
  // again for performance reasons. So there is a difference between handling of
  // GET and PUT because in GET we just don't do anything but return and decode
  // the HTTP headers with the second call, while for PUT we do it in the first
  // call and open the output file immediately to return evt. an error.
  std::map<std::string, std::string> headers;

  // If this is the first call, create an appropriate protocol handler based
  // on the headers and store it in *ptr. We should only return MHD_YES here
  // (unless error)
  if (*ptr == 0) {
    // Get the headers
    MHD_get_connection_values(connection, MHD_HEADER_KIND,
                              &HttpServer::BuildHeaderMap, (void*) &headers);
    eos::common::ProtocolHandler* handler;
    ProtocolHandlerFactory factory = ProtocolHandlerFactory();
    handler = factory.CreateProtocolHandler(method, headers, 0);

    if (!handler) {
      eos_static_err("msg=No matching protocol for request");
      return MHD_NO;
    }

    *ptr = handler;
    return MHD_YES;
  }

  // Retrieve the protocol handler stored in *ptr
  eos::common::ProtocolHandler* protocolHandler = (eos::common::ProtocolHandler*)
      * ptr;

  // For requests which have a body (i.e. uploadDataSize != 0) we must handle
  // the body data on the second reentrant call to this function. We must
  // create the response and store it inside the protocol handler, but we must
  // NOT queue the response until the third call.
  if (!protocolHandler->GetResponse() ||
      !protocolHandler->GetResponse()->GetResponseCode()) {
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
    eos::common::HttpRequest* request = new eos::common::HttpRequest(
      headers, method, url,
      query.c_str() ? query : "",
      body, uploadDataSize, cookies);
    eos_static_debug("\n\n%s", request->ToString().c_str());
    // Handle the request and build a response based on the specific protocol
    protocolHandler->HandleRequest(request);
    delete request;
  }

  eos::common::HttpResponse* response = protocolHandler->GetResponse();

  if (!response) {
    eos_static_crit("msg=\"response creation failed\"");
    return MHD_NO;
  }

  if (*uploadDataSize != 0) {
    eos_static_debug("returning MHD_NO response-code=%d to stop upload",
                     response->GetResponseCode());

    if (response->GetResponseCode()) {
      eos_static_debug("setting uploadDataSize to 0");
      *uploadDataSize = 0;

      if (response->GetResponseCode() >= 300) {
        eos_static_debug("failing request with response code %d",
                         response->GetResponseCode());
        protocolHandler->DeleteResponse();
        return MHD_NO;
      }
    }

    protocolHandler->DeleteResponse();
    return MHD_YES;
  }

  eos_static_debug("\n\n%s", response->ToString().c_str());
  // Create the MHD response
  struct MHD_Response* mhdResponse;

  if (response->mUseFileReaderCallback) {
    eos_static_debug("response length=%d", response->mResponseLength);
    mhdResponse = MHD_create_response_from_callback(response->mResponseLength,
                  4 * 1024 * 1024, /* 4M page size */
                  &HttpServer::FileReaderCallback,
                  (void*) protocolHandler, 0);
  } else {
    mhdResponse = MHD_create_response_from_buffer(response->GetBodySize(),
                  (void*) response->GetBody().c_str(),
                  MHD_RESPMEM_PERSISTENT);
  }

  if (mhdResponse) {
    // Add all the response header tags
    headers = response->GetHeaders();

    for (auto it = headers.begin(); it != headers.end(); it++) {
      MHD_add_response_header(mhdResponse, it->first.c_str(), it->second.c_str());
    }

    // Queue the response
    int ret = MHD_queue_response(connection, response->GetResponseCode(),
                                 mhdResponse);
    eos_static_debug("MHD_queue_response ret=%d", ret);
    MHD_destroy_response(mhdResponse);
    return ret;
  } else {
    eos_static_crit("msg=\"response creation failed\"");
    return MHD_NO;
  }
}

/*----------------------------------------------------------------------------*/
ssize_t
HttpServer::FileReaderCallback(void* cls, uint64_t pos, char* buf, size_t max)
{
  // Ugly ugly casting hack
  eos::common::ProtocolHandler* handler =
    static_cast<eos::common::ProtocolHandler*>(cls);
  eos::fst::HttpHandler* httpHandle = dynamic_cast<eos::fst::HttpHandler*>
                                      (handler);
  eos_static_debug("pos=%llu max=%llu current-index=%d current-offset=%llu",
                   (unsigned long long) pos,
                   (unsigned long long) max,
                   httpHandle->mCurrentCallbackOffsetIndex,
                   httpHandle->mCurrentCallbackOffset);
  size_t readsofar = 0;

  if (httpHandle && httpHandle->mFile) {
    if (httpHandle->mRangeRequest) {
      // range request
      if (httpHandle->mCurrentCallbackOffsetIndex < httpHandle->mOffsetMap.size()) {
        size_t toread = 0;

        // if the currentoffset is 0 we have to place the multipart header first
        if ((httpHandle->mOffsetMap.size() > 1) &&
            (httpHandle->mCurrentCallbackOffset == 0)) {
          eos_static_debug("place=%s", httpHandle->mMultipartHeaderMap
                           [httpHandle->mCurrentCallbackOffsetIndex].c_str());
          toread = httpHandle->mMultipartHeaderMap
                   [httpHandle->mCurrentCallbackOffsetIndex].length();
          // this is the start of a range request, copy the multipart header
          memcpy(buf,
                 httpHandle->mMultipartHeaderMap
                 [httpHandle->mCurrentCallbackOffsetIndex].c_str(),
                 toread
                );
          readsofar += toread;
        }

        auto it = httpHandle->mOffsetMap.begin();
        // advance to the index position
        std::advance(it, httpHandle->mCurrentCallbackOffsetIndex);
        int nread = 0;

        // now read from offset
        do {
          off_t offset = it->first;
          off_t indexoffset = httpHandle->mCurrentCallbackOffset;
          toread = max - readsofar;

          // see how much we can still read from this offsetmap
          if (toread > (size_t)(it->second - indexoffset)) {
            toread = (it->second - indexoffset);
          }

          eos_static_debug("toread=%llu", (unsigned long long) toread);
          // read the block
          nread = httpHandle->mFile->read(offset + indexoffset,
                                          buf + readsofar, toread);

          // there is a read error here!
          if (toread && (nread != (int) toread)) {
            return -1;
          }

          if (nread > 0) {
            readsofar += nread;
          }

          if ((it->second - indexoffset) == nread) {
            eos_static_debug("leaving");
            // we have to move to the next index;
            it++;
            httpHandle->mCurrentCallbackOffsetIndex++;
            httpHandle->mCurrentCallbackOffset = 0;
            // we stop the loop
            break;
          } else {
            if (nread > 0) {
              httpHandle->mCurrentCallbackOffset += nread;
            }

            eos_static_debug("callback-offset(now)=%llu", (unsigned long long)
                             httpHandle->mCurrentCallbackOffset);
          }
        } while ((nread > 0) &&
                 (readsofar < max) &&
                 (it != httpHandle->mOffsetMap.end()));

        eos_static_debug("read=%llu", (unsigned long long) readsofar);
        return readsofar;
      } else {
        if (httpHandle->mOffsetMap.size() > 1) {
          if (httpHandle->mBoundaryEndSent) {
            // we are done here
            return 0;
          } else {
            httpHandle->mBoundaryEndSent = true;
            memcpy(buf, httpHandle->mBoundaryEnd.c_str(),
                   httpHandle->mBoundaryEnd.length());
            eos_static_debug("read=%llu [boundary-end]", (unsigned long long)
                             httpHandle->mBoundaryEnd.length());
            return httpHandle->mBoundaryEnd.length();
          }
        } else {
          return 0;
        }
      }
    } else {
      // file streaming
      if (max) {
        size_t nread = httpHandle->mFile->read(pos, buf, max);

        if (nread == 0) {
          return -1;
        } else {
          return nread;
        }
      } else {
        return -1;
      }
    }
  }

  return 0;
}

void
HttpServer::CompleteHandler(void*                              cls,
                            struct MHD_Connection*             connection,
                            void**                             con_cls,
                            enum MHD_RequestTerminationCode    toe)
{
  std::string scode = "";

  if (toe == MHD_REQUEST_TERMINATED_COMPLETED_OK) {
    scode = "OK";
  }

  if (toe == MHD_REQUEST_TERMINATED_WITH_ERROR) {
    scode = "Error";
  }

  if (toe == MHD_REQUEST_TERMINATED_TIMEOUT_REACHED) {
    scode = "Timeout";
  }

  if (toe == MHD_REQUEST_TERMINATED_DAEMON_SHUTDOWN) {
    scode = "Shutdown";
  }

  if (toe == MHD_REQUEST_TERMINATED_READ_ERROR) {
    scode = "ReadError";
  }

  eos_static_info("msg=\"http connection disconnect\" reason=\"Request %s\" ",
                  scode.c_str());
  eos::fst::HttpHandler* httpHandle = 0;

  if ((con_cls && (*con_cls))) {
    eos::common::ProtocolHandler* handler =
      static_cast<eos::common::ProtocolHandler*>(*con_cls);
    httpHandle = dynamic_cast<eos::fst::HttpHandler*>(handler);
  }

  if (httpHandle) {
    // deal with delete-on-close logic
    if ((toe != MHD_REQUEST_TERMINATED_COMPLETED_OK)) {
      eos_static_info("msg=\"http connection disconnect\" action=\"Cleanup\" ");

      if (httpHandle && httpHandle->mFile) {
        eos_static_err("msg=\"clean-up interrupted PUT/GET request\" path=\"%s\"",
                       httpHandle->mFile->GetPath().c_str());

        // we have to disable delete-on-close for chunked uploads since files are stateful
        if (httpHandle->mFile->IsChunkedUpload()) {
          httpHandle->mFile->close();
        }
      }
    }

    // clean-up file objects
    if (httpHandle->mFile) {
      delete(httpHandle->mFile);
      httpHandle->mFile = 0;
    }

    delete httpHandle;
    *con_cls = 0;
  }
}

#endif

EOSFSTNAMESPACE_END
