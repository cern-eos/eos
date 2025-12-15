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

#include "fst/http/HttpServer.hh"
#include "fst/http/HttpHandler.hh"
#include "fst/XrdFstOfsFile.hh"
#include "common/SecEntity.hh"
#include "fst/XrdFstOfs.hh"
#include <XrdSys/XrdSysPthread.hh>
#include <XrdSfs/XrdSfsInterface.hh>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// HTTP object handler function on FST called by XrdHttp
//------------------------------------------------------------------------------
std::unique_ptr<eos::common::ProtocolHandler>
HttpServer::XrdHttpHandler(std::string& method,
                           std::string& uri,
                           std::map<std::string, std::string>& headers,
                           std::string& query,
                           std::map<std::string, std::string>& cookies,
                           std::string& body,
                           const XrdSecEntity& client)
{
  if (client.moninfo && strlen(client.moninfo)) {
    headers["ssl_client_s_dn"] = client.moninfo;
    headers["x-real-ip"] = client.host;
  }

  std::unique_ptr<eos::common::ProtocolHandler> handler {nullptr};

  if (HttpHandler::Matches(method, headers)) {
    handler = std::make_unique<eos::fst::HttpHandler>();
  } else {
    eos_static_err("msg=\"no matching protocol for request method %s\"",
                   method.c_str());
    return nullptr;
  }

  size_t bodySize = body.length();
  // Retrieve the protocol handler stored in *ptr
  std::unique_ptr<eos::common::HttpRequest> request(new eos::common::HttpRequest(
        headers, method, uri,
        query.c_str() ? query : "",
        body, &bodySize, cookies, true));

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("\n\n%s\n%s\n", request->ToString().c_str(),
                     request->GetBody().c_str());
  }

  handler->HandleRequest(request.get());

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("method=%s uri='%s' %s (warning this is not the mapped identity)",
                     method.c_str(), uri.c_str(), eos::common::SecEntity::ToString(&client,
                         "xrdhttp").c_str());
  }

  return handler;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
ssize_t
HttpServer::FileReader(eos::common::ProtocolHandler* handler, uint64_t pos,
                       char* buf, size_t max)
{
  eos::fst::HttpHandler* httpHandle = dynamic_cast<eos::fst::HttpHandler*>
                                      (handler);

  if (!httpHandle) {
    eos_static_err("error: dynamic cast to eos::fst::HttpHandler failed");
    return -1;
  }

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

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
ssize_t
HttpServer::FileWriter(eos::common::ProtocolHandler* handler,
                       std::string& method,
                       std::string& uri,
                       std::map<std::string, std::string>& headers,
                       std::string& query,
                       std::map<std::string, std::string>& cookies,
                       std::string& body)

{
  eos::fst::HttpHandler* httpHandle = dynamic_cast<eos::fst::HttpHandler*>
                                      (handler);
  size_t uploadSize = body.size();
  std::unique_ptr<eos::common::HttpRequest> request(new eos::common::HttpRequest(
        headers, method, uri,
        query.c_str(),
        body, &uploadSize, cookies,
        true));
  eos_static_debug("\n\n%s", request->ToString().c_str());
  // Handle the request and build a response based on the specific protocol
  httpHandle->HandleRequest(request.get());
  eos::common::HttpResponse* response = handler->GetResponse();

  if (response->GetResponseCode() == response->CREATED) {
    return 0;
  } else {
    return -1;
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
ssize_t
HttpServer::FileClose(eos::common::ProtocolHandler* handler, int rc, bool eskip)
{
  eos::fst::HttpHandler* httpHandle = dynamic_cast<eos::fst::HttpHandler*>
                                      (handler);

  if (httpHandle && httpHandle->mFile) {
    if (rc) {
      eos_static_err("msg=\"clean-up interrupted or IO error related PUT/GET request\" path=\"%s\"",
                     httpHandle->mFile->GetPath().c_str());

      // we have to disable delete-on-close for chunked uploads since files are stateful
      if (httpHandle->mFile->IsChunkedUpload()) {
        httpHandle->FileClose(HttpHandler::CanCache::YES);
      } else if (!eskip) {
        // under error eskip avoids closing the file before destorying
        // (closing may cause httpHandler to cache the file handle)
        httpHandle->FileClose(HttpHandler::CanCache::YES);
      }
    } else {
      httpHandle->FileClose(HttpHandler::CanCache::YES);
    }

    // clean-up file objects
    if (httpHandle->mFile) {
      delete (httpHandle->mFile);
      httpHandle->mFile = 0;
    }
  }

  return 0;
}

EOSFSTNAMESPACE_END
