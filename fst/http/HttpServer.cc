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
//#include "common/S3.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
HttpServer::HttpServer (int port) : eos::common::HttpServer::HttpServer (port)
{
}

/*----------------------------------------------------------------------------*/
HttpServer::~HttpServer ()
{
}

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
  // The handler function is called in a 'stateless' fashion, so to keep state
  // the implementation stores a HttpHandler object using **ptr.
  // libmicrohttpd moreover deals with 100-continue responses used by PUT/POST
  // in the upper protocol level, so the handler has to return for GET requests
  // just MHD_YES if there is not yet an HttpHandler and for PUT requests
  // should only create a response object if the open for the PUT failes for
  // whatever reason.
  // So when the HTTP header have arrived Handler is called the first time
  // and in following Handler calls we should not decode the headers again and
  // again for performance reasons. So there is a different handling of GET and
  // PUT because in GET we just don't do anything but return and decode
  // the HTTP headers with the second call, while for PUT we do it in the first
  // call and open the output file immedeatly to return evt. an error.

  std::map<std::string, std::string> headers;

  // If this is the first call, create an appropriate protocol handler based
  // on the headers and store it in *ptr. We should only return MHD_YES here
  // (unless error)
  if (*ptr == 0)
  {
    // Get the headers
    MHD_get_connection_values(connection, MHD_HEADER_KIND,
                              &HttpServer::BuildHeaderMap, (void*) &headers);

    eos::common::ProtocolHandler *handler;
    ProtocolHandlerFactory factory = ProtocolHandlerFactory();
    handler = factory.CreateProtocolHandler(method, headers, 0);
    if (!handler)
    {
      eos_static_err("msg=No matching protocol for request");
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


  if (!protocolHandler->GetResponse() ||
      !protocolHandler->GetResponse()->GetResponseCode())
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

  eos::common::HttpResponse *response = protocolHandler->GetResponse();
  if (!response)
  {
    eos_static_crit("msg=\"response creation failed\"");
    return MHD_NO;
  }

  // TODO: figure this bit out
  if (*uploadDataSize != 0)
  {
    eos_static_info("\n\nreturning MHD_YES");
    if (response->GetResponseCode())
    {
      eos_static_info("\n\nsetting uploadDataSize to 0");
      *uploadDataSize = 0;
      protocolHandler->DeleteResponse();
    }
    return MHD_YES;
  }

  eos_static_info("\n\n%s", response->ToString().c_str());

  // Create the MHD response
  struct MHD_Response *mhdResponse;

  if (response->mUseFileReaderCallback)
  {
    eos_static_info("response length=%d", response->mResponseLength);
    mhdResponse = MHD_create_response_from_callback(response->mResponseLength,
                                                    32 * 1024, // 32 * 1024, /* 32k page size */
                                                    &HttpServer::FileReaderCallback,
                                                    (void*) protocolHandler,
                                                    &HttpServer::FileCloseCallback);
  }
  else
  {
    mhdResponse = MHD_create_response_from_buffer(response->GetBodySize(),
                                                  (void *) response->GetBody().c_str(),
                                                  MHD_RESPMEM_PERSISTENT);
  }

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

//  bool first_call;
//
//  std::string lMethod = method ? method : "";
//
//  HttpHandler* httpHandle = 0;
//
//  // currently support only GET methods
//  if ((lMethod != "GET") &&
//      (lMethod != "PUT"))
//    return MHD_NO; /* unexpected method */
//
//  if (! *ptr)
//  {
//    first_call = true;
//  }
//  else
//  {
//    first_call = false;
//
//  }
//  if (first_call)
//  {
//    if (lMethod == "GET")
//    {
//      /* do never respond on first call for GET */
//      *ptr = (void*) 0x1;
//      eos_static_debug("rc=MHD_YES firstcall=true");
//      return MHD_YES;
//    }
//    eos_static_debug("continue firstcall=true");
//  }
//
//  if (*ptr == (void*) 0x1)
//  {
//    // reset the head/get second call indicator
//    *ptr = 0;
//  }
//
//  // now get an existing or create an HttpHandler for this session
//  if (!*ptr)
//  {
//    // create a handle and run the open;
//    httpHandle = new HttpHandler();
//    *ptr = (void*) httpHandle;
//    httpHandle->mConnection = connection;
//    if (url) httpHandle->mPath = url;
//  }
//  else
//  {
//    // get the previous handle back
//    httpHandle = (HttpHandler*) * ptr;
//  }
//
//  if (!httpHandle->mFile)
//  {
//    httpHandle->Initialize();
//  }
//
//  if (!httpHandle->mFile)
//  {
//    httpHandle->mFile = (XrdFstOfsFile*) gOFS.newFile(httpHandle->mClient.name);
//
//
//    // default modes are for GET=read
//    XrdSfsFileOpenMode open_mode = 0;
//    mode_t create_mode = 0;
//
//    if (lMethod == "PUT")
//    {
//      // use the proper creation/open flags for PUT's
//      open_mode |= SFS_O_CREAT;
//      open_mode |= SFS_O_TRUNC;
//      open_mode |= SFS_O_RDWR;
//      open_mode |= SFS_O_MKPTH;
//      create_mode |= (SFS_O_MKPTH | S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
//    }
//
//    httpHandle->mRc = httpHandle->mFile->open(httpHandle->mPath.c_str(), open_mode, create_mode, &httpHandle->mClient, httpHandle->mQuery.c_str());
//
//    httpHandle->mFileSize = httpHandle->mFile->getOpenSize();
//
//    httpHandle->mFileId = httpHandle->mFile->getFileId();
//    httpHandle->mLogId = httpHandle->mFile->logId;
//
//    // check for range requests
//    if (httpHandle->mHeader.count("Range"))
//    {
//      if (!DecodeByteRange(httpHandle->mHeader["Range"], httpHandle->mOffsetMap, httpHandle->mRangeRequestSize, httpHandle->mFileSize))
//      {
//        // indicate range decoding error
//        httpHandle->mRangeDecodingError = true;
//      }
//      else
//      {
//        httpHandle->mRangeRequest = true;
//      }
//    }
//
//    if (!httpHandle->mRangeRequest)
//    {
//      // we put the file size as request size if this is not a range request aka full file download
//      httpHandle->mRangeRequestSize = httpHandle->mFile->getOpenSize();
//    }
//  }
//
//  if (lMethod == "GET")
//  {
//    // call the HttpHandler::Get method
//    return httpHandle->Get();
//  }
//
//  if (lMethod == "PUT")
//  {
//    // call the HttpHandler::Put method
//    int rc = httpHandle->Put(upload_data, upload_data_size, first_call);
//    if ( (rc!=MHD_YES) || ((!first_call) && (upload_data_size == 0)))
//    {
//      // clean-up left-over objects on error or end-of-put
//      if (httpHandle->mFile)
//      {
//        delete httpHandle->mFile;
//        httpHandle->mFile = 0;
//      }
////      if (httpHandle->mS3)
////      {
////        delete httpHandle->mS3;
////        httpHandle->mS3 = 0;
////      }
//    }
//    return rc;
//  }
//
//  eos_static_alert("invalid program path - should never reach this point!");
//  return MHD_NO;
}

/*----------------------------------------------------------------------------*/
ssize_t
HttpServer::FileReaderCallback (void *cls, uint64_t pos, char *buf, size_t max)
{
  // Ugly ugly casting hack
  eos::common::ProtocolHandler *handler = static_cast<eos::common::ProtocolHandler*> (cls);
  eos::fst::HttpHandler *httpHandle = dynamic_cast<eos::fst::HttpHandler*> (handler);

  eos_static_info("pos=%llu max=%llu current-index=%d current-offset=%llu",
                  (unsigned long long) pos,
                  (unsigned long long) max,
                  httpHandle->mCurrentCallbackOffsetIndex,
                  httpHandle->mCurrentCallbackOffset);

  size_t readsofar = 0;
  if (httpHandle && httpHandle->mFile)
  {
    if (httpHandle->mRangeRequest)
    {
      //.........................................................................
      // range request 
      //.........................................................................
      if (httpHandle->mCurrentCallbackOffsetIndex < httpHandle->mOffsetMap.size())
      {
        size_t toread = 0;
        // if the currentoffset is 0 we have to place the multipart header first
        if ((httpHandle->mOffsetMap.size() > 1) && (httpHandle->mCurrentCallbackOffset == 0))
        {
          eos_static_info("place=%s", httpHandle->mMultipartHeaderMap[httpHandle->mCurrentCallbackOffsetIndex].c_str());
          toread = httpHandle->mMultipartHeaderMap[httpHandle->mCurrentCallbackOffsetIndex].length();
          // this is the start of a range request, copy the multipart header
          memcpy(buf,
                 httpHandle->mMultipartHeaderMap[httpHandle->mCurrentCallbackOffsetIndex].c_str(),
                 toread
                 );
          readsofar += toread;
        }

        auto it = httpHandle->mOffsetMap.begin();
        // advance to the index position
        std::advance(it, httpHandle->mCurrentCallbackOffsetIndex);

        int nread = 0;
        // now read from offset
        do
        {

          off_t offset = it->first;
          off_t indexoffset = httpHandle->mCurrentCallbackOffset;
          toread = max - readsofar;
          // see how much we can still read from this offsetmap
          if (toread > (size_t) (it->second - indexoffset))
          {
            toread = (it->second - indexoffset);
          }
          eos_static_info("toread=%llu", (unsigned long long) toread);
          // read the block
          nread = httpHandle->mFile->read(offset + indexoffset, buf + readsofar, toread);
          if (nread > 0)
          {
            readsofar += nread;
          }
          if ((it->second - indexoffset) == nread)
          {
            eos_static_info("leaving");
            // we have to move to the next index;
            it++;
            httpHandle->mCurrentCallbackOffsetIndex++;
            httpHandle->mCurrentCallbackOffset = 0;
            // we stop the loop
            break;
          }
          else
          {
            if (nread > 0)
              httpHandle->mCurrentCallbackOffset += nread;
            eos_static_info("callback-offset(now)=%llu", (unsigned long long) httpHandle->mCurrentCallbackOffset);
          }
        }
        while ((nread > 0) && (readsofar < max) && (it != httpHandle->mOffsetMap.end()));
        eos_static_info("read=%llu", (unsigned long long) readsofar);
        return readsofar;
      }
      else
      {
        if (httpHandle->mOffsetMap.size() > 1)
        {
          if (httpHandle->mBoundaryEndSent)
          {
            // we are done here  
            return 0;
          }
          else
          {
            httpHandle->mBoundaryEndSent = true;
            memcpy(buf, httpHandle->mBoundaryEnd.c_str(), httpHandle->mBoundaryEnd.length());
            eos_static_info("read=%llu [boundary-end]", (unsigned long long) httpHandle->mBoundaryEnd.length());
            return httpHandle->mBoundaryEnd.length();
          }
        }
        else
        {
          return 0;
        }
      }
    }
    else
    {
      //...........................................................................
      // file streaming
      //...........................................................................
      if (max)
      {
        return httpHandle->mFile->read(pos, buf, max);
      }
      else
      {
        return -1;
      }
    }
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
void
HttpServer::FileCloseCallback (void *cls)
{
  // callback function to close the file object
  HttpHandler* httpHandle = static_cast<HttpHandler*> (cls);
  if (httpHandle && httpHandle->mFile)
  {
    httpHandle->mCloseCode = httpHandle->mFile->close();
  }
  if (httpHandle)
  {
    // clean-up the handle
    delete httpHandle;
  }
  return;
}

#endif

EOSFSTNAMESPACE_END
