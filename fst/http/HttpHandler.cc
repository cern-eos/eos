// ----------------------------------------------------------------------
// File: HttpHandler.cc
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
#include "fst/http/HttpHandler.hh"
#include "fst/http/HttpServer.hh"
#include "common/http/HttpResponse.hh"
#include "common/http/PlainHttpResponse.hh"
#include "fst/XrdFstOfs.hh"
//#include "common/S3.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

bool
HttpHandler::Matches(const std::string &meth, HeaderMap &headers)
{
  int method =  ParseMethodString(meth);

  // We only support GET, HEAD and PUT on the FST
  if (method == GET || method == HEAD || method == PUT)
  {
    eos_static_info("Matched HTTP protocol for request");
    return true;
  }
  else return false;
}

/*----------------------------------------------------------------------------*/
void
HttpHandler::HandleRequest (eos::common::HttpRequest *request)
{
  eos_static_info("Handling HTTP request");

  if (!mFile)
  {
    Initialize(request);
  }

  if (!mFile)
  {
    mFile = (XrdFstOfsFile*) gOFS.newFile(mClient.name);


    // default modes are for GET=read
    XrdSfsFileOpenMode open_mode = 0;
    mode_t create_mode = 0;

    if (request->GetMethod() == "PUT")
    {
      // use the proper creation/open flags for PUT's
      open_mode |= SFS_O_CREAT;
      open_mode |= SFS_O_TRUNC;
      open_mode |= SFS_O_RDWR;
      open_mode |= SFS_O_MKPTH;
      create_mode |= (SFS_O_MKPTH | S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    }

    mRc = mFile->open(request->GetUrl().c_str(),
                      open_mode,
                      create_mode,
                      &mClient,
                      request->GetQuery().c_str());

    mFileSize = mFile->getOpenSize();

    mFileId = mFile->getFileId();
    mLogId = mFile->logId;

    // check for range requests
    if (request->GetHeaders().count("Range"))
    {
      if (!DecodeByteRange(request->GetHeaders()["Range"],
                           mOffsetMap,
                           mRangeRequestSize,
                           mFileSize))
      {
        // indicate range decoding error
        mRangeDecodingError = true;
      }
      else
      {
        mRangeRequest = true;
      }
    }

    if (!mRangeRequest)
    {
      // we put the file size as request size if this is not a range request aka full file download
      mRangeRequestSize = mFile->getOpenSize();
    }
  }

  if (request->GetMethod() == "GET")
  {
    // call the HttpHandler::Get method
    mHttpResponse = Get(request);
  }

  if (request->GetMethod() == "PUT")
  {

    if ( ((mUploadLeftSize > (10 * 1024 * 1024)) && ((*request->GetBodySize()) < (10 * 1024 * 1024))) )
    {
      // we want more bytes, we don't process this
      eos_static_info("msg=\"wait for more bytes\" leftsize=%llu uploadsize=%llu", mUploadLeftSize, *request->GetBodySize());
      mHttpResponse = new eos::common::PlainHttpResponse();
      mHttpResponse->SetResponseCode(0);
      return;
    }

    // call the HttpHandler::Put method
    // int rc = Put(request->GetBody(), request->GetBodySize(), first_call);
    mHttpResponse = Put(request);

    // if ( (rc != MHD_YES) || ((!first_call) && (request->GetBodySize() == 0)))

    if (!mHttpResponse || request->GetBodySize() == 0)
    {
      // clean-up left-over objects on error or end-of-put
      if (mFile)
      {
        delete mFile;
        mFile = 0;
      }
//      if (mS3)
//      {
//        delete mS3;
//        mS3 = 0;
//      }
    }
    //return rc;
  }

}

/*----------------------------------------------------------------------------*/
void
HttpHandler::Initialize (eos::common::HttpRequest *request)
{
  // decode all the header/cookie stuff
//  mQuery = "";
//  MHD_get_connection_values(mConnection, MHD_GET_ARGUMENT_KIND, &HttpServer::BuildQueryString,
//                            (void*) &mQuery);
//
//  // get the header INFO
//  MHD_get_connection_values(mConnection, MHD_HEADER_KIND, &HttpServer::BuildHeaderMap,
//                            (void*) &mHeader);
//
//  MHD_get_connection_values(mConnection, MHD_COOKIE_KIND, &HttpServer::BuildHeaderMap,
//                            (void*) &mCookies);
//
//  for (auto it = mHeader.begin(); it != mHeader.end(); it++)
//  {
//    eos_static_info("header:%s=%s", it->first.c_str(), it->second.c_str());
//  }
//
//  for (auto it = mCookies.begin(); it != mCookies.end(); it++)
//  {
//    eos_static_info("cookie:%s=%s", it->first.c_str(), it->second.c_str());
//  }

  if (request->GetCookies().count("EOSCAPABILITY"))
  {
    // if we have a capability we don't use the query CGI but that one
    request->SetQuery(request->GetCookies()["EOSCAPABILITY"]);

  }

  if (request->GetHeaders().count("Content-Length"))
  {
    mContentLength = strtoull(request->GetHeaders()["Content-Length"].c_str(), 0, 10);
    mUploadLeftSize = mContentLength;
  }

  std::string query = request->GetQuery();
  HttpServer::DecodeURI(query); // unescape '+' '/' '='
  request->SetQuery(query);

  eos_static_info("path=%s query=%s", request->GetUrl().c_str(), request->GetQuery().c_str());

  // define the client sec entity object
  strncpy(mClient.prot, "unix", XrdSecPROTOIDSIZE - 1);
  mClient.prot[XrdSecPROTOIDSIZE - 1] = '\0';
  mClient.name = strdup("nobody");
  mClient.host = strdup("localhost");
  mClient.tident = strdup("http");

//  mS3 = eos::common::S3::ParseS3(mHeader);
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
HttpHandler::Get (eos::common::HttpRequest *request)
{
  eos::common::HttpResponse *response = 0;

  if (mRangeDecodingError)
  {
    response = HttpServer::HttpError("Illegal Range request", response->REQUESTED_RANGE_NOT_SATISFIABLE);
  }
  else
  {
    if (mRc != SFS_OK)
    {
      if (mRc == SFS_REDIRECT)
      {
        response = HttpServer::HttpRedirect(request->GetUrl(),
                                            mFile->error.getErrText(),
                                            mFile->error.getErrInfo(),
                                            true);
      }
      else if (mRc == SFS_ERROR)
      {
        response = HttpServer::HttpError(mFile->error.getErrText(), mFile->error.getErrInfo());
      }
      else if (mRc == SFS_DATA)
      {
        response = HttpServer::HttpData(mFile->error.getErrText(), mFile->error.getErrInfo());
      }
      else if (mRc == SFS_STALL)
      {
        response = HttpServer::HttpStall(mFile->error.getErrText(), mFile->error.getErrInfo());
      }
      else
      {
        response = HttpServer::HttpError("unexpected result from file open", EOPNOTSUPP);
      }
      delete mFile;
      mFile = 0;
    }
    else
    {
      response = new eos::common::PlainHttpResponse();

      if (mRangeRequest)
      {
        CreateMultipartHeader("application/octet-stream");
        eos_static_info(Print());
        char clength[16];
        snprintf(clength, sizeof (clength) - 1, "%llu", (unsigned long long) mRequestSize);
        if (mOffsetMap.size() == 1)
        {
          // if there is only one range we don't send a multipart response
          response->AddHeader("Content-Type", "application/octet-stream");
          response->AddHeader("Content-Range", mSinglepartHeader);
        }
        else
        {
          // for several ranges we send a multipart response
          response->AddHeader("Content-Type", mMultipartHeader);
        }
        response->AddHeader("Content-Length", clength);
        response->SetResponseCode(response->PARTIAL_CONTENT);
//            mhd_response = MHD_HTTP_PARTIAL_CONTENT;
      }
      else
      {
        // successful http open
        char clength[16];
        snprintf(clength, sizeof (clength) - 1, "%llu", (unsigned long long) mFile->getOpenSize());
        mRequestSize = mFile->getOpenSize();
        response->mResponseLength = mRequestSize;
        response->AddHeader("Content-Type", "application/octet-stream");
        response->AddHeader("Content-Length", clength);
        response->SetResponseCode(response->OK);
//            mhd_response = MHD_HTTP_OK;
      }
    }
  }

  if (mFile)
  {
    // We want to use the file callbacks
    response->mUseFileReaderCallback = true;
  }

  return response;
}


///*----------------------------------------------------------------------------*/
//int
//HttpHandler::Get ()
//{
//  int mhd_response = MHD_HTTP_OK;
//  std::string result;
//  std::map<std::string, std::string> responseheader;
//  struct MHD_Response *response = 0;
//
//  if (mS3)
//  {
//    //...........................................................................
//    // S3 requests
//    //...........................................................................
//
//    if (mRangeDecodingError)
//    {
//      result = mS3->RestErrorResponse(mhd_response, 416, "InvalidRange", "Illegal Range request", mHeader["Range"].c_str(), "");
//    }
//    else
//    {
//      if (mRc != SFS_OK)
//      {
//        if (mFile->error.getErrInfo() == ENOENT)
//        {
//          result = mS3->RestErrorResponse(mhd_response, 404, "NoSuchKey", "The specified key does not exist", mS3->getPath(), "");
//        }
//        else
//          if (mFile->error.getErrInfo() == EPERM)
//        {
//          result = mS3->RestErrorResponse(mhd_response, 403, "AccessDenied", "Access Denied", mS3->getPath(), "");
//        }
//        else
//        {
//          result = mS3->RestErrorResponse(mhd_response, 500, "InternalError", "File currently unavailable", mS3->getPath(), "");
//        }
//        delete mFile;
//        mFile = 0;
//        delete mS3;
//      }
//      else
//      {
//        if (mRangeRequest)
//        {
//          CreateMultipartHeader(mS3->ContentType());
//          eos_static_info(Print());
//          char clength[16];
//          snprintf(clength, sizeof (clength) - 1, "%llu", (unsigned long long) mRequestSize);
//          if (mOffsetMap.size() == 1)
//          {
//            // if there is only one range we don't send a multipart response
//            responseheader["Content-Type"] = mS3->ContentType();
//            responseheader["Content-Range"] = mSinglepartHeader;
//          }
//          else
//          {
//            // for several ranges we send a multipart response
//            responseheader["Content-Type"] = mMultipartHeader;
//          }
//          responseheader["Content-Length"] = clength;
//          mhd_response = MHD_HTTP_PARTIAL_CONTENT;
//        }
//        else
//        {
//          // successful http open
//          char clength[16];
//          snprintf(clength, sizeof (clength) - 1, "%llu", (unsigned long long) mFile->getOpenSize());
//          mRequestSize = mFile->getOpenSize();
//          responseheader["Content-Type"] = mS3->ContentType();
//          responseheader["Content-Length"] = clength;
//          mhd_response = MHD_HTTP_OK;
//        }
//      }
//    }
//  }
//  else
//  {
//    //...........................................................................
//    // HTTP requests
//    //...........................................................................
//    if (mRangeDecodingError)
//    {
//      result = HttpServer::HttpError(mhd_response, responseheader, "Illegal Range request", MHD_HTTP_REQUESTED_RANGE_NOT_SATISFIABLE);
//    }
//    else
//    {
//      if (mRc != SFS_OK)
//      {
//        if (mRc == SFS_REDIRECT)
//        {
//          result = HttpServer::HttpRedirect(mhd_response, responseheader, mFile->error.getErrText(), mFile->error.getErrInfo(), mPath, mQuery, true);
//        }
//        else
//          if (mRc == SFS_ERROR)
//        {
//          result = HttpServer::HttpError(mhd_response, responseheader, mFile->error.getErrText(), mFile->error.getErrInfo());
//        }
//        else
//          if (mRc == SFS_DATA)
//        {
//          result = HttpServer::HttpData(mhd_response, responseheader, mFile->error.getErrText(), mFile->error.getErrInfo());
//        }
//        else
//          if (mRc == SFS_STALL)
//        {
//          result = HttpServer::HttpStall(mhd_response, responseheader, mFile->error.getErrText(), mFile->error.getErrInfo());
//        }
//        else
//        {
//          result = HttpServer::HttpError(mhd_response, responseheader, "unexpected result from file open", EOPNOTSUPP);
//        }
//        delete mFile;
//        mFile = 0;
//      }
//      else
//      {
//        if (mRangeRequest)
//        {
//          CreateMultipartHeader("application/octet-stream");
//          eos_static_info(Print());
//          char clength[16];
//          snprintf(clength, sizeof (clength) - 1, "%llu", (unsigned long long) mRequestSize);
//          if (mOffsetMap.size() == 1)
//          {
//            // if there is only one range we don't send a multipart response
//            responseheader["Content-Type"] = "application/octet-stream";
//            responseheader["Content-Range"] = mSinglepartHeader;
//          }
//          else
//          {
//            // for several ranges we send a multipart response
//            responseheader["Content-Type"] = mMultipartHeader;
//          }
//          responseheader["Content-Length"] = clength;
//          mhd_response = MHD_HTTP_PARTIAL_CONTENT;
//        }
//        else
//        {
//          // successful http open
//          char clength[16];
//          snprintf(clength, sizeof (clength) - 1, "%llu", (unsigned long long) mFile->getOpenSize());
//          mRequestSize = mFile->getOpenSize();
//          responseheader["Content-Type"] = "application/octet-stream";
//          responseheader["Content-Length"] = clength;
//          mhd_response = MHD_HTTP_OK;
//        }
//      }
//    }
//  }
//
//  if (mFile)
//  {
//    // GET method
//    response = MHD_create_response_from_callback(mRequestSize, 32 * 1024, /* 32k page size */
//                                                 &HttpServer::FileReaderCallback,
//                                                 (void*) this,
//                                                 &HttpServer::FileCloseCallback);
//  }
//  else
//  {
//    result = "";
//    response = MHD_create_response_from_buffer(result.length(),
//                                               (void *) result.c_str(),
//                                               MHD_RESPMEM_MUST_FREE);
//  }
//
//  if (response)
//  {
//    if (mCloseCode)
//    {
//      // close failed
//      result = HttpServer::HttpError(mhd_response, responseheader, "File close failed", MHD_HTTP_SERVICE_UNAVAILABLE);
//      response = MHD_create_response_from_buffer(result.length(),
//                                                 (void *) result.c_str(),
//                                                 MHD_RESPMEM_MUST_COPY);
//    }
//    else
//    {
//      for (auto it = responseheader.begin(); it != responseheader.end(); it++)
//      {
//        // add all the response header tags
//        MHD_add_response_header(response, it->first.c_str(), it->second.c_str());
//      }
//    }
//    eos_static_info("mhd_response=%d", mhd_response);
//    int ret = MHD_queue_response(mConnection, mhd_response, response);
//
//    return ret;
//
//  }
//  else
//  {
//    eos_static_alert("msg=\"response creation failed\"");
//    return 0;
//  }
//  return 0;
//}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
HttpHandler::Put (eos::common::HttpRequest *request)
{
  eos_static_info("method=PUT offset=%llu size=%llu size_ptr=%llu",
                  mCurrentCallbackOffset,
                  request->GetBodySize() ? *request->GetBodySize() : 0,
                  request->GetBodySize());

  eos::common::HttpResponse *response = 0;

  if (mRc)
  {
    if (mRc != SFS_OK)
    {
      if (mRc == SFS_REDIRECT)
      {
        response = HttpServer::HttpRedirect(request->GetUrl(),
                                            mFile->error.getErrText(),
                                            mFile->error.getErrInfo(),
                                            true);
      }
      else
        if (mRc == SFS_ERROR)
      {
          response = HttpServer::HttpError(mFile->error.getErrText(), mFile->error.getErrInfo());
      }
      else
        if (mRc == SFS_DATA)
      {
          response = HttpServer::HttpData(mFile->error.getErrText(), mFile->error.getErrInfo());
      }
      else
        if (mRc == SFS_STALL)
      {
          response = HttpServer::HttpStall(mFile->error.getErrText(), mFile->error.getErrInfo());
      }
      else
      {
        response = HttpServer::HttpError("unexpected result from file open", EOPNOTSUPP);
      }
      delete mFile;
      mFile = 0;

      return response;
    }
  }

  else
  {
    //...........................................................................
    // file streaming in
    //...........................................................................
    size_t *bodySize = request->GetBodySize();
    if (request->GetBody().c_str() && bodySize && (*bodySize))
    {
      size_t stored = mFile->write(mCurrentCallbackOffset,
                                   request->GetBody().c_str(),
                                   *request->GetBodySize());
      if (stored != *request->GetBodySize())
      {
        // HTTP write error
        response = HttpServer::HttpError("Write error occured",
                                         response->SERVICE_UNAVAILABLE);
        return response;
      }
      else
      {
        eos_static_info("msg=\"stored requested bytes\"");
        // decrease the upload left data size
        mUploadLeftSize -= *request->GetBodySize();
        mCurrentCallbackOffset += *request->GetBodySize();

        response = new eos::common::PlainHttpResponse();
        return response;
      }
    }

    else
    {
      eos_static_info("entering close handler");
      mCloseCode = mFile->close();
      if (mCloseCode)
      {
        response = HttpServer::HttpError("File close failed",
                                         response->SERVICE_UNAVAILABLE);
        mCloseCode = 0; // we don't want to create a second response down
        return response;
      }
      else
      {
        response = new eos::common::PlainHttpResponse();
        return response;
      }
    }
  }

  // Should never get here
  response = HttpServer::HttpError("Internal Server Error",
                                   response->INTERNAL_SERVER_ERROR);
  return response;
}

/*----------------------------------------------------------------------------*/
//int
//HttpHandler::Put (const char *upload_data,
//                 size_t *upload_data_size,
//                 bool first_call)
//{
//  int mhd_response = MHD_HTTP_OK;
//  std::string result;
//  std::map<std::string, std::string> responseheader;
//  struct MHD_Response *response = 0;
//
//  eos_static_info("method=PUT offset=%llu size=%llu size_ptr=%llu",
//                  mCurrentCallbackOffset,
//                  upload_data_size ? *upload_data_size : 0, upload_data_size
//                  );
//
//  if (mRc)
//  {
//    // check for open errors
////    if (mS3)
////    {
////      // ---------------------------------------------------------------------
////      // create S3 error responses
////      // ---------------------------------------------------------------------
////      if (mRc != SFS_OK)
////      {
////        if (mFile->error.getErrInfo() == EPERM)
////        {
////          result = mS3->RestErrorResponse(mhd_response, 403, "AccessDenied", "Access Denied", mS3->getPath(), "");
////        }
////        else
////        {
////          result = mS3->RestErrorResponse(mhd_response, 500, "InternalError", "File currently unwritable", mS3->getPath(), "");
////        }
////        delete mFile;
////        mFile = 0;
////        delete mS3;
////        mS3 = 0;
////      }
////    }
////    else
//    {
//      // ---------------------------------------------------------------------
//      // create HTTP error response
//      // ---------------------------------------------------------------------
////      if (mRc != SFS_OK)
////      {
////        if (mRc == SFS_REDIRECT)
////        {
////          result = HttpServer::HttpRedirect(mhd_response, responseheader, mFile->error.getErrText(), mFile->error.getErrInfo(), mPath, mQuery, true);
////        }
////        else
////          if (mRc == SFS_ERROR)
////        {
////          result = HttpServer::HttpError(mhd_response, responseheader, mFile->error.getErrText(), mFile->error.getErrInfo());
////        }
////        else
////          if (mRc == SFS_DATA)
////        {
////          result = HttpServer::HttpData(mhd_response, responseheader, mFile->error.getErrText(), mFile->error.getErrInfo());
////        }
////        else
////          if (mRc == SFS_STALL)
////        {
////          result = HttpServer::HttpStall(mhd_response, responseheader, mFile->error.getErrText(), mFile->error.getErrInfo());
////        }
////        else
////        {
////          result = HttpServer::HttpError(mhd_response, responseheader, "unexpected result from file open", EOPNOTSUPP);
////        }
////        delete mFile;
////        mFile = 0;
////      }
//    }
//
//    response = MHD_create_response_from_buffer(result.length(),
//                                               (void *) result.c_str(),
//                                               MHD_RESPMEM_MUST_FREE);
//    int ret = MHD_queue_response(mConnection, mhd_response, response);
//    eos_static_err("result=%s", result.c_str());
//    return ret;
//  }
//  else
//  {
//    //...........................................................................
//    // file streaming in
//    //...........................................................................
//    if (upload_data && upload_data_size && (*upload_data_size))
//    {
//
//      if ( ((mUploadLeftSize > (10 * 1024 * 1024)) && ((*upload_data_size) < (10 * 1024 * 1024))) )
//      {
//        // we want more bytes, we don't process this
//        eos_static_info("msg=\"wait for more bytes\" leftsize=%llu uploadsize=%llu", mUploadLeftSize, *upload_data_size);
//        return MHD_YES;
//      }
//
//      size_t stored = mFile->write(mCurrentCallbackOffset, upload_data, *upload_data_size);
//      if (stored != *upload_data_size)
//      {
////        if (mS3)
////        {
////          // S3 write error
////          result = mS3->RestErrorResponse(mhd_response, 500, "InternalError", "File currently unwritable (write failed)", mS3->getPath(), "");
////          delete mFile;
////          mFile = 0;
////          delete mS3;
////          mS3 = 0;
////        }
////        else
//        {
//          // HTTP write error
//          result = HttpServer::HttpError(mhd_response, responseheader, "Write error occured", MHD_HTTP_SERVICE_UNAVAILABLE);
//        }
//        response = MHD_create_response_from_buffer(result.length(),
//                                                   (void *) result.c_str(),
//                                                   MHD_RESPMEM_MUST_COPY);
//
//        int ret = MHD_queue_response(mConnection, mhd_response, response);
//        eos_static_err("result=%s", result.c_str());
//        return ret;
//      }
//      else
//      {
//        eos_static_info("msg=\"stored requested bytes\"");
//        // decrease the upload left data size
//        mUploadLeftSize -= *upload_data_size;
//        mCurrentCallbackOffset += *upload_data_size;
//        // set to the number of bytes not processed ...
//        *upload_data_size = 0;
//        // don't queue any response here
//        return MHD_YES;
//      }
//    }
//    else
//    {
//      if (first_call)
//      {
//        // if the file was opened we just return MHD_YES to allow the upper
//        // layer to send 100-CONTINUE and to call us again
//        eos_static_info("first-call 100-continue handling");
//        return MHD_YES;
//      }
//      else
//      {
//        eos_static_info("entering close handler");
//        mCloseCode = mFile->close();
//        if (mCloseCode)
//        {
//          result = HttpServer::HttpError(mhd_response, responseheader, "File close failed", MHD_HTTP_SERVICE_UNAVAILABLE);
//          response = MHD_create_response_from_buffer(result.length(),
//                                                     (void *) result.c_str(),
//                                                     MHD_RESPMEM_MUST_COPY);
//          mCloseCode = 0; // we don't want to create a second response down
//        }
//        else
//        {
//          result = "";
//          response = MHD_create_response_from_buffer(result.length(),
//                                                     (void *) result.c_str(),
//                                                     MHD_RESPMEM_MUST_FREE);
//        }
//      }
//    }
//
////    if (mS3)
////    {
////      char sFileId[16];
////      snprintf(sFileId, sizeof (sFileId) - 1, "%llu", mFileId);
////
////      // add some S3 specific tags to the response object
////      responseheader["x-amz-version-id"] = sFileId;
////      responseheader["x-amz-request-id"] = mLogId;
////      responseheader["Server"] = gOFS.HostName;
////      responseheader["Connection"] = "close";
////      responseheader["ETag"] = sFileId;
////    }
//
//    for (auto it = responseheader.begin(); it != responseheader.end(); it++)
//    {
//      // add all the response header tags
//      MHD_add_response_header(response, it->first.c_str(), it->second.c_str());
//    }
//
//    eos_static_info("mhd_response=%d", mhd_response);
//    int ret = MHD_queue_response(mConnection, mhd_response, response);
//
//    return ret;
//  }
//}


/*----------------------------------------------------------------------------*/
bool
HttpHandler::DecodeByteRange (std::string               rangeheader,
                              std::map<off_t, ssize_t> &offsetmap,
                              ssize_t                  &requestsize,
                              off_t                     filesize)
{
  std::vector<std::string> tokens;
  if (rangeheader.substr(0, 6) != "bytes=")
  {
    // this is an illegal header
    return false;
  }
  else
  {
    rangeheader.erase(0, 6);
  }

  eos::common::StringConversion::Tokenize(rangeheader, tokens, ",");
  // decode the string parts
  for (size_t i = 0; i < tokens.size(); i++)
  {
    eos_static_info("decoding %s", tokens[i].c_str());
    off_t start = 0;
    off_t stop = 0;
    off_t length = 0;

    size_t mpos = tokens[i].find("-");
    if (mpos == std::string::npos)
    {
      // there must always be a '-'
      return false;
    }
    std::string sstop = tokens[i];
    std::string sstart = tokens[i];
    sstart.erase(mpos);
    sstop.erase(0, mpos + 1);
    if (sstart.length())
    {
      start = strtoull(sstart.c_str(), 0, 10);
    }
    if (sstop.length())
    {
      stop = strtoull(sstop.c_str(), 0, 10);
    }

    if ((start > filesize) || (stop > filesize))
    {
      return false;
    }

    if (stop >= start)
    {
      length = (stop - start) + 1;
    }
    else
    {
      continue;
    }

    if (offsetmap.count(start))
    {
      if (offsetmap[start] < length)
      {
        // a previous block has been replaced with a longer one
        offsetmap[start] = length;
      }
    }
    else
    {
      offsetmap[start] = length;
    }
  }

  // now merge overlapping requests
  bool merged = true;
  while (merged)
  {
    requestsize = 0;
    if (offsetmap.begin() == offsetmap.end())
    {
      // if there is nothing in the map just return with error
      eos_static_err("msg=\"range map is empty\"");
      return false;
    }
    for (auto it = offsetmap.begin(); it != offsetmap.end(); it++)
    {
      eos_static_info("offsetmap %llu:%llu", it->first, it->second);
      auto next = it;
      next++;
      if (next != offsetmap.end())
      {
        // check if we have two overlapping requests
        if ((it->first + it->second) >= (next->first))
        {
          merged = true;
          // merge this two
          it->second = next->first + next->second - it->first;
          offsetmap.erase(next);
          break;
        }
        else
        {
          merged = false;
        }
      }
      else
      {
        merged = false;
      }
      // compute the total size
      requestsize += it->second;
    }
  }
  return true;
}

/*----------------------------------------------------------------------------*/
EOSFSTNAMESPACE_END
