// ----------------------------------------------------------------------
// File: S3Handler.cc
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
#include "fst/http/s3/S3Handler.hh"
#include "common/http/PlainHttpResponse.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <string>
#include <map>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
bool
S3Handler::Matches (const std::string &method, HeaderMap &headers)
{
  if (headers.count("Authorization"))
  {
    if (headers["Authorization"].substr(0, 3) == "AWS")
    {
      eos_static_info("info=Matched S3 protocol for request");
      return true;
    }
  }
  return false;
}

/*----------------------------------------------------------------------------*/
void
S3Handler::HandleRequest (eos::common::HttpRequest *request)
{
  eos_static_info("msg=\"handling s3 request\"");

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
    if (request->GetHeaders().count("range"))
    {
      if (!DecodeByteRange(request->GetHeaders()["range"],
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
      // we put the file size as request size if this is not a range request
      // aka full file download
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

    if ( ((mUploadLeftSize > (10 * 1024 * 1024)) &&
         ((*request->GetBodySize()) < (10 * 1024 * 1024))) )
    {
      // we want more bytes, we don't process this
      eos_static_info("msg=\"wait for more bytes\" leftsize=%llu uploadsize=%llu",
                      mUploadLeftSize, *request->GetBodySize());
      mHttpResponse = new eos::common::PlainHttpResponse();
      mHttpResponse->SetResponseCode(0);
      return;
    }

    // call the HttpHandler::Put method
    mHttpResponse = Put(request);

    if (!mHttpResponse || request->GetBodySize() == 0)
    {
      // clean-up left-over objects on error or end-of-put
      if (mFile)
      {
        delete mFile;
        mFile = 0;
      }
    }
  }

}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
S3Handler::Get (eos::common::HttpRequest *request)
{
  int mhd_response = eos::common::HttpResponse::OK;
  std::string result;
  std::map<std::string, std::string> responseheader;

  eos::common::HttpResponse *response = 0;

  if (mRangeDecodingError)
  {
    response = RestErrorResponse(416, "InvalidRange", "Illegal Range request",
                                 request->GetHeaders()["range"].c_str(), "");
  }
  else
  {
    if (mRc != SFS_OK)
    {
      if (mFile->error.getErrInfo() == ENOENT)
      {
        response = RestErrorResponse(404, "NoSuchKey", "The specified key does "
                                     "not exist", GetPath(), "");
      }
      else if (mFile->error.getErrInfo() == EPERM)
      {
        response = RestErrorResponse(403, "AccessDenied", "Access Denied",
                                     GetPath(), "");
      }
      else
      {
        response = RestErrorResponse(500, "InternalError", "File currently "
                                     "unavailable", GetPath(), "");
      }
      delete mFile;
      mFile = 0;
    }
    else
    {
      response = new eos::common::S3Response();

      if (mRangeRequest)
      {
        CreateMultipartHeader(eos::common::HttpResponse::ContentType(GetPath()));
        eos_static_info(Print());
        char clength[16];
        snprintf(clength, sizeof (clength) - 1, "%llu",
                (unsigned long long) mRequestSize);
        if (mOffsetMap.size() == 1)
        {
          // if there is only one range we don't send a multipart response
          responseheader["Content-Type"] = response->ContentType(GetPath());
          responseheader["Content-Range"] = mSinglepartHeader;
        }
        else
        {
          // for several ranges we send a multipart response
          responseheader["Content-Type"] = mMultipartHeader;
        }
        responseheader["Content-Length"] = clength;
        mhd_response = response->PARTIAL_CONTENT;
      }
      else
      {
        // successful http open
        char clength[16];
        snprintf(clength, sizeof (clength) - 1, "%llu",
                (unsigned long long) mFile->getOpenSize());
        mRequestSize = mFile->getOpenSize();
        response->mResponseLength = mRequestSize;
        responseheader["Content-Type"] = response->ContentType(GetPath());
        responseheader["Content-Length"] = clength;
        mhd_response = response->OK;
      }
    }
  }

  if (mFile)
  {
    // We want to use the file callbacks
    response->mUseFileReaderCallback = true;
  }

  response->SetHeaders(responseheader);
  response->SetResponseCode(mhd_response);
  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
S3Handler::Put (eos::common::HttpRequest *request)
{
  std::map<std::string, std::string> responseheader;
  eos::common::HttpResponse *response = 0;

  eos_static_info("method=PUT offset=%llu size=%llu size_ptr=%llu",
                  mCurrentCallbackOffset,
                  request->GetBodySize() ? *request->GetBodySize() : 0,
                  request->GetBodySize());

  if (mRc)
  {
    // check for open errors
    // create S3 error responses
    if (mRc != SFS_OK)
    {
      if (mFile->error.getErrInfo() == EPERM)
      {
        response = RestErrorResponse(403, "AccessDenied", "Access Denied",
                                     GetPath(), "");
      }
      else
      {
        response = RestErrorResponse(500, "InternalError", "File currently "
                                     "unwritable", GetPath(), "");
      }
      delete mFile;
      mFile = 0;
    }

  }
  else
  {
    // file streaming in

    size_t *bodySize = request->GetBodySize();
    if (request->GetBody().c_str() && bodySize && (*bodySize))
    {

      size_t stored = mFile->write(mCurrentCallbackOffset,
                                   request->GetBody().c_str(), *bodySize);
      if (stored != *bodySize)
      {
        // S3 write error
        response = RestErrorResponse(500, "InternalError", "File currently "
                                     "unwritable (write failed)", GetPath(), "");
        delete mFile;
        mFile = 0;
      }
      else
      {
        eos_static_info("msg=\"stored requested bytes\"");
        // decrease the upload left data size
        mUploadLeftSize -= *bodySize;
        mCurrentCallbackOffset += *bodySize;

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
      }
      else
      {
        response = new eos::common::PlainHttpResponse();
        return response;
      }
    }
  }

  char sFileId[16];
  snprintf(sFileId, sizeof (sFileId) - 1, "%llu", mFileId);

  // add some S3 specific tags to the response object
  responseheader["x-amz-version-id"] = sFileId;
  responseheader["x-amz-request-id"] = mLogId;
  responseheader["Server"] = gOFS.HostName;
  responseheader["Connection"] = "close";
  responseheader["ETag"] = sFileId;

  response = new eos::common::S3Response();
  response->SetHeaders(responseheader);
  return response;

}

/*----------------------------------------------------------------------------*/
EOSFSTNAMESPACE_END
