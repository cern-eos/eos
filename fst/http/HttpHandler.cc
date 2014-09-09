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
#include "common/Path.hh"
#include "common/http/HttpResponse.hh"
#include "common/http/OwnCloud.hh"
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
HttpHandler::Matches (const std::string &meth, HeaderMap &headers)
{
  int method = ParseMethodString(meth);

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
  eos_static_debug("Handling HTTP request");
  bool isOcChunkUpload = false;

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

    XrdOucString openUrl = request->GetUrl().c_str();
    XrdOucString query = request->GetQuery().c_str();
    if (request->GetMethod() == "PUT")
    {
      // use the proper creation/open flags for PUT's
      open_mode |= SFS_O_CREAT;

      // avoid truncation of chunked uploads
      if (!request->GetHeaders().count("OC-Chunked"))
      {
        open_mode |= SFS_O_TRUNC;
      }
      else
      {
        isOcChunkUpload = true;
      }

      open_mode |= SFS_O_RDWR;
      open_mode |= SFS_O_MKPTH;
      create_mode |= (SFS_O_MKPTH | S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    }

    mRc = mFile->open(openUrl.c_str(),
                      open_mode,
                      create_mode,
                      &mClient,
                      query.c_str());

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

    if (((mUploadLeftSize > (4 * 1024 * 1024)) &&
         ((*request->GetBodySize()) < (4 * 1024 * 1024))))
    {
      // we want more bytes, we don't process this
      eos_static_debug("msg=\"wait for more bytes\" leftsize=%llu uploadsize=%llu",
                       mUploadLeftSize, *request->GetBodySize());
      mHttpResponse = new eos::common::PlainHttpResponse();
      mHttpResponse->SetResponseCode(0);
      return;
    }

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
void
HttpHandler::Initialize (eos::common::HttpRequest *request)
{
  if (request->GetCookies().count("EOSCAPABILITY"))
  {
    // if we have a capability we don't use the query CGI but that one
    request->SetQuery(request->GetCookies()["EOSCAPABILITY"]);

  }

  if (request->GetHeaders().count("Content-Length"))
  {

    mContentLength = strtoull(request->GetHeaders()["Content-Length"].c_str(),
                              0, 10);
    mUploadLeftSize = mContentLength;
  }

  std::string query = request->GetQuery();
  HttpServer::DecodeURI(query); // unescape '+' '/' '='
  request->SetQuery(query);

  eos_static_debug("path=%s query=%s", request->GetUrl().c_str(),
                   request->GetQuery().c_str());

  // define the client sec entity object
  strncpy(mClient.prot, "unix", XrdSecPROTOIDSIZE - 1);
  mClient.prot[XrdSecPROTOIDSIZE - 1] = '\0';
  mClient.name = strdup("nobody");
  mClient.host = strdup("localhost");
  mClient.tident = strdup("http");
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
HttpHandler::Get (eos::common::HttpRequest *request)
{
  eos::common::HttpResponse *response = 0;

  if (mRangeDecodingError)
  {
    response = HttpServer::HttpError("Illegal Range request",
                                     response->REQUESTED_RANGE_NOT_SATISFIABLE);
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
        response = HttpServer::HttpError(mFile->error.getErrText(),
                                         mFile->error.getErrInfo());
      }
      else if (mRc == SFS_DATA)
      {
        response = HttpServer::HttpData(mFile->error.getErrText(),
                                        mFile->error.getErrInfo());
      }
      else if (mRc == SFS_STALL)
      {
        response = HttpServer::HttpStall(mFile->error.getErrText(),
                                         mFile->error.getErrInfo());
      }
      else
      {
        response = HttpServer::HttpError("unexpected result from file open",
                                         EOPNOTSUPP);
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
        eos_static_debug(Print());
        char clength[16];
        snprintf(clength, sizeof (clength) - 1, "%llu",
                 (unsigned long long) mRequestSize);
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
        response->mResponseLength = mRequestSize;
        response->SetResponseCode(response->PARTIAL_CONTENT);
      }
      else
      {
        // successful http open
        char clength[16];
        snprintf(clength, sizeof (clength) - 1, "%llu",
                 (unsigned long long) mFile->getOpenSize());
        mRequestSize = mFile->getOpenSize();
        response->mResponseLength = mRequestSize;
        response->AddHeader("Content-Type", "application/octet-stream");
        response->AddHeader("Content-Length", clength);
      }

      std::string query = request->GetQuery();
      if (query.find("mgm.etag") != std::string::npos)
      {
        XrdOucEnv queryenv(query.c_str());
        const char* etag = 0;
        if ((etag = queryenv.Get("mgm.etag")))
        {
          //
          response->AddHeader("ETag", etag);
        }
        response->SetResponseCode(response->OK);
      }
    }
  }

  if (mFile)
  {

    time_t mtime = mFile->GetMtime();
    response->AddHeader("Last-Modified", eos::common::Timing::utctime(mtime));
    // We want to use the file callbacks
    response->mUseFileReaderCallback = true;
  }

  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
HttpHandler::Head (eos::common::HttpRequest *request)
{
  eos::common::HttpResponse *response = Get(request);
  response->mUseFileReaderCallback = false;
  if (mFile)
  {

    mFile->close();
    delete mFile;
    mFile = 0;
  }

  return response;
}

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
        response = HttpServer::HttpError(mFile->error.getErrText(),
                                         mFile->error.getErrInfo());
      }
      else
        if (mRc == SFS_DATA)
      {
        response = HttpServer::HttpData(mFile->error.getErrText(),
                                        mFile->error.getErrInfo());
      }
      else
        if (mRc == SFS_STALL)
      {
        response = HttpServer::HttpStall(mFile->error.getErrText(),
                                         mFile->error.getErrInfo());
      }
      else
      {
        response = HttpServer::HttpError("unexpected result from file open",
                                         EOPNOTSUPP);
      }
      delete mFile;
      mFile = 0;

      return response;
    }
  }

  else
  {
    // check for chunked uploads
    if (!mCurrentCallbackOffset && request->GetHeaders().count("OC-Chunked"))
    {
      int chunk_n = 0;
      int chunk_max = 0;
      XrdOucString chunk_uuid;

      if (!eos::common::OwnCloud::getContentSize(request))
      {
        response = HttpServer::HttpError("Missing total length in OC request",
                                         response->BAD_REQUEST);
        return response;
      }

      eos::common::OwnCloud::GetChunkInfo(request->GetQuery().c_str(),
                                          chunk_n,
                                          chunk_max,
                                          chunk_uuid);

      if (chunk_n >= chunk_max)
      {
        // there is something inconsistent here
        // HTTP write error
        response = HttpServer::HttpError("Illegal chunks specified in OC request",
                                         response->BAD_REQUEST);
        return response;
      }

      unsigned long long contentlength = eos::common::StringConversion::GetSizeFromString(request->GetHeaders()["Content-Length"]);
      // recompute offset where to write
      if ((chunk_n + 1) < chunk_max)
      {
        // the first n-1 chunks have a straight forward offset
        mCurrentCallbackOffset = contentlength * chunk_n;
	eos_static_debug("setting to false %lld", mCurrentCallbackOffset);
	mLastChunk = false;
      }
      else
      {
        // the last chunks has to be written at offset=total-length - chunk-length
        mCurrentCallbackOffset =
                eos::common::StringConversion::GetSizeFromString(eos::common::OwnCloud::getContentSize(request));
        mCurrentCallbackOffset -= contentlength;
	eos_static_debug("setting to true %lld", mCurrentCallbackOffset);
	mLastChunk = true;
      }
    }


    // file streaming in
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
        eos_static_info("msg=\"stored requested bytes\" offset=%lld", mCurrentCallbackOffset);
        // decrease the upload left data size
        mUploadLeftSize -= *request->GetBodySize();
        mCurrentCallbackOffset += *request->GetBodySize();

        response = new eos::common::PlainHttpResponse();

        std::string query = request->GetQuery();
        XrdOucEnv queryenv(query.c_str());
        const char* etag = 0;
        if ((etag = queryenv.Get("mgm.etag")))
        {
          //
          response->AddHeader("ETag", etag);
        }
        return response;
      }
    }
    else
    {
      eos_static_info("entering close handler");
      eos::common::HttpRequest::HeaderMap header = request->GetHeaders();

      if (header.count("X-OC-Mtime"))
      {
        // there is an X-OC-Mtime header to force the mtime for that file
        mFile->SetForcedMtime(strtoull(header["X-OC-Mtime"].c_str(), 0, 10), 0);
      }

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
        response->AddHeader("ETag", mFile->GetETag());
        if (header.count("X-OC-Mtime") && ( mLastChunk || (!request->GetHeaders().count("OC-Chunked"))))
        {
	  // only normal uploads or the last chunk receive these extra response headers
          response->AddHeader("X-OC-Mtime", "accepted");
          // return the OC-FileId header
          std::string ocid;
          eos::common::StringConversion::GetSizeString(ocid, mFileId << 28);
          response->AddHeader("OC-FileId", ocid);
        }
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
bool
HttpHandler::DecodeByteRange (std::string rangeheader,
                              std::map<off_t, ssize_t> &offsetmap,
                              ssize_t &requestsize,
                              off_t filesize)
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
    eos_static_debug("decoding %s", tokens[i].c_str());
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
    else
    {
      start = 0;
    }

    if (sstop.length())
    {
      stop = strtoull(sstop.c_str(), 0, 10);
    }
    else
    {
      stop = filesize;
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
      eos_static_debug("offsetmap %llu:%llu", it->first, it->second);
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
