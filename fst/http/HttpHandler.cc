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

#include "fst/http/HttpHandler.hh"
#include "fst/http/HttpServer.hh"
#include "fst/checksum/Adler.hh"
#include "common/Path.hh"
#include "common/Timing.hh"
#include "common/http/HttpResponse.hh"
#include "common/http/OwnCloud.hh"
#include "common/http/PlainHttpResponse.hh"
#include "common/http/MimeTypes.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/XrdFstOfsFile.hh"
#include <XrdSys/XrdSysPthread.hh>
#include <XrdSfs/XrdSfsInterface.hh>
#include <algorithm>

EOSFSTNAMESPACE_BEGIN

XrdSysMutex HttpHandler::mOpenMutexMapMutex;
std::map<unsigned int, XrdSysMutex*> HttpHandler::mOpenMutexMap;
eos::common::MimeTypes HttpHandler::gMime;
HttpHandlerFstFileCache HttpHandler::sFileCache;

/*----------------------------------------------------------------------------*/
HttpHandler::~HttpHandler()
{
  if (mFile) {
    delete mFile;
    mFile = nullptr;
  }
}

/*----------------------------------------------------------------------------*/
bool
HttpHandler::Matches(const std::string& meth, HeaderMap& headers)
{
  int method = ParseMethodString(meth);

  // We only support GET, HEAD and PUT on the FST (CREATE is used by XrdHttp)
  if (method == GET || method == HEAD || method == PUT || method == CREATE) {
    eos_static_info("%s", "msg=\"Matched HTTP protocol for request\"");
    return true;
  } else {
    return false;
  }
}

/*----------------------------------------------------------------------------*/
void
HttpHandler::HandleRequest(eos::common::HttpRequest* request)
{
  eos_static_debug("Handling HTTP request");

  if (!mFile) {
    Initialize(request);
  }

  if (!mFile) {
    // default modes are for GET=read
    XrdSfsFileOpenMode open_mode = 0;
    mode_t create_mode = 0;
    XrdOucString openUrl = request->GetUrl().c_str();
    XrdOucString query = request->GetQuery().c_str();

    if (request->GetHeaders().count("x-upload-range")) {
      // we need to indicate to XrdFstOfsFile that this is a partial upload
      query += "&x-upload-range=";
      query += request->GetHeaders()["x-upload-range"].c_str();
    }

    if ((request->GetMethod() == "PUT") ||
        (request->GetMethod() == "CREATE")) {
      // use the proper creation/open flags for PUT's
      open_mode |= SFS_O_CREAT;

      if (EOS_LOGS_DEBUG) {
        for (auto it = request->GetHeaders().begin(); it != request->GetHeaders().end();
             ++it) {
          eos_static_debug("header %s <=> %s", it->first.c_str(), it->second.c_str());
        }
      }

      // Avoid truncation of chunked uploads
      if (!request->GetHeaders().count("oc-chunked") &&
          !request->GetHeaders().count("x-upload-range")) {
        open_mode |= SFS_O_TRUNC;
      } else {
        eos_static_info("%s", "msg=\"removing truncation flag\"");
      }

      open_mode |= SFS_O_RDWR;
      open_mode |= SFS_O_MKPTH;
      create_mode |= (SFS_O_MKPTH | S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    }

    // if we are opening for reading see if we already have an opened file
    // in the cache
    HttpHandlerFstFileCache::Key cachekey(mClient.name, openUrl.c_str(),
                                       query.c_str(), open_mode);
    mFileCacheEntry.clear();
    if (open_mode == 0) {
      mFileCacheEntry = sFileCache.remove(cachekey);
      if ( (mFile = mFileCacheEntry.getfp()) ) {
        eos_static_debug("path=%s found in open-file cache fp=%p",
                         openUrl.c_str(), mFile);
        mRc = SFS_OK;
      }
    }

    // if no cached file open a new one
    if (!mFile) {

      XrdSysMutex* hMutex = 0;
      {
        // use path dependent locks for opens
        // we will accumulate up to 64k mutexes for this
        Adler lHash;
        lHash.Add(openUrl.c_str(), openUrl.length(), 0);
        lHash.Finalize();
        {
          XrdSysMutexHelper oLock(mOpenMutexMapMutex);

          if (!mOpenMutexMap.count(lHash.GetAdler())) {
            hMutex = new XrdSysMutex();
            mOpenMutexMap[lHash.GetAdler()] = hMutex;
          } else {
            hMutex = mOpenMutexMap[lHash.GetAdler()];
          }
        }
      }

      mFile = (XrdFstOfsFile*) gOFS.newFile(mClient.name);
      {
        XrdSysMutexHelper oLock(*hMutex);
        mRc = mFile->open(openUrl.c_str(),
                          open_mode,
                          create_mode,
                          &mClient,
                          query.c_str());
      }
    }
    mFileSize = mFile->GetOpenSize();
    mFileId = mFile->GetFileId();
    mLogId = mFile->logId;

    // check for range requests
    if (request->GetHeaders().count("range")) {
      if (!DecodeByteRange(request->GetHeaders()["range"],
                           mOffsetMap,
                           mRangeRequestSize,
                           mFileSize)) {
        // indicate range decoding error
        mRangeDecodingError = true;
      } else {
        mRangeRequest = true;
      }
    }

    // if this file wasn't in opened-file cache and it's for reading
    // in a range request, save it to the cache once we're finished.
    // (perhaps we'll soon have another read-range request for the same file)
    if (open_mode == 0 && mRc == SFS_OK && mFileCacheEntry.getfp() != mFile) {
      eos_static_debug("path=%s eligible to be saved in open-file cache fp=%p",
                       openUrl.c_str(), mFile);
      mFileCacheEntry.set(cachekey, mFile);
    }

    // check for range requests
    if (request->GetHeaders().count("x-upload-range") &&
        request->GetHeaders().count("x-upload-totalsize")) {
      if (!DecodeByteRange(request->GetHeaders()["x-upload-range"],
                           mOffsetMap,
                           mRangeRequestSize,
                           std::stoul(request->GetHeaders()["x-upload-totalsize"]))) {
        // indicate range decoding error
        mRangeDecodingError = true;
      } else {
        mRangeRequest = true;
      }
    }

    if (!mRangeRequest) {
      // we put the file size as request size if this is not a range request
      // aka full file download
      mRangeRequestSize = mFile->GetOpenSize();
    }
  }

  if (request->GetMethod() == "GET") {
    // call the HttpHandler::Get method
    mHttpResponse = Get(request);
  }

  if (request->GetMethod() == "CREATE") {
    // fake method for XrdHttp bridge
    mHttpResponse = new eos::common::PlainHttpResponse();
    mHttpResponse->SetResponseCode(0);
    return;
  }

  if (request->GetMethod() == "PUT") {
    if (((mUploadLeftSize > (1 * 1024 * 1024)) &&
         ((*request->GetBodySize()) < (1 * 1024 * 1024)))) {
      // we want more bytes, we don't process this
      eos_static_debug("msg=\"wait for more bytes\" leftsize=%llu uploadsize=%llu",
                       mUploadLeftSize, *request->GetBodySize());
      mHttpResponse = new eos::common::PlainHttpResponse();
      mHttpResponse->SetResponseCode(0);
      return;
    }

    mHttpResponse = Put(request);

    if (!mHttpResponse || (*request->GetBodySize()) == 0) {
      // clean-up left-over objects on error or end-of-put
      if (mFile) {
        delete mFile;
        mFile = 0;
      }
    }
  }
}

/*----------------------------------------------------------------------------*/
void
HttpHandler::Initialize(eos::common::HttpRequest* request)
{
  if (request->GetCookies().count("EOSCAPABILITY")) {
    // if we have a capability we don't use the query CGI but that one
    request->SetQuery(request->GetCookies()["EOSCAPABILITY"]);
  }

  if (request->GetHeaders().count("content-length")) {
    mContentLength = strtoull(request->GetHeaders()["content-length"].c_str(),
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
HttpHandler::Get(eos::common::HttpRequest* request)
{
  eos::common::HttpResponse* response = nullptr;

  if (mRangeDecodingError) {
    mErrCode = eos::common::HttpResponse::REQUESTED_RANGE_NOT_SATISFIABLE;
    mErrText = "Illegal Range request";
    response = HttpServer::HttpError(mErrText.c_str(), mErrCode);
  } else {
    if (mErrCode) {
      eos_static_err("msg=\"return stored error\" errc=%d errmsg=\"%s\"", mErrCode,
                     mErrText.c_str());
      response = HttpServer::HttpError(mErrText.c_str(), mErrCode);
      return response;
    }

    if (mRc != SFS_OK) {
      if (mRc == SFS_REDIRECT) {
        mErrCode = eos::common::HttpResponse::INTERNAL_SERVER_ERROR;
        mErrText = mFile->error.getErrText();
        response = HttpServer::HttpError(mErrText.c_str(), mErrCode);
      } else if (mRc == SFS_ERROR) {
        mErrCode = mFile->error.getErrInfo();
        mErrText = mFile->error.getErrText();
        response = HttpServer::HttpError(mErrText.c_str(), mErrCode);
      } else if (mRc == SFS_DATA) {
        response = HttpServer::HttpData(mFile->error.getErrText(),
                                        mFile->error.getErrInfo());
      } else if (mRc == SFS_STALL) {
        response = HttpServer::HttpStall(mFile->error.getErrText(),
                                         mFile->error.getErrInfo());
      } else {
        response = HttpServer::HttpError("unexpected result from file open",
                                         EOPNOTSUPP);
      }

      delete mFile;
      mFile = 0;
    } else {
      response = new eos::common::PlainHttpResponse();

      if (mRangeRequest) {
        CreateMultipartHeader("application/octet-stream");
        eos_static_debug(Print());
        char clength[16];
        snprintf(clength, sizeof(clength) - 1, "%llu",
                 (unsigned long long) mRequestSize);

        if (mOffsetMap.size() == 1) {
          // if there is only one range we don't send a multipart response
          response->AddHeader("Content-Type", gMime.Match(request->GetUrl()));
          response->AddHeader("Content-Range", mSinglepartHeader);
        } else {
          // for several ranges we send a multipart response
          response->AddHeader("Content-Type", mMultipartHeader);
        }

        response->AddHeader("Content-Length", clength);
        response->mResponseLength = mRequestSize;
        response->SetResponseCode(eos::common::HttpResponse::PARTIAL_CONTENT);
      } else {
        // successful http open
        char clength[16];
        snprintf(clength, sizeof(clength) - 1, "%llu",
                 (unsigned long long) mFile->GetOpenSize());
        mRequestSize = mFile->GetOpenSize();
        response->mResponseLength = mRequestSize;
        response->AddHeader("Content-Type", gMime.Match(request->GetUrl()));
        response->AddHeader("Content-Length", clength);

        // retrieve a checksum when file is still open
        if (mFile->GetChecksum()) {
          std::string checksum_name = mFile->GetChecksum()->GetName();
          std::string checksum_val = mFile->GetFmdChecksum();

          while (checksum_val[0] == '0') {
            checksum_val.erase(0, 1);
          }

          std::string checksum_string = eos::common::OwnCloud::GetChecksumString(
                                          checksum_name,
                                          checksum_val);
          response->AddHeader("OC-Checksum", checksum_string);
          const auto& hdrs = request->GetHeaders();
          auto it = hdrs.find("want-digest");

          if (it != hdrs.end()) {
            // According to RFC 3230 the Digest reponse needs to have the
            // following format:
            // instance-digest = digest-algorithm "=" <encoded digest output>
            std::replace(checksum_string.begin(), checksum_string.end(),
                         ':', '=');
            response->AddHeader("Digest", checksum_string);
          }
        }
      }

      std::string query = request->GetQuery();

      if (query.find("mgm.etag") != std::string::npos) {
        XrdOucEnv queryenv(query.c_str());
        const char* etag = 0;

        if ((etag = queryenv.Get("mgm.etag"))) {
          response->AddHeader("ETag", etag);
        }

        if (mRangeRequest) {
          response->SetResponseCode(eos::common::HttpResponse::PARTIAL_CONTENT);
        } else {
          response->SetResponseCode(eos::common::HttpResponse::OK);
        }
      }
    }
  }

  if (mFile) {
    time_t mtime = mFile->GetMtime();
    response->AddHeader("Last-Modified", eos::common::Timing::utctime(mtime));
    // We want to use the file callbacks
    response->mUseFileReaderCallback = true;
  }

  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
HttpHandler::Head(eos::common::HttpRequest* request)
{
  eos::common::HttpResponse* response = Get(request);
  response->mUseFileReaderCallback = false;

  if (mFile) {
    FileClose(CanCache::NO);
    delete mFile;
    mFile = 0;
  }

  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
HttpHandler::Put(eos::common::HttpRequest* request)
{
  eos_static_info("method=PUT offset=%llu size=%llu size_ptr=%llu range-map-size:%u",
                  mCurrentCallbackOffset,
                  request->GetBodySize() ? *request->GetBodySize() : 0,
                  request->GetBodySize(),
                  mOffsetMap.size());
  eos::common::HttpResponse* response = nullptr;
  bool checksumError = false;
  bool checksumMatch = false;

  if (mRangeDecodingError) {
    mErrCode = eos::common::HttpResponse::REQUESTED_RANGE_NOT_SATISFIABLE;
    mErrText = "Illegal Range request";
  } else {
    if (mRangeRequest) {
      auto it = mOffsetMap.begin();

      if ((it->second) != (off_t)std::stoul(
            request->GetHeaders()["content-length"])) {
        mErrCode = eos::common::HttpResponse::REQUESTED_RANGE_NOT_SATISFIABLE;
        mErrText = "Illegal Range request - not matching content length";
        eos_static_err("range: [%lu:%lu] content-length: %lu", it->first, it->second,
                       std::stoul(request->GetHeaders()["content-length"]));
      }
    }
  }

  if (mErrCode) {
    eos_static_err("msg=\"return stored error\" errc=%d errmsg=\"%s\"", mErrCode,
                   mErrText.c_str());
    response = HttpServer::HttpError(mErrText.c_str(), mErrCode);
    delete mFile;
    mFile = 0;
    return response;
  }

  if (mRc) {
    if (mRc != SFS_OK) {
      if (mRc == SFS_REDIRECT) {
        // we cannot redirect the PUT at this point, just send an error back
        mErrCode = eos::common::HttpResponse::INTERNAL_SERVER_ERROR;
        mErrText = mFile->error.getErrText();
        response = HttpServer::HttpError(mErrText.c_str(), mErrCode);
      } else if (mRc == SFS_ERROR) {
        mErrCode = mFile->error.getErrInfo();
        mErrText = mFile->error.getErrText();
        response = HttpServer::HttpError(mErrText.c_str(), mErrCode);
      } else if (mRc == SFS_DATA) {
        response = HttpServer::HttpData(mFile->error.getErrText(),
                                        mFile->error.getErrInfo());
      } else if (mRc == SFS_STALL) {
        response = HttpServer::HttpStall(mFile->error.getErrText(),
                                         mFile->error.getErrInfo());
      } else {
        response = HttpServer::HttpError("unexpected result from file open",
                                         EOPNOTSUPP);
      }

      delete mFile;
      mFile = 0;
      return response;
    }
  } else {
    // check for chunked uploads
    if (!mCurrentCallbackOffset && request->GetHeaders().count("oc-chunked")) {
      int chunk_n = 0;
      int chunk_max = 0;
      XrdOucString chunk_uuid;

      if ((!request->GetHeaders().count("cbox-chunked-android-issue-900")) &&
          (!eos::common::OwnCloud::getContentSize(request))) {
        // -------------------------------------------------------
        // WARNING:
        // there is buggy ANDROID client not providing this header
        // but we let it pass if a special cbox header allows a
        // bypass
        // -------------------------------------------------------
        mErrCode = eos::common::HttpResponse::BAD_REQUEST;
        mErrText = "Missing total length in OC request";
        response = HttpServer::HttpError(mErrText.c_str(), mErrCode);
        delete mFile;
        mFile = 0;
        return response;
      }

      eos::common::OwnCloud::GetChunkInfo(request->GetQuery().c_str(),
                                          chunk_n, chunk_max, chunk_uuid);

      if (chunk_n >= chunk_max) {
        // there is something inconsistent here
        // HTTP write error
        mErrCode = eos::common::HttpResponse::BAD_REQUEST;
        mErrText = "Illegal chunks specified in OC request";
        response = HttpServer::HttpError(mErrText.c_str(), mErrCode);
        delete mFile;
        mFile = 0;
        return response;
      }

      unsigned long long contentlength =
        eos::common::StringConversion::GetSizeFromString(
          request->GetHeaders()["content-length"]);

      // recompute offset where to write
      if ((chunk_n + 1) < chunk_max) {
        // the first n-1 chunks have a straight forward offset
        mCurrentCallbackOffset = contentlength * chunk_n;
        eos_static_debug("setting to false %lld", mCurrentCallbackOffset);
        mLastChunk = false;
      } else {
        // -------------------------------------------------------
        // WARNING:
        // there is buggy ANDROID client not providing this header
        // in this case we have to assume 1MB chunks
        // -------------------------------------------------------
        // the last chunks has to be written at offset=total-length - chunk-length
        if (eos::common::StringConversion::GetSizeFromString(
              eos::common::OwnCloud::getContentSize(request))) {
          mCurrentCallbackOffset =
            eos::common::StringConversion::GetSizeFromString(
              eos::common::OwnCloud::getContentSize(request));
          mCurrentCallbackOffset -= contentlength;
        } else {
          mCurrentCallbackOffset = (chunk_n * (1 * 1024 * 1000)); // ANDROID client
        }

        eos_static_debug("setting to true %lld", mCurrentCallbackOffset);
        mLastChunk = true;
      }
    }

    // check for content range PUT
    if (mOffsetMap.size() == 1) {
      auto it = mOffsetMap.begin();

      // there is a range header
      if (mUploadLeftSize == mContentLength) {
        // place the offset to the initial range
        mCurrentCallbackOffset = it->first;
      }

      if (!mUploadLeftSize &&
          ((off_t) std::stoul(request->GetHeaders()["x-upload-totalsize"]) ==
           mCurrentCallbackOffset)) {
        mLastChunk = true;
      }

      if (!mUploadLeftSize) {
        if (request->GetHeaders()["x-upload-done"] == "true") {
          mLastChunk = true;
        }

        if (request->GetHeaders()["x-upload-done"] == "false") {
          mLastChunk = false;
        }
      }

      eos_static_debug("c-offset=%lu body-size=%lu ranget-offset=%lu range-size=%lu last-chunk=%d",
                       mCurrentCallbackOffset, *request->GetBodySize(), it->first, it->second,
                       mLastChunk);
    }

    // File streaming in
    size_t* bodySize = request->GetBodySize();

    if (request->GetBody().c_str() && bodySize && (*bodySize)) {
      size_t stored = mFile->write(mCurrentCallbackOffset,
                                   request->GetBody().c_str(),
                                   *request->GetBodySize());

      if (stored != *request->GetBodySize()) {
        eos_static_err("stored %lu of %lu bytes", stored, *request->GetBodySize());
        // HTTP write error
        mErrCode = eos::common::HttpResponse::INTERNAL_SERVER_ERROR;
        mErrText = "Write error occured";
        response = HttpServer::HttpError(mErrText.c_str(), mErrCode);
        delete mFile;
        mFile = 0;
        return response;
      } else {
        eos_static_info("msg=\"stored requested bytes\" offset=%lld",
                        mCurrentCallbackOffset);
        // decrease the upload left data size
        mUploadLeftSize -= *request->GetBodySize();
        mCurrentCallbackOffset += *request->GetBodySize();
        response = new eos::common::PlainHttpResponse();
        std::string query = request->GetQuery();
        XrdOucEnv queryenv(query.c_str());
        const char* etag = 0;

        if ((etag = queryenv.Get("mgm.etag"))) {
          response->AddHeader("ETag", etag);
        }

        response->SetResponseCode(eos::common::HttpResponse::CREATED);
        return response;
      }
    } else {
      eos_static_info("entering close handler");
      eos::common::HttpRequest::HeaderMap header = request->GetHeaders();

      if (header.count("x-upload-mtime")) {
        header["x-oc-mtime"] = header["x-upload-mtime"];
      }

      if (mOffsetMap.size()) {
        header["oc-chunked"] = "true";
      }

      if (header.count("x-oc-mtime")) {
        // there is an X-OC-Mtime header to force the mtime for that file
        mFile->SetForcedMtime(strtoull(header["x-oc-mtime"].c_str(), 0, 10), 0);
      }

      if ((!mLastChunk) && (header.count("oc-chunked"))) {
        // WARNING: this assumes that the last chunk is the last uploaded
        std::string cmd = "nochecksum";

        if (mFile->fctl(SFS_FCTL_SPEC1, cmd.length(), cmd.c_str(), 0)) {
          mErrCode = eos::common::HttpResponse::INTERNAL_SERVER_ERROR;
          mErrText = "Failed to disable checksum";
          response = HttpServer::HttpError(mErrText.c_str(), mErrCode);
          delete mFile;
          mFile = 0;
          return response;
        }
      } else {
        if (mFile->GetChecksum()) {
          // retrieve a checksum when file is still open
          eos_static_debug("enabled checksum lastchunk=%d checksum=%x", mLastChunk,
                           mFile->GetChecksum());
          // Call explicitly the checksum verification
          mFile->VerifyChecksum();

          if (mFile->GetChecksum()) {
            std::string checksum_name = mFile->GetChecksum()->GetName();
            std::string checksum_val = mFile->GetChecksum()->GetHexChecksum();

            while (checksum_val[0] == '0') {
              checksum_val.erase(0, 1);
            }

            eos::common::OwnCloud::checksum_t checksum = std::make_pair(
                  checksum_name,
                  checksum_val);
            // inspect if there is checksum provided
            eos::common::OwnCloud::checksum_t client_checksum =
              eos::common::OwnCloud::GetChecksum(request,
                                                 header.count("x-upload-checksum") ? "x-upload-checksum" : "oc-checksum");
            eos_static_debug("client-checksum-type=%s client-checksum-value=%s "
                             "server-checksum-type=%s server-checksum-value=%s",
                             client_checksum.first.c_str(),
                             client_checksum.second.c_str(),
                             checksum.first.c_str(),
                             checksum.second.c_str());

            if (client_checksum.first != "") {
              if (client_checksum.first == checksum.first) {
                // compare only if the algorithm is the same
                if (client_checksum.second != checksum.second) {
                  eos_static_err("msg=\"invalid checksum\" client-checksum-type=%s"
                                 " client-checksum-value=%s server-checksum-type=%s"
                                 " server-checksum-value=%s",
                                 client_checksum.first.c_str(),
                                 client_checksum.second.c_str(),
                                 checksum.first.c_str(), checksum.second.c_str());
                  checksumError = true;
                }

                checksumMatch = true;
              } else {
                eos_static_warning("msg=\"client required different checksum\" "
                                   "client-checksum-type=%s client-checksum-value=%s "
                                   "server-checksum-type=%s server-checksum-value=%s",
                                   client_checksum.first.c_str(),
                                   client_checksum.second.c_str(),
                                   checksum.first.c_str(),
                                   checksum.second.c_str());
              }
            }
          }
        }
      }

      if (checksumError) {
        response = new eos::common::PlainHttpResponse();
        response->SetResponseCode(eos::common::HttpResponse::PRECONDITION_FAILED);
        delete mFile;
        mFile = 0;
        return response;
      }

      // currently put would never be eligible for caching: but in case it was,
      // here we could not cache, as we continue to use mFile and in addition
      // once cached the file could be used but another thread.
      FileClose(CanCache::NO);

      if (mCloseCode) {
        mErrCode = eos::common::HttpResponse::INTERNAL_SERVER_ERROR;
        mErrText = "File close failed";
        response = HttpServer::HttpError(mErrText.c_str(), mErrCode);
        delete mFile;
        mFile = 0;
        return response;
      } else {
        response = new eos::common::PlainHttpResponse();

        // Add the etag only if we are not an intermediary chunk upload,
        // otherwise the cernbox client interprets it as the end of the
        // transfers and gets an error.
        if (header.count("x-oc-mtime") == 0) {
          response->AddHeader("ETag", mFile->GetETag());
        }

        if (header.count("x-oc-mtime") &&
            (mLastChunk || (!header.count("oc-chunked")))) {
          // only normal uploads or the last chunk receive these extra response headers
          response->AddHeader("ETag", mFile->GetETag());

          if (!mOffsetMap.size()) {
            response->AddHeader("X-OC-Mtime", "accepted");
            // return the OC-FileId header
            std::string ocid;
            eos::common::StringConversion::GetSizeString(ocid,
                eos::common::FileId::FidToInode(mFileId));
            response->AddHeader("OC-FileId", ocid);

            if (checksumMatch && request->GetHeaders().count("oc-checksum")) {
              response->AddHeader("OC-Checksum", request->GetHeaders()["oc-checksum"]);
            }
          } else {
            // PUT with range
            time_t mtime = mFile->GetMtime();
            response->AddHeader("Last-Modified", eos::common::Timing::utctime(mtime));
            std::string inode;
            eos::common::StringConversion::GetSizeString(inode,
                eos::common::FileId::FidToInode(mFileId));
            response->AddHeader("x-eos-inode", inode);

            if (checksumMatch && request->GetHeaders().count("x-upload-checksum")) {
              response->AddHeader("x-eos-checksum",
                                  request->GetHeaders()["x-upload-checksum"]);
            }
          }
        }

        response->SetResponseCode(eos::common::HttpResponse::CREATED);
        return response;
      }
    }
  }

  // Should never get here
  mErrCode = eos::common::HttpResponse::INTERNAL_SERVER_ERROR;
  mErrText = "Internal Server Error";
  response = HttpServer::HttpError(mErrText.c_str(), mErrCode);
  return response;
}

/*----------------------------------------------------------------------------*/
bool
HttpHandler::DecodeByteRange(std::string rangeheader,
                             std::map<off_t, ssize_t>& offsetmap,
                             off_t& requestsize,
                             off_t filesize)
{
  std::vector<std::string> tokens;

  if (rangeheader.substr(0, 6) != "bytes=") {
    // this is an illegal header
    return false;
  } else {
    rangeheader.erase(0, 6);
  }

  eos::common::StringConversion::Tokenize(rangeheader, tokens, ",");

  // decode the string parts
  for (size_t i = 0; i < tokens.size(); i++) {
    eos_static_debug("decoding %s", tokens[i].c_str());
    off_t start = 0;
    off_t stop = 0;
    off_t length = 0;
    size_t mpos = tokens[i].find("-");

    if (mpos == std::string::npos) {
      // there must always be a '-'
      return false;
    }

    std::string sstop = tokens[i];
    std::string sstart = tokens[i];
    sstart.erase(mpos);
    sstop.erase(0, mpos + 1);

    if (sstart.length()) {
      start = strtoull(sstart.c_str(), 0, 10);
    } else {
      start = 0;
    }

    if (sstop.length()) {
      stop = strtoull(sstop.c_str(), 0, 10);
    } else {
      if (filesize > 0) {
        stop = filesize - 1;
      } else {
        stop = 0;
      }
    }

    if (!sstart.length()) {
      // case '-X' = the last X bytes
      start = filesize - stop;
      stop = filesize - 1;
    }

    if ((start > filesize) || (stop > filesize)) {
      return false;
    }

    if (stop >= start) {
      length = (stop - start) + 1;
    } else {
      continue;
    }

    if (offsetmap.count(start)) {
      if (offsetmap[start] < length) {
        // a previous block has been replaced with a longer one
        offsetmap[start] = length;
      }
    } else {
      offsetmap[start] = length;
    }
  }

  // now merge overlapping requests
  bool merged = true;

  while (merged) {
    requestsize = 0;

    if (offsetmap.begin() == offsetmap.end()) {
      // if there is nothing in the map just return with error
      eos_static_err("msg=\"range map is empty\"");
      return false;
    }

    for (auto it = offsetmap.begin(); it != offsetmap.end(); it++) {
      eos_static_debug("offsetmap %llu:%llu", it->first, it->second);
      auto next = it;
      next++;

      if (next != offsetmap.end()) {
        // check if we have two overlapping requests
        if ((it->first + it->second) >= (next->first)) {
          merged = true;
          // merge this two
          it->second = next->first + next->second - it->first;
          offsetmap.erase(next);
          break;
        } else {
          merged = false;
        }
      } else {
        merged = false;
      }

      // compute the total size
      requestsize += it->second;
    }
  }

  return true;
}

/*----------------------------------------------------------------------------*/
void
HttpHandler::FileClose(enum HttpHandler::CanCache cache)
{
  if (mFile && cache == CanCache::YES && mFileCacheEntry.getfp() == mFile) {
    if (sFileCache.insert(mFileCacheEntry)) {
      eos_static_debug("path=%s saved in open-file cache fp=%p",
                       mFileCacheEntry.key_.url_.c_str(), mFile);
      // must not refer to mFile again as it could already be
      // in use by another thread
      mFile = nullptr;
      mCloseCode = 0;
    }
  }
  mFileCacheEntry.clear();
  if (mFile) {
    mCloseCode = mFile->close();
  }
  return;
}

/*----------------------------------------------------------------------------*/
EOSFSTNAMESPACE_END
