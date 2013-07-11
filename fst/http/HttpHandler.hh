// ----------------------------------------------------------------------
// File: HttpHandler.hh
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

/**
 * @file   HttpHandler.hh
 *
 * @brief  TODO
 */

#ifndef __EOSFST_HTTP_HANDLER__HH__
#define __EOSFST_HTTP_HANDLER__HH__

/*----------------------------------------------------------------------------*/
#include "common/http/HttpHandler.hh"
#include "common/http/HttpResponse.hh"
#include "fst/Namespace.hh"
#include "fst/XrdFstOfs.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <string>
#include <map>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
// Class keeping a handle to an HTTP file/range request
/*----------------------------------------------------------------------------*/

class HttpHandler : public eos::common::HttpHandler
{
public:
//  std::string mQuery; //< query CGI string
//  std::map<std::string, std::string> mHeader; //< header map
//  std::map<std::string, std::string> mCookies; //< cookie map
  int mRc; //< return code of a file open
//  struct MHD_Connection* mConnection; //< HTTP connection
  //eos::common::S3* mS3; //< s3 object ptr if one has been decoded from the headers
  XrdSecEntity mClient; //< the sec entity of the connected client
//  std::string mPath; //< the path used in the request
  XrdFstOfsFile* mFile; //< handle to a file
  std::map<off_t, ssize_t> mOffsetMap; //< map with offset+length of range requests
  std::map<int, std::string> mMultipartHeaderMap; //< multipart header map
  off_t mRangeRequestSize; //< sum of all range requests
  off_t mFileSize; //< total file size
  off_t mRequestSize; //< size of the total output including headers
  off_t mContentLength; //< size of the content provided by client
  off_t mLastUploadSize; //< size of the last upload call
  off_t mUploadLeftSize; //< size of data still to upload

  bool mRangeDecodingError; //< indicating an invalid range request
  bool mRangeRequest; //< indication if httpHandle has a range rqeuest
  std::string mBoundary; //< boundary "EOSMULTIPARBOUNDARY"
  std::string mBoundaryEnd; //< end boundary "--EOSMULTIPARTBOUNDARY--"
  std::string mMultipartHeader; //< multipart Content tag
  std::string mSinglepartHeader; //< singlepart range used if there is only one entry in mOffsetMap;
  size_t mCurrentCallbackOffsetIndex; //< current index to use in the callback
  off_t mCurrentCallbackOffset; //< next offset from where to read in the offset map at position index
  bool mBoundaryEndSent; //< true when the boundary end was sent
  std::string mPrint; //< print buffer to print the handle contents
  int mCloseCode; //< close code to return if file upload was successfull
  unsigned long long mFileId; //< file id used in EOS - determined after Ofs::Open
  std::string mLogId; //< log id used in EOS - determined after Ofs::Open

//  MHD_ContentReaderCallback     mFileReaderCallback; //!< file reader callback
//  MHD_ContentReaderFreeCallback mFileCloseCallback;  //!< file cleanup callback
//  size_t                        mCallbackBlockSize;  //!< callback block size
//  void                         *mCallbackExtra;      //!< extra callback arg

  /**
   * Constructor
   */
  HttpHandler () //: mFileReaderCallback(0), mFileCloseCallback(0), mCallbackBlockSize(32 * 1024),
      //mCallbackExtra(0)
  {
    mFile = 0;
    mRangeRequestSize = 0;
    mRangeDecodingError = 0;
    mRangeRequest = false;
    mRequestSize = 0;
    mFileSize = 0;
    mFileId = 0;
//    mConnection = 0;
    mBoundaryEnd = "\n--EOSMULTIPARTBOUNDARY--\n";
    mBoundary = "--EOSMULTIPARTBOUNDARY\n";
    mMultipartHeader = "multipart/byteranges; boundary=EOSMULTIPARTBOUNDARY";
    mCurrentCallbackOffsetIndex = 0;
    mCurrentCallbackOffset = 0;
    mBoundaryEndSent = false;
    mSinglepartHeader = "";
    mCloseCode = 0;
//    mS3 = 0;
    mRc = 0;
    mContentLength = 0;
    mLastUploadSize = 0;
    mUploadLeftSize = 0;
  }

  /**
   * Check whether the given method and headers are a match for this protocol.
   *
   * @param method  the request verb used by the client (GET, PUT, etc)
   * @param headers the map of request headers
   *
   * @return true if the protocol matches, false otherwise
   */
  static bool
  Matches (const std::string &method, HeaderMap &headers);

  /**
   * Build a response to the given HTTP request.
   *
   * @param request  the map of request headers sent by the client
   * @param method   the request verb used by the client (GET, PUT, etc)
   * @param url      the URL requested by the client
   * @param query    the GET request query string (if any)
   * @param body     the request body data sent by the client
   * @param bodysize the size of the request body
   * @param cookies  the map of cookie headers
   */
  void
  HandleRequest (eos::common::HttpRequest *request);

  /**
   * print an http handle
   * @return pointer to http printout stinrg
   */
  const char*
  Print ()
  {
    char line[4096];
    snprintf(line, sizeof (line) - 1, "range-request=%llu range-request-size=%llu request-size=%llu file-size=%llu",
             (unsigned long long) mRangeRequest,
             (unsigned long long) mRangeRequestSize,
             (unsigned long long) mRequestSize,
             (unsigned long long) mFileSize);
    mPrint = line;
    return mPrint.c_str();
  }

  /**
   * Destructor
   */
  virtual
  ~HttpHandler ()
  {
    if (mFile)
    {
      delete mFile;
      mFile = 0;
    }
  }

  /**
   * Create the map of multipart headres for each offset/length pair
   * @param contenttype content type to put into the multipart header
   */
  void
  CreateMultipartHeader (std::string contenttype)
  {
    mRequestSize = mRangeRequestSize;
    if (mOffsetMap.size() != 1)
    {
      mRequestSize += mBoundaryEnd.length();
    }
    size_t index = 0;
    for (auto it = mOffsetMap.begin(); it != mOffsetMap.end(); it++)
    {
      std::string header = "\n--EOSMULTIPARTBOUNDARY\nContent-Type: ";
      header += contenttype;
      header += "\nContent-Range: ";
      char srange[256];
      snprintf(srange,
               sizeof (srange) - 1,
               "%llu-%llu/%llu",
               (unsigned long long) it->first,
               (unsigned long long) ((it->second) ? (it->first + it->second) : mRangeRequestSize),
               (unsigned long long) mFileSize
               );
      if (mOffsetMap.size() == 1)
      {
        mSinglepartHeader = srange;
      }

      header += srange;
      header += "\n\n";
      mMultipartHeaderMap[index] = header;
      if (mOffsetMap.size() != 1)
      {
        mRequestSize += mMultipartHeaderMap[index].length();
      }
      index++;
    }
  }

  /**
   * Decode the range header tag and create canonical merged map with
   * offset/len
   *
   * @param rangeheader  the range header tag
   * @param offsetmap    canonical map with offset/length by reference
   * @param requestsize  sum of non overlapping bytes to serve
   * @param filesize     size of file
   *
   * @return true if valid request, otherwise false
   */
  bool
  DecodeByteRange (std::string               rangeheader,
                   std::map<off_t, ssize_t> &offsetmap,
                   ssize_t                  &requestsize,
                   off_t                     filesize);

  /**
   * initialize an HttpHandle using the connection information
   * @return nothing
   */

  void
  Initialize (eos::common::HttpRequest *request);

  /**
   * @deprecated
   *
   * handle a get request
   * @return like Http::Handler method
   */
  int Get ();

  /**
   * Handle an HTTP GET request.
   *
   * @param request  the client request object
   *
   * @return an HTTP response object
   */
  eos::common::HttpResponse*
  Get (eos::common::HttpRequest *request);


  /**
   * @deprecated
   *
   * handle a put request
   * @param upload_data pointer to the data to upload
   * @param reference to the size of data to upload
   * @return like Http::Handler method
   */
  int Put (const char *upload_data,
           size_t *upload_data_size, bool first_call);

  /**
   * Handle an HTTP PUT request.
   *
   * @param request  the client request object
   *
   * @return an HTTP response object
   */
  eos::common::HttpResponse*
  Put (eos::common::HttpRequest *request);

};

/*----------------------------------------------------------------------------*/
EOSFSTNAMESPACE_END

#endif
