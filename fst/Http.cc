// ----------------------------------------------------------------------
// File: Http.cc
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
#include "fst/Http.hh"
#include "common/Logging.hh"
#include "fst/XrdFstOfs.hh"
#include "common/S3.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSfs/XrdSfsInterface.hh"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN


/*----------------------------------------------------------------------------*/
#define EOSFST_HTTP_PAGE "<html><head><title>No such file or directory</title></head><body>No such file or directory</body></html>"

/*----------------------------------------------------------------------------*/
// Class keeping a handle to an HTTP file/range request                      
/*----------------------------------------------------------------------------*/

class HttpHandle
{
public:
  std::string mQuery; //< query CGI string
  std::map<std::string, std::string> mHeader; //< header map
  std::map<std::string, std::string> mCookies; //< cookie map
  int mRc; //< return code of a file open
  struct MHD_Connection* mConnection; //< HTTP connection
  eos::common::S3* mS3; //< s3 object ptr if one has been decoded from the headers
  XrdSecEntity mClient; //< the sec entity of the connected client
  std::string mPath; //< the path used in the request
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

  /**
   * Constructor
   */
  HttpHandle ()
  {
    mFile = 0;
    mRangeRequestSize = 0;
    mRangeDecodingError = 0;
    mRangeRequest = false;
    mRequestSize = 0;
    mFileSize = 0;
    mBoundaryEnd = "\n--EOSMULTIPARTBOUNDARY--\n";
    mBoundary = "--EOSMULTIPARTBOUNDARY\n";
    mMultipartHeader = "multipart/byteranges; boundary=EOSMULTIPARTBOUNDARY";
    mCurrentCallbackOffsetIndex = 0;
    mCurrentCallbackOffset = 0;
    mBoundaryEndSent = false;
    mSinglepartHeader = "";
    mCloseCode = 0;
    mS3 = 0;
    mRc = 0;
    mContentLength = 0;
    mLastUploadSize = 0;
    mUploadLeftSize = 0;
  }

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
  ~HttpHandle ()
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
   * initialize an HttpHandle using the connection information
   * @return nothing
   */

  void Initialize ();

  /**
   * handle a get request
   * @return like Http::Handler method
   */
  int Get ();

  /**
   * handle a put request
   * @param upload_data pointer to the data to upload
   * @param reference to the size of data to upload
   * @return like Http::Handler method
   */
  int Put (const char *upload_data,
           size_t *upload_data_size, bool first_call);

};

/*----------------------------------------------------------------------------*/
Http::Http (int port) : eos::common::Http::Http (port) {
  //.............................................................................
  // Constructor
  //.............................................................................
}

/*----------------------------------------------------------------------------*/
Http::~Http () {
  //.............................................................................
  // Destructor
  //.............................................................................
}

#ifdef EOS_MICRO_HTTPD 

/*----------------------------------------------------------------------------*/
ssize_t
Http::FileReaderCallback (void *cls, uint64_t pos, char *buf, size_t max)
{
  // call back function to read from a file object
  HttpHandle* httpHandle = static_cast<HttpHandle*> (cls);

  eos_static_info("pos=%llu max=%llu current-index=%d current-offset=%llu",
                  (unsigned long long) pos,
                  (unsigned long long) max,
                  httpHandle->mCurrentCallbackOffsetIndex,
                  httpHandle->mCurrentCallbackOffset
                  );

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
      // file streaminig
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
Http::FileCloseCallback (void *cls)
{
  // callback function to close the file object
  HttpHandle* httpHandle = static_cast<HttpHandle*> (cls);
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

/*----------------------------------------------------------------------------*/
int
Http::Handler (void *cls,
               struct MHD_Connection *connection,
               const char *url,
               const char *method,
               const char *version,
               const char *upload_data,
               size_t *upload_data_size, void **ptr)
{
  // The handler function is called in a 'stateless' fashion, so to keep state
  // the implementation stores a HttpHandle object using **ptr.
  // libmicrohttpd moreover deals with 100-continue responses used by PUT/POST
  // in the upper protocol level, so the handler has to return for GET requests
  // just MHD_YES if there is not yet an HttpHandle and for PUT requests
  // should only create a response object if the open for the PUT failes for 
  // whatever reason.
  // So when the HTTP header have arrived Handler is called the first time
  // and in following Handler calls we should not decode the headers again and
  // again for performance reasons. So there is a different handling of GET and
  // PUT because in GET we just don't do anything but return and decode 
  // the HTTP headers with the second call, while for PUT we do it in the first
  // call and open the output file immedeatly to return evt. an error.

  bool first_call;

  std::string lMethod = method ? method : "";

  HttpHandle* httpHandle = 0;

  // currently support only GET methods
  if ((lMethod != "GET") &&
      (lMethod != "PUT"))
    return MHD_NO; /* unexpected method */

  if (! *ptr)
  {
    first_call = true;
  }
  else
  {
    first_call = false;

  }
  if (first_call)
  {
    if (lMethod == "GET")
    {
      /* do never respond on first call for GET */
      *ptr = (void*) 0x1;
      eos_static_debug("rc=MHD_YES firstcall=true");
      return MHD_YES;
    }
    eos_static_debug("continue firstcall=true");
  }

  if (*ptr == (void*) 0x1)
  {
    // reset the head/get second call indicator
    *ptr = 0;
  }

  // now get an existing or create an HttpHandle for this session
  if (!*ptr)
  {
    // create a handle and run the open;
    httpHandle = new HttpHandle();
    *ptr = (void*) httpHandle;
    httpHandle->mConnection = connection;
    if (url) httpHandle->mPath = url;
  }
  else
  {
    // get the previous handle back
    httpHandle = (HttpHandle*) * ptr;
  }

  if (!httpHandle->mFile)
  {
    httpHandle->Initialize();
  }

  if (!httpHandle->mFile)
  {
    httpHandle->mFile = (XrdFstOfsFile*) gOFS.newFile(httpHandle->mClient.name);


    // default modes are for GET=read
    XrdSfsFileOpenMode open_mode = 0;
    mode_t create_mode = 0;

    if (lMethod == "PUT")
    {
      // use the proper creation/open flags for PUT's
      open_mode |= SFS_O_CREAT;
      open_mode |= SFS_O_TRUNC;
      open_mode |= SFS_O_RDWR;
      open_mode |= SFS_O_MKPTH;
      create_mode |= (SFS_O_MKPTH | S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    }

    httpHandle->mRc = httpHandle->mFile->open(httpHandle->mPath.c_str(), open_mode, create_mode, &httpHandle->mClient, httpHandle->mQuery.c_str());

    httpHandle->mFileSize = httpHandle->mFile->getOpenSize();

    httpHandle->mFileId = httpHandle->mFile->getFileId();
    httpHandle->mLogId = httpHandle->mFile->logId;

    // check for range requests
    if (httpHandle->mHeader.count("Range"))
    {
      if (!DecodeByteRange(httpHandle->mHeader["Range"], httpHandle->mOffsetMap, httpHandle->mRangeRequestSize, httpHandle->mFileSize))
      {
        // indicate range decoding error
        httpHandle->mRangeDecodingError = true;
      }
      else
      {
        httpHandle->mRangeRequest = true;
      }
    }

    if (!httpHandle->mRangeRequest)
    {
      // we put the file size as request size if this is not a range request aka full file download
      httpHandle->mRangeRequestSize = httpHandle->mFile->getOpenSize();
    }
  }

  if (lMethod == "GET")
  {
    // call the HttpHandle::Get method
    return httpHandle->Get();
  }

  if (lMethod == "PUT")
  {
    // call the HttpHandle::Put method
    int rc = httpHandle->Put(upload_data, upload_data_size, first_call);
    if (rc || ( (!first_call) && (upload_data_size == 0 ) ) ) {
      // clean-up left-over objects on error or end-of-put
      if (httpHandle->mFile) {
        delete httpHandle->mFile;
        httpHandle->mFile = 0;
      }
      if (httpHandle->mS3) {
        delete httpHandle->mS3;
        httpHandle->mS3 = 0;
      }
    }
  }

  eos_static_alert("invalid program path - should never reach this point!");
  return MHD_NO;
}

void
HttpHandle::Initialize ()
{
  // decode all the header/cookie stuff

  MHD_get_connection_values(mConnection, MHD_GET_ARGUMENT_KIND, &Http::BuildQueryString,
                            (void*) &mQuery);

  // get the header INFO
  MHD_get_connection_values(mConnection, MHD_HEADER_KIND, &Http::BuildHeaderMap,
                            (void*) &mHeader);

  MHD_get_connection_values(mConnection, MHD_COOKIE_KIND, &Http::BuildHeaderMap,
                            (void*) &mCookies);

  for (auto it = mHeader.begin(); it != mHeader.end(); it++)
  {
    eos_static_info("header:%s=%s", it->first.c_str(), it->second.c_str());
  }

  for (auto it = mCookies.begin(); it != mCookies.end(); it++)
  {
    eos_static_info("cookie:%s=%s", it->first.c_str(), it->second.c_str());
  }

  if (mCookies.count("EOSCAPABILITY"))
  {
    // if we have a capability we don't use the query CGI but that one
    mQuery = mCookies["EOSCAPABILITY"];

  }

  if (mHeader.count("Content-Length"))
  {
    mContentLength = strtoull(mHeader["Content-Length"].c_str(), 0, 10);
    mUploadLeftSize = mContentLength;
  }

  Http::DecodeURI(mQuery); // unescape '+' '/' '='


  eos_static_info("path=%s query=%s", mPath.c_str(), mQuery.c_str());


  // define the client sec entity object
  strncpy(mClient.prot, "unix", XrdSecPROTOIDSIZE - 1);
  mClient.prot[XrdSecPROTOIDSIZE - 1] = '\0';
  mClient.name = strdup("nobody");
  mClient.host = strdup("localhost");
  mClient.tident = strdup("http");

  mS3 = eos::common::S3::ParseS3(mHeader);
}

/*----------------------------------------------------------------------------*/
int
HttpHandle::Get ()
{
  int mhd_response = MHD_HTTP_OK;
  std::string result;
  std::map<std::string, std::string> responseheader;
  struct MHD_Response *response = 0;

  if (mS3)
  {
    //...........................................................................
    // S3 requests
    //...........................................................................

    if (mRangeDecodingError)
    {
      result = mS3->RestErrorResponse(mhd_response, 416, "InvalidRange", "Illegal Range request", mHeader["Range"].c_str(), "");
    }
    else
    {
      if (mRc != SFS_OK)
      {
        if (mFile->error.getErrInfo() == ENOENT)
        {
          result = mS3->RestErrorResponse(mhd_response, 404, "NoSuchKey", "The specified key does not exist", mS3->getPath(), "");
        }
        else
          if (mFile->error.getErrInfo() == EPERM)
        {
          result = mS3->RestErrorResponse(mhd_response, 403, "AccessDenied", "Access Denied", mS3->getPath(), "");
        }
        else
        {
          result = mS3->RestErrorResponse(mhd_response, 500, "InternalError", "File currently unavailable", mS3->getPath(), "");
        }
        delete mFile;
        mFile = 0;
        delete mS3;
      }
      else
      {
        if (mRangeRequest)
        {
          CreateMultipartHeader(mS3->ContentType());
          eos_static_info(Print());
          char clength[16];
          snprintf(clength, sizeof (clength) - 1, "%llu", (unsigned long long) mRequestSize);
          if (mOffsetMap.size() == 1)
          {
            // if there is only one range we don't send a multipart response
            responseheader["Content-Type"] = mS3->ContentType();
            responseheader["Content-Range"] = mSinglepartHeader;
          }
          else
          {
            // for several ranges we send a multipart response
            responseheader["Content-Type"] = mMultipartHeader;
          }
          responseheader["Content-Length"] = clength;
          mhd_response = MHD_HTTP_PARTIAL_CONTENT;
        }
        else
        {
          // successful http open
          char clength[16];
          snprintf(clength, sizeof (clength) - 1, "%llu", (unsigned long long) mFile->getOpenSize());
          mRequestSize = mFile->getOpenSize();
          responseheader["Content-Type"] = mS3->ContentType();
          responseheader["Content-Length"] = clength;
          mhd_response = MHD_HTTP_OK;
        }
      }
    }
  }
  else
  {
    //...........................................................................
    // HTTP requests
    //...........................................................................
    if (mRangeDecodingError)
    {
      result = Http::HttpError(mhd_response, responseheader, "Illegal Range request", MHD_HTTP_REQUESTED_RANGE_NOT_SATISFIABLE);
    }
    else
    {
      if (mRc != SFS_OK)
      {
        if (mRc == SFS_REDIRECT)
        {
          result = Http::HttpRedirect(mhd_response, responseheader, mFile->error.getErrText(), mFile->error.getErrInfo(), mPath, mQuery, true);
        }
        else
          if (mRc == SFS_ERROR)
        {
          result = Http::HttpError(mhd_response, responseheader, mFile->error.getErrText(), mFile->error.getErrInfo());
        }
        else
          if (mRc == SFS_DATA)
        {
          result = Http::HttpData(mhd_response, responseheader, mFile->error.getErrText(), mFile->error.getErrInfo());
        }
        else
          if (mRc == SFS_STALL)
        {
          result = Http::HttpStall(mhd_response, responseheader, mFile->error.getErrText(), mFile->error.getErrInfo());
        }
        else
        {
          result = Http::HttpError(mhd_response, responseheader, "unexpected result from file open", EOPNOTSUPP);
        }
        delete mFile;
        mFile = 0;
      }
      else
      {
        if (mRangeRequest)
        {
          CreateMultipartHeader("application/octet-stream");
          eos_static_info(Print());
          char clength[16];
          snprintf(clength, sizeof (clength) - 1, "%llu", (unsigned long long) mRequestSize);
          if (mOffsetMap.size() == 1)
          {
            // if there is only one range we don't send a multipart response
            responseheader["Content-Type"] = "application/octet-stream";
            responseheader["Content-Range"] = mSinglepartHeader;
          }
          else
          {
            // for several ranges we send a multipart response
            responseheader["Content-Type"] = mMultipartHeader;
          }
          responseheader["Content-Length"] = clength;
          mhd_response = MHD_HTTP_PARTIAL_CONTENT;
        }
        else
        {
          // successful http open
          char clength[16];
          snprintf(clength, sizeof (clength) - 1, "%llu", (unsigned long long) mFile->getOpenSize());
          mRequestSize = mFile->getOpenSize();
          responseheader["Content-Type"] = "application/octet-stream";
          responseheader["Content-Length"] = clength;
          mhd_response = MHD_HTTP_OK;
        }
      }
    }
  }

  if (mFile)
  {
    // GET method
    response = MHD_create_response_from_callback(mRequestSize, 32 * 1024, /* 32k page size */
                                                 &Http::FileReaderCallback,
                                                 (void*) this,
                                                 &Http::FileCloseCallback);
  }
  else
  {
    result = "";
    response = MHD_create_response_from_buffer(result.length(),
                                               (void *) result.c_str(),
                                               MHD_RESPMEM_MUST_FREE);
  }

  if (response)
  {
    if (mCloseCode)
    {
      // close failed
      result = Http::HttpError(mhd_response, responseheader, "File close failed", MHD_HTTP_SERVICE_UNAVAILABLE);
      response = MHD_create_response_from_buffer(result.length(),
                                                 (void *) result.c_str(),
                                                 MHD_RESPMEM_MUST_COPY);
    }
    else
    {
      for (auto it = responseheader.begin(); it != responseheader.end(); it++)
      {
        // add all the response header tags
        MHD_add_response_header(response, it->first.c_str(), it->second.c_str());
      }
    }
    eos_static_info("mhd_response=%d", mhd_response);
    int ret = MHD_queue_response(mConnection, mhd_response, response);

    return ret;

  }
  else
  {
    eos_static_alert("msg=\"response creation failed\"");
    return 0;
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
int
HttpHandle::Put (const char *upload_data,
                 size_t *upload_data_size,
                 bool first_call)
{
  int mhd_response = MHD_HTTP_OK;
  std::string result;
  std::map<std::string, std::string> responseheader;
  struct MHD_Response *response = 0;

  eos_static_info("method=PUT offset=%llu size=%llu size_ptr=%llu",
                  mCurrentCallbackOffset,
                  upload_data_size ? *upload_data_size : 0, upload_data_size
                  );

  if (mRc)
  {
    // check for open errors
    if (mS3)
    {
      // ---------------------------------------------------------------------
      // create S3 error responses
      // ---------------------------------------------------------------------
      if (mRc != SFS_OK)
      {
        if (mFile->error.getErrInfo() == EPERM)
        {
          result = mS3->RestErrorResponse(mhd_response, 403, "AccessDenied", "Access Denied", mS3->getPath(), "");
        }
        else
        {
          result = mS3->RestErrorResponse(mhd_response, 500, "InternalError", "File currently unwritable", mS3->getPath(), "");
        }
        delete mFile;
        mFile = 0;
        delete mS3;
      }
    }
    else
    {
      // ---------------------------------------------------------------------
      // create HTTP error response
      // ---------------------------------------------------------------------
      if (mRc != SFS_OK)
      {
        if (mRc == SFS_REDIRECT)
        {
          result = Http::HttpRedirect(mhd_response, responseheader, mFile->error.getErrText(), mFile->error.getErrInfo(), mPath, mQuery, true);
        }
        else
          if (mRc == SFS_ERROR)
        {
          result = Http::HttpError(mhd_response, responseheader, mFile->error.getErrText(), mFile->error.getErrInfo());
        }
        else
          if (mRc == SFS_DATA)
        {
          result = Http::HttpData(mhd_response, responseheader, mFile->error.getErrText(), mFile->error.getErrInfo());
        }
        else
          if (mRc == SFS_STALL)
        {
          result = Http::HttpStall(mhd_response, responseheader, mFile->error.getErrText(), mFile->error.getErrInfo());
        }
        else
        {
          result = Http::HttpError(mhd_response, responseheader, "unexpected result from file open", EOPNOTSUPP);
        }
        delete mFile;
        mFile = 0;
      }
    }

    response = MHD_create_response_from_buffer(result.length(),
                                               (void *) result.c_str(),
                                               MHD_RESPMEM_MUST_FREE);
    int ret = MHD_queue_response(mConnection, mhd_response, response);

    return ret;
  }
  else
  {
    //...........................................................................
    // file streaming in 
    //...........................................................................
    if (upload_data && upload_data_size && (*upload_data_size))
    {

      if ((mUploadLeftSize > (1 * 1024 * 1024) && ((*upload_data_size) < (1 * 1024 * 1024))))
      {
        // we want more bytes, we don't process this
        return MHD_YES;
      }

      size_t stored = mFile->write(mCurrentCallbackOffset, upload_data, *upload_data_size);
      if (stored != *upload_data_size)
      {
        if (mS3)
        {
          // S3 write error

        }
        else
        {
          // HTTP write error
          result = Http::HttpError(mhd_response, responseheader, "Write error occured", MHD_HTTP_SERVICE_UNAVAILABLE);
          response = MHD_create_response_from_buffer(result.length(),
                                                     (void *) result.c_str(),
                                                     MHD_RESPMEM_MUST_COPY);
        }
      }
      else
      {
        // decrease the upload left data size
        mUploadLeftSize -= *upload_data_size;
        mCurrentCallbackOffset += *upload_data_size;
        // set to the number of bytes not processed ...
        *upload_data_size = 0;
        // don't queue any response here
        return MHD_YES;
      }
    }
    else
    {
      if (first_call)
      {
        // if the file was opened we just return MHD_YES to allow the upper
        // layer to send 100-CONTINUE and to call us again
        return MHD_YES;
      }
      else
      {
        mCloseCode = mFile->close();
        if (mCloseCode)
        {
          result = Http::HttpError(mhd_response, responseheader, "File close failed", MHD_HTTP_SERVICE_UNAVAILABLE);
          response = MHD_create_response_from_buffer(result.length(),
                                                     (void *) result.c_str(),
                                                     MHD_RESPMEM_MUST_COPY);
          mCloseCode = 0; // we don't want to create a second response down
        }
        else
        {
          result = "";
          response = MHD_create_response_from_buffer(result.length(),
                                                     (void *) result.c_str(),
                                                     MHD_RESPMEM_MUST_FREE);
        }
      }
    }

    if (mS3)
    {
      char sFileId[16];
      snprintf(sFileId, sizeof (sFileId) - 1, "%llu", mFileId);

      // add some S3 specific tags to the response object
      responseheader["x-amz-version-id"] = sFileId;
      responseheader["x-amz-request-id"] = mLogId;
      responseheader["Server"] = gOFS.HostName;
      responseheader["Connection"] = "close";
      responseheader["ETag"] = sFileId;
    }

    for (auto it = responseheader.begin(); it != responseheader.end(); it++)
    {
      // add all the response header tags
      MHD_add_response_header(response, it->first.c_str(), it->second.c_str());
    }

    eos_static_info("mhd_response=%d", mhd_response);
    int ret = MHD_queue_response(mConnection, mhd_response, response);

    return ret;
  }
}

#endif

EOSFSTNAMESPACE_END
