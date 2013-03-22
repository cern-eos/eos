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
#include "fst/XrdFstOfs.hh"
#include "common/Logging.hh"
#include "common/S3.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSfs/XrdSfsInterface.hh"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN


/*----------------------------------------------------------------------------*/
#define EOSFST_HTTP_PAGE "<html><head><title>No such file or directory</title></head><body>No such file or directory</body></html>"

/*----------------------------------------------------------------------------*/
/**
 * Class keeping a handle to an HTTP file/range request
 */
/*----------------------------------------------------------------------------*/
class HttpHandle
{
public:
  XrdFstOfsFile* mFile; //< handle to a file
  std::map<off_t, ssize_t> mOffsetMap; //< map with offset+length of range requests
  std::map<int, std::string> mMultipartHeaderMap; //< multipart header map
  off_t mRangeRequestSize; //< sum of all range requests
  off_t mFileSize; //< total file size
  off_t mRequestSize; //< size of the total output including headers
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
    httpHandle->mFile->close();
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
  static int aptr;
  struct MHD_Response *response;

  std::string query;
  std::map<std::string, std::string> header;
  std::map<std::string, std::string> cookies;

  // currently support only GET methods
  if (0 != strcmp(method, MHD_HTTP_METHOD_GET))
    return MHD_NO; /* unexpected method */

  if (&aptr != *ptr)
  {
    /* do never respond on first call */
    *ptr = &aptr;
    return MHD_YES;
  }

  MHD_get_connection_values(connection, MHD_GET_ARGUMENT_KIND, &Http::BuildQueryString,
                            (void*) &query);

  // get the header INFO
  MHD_get_connection_values(connection, MHD_HEADER_KIND, &Http::BuildHeaderMap,
                            (void*) &header);

  MHD_get_connection_values(connection, MHD_COOKIE_KIND, &Http::BuildHeaderMap,
                            (void*) &cookies);

  *ptr = NULL; /* reset when done */

  for (auto it = header.begin(); it != header.end(); it++)
  {
    eos_static_info("header:%s=%s", it->first.c_str(), it->second.c_str());
  }

  for (auto it = cookies.begin(); it != cookies.end(); it++)
  {
    eos_static_info("cookie:%s=%s", it->first.c_str(), it->second.c_str());
  }

  if (cookies.count("EOSCAPABILITY"))
  {
    // if we have a capability we don't use the query CGI but that one
    query = cookies["EOSCAPABILITY"];

  }

  DecodeURI(query); // unescape '+' '/' '='

  std::string path = url;

  eos_static_info("path=%s query=%s", url ? url : "", path.c_str() ? query.c_str() : "");

  // if there is a capability COOKIE, we add it to the query string


  XrdOucString spath = path.c_str();

  XrdSecEntity client("unix");

  client.name = strdup("nobody");
  client.host = strdup("localhost");
  client.tident = strdup("http");

  std::string result;

  int mhd_response = MHD_HTTP_OK;

  std::map<std::string, std::string> responseheader;

  XrdFstOfsFile* file = 0;

  HttpHandle* httpHandle = new HttpHandle();

  eos::common::S3* s3 = eos::common::S3::ParseS3(header);

  // only assume FILE requests
  file = (XrdFstOfsFile*) gOFS.newFile(client.name);

  // store the file handle
  httpHandle->mFile = file;

  int rc = file->open(path.c_str(), 0, 0, &client, query.c_str());

  httpHandle->mFileSize = file->getOpenSize();

  // check for range requests
  if (header.count("Range"))
  {
    if (!DecodeByteRange(header["Range"], httpHandle->mOffsetMap, httpHandle->mRangeRequestSize, httpHandle->mFileSize))
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
    httpHandle->mRangeRequestSize = file->getOpenSize();
  }

  if (s3)
  {
    //...........................................................................
    // S3 requests
    //...........................................................................

    if (httpHandle->mRangeDecodingError)
    {
      result = s3->RestErrorResponse(mhd_response, 416, "InvalidRange", "Illegal Range request", header["Range"].c_str(), "");
    }
    else
    {
      if (rc != SFS_OK)
      {
        if (file->error.getErrInfo() == ENOENT)
        {
          result = s3->RestErrorResponse(mhd_response, 404, "NoSuchKey", "The specified key does not exist", s3->getPath(), "");
        }
        else
          if (file->error.getErrInfo() == EPERM)
        {
          result = s3->RestErrorResponse(mhd_response, 403, "AccessDenied", "Access Denied", s3->getPath(), "");
        }
        else
        {
          result = s3->RestErrorResponse(mhd_response, 500, "InternalError", "File currently unavailable", s3->getPath(), "");
        }
        delete file;
        file = 0;
      }
      else
      {
        if (httpHandle->mRangeRequest)
        {
          httpHandle->CreateMultipartHeader(s3->ContentType());
          eos_static_info(httpHandle->Print());
          char clength[16];
          snprintf(clength, sizeof (clength) - 1, "%llu", (unsigned long long) httpHandle->mRequestSize);
          if (httpHandle->mOffsetMap.size() == 1)
          {
            // if there is only one range we don't send a multipart response
            responseheader["Content-Type"] = s3->ContentType();
            responseheader["Content-Range"] = httpHandle->mSinglepartHeader;
          }
          else
          {
            // for several ranges we send a multipart response
            responseheader["Content-Type"] = httpHandle->mMultipartHeader;
          }
          responseheader["Content-Length"] = clength;
          mhd_response = MHD_HTTP_PARTIAL_CONTENT;
        }
        else
        {
          // successful http open
          char clength[16];
          snprintf(clength, sizeof (clength) - 1, "%llu", (unsigned long long) file->getOpenSize());
          httpHandle->mRequestSize = file->getOpenSize();
          responseheader["Content-Type"] = s3->ContentType();
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
    if (httpHandle->mRangeDecodingError)
    {
      result = HttpError(mhd_response, responseheader, "Illegal Range request", MHD_HTTP_REQUESTED_RANGE_NOT_SATISFIABLE);
    }
    else
    {
      if (rc != SFS_OK)
      {
        if (rc == SFS_REDIRECT)
        {
          result = HttpRedirect(mhd_response, responseheader, file->error.getErrText(), file->error.getErrInfo(), path, query, true);
        }
        else
          if (rc == SFS_ERROR)
        {
          result = HttpError(mhd_response, responseheader, file->error.getErrText(), file->error.getErrInfo());
        }
        else
          if (rc == SFS_DATA)
        {
          result = HttpData(mhd_response, responseheader, file->error.getErrText(), file->error.getErrInfo());
        }
        else
          if (rc == SFS_STALL)
        {
          result = HttpStall(mhd_response, responseheader, file->error.getErrText(), file->error.getErrInfo());
        }
        else
        {
          result = HttpError(mhd_response, responseheader, "unexpected result from file open", EOPNOTSUPP);
        }
        delete file;
        file = 0;
      }
      else
      {
        if (httpHandle->mRangeRequest)
        {
          httpHandle->CreateMultipartHeader("application/octet-stream");
          eos_static_info(httpHandle->Print());
          char clength[16];
          snprintf(clength, sizeof (clength) - 1, "%llu", (unsigned long long) httpHandle->mRequestSize);
          if (httpHandle->mOffsetMap.size() == 1)
          {
            // if there is only one range we don't send a multipart response
            responseheader["Content-Type"] = "application/octet-stream";
            responseheader["Content-Range"] = httpHandle->mSinglepartHeader;
          }
          else
          {
            // for several ranges we send a multipart response
            responseheader["Content-Type"] = httpHandle->mMultipartHeader;
          }
          responseheader["Content-Length"] = clength;
          mhd_response = MHD_HTTP_PARTIAL_CONTENT;
        }
        else
        {
          // successful http open
          char clength[16];
          snprintf(clength, sizeof (clength) - 1, "%llu", (unsigned long long) file->getOpenSize());
          httpHandle->mRequestSize = file->getOpenSize();
          responseheader["Content-Type"] = "application/octet-stream";
          responseheader["Content-Length"] = clength;
          mhd_response = MHD_HTTP_OK;
        }
      }
    }
  }

  if (file)
  {
    response = MHD_create_response_from_callback(httpHandle->mRequestSize, 32 * 1024, /* 32k page size */
                                                 &Http::FileReaderCallback,
                                                 (void*) httpHandle,
                                                 &Http::FileCloseCallback);
  }
  else
  {
    response = MHD_create_response_from_buffer(result.length(),
                                               (void *) result.c_str(),
                                               MHD_RESPMEM_MUST_COPY);
  }

  if (response)
  {
    for (auto it = responseheader.begin(); it != responseheader.end(); it++)
    {
      // add all the response header tags
      MHD_add_response_header(response, it->first.c_str(), it->second.c_str());
    }
    int ret = MHD_queue_response(connection, mhd_response, response);

    return ret;
  }
  else
  {
    eos_static_alert("msg=\"response creation failed\"");
    return 0;
  }
}

#endif

EOSFSTNAMESPACE_END
