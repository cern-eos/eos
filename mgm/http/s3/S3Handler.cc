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
#include "mgm/http/s3/S3Handler.hh"
#include "mgm/XrdMgmOfs.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <string>
#include <map>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

S3Store *S3Handler::mS3Store = 0;

/*----------------------------------------------------------------------------*/
S3Handler::S3Handler (eos::common::Mapping::VirtualIdentity *vid) :
  eos::common::ProtocolHandler(vid)
{
  mIsS3 = false;
  mId = mSignature = mHost = mContentMD5 = mContentType = mUserAgent = "";
  mHttpMethod = mPath = mQuery = mBucket = mDate = "";
  mVirtualHost = false;

  if (!mS3Store)
  {
    // create the store if it does not exist yet
    mS3Store = new S3Store(gOFS->MgmProcPath.c_str());
  }
}

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

  // Parse the headers
  ParseHeader(request);

  eos::common::HttpResponse *response = 0;
  std::string result;
  mS3Store->Refresh();

  // REMOVE THESE!!
  int responseCode = 0;
  std::map<std::string, std::string> responseHeaders;
  responseHeaders["Query"] = request->GetQuery();


  if (!mS3Store->VerifySignature(*this))
  {
    response = RestErrorResponse(response->FORBIDDEN,
                               "SignatureDoesNotMatch", "", getBucket(), "");
  }
  else
  {
    response = new eos::common::S3Response();

    if (request->GetMethod() == "GET")
    {

      if (getBucket() == "")
      {
        // GET SERVICE REQUEST
        result = mS3Store->ListBuckets(responseCode, *this, responseHeaders);
      }
      else
      {
        if (getPath() == "/")
        {
          // GET BUCKET LISTING REQUEST
          result = mS3Store->ListBucket(responseCode, *this, responseHeaders);
        }
        else
        {
          // GET OBJECT REQUEST
          result = mS3Store->GetObject(responseCode, *this, responseHeaders);
        }
      }
    }
    else
    {
      if (request->GetMethod() == "HEAD")
      {
        if (getPath() == "/")
        {
          // HEAD BUCKET REQUEST
          result = mS3Store->HeadBucket(responseCode, *this, responseHeaders);
        }
        else
        {
          // HEAD OBJECT REQUEST
          result = mS3Store->HeadObject(responseCode, *this, responseHeaders);
        }
      }
      else
      {
        // PUT REQUEST ...
        result = mS3Store->PutObject(responseCode, *this, responseHeaders);
      }
    }

    if (!responseHeaders.empty())
      response->SetHeaders(responseHeaders);
  }

  // TODO: remove this
  if (responseCode)
    response->SetResponseCode(responseCode);
  if (result.size())
    response->SetBody(result);
  mHttpResponse = response;
}

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END
