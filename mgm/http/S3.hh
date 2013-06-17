// ----------------------------------------------------------------------
// File: S3.hh
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

/**
 * @file   S3.hh
 *
 * @brief  dealing with all S3 goodies
 */

#ifndef __EOSMGM_S3__HH__
#define __EOSMGM_S3__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/http/ProtocolHandler.hh"
#include "mgm/http/S3Store.hh"
#include "mgm/Namespace.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <string>
#include <map>
/*----------------------------------------------------------------------------*/

#define XML_V1_UTF8 "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"

EOSMGMNAMESPACE_BEGIN

class S3Store;

class S3 : public eos::mgm::ProtocolHandler
{

protected:
  static S3Store *mS3Store;
  bool            mIsS3;           //< indicates if this is a valid S3 object
  std::string     mId;             //< the S3 id of the client
  std::string     mSignature;      //< the S3 signature of the client
  std::string     mHost;           //< header host
  std::string     mContentMD5;     //< header MD5
  std::string     mContentType;    //< header content type
  std::string     mUserAgent;      //< header user agent
  std::string     mHttpMethod;     //< http method
  std::string     mPath;           //< http path
  std::string     mQuery;          //< http query
  std::string     mSubResource;    //< S3 sub resource
  HeaderMap       mSubResourceMap; //< map with S3 subresource key/vals
  std::string     mBucket;         //< http bucket
  std::string     mDate;           //< http date
  HeaderMap       mAmzMap;         //< canonical amz map
  std::string     mCanonicalizedAmzHeaders; //< canonical resource build from
                                            //< canonical amz map
  bool            mVirtualHost;    //< true if bucket name comes via virtual
                                   //< host, otherwise false (relevant for
                                   //< signature verification)

public:

  /**
   * Constructor
   */
  S3 ();

  /**
   * Destructor
   */
  virtual ~S3 ();

  /**
   *
   */
  static bool
  Matches(std::string &method, HeaderMap &headers);

  /**
   *
   */
  virtual void
  ParseHeader(HeaderMap &headers);

  /**
   *
   */
  virtual std::string
  HandleRequest(HeaderMap request, HeaderMap response, int error);

  /**
   * getter for S3 id
   * @return s3 id
   */
  std::string
  getId () const
  {
    return mId;
  }

  /**
   * getter for S3 signature
   * @return s3 signature
   */
  std::string
  getSignature () const
  {
    return mSignature;
  }

  std::string
  getHost () const
  {
    return mHost;
  }

  std::string
  getContentMD5 () const
  {
    return mContentMD5;
  }

  std::string
  getContentType () const
  {
    return mContentType;
  }

  std::string
  getUserAgent () const
  {
    return mUserAgent;
  }

  std::string
  getHttpMethod () const
  {
    return mHttpMethod;
  }

  std::string
  getPath () const
  {
    return mPath;
  }

  std::string
  getQuery () const
  {
    return mQuery;
  }

  std::string
  getSubResource () const
  {
    return mSubResource;
  }

  std::string
  extractSubResource();

  std::string
  getBucket () const
  {
    return mBucket;
  }

  std::string
  getDate () const
  {
    return mDate;
  }

  std::string
  getCanonicalizedAmzHeaders () const
  {
    return mCanonicalizedAmzHeaders;
  }

  /**
   * Check if the current S3 object is containing all the relevant S3 tags
   * @return true if S3 headers have been provided otherwise false
   */
  bool IsS3 ();

  /**
   * Verify the AWS signature
   * @param secure_key secret AWS key
   * @return true if S3 signature ok
   */
  bool VerifySignature (std::string secure_key);

  /**
   * Print the current S3 object
   */
  void Print (std::string &out);

//  /**
//   * Factory function parsing header map
//   * @param key-value string map containing HTTP header
//   * @return S3 object
//   */
//  static S3* ParseS3 (std::map<std::string, std::string>&);

  /**
   * return rest error response string
   * @param reponse_code set to http_code or error_code
   * @param http_code to put for the response
   * @param errcode as string
   * @param errmsg as string
   * @param resource as string
   * @param requestid as string
   * @return rest error response string
   */
  static std::string
  RestErrorResponse (int &response_code, int http_code, std::string errcode, std::string errmsg, std::string resource, std::string requestid);

  /**
   * return content type for an s3 request object
   * @return content-type string
   */
  std::string ContentType ();

  /**
   * @param hostname from where to extract subdomain
   * @return subdomain
   */
  std::string SubDomain (std::string hostname);
};

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END

#endif

