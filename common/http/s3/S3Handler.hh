// ----------------------------------------------------------------------
// File: S3Handler.hh
// Author: Justin Lewis Salmon - CERN
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
 * @file   S3Handler.hh
 *
 * @brief  Dealing with all S3 goodies
 */

#ifndef __EOSCOMMON_S3_HANDLER__HH__
#define __EOSCOMMON_S3_HANDLER__HH__

/*----------------------------------------------------------------------------*/
#include "common/http/ProtocolHandler.hh"
#include "common/http/s3/S3Response.hh"
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <string>
#include <map>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

#define XML_V1_UTF8 "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"

class S3Handler : virtual public eos::common::ProtocolHandler
{

protected:
  bool            mIsS3;           //!< indicates if this is a valid S3Handler object
  std::string     mId;             //!< the S3Handler id of the client
  std::string     mSignature;      //!< the S3Handler signature of the client
  std::string     mHost;           //!< header host
  std::string     mContentMD5;     //!< header MD5
  std::string     mContentType;    //!< header content type
  std::string     mUserAgent;      //!< header user agent
  std::string     mHttpMethod;     //!< http method
  std::string     mPath;           //!< http path
  std::string     mQuery;          //!< http query
  std::string     mSubResource;    //!< S3Handler sub resource
  HeaderMap       mSubResourceMap; //!< map with S3Handler subresource key/vals
  std::string     mBucket;         //!< http bucket
  std::string     mDate;           //!< http date
  HeaderMap       mAmzMap;         //!< canonical amz map
  std::string     mCanonicalizedAmzHeaders; //!< canonical resource built from
                                            //!< canonical amz map
  bool            mVirtualHost;    //!< true if bucket name comes via virtual
                                   //!< host, otherwise false (relevant for
                                   //!< signature verification)

public:

  /**
   * Constructor
   */
  S3Handler () : mIsS3(false), mVirtualHost(false) {};

  /**
   * Destructor
   */
  virtual ~S3Handler () {};

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
   * Build a response to the given S3 request.
   *
   * @param request  the client request object
   */
  void
  HandleRequest (eos::common::HttpRequest *request) = 0;

  /**
   * Analyze the header map, searching for HTTP and Amazon headers
   *
   * @param request  the client request object
   */
  void
  ParseHeader (eos::common::HttpRequest *request);

  /**
   * @return the client S3Handler ID
   */
  inline std::string
  getId () const { return mId; }

  /**
   * @return the client S3Handler signature
   */
  inline std::string
  getSignature () const { return mSignature; }

  /**
   * @return the client hostname
   */
  inline std::string
  getHost () const { return mHost; }

  /**
   * @return the md5 hash of the request content
   */
  inline std::string
  getContentMD5 () const { return mContentMD5; }

  /**
   * @return the request content type
   */
  inline std::string
  getContentType () const { return mContentType; }

  /**
   * @return the request user agent
   */
  inline std::string
  getUserAgent () const { return mUserAgent; }

  /**
   * @return the request method
   */
  inline std::string
  getHttpMethod () const { return mHttpMethod; }

  /**
   * @return the request path
   */
  inline std::string
  getPath () const { return mPath; }

  /**
   * @return the request query string
   */
  inline std::string
  getQuery () const { return mQuery; }

  /**
   * @return
   */
  inline std::string
  getSubResource () const { return mSubResource; }

  /**
   * @return the request sub respurce (used for signatures)
   */
  std::string
  extractSubResource();

  /**
   * @return the requested bucket
   */
  inline std::string
  getBucket () const { return mBucket; }

  /**
   * @return the request date
   */
  inline std::string
  getDate () const { return mDate; }

  /**
   * @return the canonicalized amazon request headers
   */
  inline std::string
  getCanonicalizedAmzHeaders () const { return mCanonicalizedAmzHeaders; }

  /**
   * Check if the current S3 object is containing all the relevant S3 tags
   *
   * @return true if S3 headers have been provided otherwise false
   */
  bool IsS3 ();

  /**
   * Verify the AWS signature
   *
   * @param secure_key  the secret AWS key
   *
   * @return true if S3Handler signature is verified
   */
  bool VerifySignature (std::string secure_key);

  /**
   * Print the current S3 object
   *
   * TODO: change this to ToString()
   */
  void Print (std::string &out);

  /**
   * @deprecated
   *
   * return rest respcode response string
   * @param reponse_code set to http_code or respcode_code
   * @param http_code to put for the response
   * @param errcode as string
   * @param errmsg as string
   * @param resource as string
   * @param requestid as string
   * @return rest error response string
   */
  static std::string
  RestErrorResponse (int        &response_code,
                     int         http_code,
                     std::string errcode,
                     std::string errmsg,
                     std::string resource,
                     std::string requestid)
  {
    response_code = http_code;
    std::string result = XML_V1_UTF8;
    result += "<Error><Code>";
    result += errcode;
    result += "</Code>";
    result += "<Message>";
    result += errmsg;
    result += "</Message>";
    result += "<Resource>";
    result += resource;
    result += "</Resource>";
    result += "<RequestId>";
    result += requestid;
    result += "</RequestId>";
    result += "</Error>";
    return result;
  }

  /**
   * Create an S3 REST error response object
   *
   * @param responseCode  the error response code
   * @param errorCode     error response code as string
   * @param errorMessage  the error message to display
   * @param resource      the requested resource as string
   * @param requestId     the request id as string
   *
   * @return the S3 error response object
   */
  static eos::common::HttpResponse*
  RestErrorResponse (int         responseCode,
                     std::string errorCode,
                     std::string errorMessage,
                     std::string resource,
                     std::string requestId)

  {
    eos::common::HttpResponse *response = new eos::common::S3Response();
    response->SetResponseCode(responseCode);

    std::string result = XML_V1_UTF8;
    result += "<Error><Code>";
    result += errorCode;
    result += "</Code>";
    result += "<Message>";
    result += errorMessage;
    result += "</Message>";
    result += "<Resource>";
    result += resource;
    result += "</Resource>";
    result += "<RequestId>";
    result += requestId;
    result += "</RequestId>";
    result += "</Error>";

    response->SetBody(result);
    return response;
  }

  /**
   * Extract a subdomain name from the given hostname.
   *
   * @param hostname  from where to extract subdomain
   *
   * @return the extracted subdomain
   */
  std::string SubDomain (std::string hostname);
};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif

