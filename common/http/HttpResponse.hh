// ----------------------------------------------------------------------
// File: HttpResponse.hh
// Author: Justin Lewis Salmon - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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
 * @file   HttpResponse.hh
 *
 * @brief  Holds all information related to a pure HTTP server response,
 *         such as status code, response headers and response body.
 */

#ifndef __EOSCOMMON_HTTP_RESPONSE__HH__
#define __EOSCOMMON_HTTP_RESPONSE__HH__

/*----------------------------------------------------------------------------*/
#include "common/http/HttpRequest.hh"
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <map>
#include <string>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class HttpResponse
{

public:
  /**
   * Standard HTTP response codes which we use
   */
  enum ResponseCodes
  {
    // Informational 1xx
    CONTINUE                        = 100,

    // Successful 2xx
    OK                              = 200,
    CREATED                         = 201,
    NO_CONTENT                      = 204,
    PARTIAL_CONTENT                 = 206,
    MULTI_STATUS                    = 207,

    // Redirection 3xx
    NOT_MODIFIED                    = 304,
    TEMPORARY_REDIRECT              = 307,

    // Client Error 4xx
    BAD_REQUEST                     = 400,
    UNAUTHORIZED                    = 401,
    FORBIDDEN                       = 403,
    NOT_FOUND                       = 404,
    METHOD_NOT_ALLOWED              = 405,
    CONFLICT                        = 409,
    PRECONDITION_FAILED             = 412,
    UNSUPPORTED_MEDIA_TYPE          = 415,
    REQUESTED_RANGE_NOT_SATISFIABLE = 416,

    // Server Error 5xx
    INTERNAL_SERVER_ERROR           = 500,
    NOT_IMPLEMENTED                 = 501,
    BAD_GATEWAY                     = 502,
    SERVICE_UNAVAILABLE             = 503,
    INSUFFICIENT_STORAGE            = 507,
  };

public:
  typedef std::map<std::string, std::string> HeaderMap;

protected:
  HeaderMap    mResponseHeaders;       //!< the response headers to be filled
  std::string  mResponseBody;          //!< the response body to be created
  int          mResponseCode;          //!< the response code to be determined

public:
  off_t        mResponseLength;        //!< length of the response
  bool         mUseFileReaderCallback; //!< read the file using callbacks

public:

  /**
   * Constructor
   */
  HttpResponse () :
    mResponseCode(OK), mResponseLength(0), mUseFileReaderCallback(false) {};

  /**
   * Destructor
   */
  virtual ~HttpResponse () {};

  /**
   * Build an appropriate response to the given request. This will be
   * implemented by the various protocol response types (WebDAV, S3)
   *
   * @param request  the client request object
   *
   * @return the newly built response object
   */
  virtual HttpResponse*
  BuildResponse (eos::common::HttpRequest *request) = 0;

  /**
   * @return the map of server response headers
   */
  inline HeaderMap&
  GetHeaders () { return mResponseHeaders; }

  /**
   * Set all server response headers at once
   *
   * @param headers  the server response headers to set
   */
  inline void
  SetHeaders (HeaderMap headers) { mResponseHeaders = headers; }

  /**
   * Add a header into the server response header map.
   *
   * @param key    the header key, e.g. Content-Type
   * @param value  the header value, e.g. "text/plain"
   */
  void
  AddHeader (const std::string key, const std::string value);

  /**
   * @return the server response body
   */
  inline const std::string&
  GetBody () { return mResponseBody; }

  /**
   * Set the server response body.
   *
   * @param body  the server response body to be set
   */
  inline void
  SetBody (std::string body) { mResponseBody = body; };

  /**
   * @return the size of the current response body
   */
  inline size_t
  GetBodySize () { return mResponseBody.length(); }

  /**
   * @return the server response code
   */
  inline int
  GetResponseCode () { return mResponseCode; }

  /**
   * Set the server response code
   *
   * @param responseCode  the new response code to be set
   */
  inline void
  SetResponseCode (int responseCode) { mResponseCode = responseCode; };

  /**
   * Deduce an appropriate MIME type for the given path, based on the file
   * extension. Note: the default MIME type is "text/plain".
   *
   * @param path  the filename to get a content type for
   *
   * @return the MIME type string, e.g. "application/xml"
   */
  static std::string
  ContentType (const std::string &path);

  /**
   * @return nicely formatted response, ready for printing
   */
  std::string
  ToString ();

};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif /* __EOSCOMMON_HTTP_RESPONSE__HH__ */
