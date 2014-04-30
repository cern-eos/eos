// ----------------------------------------------------------------------
// File: HttpRequest.hh
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
 * @file   HttpRequest.hh
 *
 * @brief  Simple utility class to hold client request parameters.
 */

#ifndef __EOSCOMMON_HTTP_REQUEST__HH__
#define __EOSCOMMON_HTTP_REQUEST__HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <map>
#include <string>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class HttpRequest
{

public:
  typedef std::map<std::string, std::string> HeaderMap;

private:
  HeaderMap         mRequestHeaders;  //!< the map of client request headers
  const std::string mRequestMethod;   //!< the client request method
  const std::string mRequestUrl;      //!< the client request URL
  std::string       mRequestQuery;    //!< the client request query string
  const std::string mRequestBody;     //!< the client request body
  size_t           *mRequestBodySize; //!< the size of the client request body
  HeaderMap         mRequestCookies;  //!< the client request cookie header map

public:

  /**
   * Constructor
   *
   * @param headers  the map of request headers sent by the client
   * @param method   the request verb used by the client (GET, PUT, etc)
   * @param url      the URL requested by the client
   * @param query    the GET request query string (if any)
   * @param body     the request body data sent by the client
   * @param bodysize the size of the request body
   * @param cookies  the map of cookie headers
   */
  HttpRequest (HeaderMap          headers,
               const std::string &method,
               const std::string &url,
               const std::string &query,
               const std::string &uploadData,
               size_t            *uploadDataSize,
               HeaderMap          cookies);

  /**
   * Destructor
   */
  virtual ~HttpRequest () {};

  /**
   * @return the map of request headers
   */
  inline HeaderMap&
  GetHeaders () { return mRequestHeaders; }

  /**
   * @return the client request method
   */
  inline const std::string&
  GetMethod () { return mRequestMethod; }

  /**
   * @return the client request URL
   */
  inline const std::string
  GetUrl (bool orig=false) { 
    std::string ocurl;
    if (orig || (mRequestUrl.find("remote.php/webdav") == std::string::npos))
      return mRequestUrl;
    else 
    {
      // ---------------
      // owncloud patch
      // ---------------
      ocurl = mRequestUrl;
      if (mRequestUrl.find("remote.php/webdav/") != std::string::npos)
	ocurl.erase(mRequestUrl.find("remote.php/webdav/"),18);
      else
	ocurl.erase(mRequestUrl.find("remote.php/webdav"),17);
      return ocurl;
    }
  }
  
  /**
   * @return the client request query string (GET parameters)
   */
  inline const std::string&
  GetQuery () { return mRequestQuery; }

  /**
   * @return the client request body
   */
  inline const std::string&
  GetBody () { return mRequestBody; }

  /**
   * @return the size of the client request body
   */
  inline size_t*
  GetBodySize () { return mRequestBodySize; }

  /**
   * @return the map of client request cookie headers
   */
  inline HeaderMap&
  GetCookies () { return mRequestCookies; }

  /**
   * Change the request query string, useful in the case where a capability
   * cookie should override the request query
   *
   * @param query  the new query string to use
   */
  inline void
  SetQuery (std::string query) { mRequestQuery = query; }

  /**
   * @return nicely formatted request, ready for printing
   */
  std::string
  ToString();
};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif /* __EOSCOMMON_HTTP_REQUEST__HH__ */
