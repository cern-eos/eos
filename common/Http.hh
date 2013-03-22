// ----------------------------------------------------------------------
// File: Http.hh
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

#ifndef __EOSCOMMON_HTTP__HH__
#define __EOSCOMMON_HTTP__HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <map>
/*----------------------------------------------------------------------------*/
#ifdef EOS_MICRO_HTTPD
#include <microhttpd.h>
#endif

/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class Http
{
  // -------------------------------------------------------------
  // ! creates an embedded http server instance
  // -------------------------------------------------------------
protected:
#ifdef EOS_MICRO_HTTPD
  struct MHD_Daemon *mDaemon;
#endif
  int mPort;
  pthread_t mThreadId;
  bool mRunning;

#ifdef EOS_MICRO_HTTPD
  /**
   * Returns the query string for an HTTP request 
   * @param cls in-out address of a std::string containing the query string
   * @param kind request type
   * @param key ...
   * @param value ...
   * @return ...
   */
  static int BuildQueryString (void *cls,
                               enum MHD_ValueKind kind,
                               const char *key,
                               const char *value);

  /**
   * Returns the header map for an HTTP request 
   * @param cls in-out address of a std::map<std::string,std::string> containing the query string
   * @param kind request type
   * @param key ...
   * @param value ...
   * @return ...
   */
  static int BuildHeaderMap (void *cls,
                             enum MHD_ValueKind kind,
                             const char *key,
                             const char *value);
#endif

public:

  /**
   * Constructor
   */
  Http (int port = 8000);

  /**
   * Destructor
   */
  virtual ~Http ();

  /**
   * Start the listening HTTP server
   * @return true if server running otherwise false
   */
  virtual bool Start ();

  /**
   * Start a thread running the embedded server
   * @return void
   */
  static void* StaticHttp (void* arg);

  /**
   * Create the embedded server and run
   */
  void* Run ();


  /**
   * returns redirection HTTP
   * @param return response code
   * @param host_cgi host?redirect_query
   * @param port port number
   * @param path path of the request
   * @param query cgi of the request
   * @param cookie (true use cookies, false use cgi)
   * @return HTTP output page
   */
  static
  std::string HttpRedirect (int& response_code, std::map<std::string, std::string>& response_header, const char* host_cgi, int port, std::string& path, std::string& query, bool cookie = true);

  /**
   * returns error HTTP
   * @param return response code
   * @param errtxt text to display
   * @param errc error code to map
   * @return HTTP error page
   */
  static
  std::string HttpError (int& response_code, std::map<std::string, std::string>& response_header, const char* errtxt, int errc);

  /**
   * returns data
   * @param return response code
   * @param point to data
   * @param length of data
   * @return HTTP data page
   */
  static
  std::string HttpData (int& response_code, std::map<std::string, std::string>& response_header, const char* data, int length);

  /**
   * returns a stall message
   * @param return response code
   * @param stalltxt text to display
   * @param stallsec seconds to stall
   * @return HTTP stall page
   */
  static
  std::string HttpStall (int& response_code, std::map<std::string, std::string>& response_header, const char* stallxt, int stallsec);

  /**
   * encode the provided CGI string escaping '/' '+' '='
   * @param cgi to encode
   */
  static
  void EncodeURI (std::string& cgi);

  /**
   * deocde the provided CGI string unesacping '/' '+' '='
   * @param cgi to decode
   */
  static
  void DecodeURI (std::string& cgi);

  /**
   * decode the range header tag and create canonical merged map with offset/len
   * @param range header
   * @param offsetmap canonical map with offset/length by reference
   * @param requestsize sum of non overlapping bytes to serve
   * @param filesize size of file
   * @return true if valid request, otherwise false
   */
  bool
  DecodeByteRange (std::string rangeheader, std::map<off_t, ssize_t>& offsetmap, ssize_t& requestsize, off_t filesize);

#ifdef EOS_MICRO_HTTPD
  /**
   * calls the instance handler function of the Http object
   * @return 
   */
  static int StaticHandler (void *cls,
                            struct MHD_Connection *connection,
                            const char *url,
                            const char *method,
                            const char *version,
                            const char *upload_data,
                            size_t *upload_data_size, void **ptr);


  /**
   * http object handler function
   * @return see implementation
   */
  virtual int Handler (void *cls,
                       struct MHD_Connection *connection,
                       const char *url,
                       const char *method,
                       const char *version,
                       const char *upload_data,
                       size_t *upload_data_size, void **ptr);

#endif
  static Http* gHttp; //< this is the instance of the http server allowing allowing the Handler function to call class member functions
};

EOSCOMMONNAMESPACE_END

#endif
