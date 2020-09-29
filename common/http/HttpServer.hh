// ----------------------------------------------------------------------
// File: HttpServer.hh
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
 * @file   HttpServer.hh
 *
 * @brief  Class running an HTTP daemon. Creates an embedded HTTP server
 *         instance
 */

#pragma once
#include "common/http/HttpRequest.hh"
#include "common/http/HttpResponse.hh"
#include "common/AssistedThread.hh"
#include "common/Logging.hh"
#include "common/Namespace.hh"

#ifdef EOS_MICRO_HTTPD
#include <microhttpd.h>
#endif

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class HttpServer
//------------------------------------------------------------------------------
class HttpServer
{
public:
  //! Instance of the HTTP server allowing the Handler function to call
  //!<class member functions
  static HttpServer* gHttp;

  /**
   * Constructor
   */
  HttpServer(int port = 8000);

  /**
   * Destructor
   */
  virtual ~HttpServer()
  {
    eos_static_info("%s", "msg=\"Common HttpServer destructor\"");
    mThreadId.join();
  }

  /**
   * Return port number of service
   */
  int Port()
  {
    return mPort;
  }

  /**
   * Start the listening HTTP server
   *
   * @return true if server running otherwise false
   */
  virtual bool Start();

  /**
   * Create the embedded server and run
   */
  void Run(ThreadAssistant& assistant) noexcept;

  /**
   * Get an HTTP redirect response object.
   *
   * @param url      the url to redirect to
   * @param hostCGI  the string returned by the open() call that told us to
   *                 redirect. Contains hostname?redirect_query
   * @param port     the port number to redirect to
   * @param cookie   true if we should use cookies, false for CGI
   *
   * @return an HTTP response object
   */
  static HttpResponse*
  HttpRedirect(const std::string& url,
               const std::string& hostCGI,
               int                port,
               bool               cookie);

  /**
   * Get an HTTP error response object containing an HTML error page inside
   * the body.
   *
   * @param errorText  the error message to be displayed on the error page
   * @param errorCode  the derived HTTP error code
   *
   * @return an HTTP response object
   */
  static HttpResponse*
  HttpError(const char* errorText, int errorCode);

  /**
   * Get an HTTP HEAD response object with an empty
   * body.
   *
   * @param length  the size of the data page
   * @param name basename to add to header
   *
   * @return an HTTP response object
   */
  static HttpResponse*
  HttpHead(off_t length, std::string name);


  /**
   * Get an HTTP data response object containing an HTML data page inside the
   * body.
   *
   * @param data    the HTML data page string
   * @param length  the size of the data page
   *
   * @return an HTTP response object
   */
  static HttpResponse*
  HttpData(const char* data, int length);

  /**
   * Get an HTTP stall response object.
   *
   * @param stallText  the stall text to display
   * @param seconds    number of seconds to stall for
   *
   * @return an HTTP response object
   */
  static HttpResponse*
  HttpStall(const char* stallText, int seconds);

  /**
   * Encode the provided CGI string, escaping '/' '+' '=' characters
   *
   * @param cgi  the CGI string to encode
   */
  static void
  EncodeURI(std::string& cgi);

  /**
   * Deocde the provided CGI string, unesacping '/' '+' '=' characters
   *
   * @param cgi  the CGI string to decode
   */
  static void
  DecodeURI(std::string& cgi);

#ifdef EOS_MICRO_HTTPD

#if MHD_VERSION >= 0x00097002
#define MHD_RESULT enum MHD_Result
#else
#define MHD_RESULT int
#endif

  /**
   * Compatibility hacks for MHD versions
   */
  static MHD_RESULT convertToMHD_RESULT(int code)
  {
#if MHD_VERSION >= 0x00097002

    if (code == 1) {
      return MHD_YES;
    }

    return MHD_NO;
#else
    return code;
#endif
  }

  /**
   * Calls the instance handler function of the Http object
   *
   * @return see implementation
   */
  static MHD_RESULT
  StaticHandler(void*                  cls,
                struct MHD_Connection* connection,
                const char*            url,
                const char*            method,
                const char*            version,
                const char*            upload_data,
                size_t*                upload_data_size,
                void**                 ptr);

  /**
   * Calls the instance handler function of the Http object
   *
   * @return nothing
   */
  static void
  StaticCompleteHandler(void*                  cls,
                        struct MHD_Connection* connection,
                        void** con_cls,
                        enum MHD_RequestTerminationCode toe);

  /**
   * HTTP object handler function
   *
   * @return see implementation
   */
  virtual int
  Handler(void*                  cls,
          struct MHD_Connection* connection,
          const char*            url,
          const char*            method,
          const char*            version,
          const char*            upload_data,
          size_t*                upload_data_size,
          void**                 ptr) = 0;

  /**
   * HTTP complete handler function
   *
   * @return nothing
   */
  virtual void
  CompleteHandler(void*                  cls,
                  struct MHD_Connection* connection,
                  void**                 con_cls,
                  enum MHD_RequestTerminationCode toe) = 0;

  /**
   * Returns the query string for an HTTP request
   *
   * @param cls    in-out address of a std::string containing the query string
   * @param kind   the request type
   * @param key    the query parameter key
   * @param value  the query parameter value
   *
   * @return MHD_YES
   */
  static MHD_RESULT
  BuildQueryString(void*              cls,
                   enum MHD_ValueKind kind,
                   const char*        key,
                   const char*        value);

  /**
   * Returns the header map for an HTTP request
   *
   * @param cls    in-out address of a std::map<std::string,std::string>
   *               containing the query string
   * @param kind   the request type
   * @param key    the query parameter key
   * @param value  the query parameter value
   *
   * @return MHD_YES
   */
  static MHD_RESULT
  BuildHeaderMap(void*              cls,
                 enum MHD_ValueKind kind,
                 const char*        key,
                 const char*        value);

  /**
   * Cleans closed connections earlier than MHD_run
   */

  void
  CleanupConnections();

#endif

protected:
#ifdef EOS_MICRO_HTTPD
  struct MHD_Daemon* mDaemon {
    nullptr
  };   //!< MicroHttpd daemon instance
#endif
  int                mPort;     //!< The port this server listens on
  std::atomic<bool>  mRunning;  //!< Is this server running?
  AssistedThread     mThreadId; //!< This thread's ID
};

EOSCOMMONNAMESPACE_END
