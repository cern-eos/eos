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
 * @brief  Creates an Http redirector instance running on the MGM
 */

#ifndef __EOSMGM_HTTPSERVER__HH__
#define __EOSMGM_HTTPSERVER__HH__

#include "mgm/Namespace.hh"
#include "common/http/HttpServer.hh"
#include "common/http/ProtocolHandler.hh"
#include "common/Mapping.hh"
#include "mgm/http/rest-api/handler/tape/TapeRestHandler.hh"
#include <XrdHttp/XrdHttpExtHandler.hh>
#include <map>
#include <string>

//! Forward declaration
class XrdAccAuthorize;

EOSMGMNAMESPACE_BEGIN

class HttpServer : public eos::common::HttpServer
{
public:
  /**
   * Constructor
   */
  HttpServer(int port = 8000) :
    eos::common::HttpServer(port), mGridMapFileLastModTime{0} {}

  /**
   * Destructor
   */
  virtual ~HttpServer()
  {
    eos_static_info("%s", "msg=\"MGM HttpServer destructor\"");
    mThreadId.join();
  }

#ifdef EOS_MICRO_HTTPD
  /**
   * HTTP object handler function on MGM
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
          void**                 ptr);

  /**
   * HTTP complete handler function on MGM
   *
   * @return see implementation
   */

  virtual void
  CompleteHandler(void*                              cls,
                  struct MHD_Connection*             connection,
                  void**                             con_cls,
                  enum MHD_RequestTerminationCode    toe);



#endif

  /**
   * Authenticate the client request by inspecting the SSL headers which were
   * transmitted by the reverse proxy server and attempting to map the client
   * DN to the gridmap file.
   *
   * If the client is not in the gridmap file, (s)he will be mapped to the
   * user "nobody" and have limited access.
   *
   * @param headers  the map of client request headers
   *
   * @return an appropriately filled virtual identity
   */
  eos::common::VirtualIdentity*
  Authenticate(std::map<std::string, std::string>& headers);

  //----------------------------------------------------------------------------
  //! HTTP object handler function on MGM called by XrdHttp
  //!
  //! @param method HTTP verb
  //! @param uri HTTP URI
  //! @param headers map fo the headers
  //! @param cookie cookies
  //! @param body HTTP body
  //! @param client XrdSecEntity for current request
  //! @param authz_obj authorization library
  //! @param err_msg error message in case of failure
  //!
  //! @return protocol handler or null in case of failure
  //----------------------------------------------------------------------------
  virtual std::unique_ptr<eos::common::ProtocolHandler>
  XrdHttpHandler(std::string& method,
                 std::string& uri,
                 std::map<std::string, std::string>& headers,
                 std::map<std::string, std::string>& cookies,
                 std::string& body,
                 const XrdSecEntity& client,
                 XrdAccAuthorize* authz_obj,
                 std::string& err_msg);

private:
#ifdef IN_TEST_HARNESS
public:
#endif
  std::string     mGridMapFile;            //!< contents of the gridmap file
  struct timespec mGridMapFileLastModTime; //!< last modification time of the
  //!< gridmap file

  //----------------------------------------------------------------------------
  //! Handle clientDN specified using RFC2253 (and RFC4514) where the
  //! separator is "," instead of the usual "/" and also the order of the DNs
  //! is reversed
  //!
  //! @param cnd input clientDN
  //!
  //! @return clientDN formatted according to the legacy standard
  //----------------------------------------------------------------------------
  std::string ProcessClientDN(const std::string& cnd) const;

  //----------------------------------------------------------------------------
  //! Build path and opaque information based on the HTTP headers
  //!
  //! @param normalized_headers HTTP headers
  //! @param path canonical path of the HTTP request
  //! @param env_opaque opaque information stored in XrdOucEnv object
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  static bool
  BuildPathAndEnvOpaque(const std::map<std::string, std::string>&
                        normalized_headers, std::string& path,
                        std::unique_ptr<XrdOucEnv>& env_opaque);
};

EOSMGMNAMESPACE_END

#endif
