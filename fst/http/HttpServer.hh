//------------------------------------------------------------------------------
// File: HttpServer.hh
// Author: Andreas-Joachim Peters & Justin Lewis Salmon - CERN
//------------------------------------------------------------------------------

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


#pragma once
#include "fst/Namespace.hh"
#include "common/Logging.hh"
#include "common/http/HttpServer.hh"
#include "common/http/ProtocolHandler.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class HttpServer
//------------------------------------------------------------------------------
class HttpServer : public eos::common::HttpServer
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  HttpServer(int port = 8001) :
    eos::common::HttpServer::HttpServer(port)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~HttpServer()
  {
    eos_static_info("%s", "msg=\"FST HttpServer destructor\"");
    mThreadId.join();
  }

  //----------------------------------------------------------------------------
  //! HTTP object handler function on FST called by XrdHttp
  //!
  //! @return see implementation
  //----------------------------------------------------------------------------
  virtual std::unique_ptr<eos::common::ProtocolHandler>
  XrdHttpHandler(std::string& method,
                 std::string& uri,
                 std::map<std::string, std::string>& headers,
                 std::string& query,
                 std::map<std::string, std::string>& cookies,
                 std::string& body,
                 const XrdSecEntity& client);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  virtual ssize_t
  FileReader(eos::common::ProtocolHandler* handler, uint64_t pos, char* buf,
             size_t max);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  virtual ssize_t
  FileWriter(eos::common::ProtocolHandler* handler,
             std::string& method,
             std::string& uri,
             std::map<std::string, std::string>& headers,
             std::string& query,
             std::map<std::string, std::string>& cookies,
             std::string& body);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  virtual ssize_t
  FileClose(eos::common::ProtocolHandler* handler, int rc, bool eskip);
};

EOSFSTNAMESPACE_END
