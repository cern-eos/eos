// ----------------------------------------------------------------------
// File: HttpServer.hh
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
 * @file   HttpServer.hh
 *
 * @brief  Creates an Http redirector instance running on the MGM
 */

#ifndef __EOSMGM_HTTPSERVER__HH__
#define __EOSMGM_HTTPSERVER__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/http/ProtocolHandler.hh"
#include "mgm/Namespace.hh"
#include "common/HttpServer.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class HttpServer : public eos::common::HttpServer
{

public:

  /**
   * Constructor
   */
  HttpServer (int port = 8000) : eos::common::HttpServer(port) {}

  /**
   * Destructor
   */
  virtual ~HttpServer () {};

#ifdef EOS_MICRO_HTTPD
  /**
   * http object handler function on MGM
   *
   * @return see implementation
   */
  virtual int
  Handler (void                  *cls,
           struct MHD_Connection *connection,
           const char            *url,
           const char            *method,
           const char            *version,
           const char            *upload_data,
           size_t                *upload_data_size,
           void                 **ptr);
#endif
};

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END

#endif
