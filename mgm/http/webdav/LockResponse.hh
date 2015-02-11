// ----------------------------------------------------------------------
// File: LockResponse.hh
// Author: Andreas-Joachim Peters - CERN
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
 * @file   LockResponse.hh
 *
 * @brief  Class responsible for parsing a WebDAV LOCK request and
 *         building a dummy response.
 */

#ifndef __EOSMGM_LOCK_RESPONSE__HH__
#define __EOSMGM_LOCK_RESPONSE__HH__


/*----------------------------------------------------------------------------*/
#include "mgm/http/webdav/WebDAVResponse.hh"
#include "mgm/Namespace.hh"
#include "common/Mapping.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
// rapidjason header file violates ordered prototype delcaration
#if __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 6)
// =============================================================================
#pragma GCC diagnostic push
#pragma GCC diagnostic warning "-fpermissive"
#include "mgm/http/rapidxml/rapidxml.hpp"
#include "mgm/http/rapidxml/rapidxml_print.hpp"
#pragma GCC diagnostic pop
// =============================================================================
#else
// =============================================================================
#pragma GCC diagnostic warning "-fpermissive"
#include "mgm/http/rapidxml/rapidxml.hpp"
#include "mgm/http/rapidxml/rapidxml_print.hpp"
// =============================================================================
#endif

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN;


class LockResponse : public WebDAVResponse {
public:

protected:
  eos::common::Mapping::VirtualIdentity *mVirtualIdentity; //!< virtual identity for this client

public:

  /**
   * Constructor
   *
   * @param request  the client request object
   */
  LockResponse (eos::common::HttpRequest *request,
                    eos::common::Mapping::VirtualIdentity *vid) :
  WebDAVResponse (request),
  mVirtualIdentity (vid)
  {
  };

  /**
   * Destructor
   */
  virtual ~LockResponse ()
  {
  };


  /**
   * Build an appropriate response to the given LOCK request.
   *
   * @param request  the client request object
   *
   * @return the newly built response object
   */
  HttpResponse*
  BuildResponse (eos::common::HttpRequest *request);
};

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END

#endif /* __EOSMGM_LOCK_RESPONSE__HH__ */
