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

#ifndef __EOSMGM_HTTP__HH__
#define __EOSMGM_HTTP__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "mgm/S3Store.hh"
#include "common/Http.hh"

/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class Http : public eos::common::Http
{
  // -------------------------------------------------------------
  // ! creates an Http redirector instance running on the MGM
  // -------------------------------------------------------------
private:
  S3Store* mS3Store;

public:

  /**
   * Constructor
   */
  Http (int port = 8000);

  /**
   * Destructor
   */
  virtual ~Http ();

#ifdef EOS_MICRO_HTTPD
  /**
   * http object handler function on MGM
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
};

EOSMGMNAMESPACE_END

#endif
