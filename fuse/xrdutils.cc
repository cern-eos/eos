//------------------------------------------------------------------------------
//! @file xrdutils.cc
//! @author Geoffray Adde
//! @brief some auxilliary XRootd functions
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
#include "xrdutils.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include "common/Logging.hh"
#include "common/XrdErrorMap.hh"
#include "fuse/filesystem.hh"
//! Sometimes, XRootd gives a NULL responses on some calls, this is a bug.
//! When it happens we retry.
int xrootd_nullresponsebug_retrycount = 3;
//! Sometimes, XRootd gives a NULL responses on some calls, this is a bug.
//! When it happens we sleep between attempts.
int xrootd_nullresponsebug_retrysleep = 1;

//------------------------------------------------------------------------------
// Issue an Xrootd request.
// If a null response or a null buffer is got, try up to
// xrootd_nullresponsebug_retrycount times waiting
// xrootd_nullresponsebug_retrysleep ms between the tries
///------------------------------------------------------------------------------
XrdCl::XRootDStatus xrdreq_retryonnullbuf(XrdCl::FileSystem& fs,
    XrdCl::Buffer& arg,
    XrdCl::Buffer*& response)
{
  XrdCl::XRootDStatus status;

  for (int retrycount = 0; retrycount < xrootd_nullresponsebug_retrycount;
       retrycount++) {
    status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg, response);

    if (status.IsOK()) {
      // We get a well-formatted response
      if (response && response->GetBuffer()) {
        if (retrycount) {
          eos_static_warning("%d retries were needed to get a non null response to %s",
                             retrycount, arg.GetBuffer());
        }

        break;
      } else { // we get a wrongly formatted response
        if (retrycount + 1 < xrootd_nullresponsebug_retrycount) {
          if (response) {
            // We'll retry so delete the previous reponse if any not to leak memory
            delete response;
            response = 0;
          }

          XrdSysTimer sleeper;

          if (xrootd_nullresponsebug_retrysleep) {
            sleeper.Wait(xrootd_nullresponsebug_retrysleep);
          }

          continue;
        } else {
          eos_static_err("no non null response received to %s after %d attempts",
                         arg.GetBuffer(), retrycount + 1);
        }
      }
    } else {
      eos_static_err("status is NOT ok : %s", status.ToString().c_str());
    }

    errno = (status.code == XrdCl::errAuthFailed) ? EPERM : EFAULT;

    if (status.code == XrdCl::errErrorResponse) {
      eos::common::error_retc_map(status.errNo);
      eos_static_debug("setting errno to %d", errno);
    }

    break;
  }

  return status;
}
