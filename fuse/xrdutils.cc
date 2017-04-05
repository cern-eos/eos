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
#include "SyncResponseHandler.hh"
#include "common/Logging.hh"
#include "fuse/filesystem.hh"
int xrootd_nullresponsebug_retrycount=3; ///< sometimes, XRootd gives a NULL responses on some calls, this is a bug. When it happens we retry.
int xrootd_nullresponsebug_retrysleep=1; ///< sometimes, XRootd gives a NULL responses on some calls, this is a bug. When it happens we sleep between attempts.


//------------------------------------------------------------------------------
// Issue an Xrootd request.
// If a null response or a null buffer is got, try up to
// xrootd_nullresponsebug_retrycount times waiting
// xrootd_nullresponsebug_retrysleep ms between the tries
//------------------------------------------------------------------------------
//------------------------------------------------------------------------------

XrdCl::XRootDStatus xrdreq_retryonnullbuf(XrdCl::FileSystem &fs,
                                                      XrdCl::Buffer &arg,
                                                      XrdCl::Buffer *&response)
{
  XrdCl::XRootDStatus status;
  for (int retrycount = 0; retrycount < xrootd_nullresponsebug_retrycount; retrycount++)
  {
    SyncResponseHandler handler;
    fs.Query (XrdCl::QueryCode::OpaqueFile, arg, &handler);
    status = handler.Sync (response);

    if (status.IsOK ())
    {
      if (response && response->GetBuffer ()) // we get a well-formatted response
      {
        if(retrycount) eos_static_warning("%d retries were needed to get a non null response to %s",retrycount,arg.GetBuffer());

	{
	  // WARNING!
	  // be careful not to merge these lines into CITRINE!
	  // we need this because the SyncResponseHandler destroys the response when leaving
	  // the scope!
 	  XrdCl::Buffer *oldbuffer = response;
	  response = new XrdCl::Buffer(oldbuffer->GetSize());
	  memcpy(response->GetBuffer(), oldbuffer->GetBuffer(), oldbuffer->GetSize());
	}
	
        break;
      }
      else // we get a wrongly formatted response
      {
        if (retrycount + 1 < xrootd_nullresponsebug_retrycount)
        {
          if (response)
          { // we are going to retry so delete the previous reponse if any not to leak memory
	    {
	      // WARNING!
	      // be careful to uncomment this line in CITRINE!
	      // delete response;
	    }
            response = 0;
          }
          XrdSysTimer sleeper;
          if (xrootd_nullresponsebug_retrysleep) sleeper.Wait (xrootd_nullresponsebug_retrysleep);

          continue;
        }
        else
          eos_static_err("no non null response received to %s after %d attempts",arg.GetBuffer(), retrycount+1);
      }
    }
    else
    {
      eos_static_err("status is NOT ok : %s", status.ToString ().c_str ());
    }
    errno = (status.code == XrdCl::errAuthFailed) ? EPERM : EFAULT;
    if( status.code == XrdCl::errErrorResponse )
    {
      filesystem::error_retc_map(status.errNo);
      eos_static_debug("setting errno to %d", errno);
    }

    break;
  }
  return status;
}
