// ----------------------------------------------------------------------
// File: XrdErrMap.hh
// Author: CERN
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
 * @file   XrdErrorMap.hh
 * 
 * @brief  XRootD error to errno translation
 * 
 * 
 */

#ifndef __EOSCOMMON_XRDERRORMAP_HH__
#define __EOSCOMMON_XRDERRORMAP_HH__

#include "XProtocol/XProtocol.hh"
#include "common/Namespace.hh"

EOSCOMMONNAMESPACE_BEGIN

int
error_retc_map (int retc)
{
  if (retc) errno = retc;
  if (retc == kXR_ArgInvalid)
    errno = EINVAL;

  if (retc == kXR_ArgMissing)
    errno = EINVAL;

  if (retc == kXR_ArgTooLong)
    errno = E2BIG;

  if (retc == kXR_FileNotOpen)
    errno = EBADF;

  if (retc == kXR_FSError)
    errno = EIO;

  if (retc == kXR_InvalidRequest)
    errno = EINVAL;

  if (retc == kXR_IOError)
    errno = EIO;

  if (retc == kXR_NoMemory)
    errno = ENOMEM;

  if (retc == kXR_NoSpace)
    errno = ENOSPC;

  if (retc == kXR_ServerError)
    errno = EIO;

  if (retc == kXR_NotAuthorized)
    errno = EACCES;

  if (retc == kXR_NotFound)
    errno = ENOENT;

  if (retc == kXR_Unsupported)
    errno = ENOTSUP;

  if (retc == kXR_NotFile)
    errno = EISDIR;

  if (retc == kXR_isDirectory)
    errno = EISDIR;

  if (retc == kXR_Cancelled)
    errno = ECANCELED;

  if (retc == kXR_ChkLenErr)
    errno = ERANGE;

  if (retc == kXR_ChkSumErr)
    errno = ERANGE;

  if (retc == kXR_inProgress)
    errno = EAGAIN;

  if (retc)
    return -1;

  return 0;
}

EOSCOMMONNAMESPACE_END

#endif
