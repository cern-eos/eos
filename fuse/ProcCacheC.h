// ----------------------------------------------------------------------
// File: ProcCacheC.hh
// Author: Geoffray Adde - CERN
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

#ifndef __PROCCACHEC__HH__
#define __PROCCACHEC__HH__

#ifdef __cplusplus
#include "fuse/ProcCache.hh"
extern ProcCache gProcCache;
extern "C"
{
#endif

//! returns 0 if the proccache does NOT have an entry for the given pid
//! returns 1 if the proccache DOES have an entry for the given pid
  int proccache_HasEntry (int pid);

//! returns 0 if the cache has an up-to-date entry after the call.
//! returns 1 if the proccache does not have an entry for the given pid
  int proccache_InsertEntry (int pid);

//! returns 0 if the entry is removed after the call
//! returns 1 if the proccache does not have an entry for the given pid
  int proccache_RemoveEntry (int pid);

//! returns 0 if the authentication method was written in the buffer
//! returns 1 if the proccache does not have an entry for the given pid
//! returns 2 if the authentication method could not be retrieved
//! returns 3 if the buffer is too short to write the value of kerberos user name
  int proccache_GetAuthMethod (int pid, char *buffer, size_t bufsize);

//! returns 0 if the authentication method was written in the buffer
//! returns 1 if the proccache does not have an entry for the given pid
//! returns 2 if something else wrong happened
  int proccache_SetAuthMethod (int pid, const char *buffer);

//! returns 0 if the fsuid and the fsgid were written to the pointees passed in argument
//! returns 1 if the proccache does not have an entry for the given pid
  int proccache_GetFsUidGid (int pid, uid_t *uid, gid_t *gid);

//! returns 0 if the sid was written to the pointee passed in argument
//! returns 1 if the proccache does not have an entry for the given pid
  int proccache_GetSid (int pid, pid_t *sid);

//! returns 0 if the the startup time was written to the pointee passed in argument
//! returns 1 if the proccache does not have an entry for the given pid
  int proccache_GetStartupTime (int pid, time_t *sut);

    //! returns 0 if the kerberos user name if the value of the env variable was written in the buffer
//! returns 1 if the proccache does not have an entry for the given pid
//! returns 2 if the process commad line could not be read
//! returns 3 if the buffer is too short to write the value of the process command line
  int proccache_GetArgsStr (int pid, char*buffer, size_t bufsize);

//! returns 0 if the pid entry in the cache does NOT have any error
//! returns 1 if the proccache does not have an entry for the given pid
//! returns 2 if the pid entry in the cache DOES have an error
  int proccache_HasError (int pid);

//! returns 0 if the error message was copied to the buffer
//! returns 1 if the proccache does not have an entry for the given pid
//! returns 2 if the is no error message for the given pid
//! returns 3 if the buffer is too short to write the value of the error message
  int proccache_GetErrorMessage (int pid, char*buffer, size_t bufsize);

//! returns 0 if the start time of the pid process has been written to the pointee
//! returns 1 if the proccache does not have an entry for the given pid
//! returns 2 if the start time of the pid process could not be obtained
  int proccache_GetPsStartTime (int pid, time_t*startTime);

#ifdef __cplusplus
}
#endif

#endif
