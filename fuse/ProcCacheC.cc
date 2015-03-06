// ----------------------------------------------------------------------
// File: ProcCacheC.cc
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

#include "ProcCache.hh"

ProcCache gProcCache;

#ifdef __cplusplus
extern "C" {
#endif

int proccache_HasEntry (int pid)
{
  return gProcCache.HasEntry(pid);
}

int proccache_InsertEntry (int pid, bool useKrb5)
{
  return gProcCache.InsertEntry(pid,useKrb5);
}

int proccache_RemoveEntry (int pid)
{
  return !gProcCache.RemoveEntry(pid);
}

int proccache_GetEnv (int pid , const char* varName, char *buffer, size_t bufsize)
{
  if(!gProcCache.HasEntry(pid))
    return 1;

  std::string value;
  if(!gProcCache.GetEntry(pid)->GetEnv(varName,value))
    return 2;

  if(value.length()+1>bufsize)
    return 3;

  strcpy(buffer,value.c_str());
  return 0;
}

int proccache_GetKrb5UserName (int pid , char *buffer, size_t bufsize)
{
  if(!gProcCache.HasEntry(pid))
    return 1;

  std::string usrname;
  if(!gProcCache.GetEntry(pid)->GetKrb5UserName(usrname))
    return 2;

  if(usrname.length()+1>bufsize)
    return 3;

  strcpy(buffer,usrname.c_str());
  return 0;
}

int proccache_GetFsUidGid (int pid , uid_t *uid, gid_t *gid)
{
  if(!gProcCache.HasEntry(pid))
    return 1;
  return gProcCache.GetEntry(pid)->GetFsUidGid(*uid,*gid)?0:2;
}

int proccache_GetArgsStr (int pid, char*buffer, size_t bufsize)
{
  if(!gProcCache.HasEntry(pid))
    return 1;

  std::string value = gProcCache.GetEntry(pid)->GetArgsStr() ;
  if(value.empty())
    return 2;

  if(value.length()+1>bufsize)
    return 3;

  strcpy(buffer,value.c_str());
  return 0;
}

int proccache_HasError (int pid)
{
  if(!gProcCache.HasEntry(pid))
    return 1;

  return gProcCache.GetEntry(pid)->HasError()?2:0;
}

int proccache_GetErrorMessage(int pid, char*buffer, size_t bufsize)
{
  if(!gProcCache.HasEntry(pid))
    return 1;

  std::string errMesg = gProcCache.GetEntry(pid)->GetErrorMessage();
  if(errMesg.empty())
    return 2;

  if(errMesg.length()+1>bufsize)
    return 3;

  strcpy(buffer,errMesg.c_str());
  return 0;
}

#ifdef __cplusplus
}
#endif



