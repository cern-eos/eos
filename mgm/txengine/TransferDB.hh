// ----------------------------------------------------------------------
// File: TransferDB.hh
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

#ifndef __EOSMGM_TRANSFERDB__HH__
#define __EOSMGM_TRANSFERDB__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/Mapping.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <string>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class TransferDB {
  
public:

  typedef std::map< std::string, std::string > transfer_t;

  TransferDB() {};
  virtual ~TransferDB() {};
  virtual bool Init(const char* dbspec="/var/eos/tx/") = 0;
  virtual int Ls(XrdOucString& id, XrdOucString& option, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, uid_t uid, gid_t gid) = 0;
  virtual int Submit(XrdOucString& src, XrdOucString& dst, XrdOucString& rate, XrdOucString& streams, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, uid_t uid, gid_t gid, time_t exptime, XrdOucString& credentials, XrdOucString& submissionhost, bool sync, bool noauth) = 0;
  virtual int Cancel(long long id, XrdOucString& stdOut, XrdOucString& stdErr, bool nolock=false) = 0;
  virtual int Archive(long long id, XrdOucString& stdOUt, XrdOucString& stdErr, bool nolock=false) = 0;
  virtual int Clear(XrdOucString& stdOut, XrdOucString& stdErr) = 0;

  virtual bool SetState(long long id, int status) = 0;
  virtual bool SetProgress(long long id, float progress) = 0;
  virtual bool SetExecutionHost(long long id, std::string& exechost) = 0;
  virtual bool SetCredential(long long id, std::string credential, time_t exptime) = 0;
  virtual bool SetLog(long long id, std::string log) = 0;
  virtual std::vector<long long> QueryByGroup(XrdOucString& group) = 0;
  virtual std::vector<long long> QueryByState(XrdOucString& state) = 0;
  virtual std::vector<long long> QueryByUid(uid_t uid) = 0;
  virtual transfer_t GetNextTransfer(int status) = 0;
  virtual transfer_t GetTransfer(long long id, bool nolock=false) = 0;

};

EOSMGMNAMESPACE_END

#endif
