// ----------------------------------------------------------------------
// File: TransferFsDB.hh
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

#ifndef __EOSMGM_TRANSFERFSDB__HH__
#define __EOSMGM_TRANSFERFSDB__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "mgm/txengine/TransferDB.hh"
#include "common/Mapping.hh"
#include "common/Logging.hh"
#include "common/sqlite/sqlite3.h"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <string>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class TransferFsDB : public TransferDB, eos::common::LogId {
  typedef std::vector<std::map< std::string, std::string > > qr_result_t;
private:
  sqlite3 *DB;
  FILE* fdArchive;
  qr_result_t Qr;
  char* ErrMsg;
  XrdSysMutex Lock;

public:
  static int CallBack(void *NotUsed, int argc, char **argv, char **ColName);
  TransferFsDB();
  bool Init(const char* dbpath="/var/eos/tx/");
  virtual ~TransferFsDB();
  
  virtual int Ls(XrdOucString& id, XrdOucString& option, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, uid_t uid, gid_t gid);

  virtual int Submit(XrdOucString& src, XrdOucString& dst, XrdOucString& rate, XrdOucString& streams, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, uid_t uid, gid_t gid, time_t exptime, XrdOucString& credentials, XrdOucString& submissionhost, bool sync);
  virtual int Cancel(long long id, XrdOucString& stdOut, XrdOucString& stdErr, bool nolock=false);
  virtual int Archive(long long id, XrdOucString& stdOUt, XrdOucString& sttdErr, bool nolock=false);
  virtual int Clear(XrdOucString& stdOut, XrdOucString& stdErr);
  virtual std::vector<long long> QueryByGroup(XrdOucString& group);
  virtual std::vector<long long> QueryByState(XrdOucString& state);
  virtual std::vector<long long> QueryByUid(uid_t uid);

  virtual bool SetState(long long id, int status);
  virtual bool SetProgress(long long id, float progress);
  virtual bool SetExecutionHost(long long id, std::string& exechost);
  virtual bool SetCredential(long long id, std::string credential, time_t exptime);
  virtual bool SetLog(long long id, std::string log);
  virtual transfer_t GetNextTransfer(int status);
  virtual transfer_t GetTransfer(long long id, bool nolock=false);
};

EOSMGMNAMESPACE_END

#endif
