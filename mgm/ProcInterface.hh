// ----------------------------------------------------------------------
// File: ProcInterface.hh
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

#ifndef __EOSMGM_PROCINTERFACE__HH__
#define __EOSMGM_PROCINTERFACE__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/Mapping.hh"
#include "proc/proc_fs.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSec/XrdSecEntity.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class ProcCommand : public eos::common::LogId {
private:
  XrdOucString path;
  eos::common::Mapping::VirtualIdentity* pVid;
  XrdOucString cmd;
  XrdOucString subcmd;
  XrdOucString args;

  XrdOucString stdOut;
  XrdOucString stdErr;
  int retc;
  XrdOucString resultStream;

  // the 'find' command writes results into temporary files
  FILE* fstdout;
  FILE* fstderr;
  FILE* fresultStream;
  XrdOucString fstdoutfilename;
  XrdOucString fstderrfilename;
  XrdOucString fresultStreamfilename;

  XrdOucErrInfo*   error;

  size_t len;
  off_t  offset;
  void MakeResult(bool dosort=false, bool fuseformat=false);

  bool adminCmd;
  bool userCmd; 

public:

  int open(const char* path, const char* info, eos::common::Mapping::VirtualIdentity &vid, XrdOucErrInfo *error);
  int read(XrdSfsFileOffset offset, char *buff, XrdSfsXferSize blen);
  int stat(struct stat* buf);
  int close();

  void AddOutput(XrdOucString &lStdOut, XrdOucString &lStdErr) {
    lStdOut += stdOut;
    lStdErr += stdErr;
  }

  bool OpenTemporaryOutputFiles();

  ProcCommand();
  ~ProcCommand();
}; 

class ProcInterface {
private:

public:

  static bool IsProcAccess(const char* path);
  static bool Authorize(const char* path, const char* info, eos::common::Mapping::VirtualIdentity &vid, const XrdSecEntity* entity);

  ProcInterface();
  ~ProcInterface();
};

EOSMGMNAMESPACE_END

#endif
