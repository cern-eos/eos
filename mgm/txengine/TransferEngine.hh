// ----------------------------------------------------------------------
// File: TransferEngine.hh
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

#ifndef __EOSMGM_TRANSFERENGINE__HH__
#define __EOSMGM_TRANSFERENGINE__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "mgm/txengine/TransferDB.hh"
#include "common/Mapping.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <string>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class TransferEngine;

extern TransferEngine gTransferEngine;

class TransferEngine {
private:
  TransferDB* xDB;
public:

  static const char* GetTransferState(int state) {
    if (state == kNone)      return "none";
    if (state == kInserted)  return "inserted";
    if (state == kValidated) return "validated";
    if (state == kScheduled) return "scheduled";
    if (state == kRunning)   return "running";
    if (state == kDone)      return "done";
    if (state == kFailed)    return "failed";
    if (state == kRetry)    return "retry";
    return "unknown";
  }

  enum eTransferState {kNone=0, kInserted, kValidated, kScheduled, kRunning, kDone, kFailed, kRetry};

  TransferEngine();
  virtual ~TransferEngine();
  bool Init(const char* connectstring = 0 );

  int Submit(XrdOucString& src, XrdOucString& dst, XrdOucString& rate, XrdOucString& streams, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, eos::common::Mapping::VirtualIdentity& vid);
  int Ls(XrdOucString& option, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, eos::common::Mapping::VirtualIdentity& vid);
  int Cancel(XrdOucString& id, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, eos::common::Mapping::VirtualIdentity& vid);
};

EOSMGMNAMESPACE_END

#endif
