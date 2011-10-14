// ----------------------------------------------------------------------
// File: Report.hh
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

#ifndef __EOSCOMMON_REPORT__
#define __EOSCOMMON_REPORT__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <vector>
#include <string>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class Report {
  // the creator of the XrdOucEnv input is defined in XrdFstOfsFile.hh MakeReportEnv
private:
public:
  unsigned long long ots;  // timestamp of open
  unsigned long long cts;  // timestamp of close
  unsigned long long otms; // ms of open
  unsigned long long ctms; // ms of close
  std::string logid;       // logid
  std::string path;        // logical path or replicate:<fid>
  uid_t uid;               // user id
  gid_t gid;               // group id
  std::string td;          // trace identifer
  std::string host;        // server host
  unsigned long lid;       // layout id
  unsigned long long fid;  // file id
  unsigned long fsid;      // filesystem id
  unsigned long long rb;   // bytes read
  unsigned long long wb;   // bytes written
  unsigned long long srb;  // seeked bytes for read
  unsigned long long swb;  // seeked bytes for write
  unsigned long long nrc;  // number of read calls
  unsigned long long nwc;  // number of write calls
  float  rt;               // disk time spent for read
  float  wt;               // disk time spent for write
  unsigned long long osize;// size when file was opened
  unsigned long long csize;// size when file was closed

  Report(XrdOucEnv &report);
  ~Report() {};

  void Dump(XrdOucString &out);
};

EOSCOMMONNAMESPACE_END

#endif

