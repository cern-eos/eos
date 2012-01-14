// ----------------------------------------------------------------------
// File: TransferEngine.cc
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

/*----------------------------------------------------------------------------*/
#include "mgm/txengine/TransferEngine.hh"
#include "mgm/txengine/TransferFsDB.hh"
#include "common/StringConversion.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

TransferEngine gTransferEngine;

/*----------------------------------------------------------------------------*/
TransferEngine::TransferEngine()
{
  xDB = (TransferDB*) new TransferFsDB();
}

/*----------------------------------------------------------------------------*/
TransferEngine::~TransferEngine()
{
}

/*----------------------------------------------------------------------------*/
bool TransferEngine::Init(const char* connectstring)
{
  if (xDB) return xDB->Init(connectstring);
  return false;
}

/*----------------------------------------------------------------------------*/
int 
TransferEngine::Submit(XrdOucString& src, XrdOucString& dst, XrdOucString& rate, XrdOucString& streams, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, eos::common::Mapping::VirtualIdentity& vid)
{
  if ( ((!src.beginswith("root://")) &&
	(!src.beginswith("/eos/"))) ||
       ((!dst.beginswith("root://")) &&
	(!dst.beginswith("/eos/"))) ) {
    stdErr += "error: invalid source or destination URL!";
    return EINVAL;
  }
  
  int  irate = atoi(rate.c_str());
  XrdOucString sirate = ""; sirate += irate;
  
  if ( (irate <0) || 
       (sirate != rate) || 
       (irate > 1000000) ) {
    stdErr += "error: rate has to be a positive integer value!";
    return EINVAL;
  }

  int  istreams = atoi(streams.c_str());
  XrdOucString sistreams = ""; sistreams += istreams;
  
  if ( (istreams <0) || 
       (sistreams != streams) || 
       (istreams > 64) ) {
    stdErr += "error: streams has to be a positive integer value and <= 64!";
    return EINVAL;
  }

  if ( group.length() > 128 ) {
    stdErr += "error: the maximum group string can have 128 characters!";
    return EINVAL;
  }
  
  XrdOucString submissionhost=vid.tident.c_str();
  return xDB->Submit(src,dst,rate,streams,group,stdOut,stdErr,vid.uid,vid.gid,submissionhost);
}

/*----------------------------------------------------------------------------*/
int 
TransferEngine::Ls(XrdOucString& option, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, eos::common::Mapping::VirtualIdentity& vid)
{
  return xDB->Ls(option,group,stdOut,stdErr,vid.uid,vid.gid);
}

/*----------------------------------------------------------------------------*/
int 
TransferEngine::Cancel(XrdOucString& id, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, eos::common::Mapping::VirtualIdentity& vid)
{
  return 0;
}

EOSMGMNAMESPACE_END
