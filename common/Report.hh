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

/**
 * @file   Report.hh 
 *
 * @brief  Class to store file transaction reports.
 * 
 * 
 */

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

/*----------------------------------------------------------------------------*/
//! Class storing file transaction reports constructed by a FST report string
/*----------------------------------------------------------------------------*/

class Report {
  // ---------------------------------------------------------------------------
  // the creator of the XrdOucEnv input is defined in XrdFstOfsFile.hh MakeReportEnv
  // ---------------------------------------------------------------------------
private:

public:
  unsigned long long ots;  //< timestamp of open
  unsigned long long cts;  //< timestamp of close
  unsigned long long otms; //< ms of open
  unsigned long long ctms; //< ms of close
  std::string logid;       //< logid
  std::string path;        //< logical path or replicate:<fid>
  uid_t uid;               //< user id
  gid_t gid;               //< group id
  std::string td;          //< trace identifer
  std::string host;        //< server host
  std::string server_name; //< server name without domain
  std::string server_domain;//< server domain without server name
  unsigned long lid;       //< layout id
  unsigned long long fid;  //< file id
  unsigned long fsid;      //< filesystem id
  unsigned long long rb;   //< bytes read
  unsigned long long rb_min;   //< bytes read min
  unsigned long long rb_max;   //< bytes read max
  double             rb_sigma; //< bytes read sigma
  unsigned long long wb;       //< bytes written
  unsigned long long wb_min;   //< bytes written min 
  unsigned long long wb_max;   //< bytes written max
  double             wb_sigma; //< bytes written sigma
  unsigned long long sfwdb;  //< seeked bytes forward
  unsigned long long sbwdb;  //< seeked bytes backward
  unsigned long long sxlfwdb;  //< seeked bytes forward in seeks >4M
  unsigned long long sxlbwdb;  //< seeked bytes backward in seeks >4M
  unsigned long long nrc;  //< number of read calls
  unsigned long long nwc;  //< number of write calls
  unsigned long long nfwds;  //< number of forward seeks
  unsigned long long nbwds;  //< number of backwards seeks
  unsigned long long nxlfwds;  //< number of large forward seeks
  unsigned long long nxlbwds;  //< number of large backwards eeks
  float  rt;               //< disk time spent for read
  float  wt;               //< disk time spent for write
  unsigned long long osize;//< size when file was opened
  unsigned long long csize;//< size when file was closed
  std::string sec_prot;    //< auth protocol
  std::string sec_name;    //< auth name
  std::string sec_host;    //< auth client host
  std::string sec_domain;  //< auth domain
  std::string sec_vorg;    //< auth vorg
  std::string sec_grps;    //< auth grps
  std::string sec_role;    //< auth role
  std::string sec_info;    //< auth info (=dn if moninfo configuredin GSI plugin)
  std::string sec_app;     //< auth application

  // ---------------------------------------------------------------------------
  //! Constructor by report env 
  // ---------------------------------------------------------------------------
  Report(XrdOucEnv &report);

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~Report() {};

  // ---------------------------------------------------------------------------
  //! Dump the report contents into a string
  // ---------------------------------------------------------------------------
  void Dump(XrdOucString &out, bool dumpsec=false);
};

/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_END

#endif

