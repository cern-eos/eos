// ----------------------------------------------------------------------
// File: TransferFsDB.cc
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
#include "mgm/txengine/TransferFsDB.hh"
#include "mgm/txengine/TransferEngine.hh"
#include "common/Logging.hh"
#include "common/Path.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
TransferFsDB::TransferFsDB()
{
  SetLogId("TransferDB","<service>");
}



/*----------------------------------------------------------------------------*/
int
TransferFsDB::CallBack(void *object, int argc, char **argv, char **ColName)
{
  TransferFsDB* tx = (TransferFsDB*) object;
  int i=tx->Qr.size();
  tx->Qr.resize(i+1);
  for (int k=0; k< argc; k++) {
    tx->Qr[i][ColName[k]] = argv[k]?argv[k]:"";
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
bool TransferFsDB::Init(const char* dbpath)
{
  XrdOucString dpath = dbpath; dpath += "/transfers.sql3.db";
  while (dpath.replace("//","/")) {};
  eos::common::Path cPath(dpath.c_str());

  
  if (!cPath.MakeParentPath(S_IRWXU)) {
    eos_err("unable to create txfs store under %s\n", cPath.GetParentPath());
    return false;
  }

  if ((sqlite3_open(dpath.c_str(),&DB) == SQLITE_OK)) {
    XrdOucString createtable = "CREATE TABLE if not exists transfers (src varchar(256), dst varchar(256), rate smallint, streams smallint, groupname varchar(128), status varchar(32), submissionhost varchar(64), log clob, uid smallint, gid smallint, expires int, credential clob, id integer PRIMARY KEY AUTOINCREMENT )";

    //    XrdOucString createtable = "CREATE TABLE if not exists transfers (src varchar(256), dst varchar(256), rate smallint, streams smallint, groupname varchar(128), status varchar(32), submissionhost varchar(64), log clob, uid smallint, gid smallint, id integer PRIMARY KEY AUTOINCREMENT )";

    if ((sqlite3_exec(DB,createtable.c_str(), CallBack, this, &ErrMsg))) {
      eos_err("unable to create <transfers> table - msg=%s\n",ErrMsg);
      return false;
    }
    return true;
  } else {
    eos_err("failed to open sqlite3 database file %s - msg=%s\n", dpath.c_str(), sqlite3_errmsg(DB));
    return false;
  }
}

/*----------------------------------------------------------------------------*/
TransferFsDB::~TransferFsDB()
{
  sqlite3_close(DB);
}

/*----------------------------------------------------------------------------*/
int 
TransferFsDB::Ls(XrdOucString& option, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, uid_t uid, gid_t gid)
{
  XrdSysMutexHelper lock(Lock);
  Qr.clear();
  bool monitoring = false;
  bool all = false;
  if ( (option.find("m") != STR_NPOS)) {
    monitoring = true;
  }
  if ( (option.find("a") != STR_NPOS)) {
    all = true;
  }

  XrdOucString query="";

  query = "select * from transfers";

  if ((sqlite3_exec(DB,query.c_str(), CallBack, this, &ErrMsg))) {
    eos_err("unable to query - msg=%s\n",ErrMsg);
    stdErr = "error: " ; stdErr += ErrMsg;
    return -1;
  }
  
  qr_result_t::const_iterator it;

  char outline[16384];

  if (!monitoring) {
    snprintf(outline, sizeof(outline)-1,"%-8s %-8s %-16s %-4s %-6s %-4s %-4s %-8s %-48s %-48s %-16s\n","ID","STATUS", "GROUP","RATE","STREAM", "UID","GID","EXPTIME", "SOURCE","DESTINATION","SUBMISSIONHOST");
    stdOut += outline;
    stdOut += "________ ________ ________________ ____ ______ ____ ____ ________ ________________________________________________ ________________________________________________ ________________\n";
    for (size_t i = 0; i< Qr.size(); i++) {
      
      snprintf(outline, sizeof(outline)-1,"%-8s %-8s %-16s %-4s %-6s %-4s %-4s %-8s %-48s %-48s %-16s\n",
	       Qr[i]["id"].c_str(),
	       Qr[i]["status"].c_str(),
	       Qr[i]["groupname"].c_str(),
	       Qr[i]["rate"].c_str(),
	       Qr[i]["streams"].c_str(),
	       Qr[i]["uid"].c_str(),
	       Qr[i]["gid"].c_str(),
	       Qr[i]["expires"].c_str(),
	       Qr[i]["src"].c_str(),
	       Qr[i]["dst"].c_str(),
	       Qr[i]["submissionhost"].c_str());
      
      stdOut += outline;
    }
  } else {
    for (size_t i = 0; i< Qr.size(); i++) {
      std::map<std::string, std::string>::const_iterator it;
      for (it=Qr[i].begin(); it != Qr[i].end(); it++) {
	stdOut += it->first.c_str();
	stdOut += "=";
	stdOut += it->second.c_str();
	stdOut += " ";
      }
      stdOut +="\n";
    }
  }
  return 0;
}

int 
TransferFsDB::Submit(XrdOucString& src, XrdOucString& dst, XrdOucString& rate, XrdOucString& streams, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, uid_t uid, gid_t gid, time_t exptime, XrdOucString &credential, XrdOucString& submissionhost)
{
  XrdSysMutexHelper lock(Lock);
  Qr.clear();
  
  XrdOucString insert="";
  XrdOucString suid=""; suid += (int)uid;
  XrdOucString sgid=""; sgid += (int)gid;

  insert = "insert into transfers(src,dst,rate,streams,groupname,status,submissionhost,log,uid,gid,expires,credential,id) values(";
  insert += "'"; insert += src;     insert += "',";
  insert += "'"; insert += dst;     insert += "',";
  insert += "'"; insert += rate;    insert += "',";
  insert += "'"; insert += streams; insert += "',";
  insert += "'"; insert += group;   insert += "',";
  insert += "'";
  insert += TransferEngine::GetTransferState(TransferEngine::kInserted); insert += "',";
  //  insert += "'inserted'";           insert += ",";
  insert += "'"; insert += submissionhost; insert += "',";
  insert += "'"; insert += "";      insert += "',";
  insert += "'"; insert += suid;    insert += "',";
  insert += "'"; insert += sgid;    insert += "',";
  char sexptime[1024];
  snprintf(sexptime,sizeof(sexptime)-1,"%lu",  exptime);
  insert += "'"; insert += sexptime; insert += "',";
  insert += "'"; insert += credential; insert += "',";
  insert += "NULL";
  insert += ")";

  fprintf(stderr,"%s\n", insert.c_str());
  if ((sqlite3_exec(DB,insert.c_str(), CallBack, this, &ErrMsg))) {
    eos_err("unable to insert - msg=%s\n",ErrMsg);
    stdErr = "error: " ; stdErr += ErrMsg;
    return -1;
  }

  long long rowid = sqlite3_last_insert_rowid(DB);
  char srowid [256]; snprintf(srowid,sizeof(srowid)-1, "%lld", rowid);
  stdOut += "success: submitted transfer id="; stdOut += srowid;
  return 0;
}

EOSMGMNAMESPACE_END
