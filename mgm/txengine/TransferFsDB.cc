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
TransferFsDB::TransferFsDB ()
{
  SetLogId("TransferDB", "<service>");
}

/*----------------------------------------------------------------------------*/
int
TransferFsDB::CallBack (void *object, int argc, char **argv, char **ColName)
{
  TransferFsDB* tx = (TransferFsDB*) object;
  int i = tx->Qr.size();
  tx->Qr.resize(i + 1);
  for (int k = 0; k < argc; k++)
  {
    tx->Qr[i][ColName[k]] = argv[k] ? argv[k] : "";
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
bool
TransferFsDB::Init (const char* dbpath)

{
  XrdSysMutexHelper lock(Lock);
  XrdOucString dpath = dbpath;
  dpath += "/transfers.sql";
  XrdOucString archivepath = dbpath;
  archivepath += "/transfer-archive.log";
  while (dpath.replace("//", "/"))
  {
  };
  eos::common::Path cPath(dpath.c_str());

  ErrMsg = 0;

  if (!cPath.MakeParentPath(S_IRWXU))
  {
    eos_err("unable to create txfs store under %s\n", cPath.GetParentPath());
    return false;
  }

  if ((sqlite3_open(dpath.c_str(), &DB) == SQLITE_OK))
  {
    if (chmod(dpath.c_str(), S_IRUSR | S_IWUSR))
    {
      eos_warning("failed to set private permissions on %s", dpath.c_str());
    }

    XrdOucString createtable = "CREATE TABLE if not exists transfers (src varchar(256), dst varchar(256), rate smallint, streams smallint, groupname varchar(128), status varchar(32), progress double, exechost varchar(64), submissionhost varchar(64), log clob, uid smallint, gid smallint, expires int, credential clob, sync smallint, noauth smallint, id integer PRIMARY KEY AUTOINCREMENT )";

    //    XrdOucString createtable = "CREATE TABLE if not exists transfers (src varchar(256), dst varchar(256), rate smallint, streams smallint, groupname varchar(128), status varchar(32), submissionhost varchar(64), log clob, uid smallint, gid smallint, id integer PRIMARY KEY AUTOINCREMENT )";

    if ((sqlite3_exec(DB, createtable.c_str(), CallBack, this, &ErrMsg)))
    {
      eos_err("unable to create <transfers> table - msg=%s\n", ErrMsg);
      return false;
    }

    fdArchive = fopen(archivepath.c_str(), "a+");
    if (!fdArchive)
    {
      eos_err("failed to open archive file %s - errno=%d\n", archivepath.c_str(), errno);
      return false;
    }

    // set auto-vacuum mode
    if ((sqlite3_exec(DB, "PRAGMA auto_vacuum=FULL", CallBack, this, &ErrMsg)))
    {
      eos_err("failed to set auto-vaccum mode %s - errno=%d\n", ErrMsg, errno);
      return false;
    }


    return true;
  }
  else
  {
    eos_err("failed to open sqlite3 database file %s - msg=%s\n", dpath.c_str(), sqlite3_errmsg(DB));
    return false;
  }
}

/*----------------------------------------------------------------------------*/
TransferFsDB::~TransferFsDB ()
{
  sqlite3_close(DB);
}

/*----------------------------------------------------------------------------*/
int
TransferFsDB::Ls (XrdOucString& sid, XrdOucString& option, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, uid_t uid, gid_t gid)
{
  XrdSysMutexHelper lock(Lock);
  Qr.clear();
  bool monitoring = false;
  bool all = false;
  bool summary = false;
  bool onlyprogress = false;
  std::string id = sid.c_str();

  ErrMsg = 0;

  if ((option.find("m") != STR_NPOS))
  {
    monitoring = true;
  }
  if ((option.find("a") != STR_NPOS))
  {
    all = true;
  }
  if ((option.find("s") != STR_NPOS))
  {
    summary = true;
  }
  if ((option.find("p") != STR_NPOS))
  {
    onlyprogress = true;
  }

  XrdOucString query = "";

  query = "select * from transfers";

  if (group.length())
  {
    query += " where groupname='";
    query += group;
    query += "'";
    if (!all)
    {
      query += " and uid=";
      query += (int) uid;
    }
  }
  else
  {
    if (!all)
    {
      query += " where uid=";
      query += (int) uid;
    }
  }

  if ((sqlite3_exec(DB, query.c_str(), CallBack, this, &ErrMsg)))
  {
    eos_err("unable to query - msg=%s\n", ErrMsg);
    stdErr = "error: ";
    stdErr += ErrMsg;
    return -1;
  }

  qr_result_t::const_iterator it;

  char outline[16384];
  outline[0] = 0;

  std::map<std::string, int> groupby;

  if (!monitoring)
  {
    if ((!summary) && (!monitoring))
    {
      snprintf(outline, sizeof (outline) - 1, "%-8s %-8s %-8s %-16s %-4s %-6s %-4s %-4s %-8s %-48s %-48s\n", "ID", "STATUS", "PROGRESS", "GROUP", "RATE", "STREAM", "UID", "GID", "EXPTIME", "EXECHOST", "SUBMISSIONHOST");
      stdOut += outline;
      stdOut += "________ ________ ________ ________________ ____ ______ ____ ____ ________ ________________________________________________ ________________________________________________\n";
    }
    for (size_t i = 0; i < Qr.size(); i++)
    {

      long long etime = strtoul(Qr[i]["expires"].c_str(), 0, 10) - time(NULL);
      char setime[16];
      char progress[16];
      snprintf(progress, sizeof (progress), "%.02f", strtof(Qr[i]["progress"].c_str(), 0));

      if (etime < 0)
      {
        snprintf(setime, sizeof (setime) - 1, "expired");
      }
      else
      {
        snprintf(setime, sizeof (setime) - 1, "%lu", (unsigned long) etime);
      }
      snprintf(outline, sizeof (outline) - 1, "%-8s %-8s %-8s %-16s %-4s %-6s %-4s %-4s %-8s %-48s %-48s%s\n",
               Qr[i]["id"].c_str(),
               Qr[i]["status"].c_str(),
               progress,
               Qr[i]["groupname"].c_str(),
               Qr[i]["rate"].c_str(),
               Qr[i]["streams"].c_str(),
               Qr[i]["uid"].c_str(),
               Qr[i]["gid"].c_str(),
               setime,
               Qr[i]["exechost"].c_str(),
               Qr[i]["submissionhost"].c_str(),
               (Qr[i]["sync"] == "1") ? "*" : "");


      if (!summary)
      {
        if ((!id.length()) || (id == Qr[i]["id"]))
        {
          // list all or by id
          stdOut += outline;
          stdOut += "........ ........ ................ .... ...... .... .... ........  ................................................ ................................................\n";
          stdOut += "         src..... ";
          stdOut += Qr[i]["src"].c_str();
          stdOut += "\n";
          stdOut += "         dst..... ";
          stdOut += Qr[i]["dst"].c_str();
          stdOut += "\n";
          stdOut += "........ ........ ................ .... ...... .... .... ........  ................................................ ................................................\n";
        }
      }
      groupby[Qr[i]["status"]]++;
    }
  }
  else
  {
    for (size_t i = 0; i < Qr.size(); i++)
    {
      std::map<std::string, std::string>::const_iterator it;
      if (!summary)
      {
        if ((!id.length()) || (id == Qr[i]["id"]))
        {
          for (it = Qr[i].begin(); it != Qr[i].end(); it++)
          {
            if (!onlyprogress || (it->first == "progress") || (it->first == "status"))
            {
              stdOut += "tx.";
              stdOut += it->first.c_str();
              stdOut += "=";
              stdOut += it->second.c_str();
              stdOut += " ";
            }
          }
          stdOut += "\n";
        }
      }
      groupby[Qr[i]["status"]]++;
    }
  }


  if (summary)
  {
    if (!monitoring)
    {
      stdOut += "# ------------------------------------------\n";
      std::map<std::string, int>::const_iterator it;
      for (it = groupby.begin(); it != groupby.end(); it++)
      {
        char sline[1024];
        snprintf(sline, sizeof (sline) - 1, "# %-16s := %d\n", it->first.c_str(), it->second);
        stdOut += sline;
      }
    }
    else
    {
      std::map<std::string, int>::const_iterator it;
      for (it = groupby.begin(); it != groupby.end(); it++)
      {
        char sline[1024];
        snprintf(sline, sizeof (sline) - 1, "tx.n.%s=%d ", it->first.c_str(), it->second);
        stdOut += sline;
      }
      stdOut += "\n";
    }
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
TransferDB::transfer_t
TransferFsDB::GetNextTransfer (int status)
{
  XrdSysMutexHelper lock(Lock);
  Qr.clear();
  ErrMsg = 0;

  transfer_t transfer;

  XrdOucString query = "";

  query = "select * from transfers where status='";
  query += TransferEngine::GetTransferState(status);
  query += "' limit 1";

  if ((sqlite3_exec(DB, query.c_str(), CallBack, this, &ErrMsg)))
  {
    eos_err("unable to query - msg=%s\n", ErrMsg ? ErrMsg : "<none>");
    if (ErrMsg)
    {
      transfer["error"] = "error: ";
      transfer["error"] += ErrMsg;
    }
    else
    {
      transfer["error"] = "<none>";
    }
    return transfer;
  }

  if (Qr.size() == 1)
  {
    return Qr[0];
  }
  else
  {
    return transfer;
  }
}

/*----------------------------------------------------------------------------*/
bool
TransferFsDB::SetState (long long id, int state)
{
  XrdSysMutexHelper lock(Lock);

  TransferDB::transfer_t transfer = GetTransfer(id, true);

  if ((id != 0) && (!transfer.count("status")))
  {
    return false;
  }
  // for id=0 it sets the state on all ids
  XrdOucString query = "";

  query = "update transfers set status='";
  query += TransferEngine::GetTransferState(state);

  if (state == TransferEngine::kInserted)
  {
    time_t nexp = time(NULL) + 86400;
    char snepx[256];
    snprintf(snepx, sizeof (snepx), "%lu", nexp);
    // we update the expiration time to 1 day
    query += "', expires=";
    query += snepx;
    query += ", progress=0.0";
  }
  else
  {
    if (state == TransferEngine::kDone)
    {
      query += "', progress=100.0";
    }
    else
    {
      query += "'";
    }
  }

  if (id == 0)
  {
    query += " where 1 ";
  }
  else
  {
    query += " where id = ";
    char sid[16];
    snprintf(sid, sizeof (sid) - 1, "%lld", id);
    query += sid;
  }

  if ((sqlite3_exec(DB, query.c_str(), CallBack, this, &ErrMsg)))
  {
    eos_err("unable to update - msg=%s\n", ErrMsg);
    return false;
  }

  if (state == TransferEngine::kDone)
  {
    // check if this is an interactive transfer or asynchronous
    if (transfer["sync"] != "1")
    {
      // auto archive this transfer
      XrdOucString out, err;
      if (!Archive(id, out, err, true))
      {
        if (!Cancel(id, out, err, true))
        {
          return true;
        }
        else
        {
          eos_static_err("failed to cancel id=%lld in auto-archiving after <done> state", id);
          return false;
        }
      }
      else
      {
        eos_static_err("failed to archive id=%lld in auto-archiving after <done> state", id);
        return false;
      }
    }
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
TransferFsDB::SetProgress (long long id, float progress)
{
  XrdSysMutexHelper lock(Lock);

  TransferDB::transfer_t transfer = GetTransfer(id, true);

  if (!transfer.count("status"))
  {
    return false;
  }

  // for id=0 it sets the state on all ids
  XrdOucString query = "";
  char sprogress[16];
  snprintf(sprogress, sizeof (sprogress) - 1, "%.02f", progress);

  query = "update transfers set progress=";
  query += sprogress;

  query += " where id = ";
  char sid[16];
  snprintf(sid, sizeof (sid) - 1, "%lld", id);
  query += sid;

  if ((sqlite3_exec(DB, query.c_str(), CallBack, this, &ErrMsg)))
  {
    eos_err("unable to update - msg=%s\n", ErrMsg);
    return false;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool

TransferFsDB::SetExecutionHost (long long id, std::string& exechost)
{
  XrdSysMutexHelper lock(Lock);
  // for id=0 it sets the state on all ids
  XrdOucString query = "";

  query = "update transfers set exechost='";
  query += exechost.c_str();
  if (id == 0)
  {
    query += "' where 1 ";
  }
  else
  {
    query += "' where id = ";
    char sid[16];
    snprintf(sid, sizeof (sid) - 1, "%lld", id);
    query += sid;
  }

  if ((sqlite3_exec(DB, query.c_str(), CallBack, this, &ErrMsg)))
  {
    eos_err("unable to update - msg=%s\n", ErrMsg);
    return false;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
TransferFsDB::SetCredential (long long id, std::string credential, time_t exptime)
{
  XrdSysMutexHelper lock(Lock);
  XrdOucString query = "";

  query = "update transfers set credential='";
  query += credential.c_str();
  query += "' expires= ";
  char sid[16];
  snprintf(sid, sizeof (sid) - 1, "%lu", (unsigned long) exptime);
  query += sid;

  query += "  where id = ";
  snprintf(sid, sizeof (sid) - 1, "%lld", id);
  query += sid;

  if ((sqlite3_exec(DB, query.c_str(), CallBack, this, &ErrMsg)))
  {
    eos_err("unable to update - msg=%s\n", ErrMsg);
    return false;
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool
TransferFsDB::SetLog (long long id, std::string log)
{
  XrdSysMutexHelper lock(Lock);
  XrdOucString query = "";

  XrdOucString slog = log.c_str();
  while (slog.replace("'", "\""))
  {
  }

  query = "update transfers set log='";
  query += slog.c_str();
  query += "'  where id = ";
  char sid[16];
  snprintf(sid, sizeof (sid) - 1, "%lld", id);
  query += sid;

  if ((sqlite3_exec(DB, query.c_str(), CallBack, this, &ErrMsg)))
  {
    eos_err("unable to update - msg=%s\n", ErrMsg);
    return false;
  }
  return true;
}

/*----------------------------------------------------------------------------*/
TransferDB::transfer_t
TransferFsDB::GetTransfer (long long id, bool nolock)
{

  if (!nolock)
  {
    Lock.Lock();
  }
  Qr.clear();

  transfer_t transfer;

  XrdOucString query = "";

  query = "select * from transfers where id=";
  char sid[16];
  snprintf(sid, sizeof (sid) - 1, "%lld", id);
  query += sid;

  if ((sqlite3_exec(DB, query.c_str(), CallBack, this, &ErrMsg)))
  {
    eos_err("unable to query - msg=%s\n", ErrMsg);
    transfer["error"] = "error: ";
    transfer["error"] += ErrMsg;
    if (!nolock)
    {
      Lock.UnLock();
    }
    return transfer;
  }

  if (Qr.size() == 1)
  {
    transfer = Qr[0];
  }
  if (!nolock)
  {
    Lock.UnLock();
  }
  return transfer;

}

/*----------------------------------------------------------------------------*/
int
TransferFsDB::Submit (XrdOucString& src, XrdOucString& dst, XrdOucString& rate, XrdOucString& streams, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, uid_t uid, gid_t gid, time_t exptime, XrdOucString &credential, XrdOucString& submissionhost, bool sync, bool noauth)
{
  XrdSysMutexHelper lock(Lock);
  Qr.clear();

  XrdOucString insert = "";
  XrdOucString suid = "";
  suid += (int) uid;
  XrdOucString sgid = "";
  sgid += (int) gid;

  insert = "insert into transfers(src,dst,rate,streams,groupname,status,progress, submissionhost,log,uid,gid,expires,sync,noauth,credential,id) values(";
  insert += "'";
  insert += src;
  insert += "',";
  insert += "'";
  insert += dst;
  insert += "',";
  insert += "'";
  insert += rate;
  insert += "',";
  insert += "'";
  insert += streams;
  insert += "',";
  insert += "'";
  insert += group;
  insert += "',";
  insert += "'";
  insert += TransferEngine::GetTransferState(TransferEngine::kInserted);
  insert += "',";
  insert += "0.0";
  insert += ",";
  insert += "'";
  insert += submissionhost;
  insert += "',";
  insert += "'";
  insert += "";
  insert += "',";
  insert += "'";
  insert += suid;
  insert += "',";
  insert += "'";
  insert += sgid;
  insert += "',";
  char sexptime[1024];
  snprintf(sexptime, sizeof (sexptime) - 1, "%lu", exptime);
  insert += "'";
  insert += sexptime;
  insert += "',";

  if (sync)
  {
    insert += "'1',";
  }
  else
  {
    insert += "'0',";
  }

  if (noauth)
  {
    insert += "'1',";
  }
  else 
  {
    insert += "'0',";
  }

  insert += "'";
  insert += credential;
  insert += "',";
  insert += "NULL";
  insert += ")";

  if ((sqlite3_exec(DB, insert.c_str(), CallBack, this, &ErrMsg)))
  {
    eos_err("unable to insert - msg=%s\n", ErrMsg);
    stdErr = "error: ";
    stdErr += ErrMsg;
    return -1;
  }

  long long rowid = sqlite3_last_insert_rowid(DB);
  char srowid [256];
  snprintf(srowid, sizeof (srowid) - 1, "%lld", rowid);
  stdOut += "success: submitted transfer id=";
  stdOut += srowid;
  return 0;
}

/*----------------------------------------------------------------------------*/
int
TransferFsDB::Cancel (long long id, XrdOucString& stdOut, XrdOucString& stdErr, bool nolock)
{

  if (!nolock) Lock.Lock();
  XrdOucString query = "";

  query = "delete from transfers ";
  query += "where id = ";
  char sid[16];
  snprintf(sid, sizeof (sid) - 1, "%lld", id);
  query += sid;

  if ((sqlite3_exec(DB, query.c_str(), CallBack, this, &ErrMsg)))
  {
    eos_err("unable to delete - msg=%s\n", ErrMsg);
    stdErr += "error: unable to delete - msg=";
    stdErr += ErrMsg;
    stdErr += "\n";
    if (!nolock) Lock.UnLock();
    return -1;
  }
  stdOut += "success: canceled transfer id=";
  stdOut += sid;
  stdOut += "\n";
  if (!nolock) Lock.UnLock();
  return 0;
}

/*----------------------------------------------------------------------------*/
int
TransferFsDB::Archive (long long id, XrdOucString& stdOut, XrdOucString& stdErr, bool nolock)
{
  if (!nolock) Lock.Lock();
  Qr.clear();
  XrdOucString query = "";

  query = "select * from transfers ";
  query += "where id = ";
  char sid[16];
  snprintf(sid, sizeof (sid) - 1, "%lld", id);
  query += sid;

  if ((sqlite3_exec(DB, query.c_str(), CallBack, this, &ErrMsg)))
  {
    eos_err("unable to select - msg=%s\n", ErrMsg);
    stdErr += "error: unable to select - msg=";
    stdErr += ErrMsg;
    stdErr += "\n";
    if (!nolock) Lock.UnLock();
    return -1;
  }

  if (Qr.size())
  {
    bool ferror = false;
    if ((fprintf(fdArchive, "# ==========================================================================\n")) < 0) ferror = true;
    if ((fprintf(fdArchive, "# id=%s uid=%s gid=%s group=%s rate=%s streams=%s state=%s\n", Qr[0]["id"].c_str(), Qr[0]["uid"].c_str(), Qr[0]["gid"].c_str(), Qr[0]["groupname"].c_str(), Qr[0]["rate"].c_str(), Qr[0]["streams"].c_str(), Qr[0]["status"].c_str())) < 0) ferror = true;
    if ((fprintf(fdArchive, "# executionhost=%s\n", Qr[0]["exechost"].c_str())) < 0) ferror = true;
    if ((fprintf(fdArchive, "# submissionhost=%s\n", Qr[0]["submissionhost"].c_str())) < 0) ferror = true;
    if ((fprintf(fdArchive, "# src=%s\n", Qr[0]["src"].c_str())) < 0) ferror = true;
    if ((fprintf(fdArchive, "# dst=%s\n", Qr[0]["dst"].c_str())) < 0) ferror = true;
    if ((fprintf(fdArchive, "# --------------------------------------------------------------------------\n")) < 0) ferror = true;
    if ((fprintf(fdArchive, "%s\n", Qr[0]["log"].c_str())) < 0) ferror = true;
    if ((fprintf(fdArchive, "# --------------------------------------------------------------------------\n")) < 0) ferror = true;
    fflush(fdArchive);
    if (ferror)
    {
      stdErr += "error: failed to write to archive file - errno=";
      stdErr += (int) errno;
      if (!nolock) Lock.UnLock();
      return -1;
    }
    else
    {
      stdOut += "success: archived transfer id=";
      stdOut += sid;
      stdOut += "\n";
      if (!nolock) Lock.UnLock();
      return 0;
    }
  }
  else
  {
    stdErr += "error: query didn't return any transfer\n";
    if (!nolock) Lock.UnLock();
    return -1;
  }
}

/*----------------------------------------------------------------------------*/
int
TransferFsDB::Clear (XrdOucString& stdOut, XrdOucString& stdErr)
{
  XrdSysMutexHelper lock(Lock);
  XrdOucString query = "";

  query = "delete from transfers ";
  query += "  where 1";

  if ((sqlite3_exec(DB, query.c_str(), CallBack, this, &ErrMsg)))
  {
    eos_err("unable to delete - msg=%s\n", ErrMsg);
    stdErr += "error: unable to delete - msg=";
    stdErr += ErrMsg;
    stdErr += "\n";
    return -1;
  }
  stdOut += "success: cleared all transfers";
  stdOut += "\n";
  return 0;
}

/*----------------------------------------------------------------------------*/
std::vector<long long>
TransferFsDB::QueryByGroup (XrdOucString& group)
{
  XrdSysMutexHelper lock(Lock);
  Qr.clear();
  XrdOucString query = "";
  std::vector<long long> ids;

  query = "select id from transfers ";
  query += " where groupname='";
  query += group.c_str();
  query += "'";

  if ((sqlite3_exec(DB, query.c_str(), CallBack, this, &ErrMsg)))
  {
    eos_err("unable to query by group - msg=%s\n", ErrMsg);
    return ids;
  }

  for (size_t i = 0; i < Qr.size(); i++)
  {
    ids.push_back(strtoll(Qr[i]["id"].c_str(), 0, 10));
  }

  return ids;
}

/*----------------------------------------------------------------------------*/
std::vector<long long>
TransferFsDB::QueryByUid (uid_t uid)
{
  XrdSysMutexHelper lock(Lock);
  Qr.clear();
  XrdOucString query = "";
  std::vector<long long> ids;

  query = "select id from transfers ";
  query += " where uid=";
  query += (int) uid;

  if ((sqlite3_exec(DB, query.c_str(), CallBack, this, &ErrMsg)))
  {
    eos_err("unable to query by group - msg=%s\n", ErrMsg);
    return ids;
  }

  for (size_t i = 0; i < Qr.size(); i++)
  {
    ids.push_back(strtoll(Qr[i]["id"].c_str(), 0, 10));
  }

  return ids;
}

/*----------------------------------------------------------------------------*/
std::vector<long long>
TransferFsDB::QueryByState (XrdOucString& state)
{
  XrdSysMutexHelper lock(Lock);
  Qr.clear();
  XrdOucString query = "";
  std::vector<long long> ids;

  query = "select id from transfers ";
  query += " where status='";
  query += state.c_str();
  query += "'";

  if ((sqlite3_exec(DB, query.c_str(), CallBack, this, &ErrMsg)))
  {
    eos_err("unable to query by state - msg=%s\n", ErrMsg);
    return ids;
  }

  for (size_t i = 0; i < Qr.size(); i++)
  {
    ids.push_back(strtoll(Qr[i]["id"].c_str(), 0, 10));
  }

  return ids;
}

EOSMGMNAMESPACE_END
