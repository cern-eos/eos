// ----------------------------------------------------------------------
// File: ProcInterface.cc
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/Mapping.hh"
#include "common/StringConversion.hh"
#include "common/Path.hh"
#include "mgm/Acl.hh"
#include "mgm/Access.hh"
#include "mgm/FileSystem.hh"
#include "mgm/Policy.hh"
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/FsView.hh"
#include "mgm/Master.hh"
#include "namespace/persistency/LogManager.hh"
#include "namespace/utils/DataHelper.hh"
#include "namespace/views/HierarchicalView.hh"
#include "namespace/persistency/ChangeLogContainerMDSvc.hh"
#include "namespace/persistency/ChangeLogFileMDSvc.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/
#include <iostream>
#include <fstream>
/*----------------------------------------------------------------------------*/

#include <vector>
#include <map>
#include <string>
#include <math.h>

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
ProcInterface::ProcInterface () { }

/*----------------------------------------------------------------------------*/
ProcInterface::~ProcInterface () { }

/*----------------------------------------------------------------------------*/
bool
ProcInterface::IsProcAccess (const char* path)
{
 XrdOucString inpath = path;
 if (inpath.beginswith("/proc/"))
 {
   return true;
 }
 return false;
}

/*----------------------------------------------------------------------------*/
bool
ProcInterface::IsWriteAccess (const char* path, const char* info)
{
 XrdOucString inpath = (path ? path : "");
 XrdOucString ininfo = (info ? info : "");

 if (!inpath.beginswith("/proc/"))
 {
   return false;
 }

 XrdOucEnv procEnv(ininfo.c_str());
 XrdOucString cmd = procEnv.Get("mgm.cmd");
 XrdOucString subcmd = procEnv.Get("mgm.subcmd");

 // filter here all namespace modifying proc messages
 if (((cmd == "file") &&
      ((subcmd == "adjustreplica") ||
       (subcmd == "drop") ||
       (subcmd == "layout") ||
       (subcmd == "verify") ||
       (subcmd == "rename"))) ||
     ((cmd == "attr") &&
      ((subcmd == "set") ||
       (subcmd == "rm"))) ||
     ((cmd == "mkdir")) ||
     ((cmd == "rmdir")) ||
     ((cmd == "rm")) ||
     ((cmd == "chown")) ||
     ((cmd == "chmod")) ||
     ((cmd == "fs") &&
      ((subcmd == "config") ||
       (subcmd == "boot") ||
       (subcmd == "dropfiles") ||
       (subcmd == "add") ||
       (subcmd == "mv") ||
       (subcmd == "rm"))) ||
     ((cmd == "space") &&
      ((subcmd == "config") ||
       (subcmd == "define") ||
       (subcmd == "set") ||
       (subcmd == "rm") ||
       (subcmd == "quota"))) ||
     ((cmd == "node") &&
      ((subcmd == "rm") ||
       (subcmd == "config") ||
       (subcmd == "set") ||
       (subcmd == "register") ||
       (subcmd == "gw"))) ||
     ((cmd == "group") &&
      ((subcmd == "set") ||
       (subcmd == "rm"))) ||
     ((cmd == "map") &&
      ((subcmd == "link") ||
       (subcmd == "unlink"))) ||
     ((cmd == "quota") &&
      ((subcmd != "ls"))) ||
     ((cmd == "vid") &&
      ((subcmd != "ls"))) ||
     ((cmd == "transfer")))
 {

   return true;
 }

 return false;
}

/*----------------------------------------------------------------------------*/
bool
ProcInterface::Authorize (const char* path,
                          const char* info,
                          eos::common::Mapping::VirtualIdentity& vid,
                          const XrdSecEntity* entity)
{
 XrdOucString inpath = path;

 // administrator access
 if (inpath.beginswith("/proc/admin/"))
 {
   // hosts with 'sss' authentication can run 'admin' commands
   std::string protocol = entity ? entity->prot : "";
   // we allow sss only with the daemon login is admin
   if ((protocol == "sss") && (eos::common::Mapping::HasUid(2, vid.uid_list)))
   {
     return true;
   }

   // root can do it
   if (!vid.uid)
   {
     return true;
   }

   // one has to be part of the virtual users 2(daemon) || 3(adm)/4(adm) 
   return ( (eos::common::Mapping::HasUid(2, vid.uid_list)) ||
           (eos::common::Mapping::HasUid(3, vid.uid_list)) ||
           (eos::common::Mapping::HasGid(4, vid.gid_list)));
 }

 // user access
 if (inpath.beginswith("/proc/user/"))
 {
   return true;
 }

 // fst access
 if (inpath.beginswith("/proc/fst/"))
 {
   return false;
 }

 return false;
}

/*----------------------------------------------------------------------------*/
ProcCommand::ProcCommand ()
{
 stdOut = "";
 stdErr = "";
 stdJson = "";
 retc = 0;
 resultStream = "";
 offset = 0;
 len = 0;
 pVid = 0;
 path = "";
 adminCmd = userCmd = 0;
 error = 0;
 comment = "";
 args = "";
 exectime = time(NULL);
 closed = true;
 opaque = 0;
 ininfo = 0;
 fstdout = fstderr = fresultStream = 0;
 fstdoutfilename = fstderrfilename = fresultStreamfilename = "";
}

/*----------------------------------------------------------------------------*/
ProcCommand::~ProcCommand ()
{
 if (fstdout)
 {
   fclose(fstdout);
   fstdout = 0;
   unlink(fstdoutfilename.c_str());
 }

 if (fstderr)
 {
   fclose(fstderr);
   fstderr = 0;
   unlink(fstderrfilename.c_str());
 }

 if (fresultStream)
 {
   fclose(fresultStream);
   fresultStream = 0;
   unlink(fresultStreamfilename.c_str());
 }

 if (opaque)
 {
   delete opaque;
   opaque = 0;
 }
}

/*----------------------------------------------------------------------------*/
bool
ProcCommand::OpenTemporaryOutputFiles ()
{
 char tmpdir [4096];
 snprintf(tmpdir, sizeof (tmpdir) - 1, "/tmp/eos.mgm/%llu", (unsigned long long) XrdSysThread::ID());
 fstdoutfilename = tmpdir;
 fstdoutfilename += ".stdout";
 fstderrfilename = tmpdir;
 fstderrfilename += ".stderr";
 fresultStreamfilename = tmpdir;
 fresultStreamfilename += ".resultstream";

 eos::common::Path cPath(fstdoutfilename.c_str());

 if (!cPath.MakeParentPath(S_IRWXU))
 {
   eos_err("Unable to create temporary outputfile directory %s", tmpdir);
   return false;
 }

 // own the directory by daemon
 if (::chown(cPath.GetParentPath(), 2, 2))
 {
   eos_err("Unable to own temporary outputfile directory %s", cPath.GetParentPath());
 }

 fstdout = fopen(fstdoutfilename.c_str(), "w");
 fstderr = fopen(fstderrfilename.c_str(), "w");
 fresultStream = fopen(fresultStreamfilename.c_str(), "w+");

 if ((!fstdout) || (!fstderr) || (!fresultStream))
 {
   if (fstdout) fclose(fstdout);
   if (fstderr) fclose(fstderr);
   if (fresultStream) fclose(fresultStream);
   return false;
 }
 return true;
}

/*----------------------------------------------------------------------------*/
int
ProcCommand::open (const char* inpath, const char* info, eos::common::Mapping::VirtualIdentity &vid_in, XrdOucErrInfo *error)
{
 pVid = &vid_in;
 closed = false;
 path = inpath;
 bool dosort = false;

 ininfo = info;
 if ((path.beginswith("/proc/admin")))
 {
   adminCmd = true;
 }
 if (path.beginswith("/proc/user"))
 {
   userCmd = true;
 }

 opaque = new XrdOucEnv(ininfo);

 if (!opaque)
 {
   // alloc failed 
   return SFS_ERROR;
 }

 cmd = opaque->Get("mgm.cmd");
 subcmd = opaque->Get("mgm.subcmd");
 outformat = opaque->Get("mgm.outformat");
 selection = opaque->Get("mgm.selection");
 comment = opaque->Get("mgm.comment") ? opaque->Get("mgm.comment") : "";
 int envlen = 0;
 args = opaque->Env(envlen);

 fuseformat = false;
 jsonformat = false;
 XrdOucString format = opaque->Get("mgm.format"); // if set to FUSE, don't print the stdout,stderr tags and we guarantee a line feed in the end

 if (format == "fuse")
 {
   fuseformat = true;
 }
 if (format == "json")
 {
   jsonformat = true;
 }

 stdOut = "";
 stdErr = "";
 retc = 0;
 resultStream = "";
 offset = 0;
 len = 0;
 dosort = true;


 // admin command section
 if (adminCmd)
 {
   if (cmd == "access")
   {
     Access();
   }
   else

     if (cmd == "config")
   {
     Config();
   }
   else

     if (cmd == "node")
   {
     Node();
   }
   else
     if (cmd == "space")
   {
     Space();
   }
   else

     if (cmd == "group")
   {
     Group();
   }
   else
     if (cmd == "fs")
   {
     Fs();
   }
   else
     if (cmd == "ns")
   {
     Ns();
   }
   else
     if (cmd == "io")
   {
     Io();
   }
   else
     if (cmd == "fsck")
   {
     Fsck();
   }
   else
     if (cmd == "quota")
   {
     Quota();
   }
   else
     if (cmd == "transfer")
   {
     Transfer();
     dosort = false;
   }
   else
     if (cmd == "debug")
   {
     Debug();
   }
   else
     if (cmd == "vid")
   {
     Vid();
   }
   else
     if (cmd == "rtlog")
   {
     Rtlog();
   }
   else
     if (cmd == "chown")
   {
     Chown();
   }
   else
   {
     stdErr += "errro: no such admin command '";
     stdErr += cmd;
     stdErr += "'";
     retc = EINVAL;
   }

   MakeResult();
   return SFS_OK;
 }

 if (userCmd)
 {
   if (cmd == "motd")
   {
     Motd();
     dosort = false;
   }
   else
     if (cmd == "version")
   {
     Version();
     dosort = false;
   }
   else
     if (cmd == "quota")
   {
     Quota();
     dosort = false;
   }
   else
     if (cmd == "who")
   {
     Who();
     dosort = false;
   }
   else
     if (cmd == "fuse")
   {
     return Fuse();
   }
   else
     if (cmd == "file")
   {
     File();
   }
   else
     if (cmd == "fileinfo")
   {
     Fileinfo();
   }
   else
     if (cmd == "mkdir")
   {
     Mkdir();
   }
   else
     if (cmd == "rmdir")
   {
     Rmdir();
   }
   else
     if (cmd == "cd")
   {
     Cd();
     dosort = false;
   }
   else
     if (cmd == "ls")
   {
     Ls();
     dosort = false;
   }
   else
     if (cmd == "rm")
   {
     Rm();
   }
   else
     if (cmd == "whoami")
   {
     Whoami();
     dosort = false;
   }
   else
     if (cmd == "find")
   {
     Find();
   }
   else
     if (cmd == "map")
   {
     Map();
   }
   else
     if (cmd == "attr")
   {
     Attr();
   }
   else
     if (cmd == "chmod")
   {
     if (Chmod() == SFS_OK) return SFS_OK;
   }
   else
   {
     stdErr += "errro: no such user command '";
     stdErr += cmd;
     stdErr += "'";
     retc = EINVAL;
   }
   MakeResult();
   return SFS_OK;
 }

 return gOFS->Emsg((const char*) "open", *error, EINVAL, "execute command - not implemented ", ininfo);
}

/*----------------------------------------------------------------------------*/
int
ProcCommand::read (XrdSfsFileOffset offset, char* buff, XrdSfsXferSize blen)
{
 if (fresultStream)
 {
   // file based results go here ...
   if ((fseek(fresultStream, offset, 0)) == 0)
   {
     size_t nread = fread(buff, 1, blen, fresultStream);
     if (nread > 0)
       return nread;
   }
   else
   {
     eos_err("seek to %llu failed\n", offset);
   }
   return 0;
 }
 else
 {
   // memory based results go here ...
   if (((unsigned int) blen <= (len - offset)))
   {
     memcpy(buff, resultStream.c_str() + offset, blen);
     return blen;
   }
   else
   {
     memcpy(buff, resultStream.c_str() + offset, (len - offset));
     return (len - offset);
   }
 }
}

/*----------------------------------------------------------------------------*/
int
ProcCommand::stat (struct stat* buf)
{
 memset(buf, 0, sizeof (struct stat));
 buf->st_size = len;

 return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
ProcCommand::close ()
{
 if (!closed)
 {
   // only instance users or sudoers can add to the log book
   if ((pVid->uid <= 2) || (pVid->sudoer))
   {
     if (comment.length() && gOFS->commentLog)
     {
       if (!gOFS->commentLog->Add(exectime, cmd.c_str(), subcmd.c_str(), args.c_str(), comment.c_str(), stdErr.c_str(), retc))
       {
         eos_err("failed to log to comment log file");
       }
     }
   }
   closed = true;
 }
 return retc;
}

/*----------------------------------------------------------------------------*/
void
ProcCommand::MakeResult ()
{
 resultStream = "";

 if (!fstdout)
 {
   XrdMqMessage::Sort(stdOut, dosort);
   if ((!fuseformat && !jsonformat))
   {
     // the default format
     resultStream = "mgm.proc.stdout=";
     resultStream += XrdMqMessage::Seal(stdOut);
     resultStream += "&mgm.proc.stderr=";
     resultStream += XrdMqMessage::Seal(stdErr);
     resultStream += "&mgm.proc.retc=";
     resultStream += retc;

   }
   if (fuseformat)
   {
     resultStream += stdOut;
   }
   if (jsonformat)
   {
     if (!stdJson.length())
     {
       stdJson = "{\n  \"error\": \"command does not provide JSON output\",\n  \"errc\": 93\n}";
     }
     resultStream = "mgm.proc.json=";
     resultStream += stdJson;
   }
   if (!resultStream.endswith('\n'))
   {
     resultStream += "\n";
   }
   if (retc)
   {
     eos_static_err("%s (errno=%u)", stdErr.c_str(), retc);
   }
   len = resultStream.length();
   offset = 0;
 }
 else
 {
   // file based results CANNOT be sorted and don't have fuseformat
   if (!fuseformat)
   {
     // create the stdout result
     if (!fseek(fstdout, 0, 0) && !fseek(fstderr, 0, 0) && !fseek(fresultStream, 0, 0))
     {
       fprintf(fresultStream, "&mgm.proc.stdout=");

       std::ifstream inStdout(fstdoutfilename.c_str());
       std::ifstream inStderr(fstderrfilename.c_str());
       std::string entry;

       while (std::getline(inStdout, entry))
       {
         XrdOucString sentry = entry.c_str();
         sentry += "\n";
         if (!fuseformat)
         {
           XrdMqMessage::Seal(sentry);
         }
         fprintf(fresultStream, "%s", sentry.c_str());
       }
       // close and remove - if this fails there is nothing to recover anyway
       fclose(fstdout);
       fstdout = 0;
       unlink(fstdoutfilename.c_str());
       // create the stderr result
       fprintf(fresultStream, "&mgm.proc.stderr=");
       while (std::getline(inStdout, entry))
       {
         XrdOucString sentry = entry.c_str();
         sentry += "\n";
         XrdMqMessage::Seal(sentry);
         fprintf(fresultStream, "%s", sentry.c_str());
       }
       // close and remove - if this fails there is nothing to recover anyway
       fclose(fstderr);
       fstderr = 0;
       unlink(fstderrfilename.c_str());

       fprintf(fresultStream, "&mgm.proc.retc=%d", retc);
       len = ftell(fresultStream);

       // spool the resultstream to the beginning
       fseek(fresultStream, 0, 0);
     }
     else
     {
       eos_static_err("cannot seek to position 0 in result files");
     }
   }
 }
}

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_END

