// ----------------------------------------------------------------------
// File: proc/user/Ls.cc
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
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Access.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Ls ()
{
 eos_info("calling ls");
 gOFS->MgmStats.Add("Ls", pVid->uid, pVid->gid, 1);
 XrdOucString spath = opaque->Get("mgm.path");
 eos::common::Path cPath(spath.c_str());
 const char* inpath = cPath.GetPath();

 NAMESPACEMAP;
 info = 0;
 if (info)info = 0; // for compiler happyness
 PROC_BOUNCE_ILLEGAL_NAMES;
 PROC_BOUNCE_NOT_ALLOWED;

 eos_info("mapped to %s", path);

 spath = path;

 XrdOucString option = opaque->Get("mgm.option");
 if (!spath.length())
 {
   stdErr = "error: you have to give a path name to call 'ls'";
   retc = EINVAL;
 }
 else
 {
   XrdMgmOfsDirectory dir;
   struct stat buf;
   int listrc = 0;
   XrdOucString filter = "";

   XrdOucString ls_file;

   if (gOFS->_stat(spath.c_str(), &buf, *error, *pVid, (const char*) 0))
   {
     stdErr = error->getErrText();
     retc = errno;
   }
   else
   {
     // if this is a directory open it and list
     if (S_ISDIR(buf.st_mode))
     {
       listrc = dir.open(spath.c_str(), *pVid, (const char*) 0);
     }
     else
     {
       // if this is a file, open the parent and set the filter
       if (spath.endswith("/"))
       {
         spath.erase(spath.length() - 1);
       }
       int rpos = spath.rfind("/");
       if (rpos == STR_NPOS)
       {
         listrc = SFS_ERROR;
         retc = ENOENT;
       }
       else
       {
         // this is an 'ls <file>' command which has to return only one entry!
         ls_file.assign(spath, rpos + 1);
         spath.erase(rpos);
         listrc = 0;
       }
     }

     bool translateids = true;
     if ((option.find("n")) != STR_NPOS)
     {
       translateids = false;
     }

     if ((option.find("s")) != STR_NPOS)
     {
       // just return '0' if this is a directory
       return SFS_OK;
     }

     if (!listrc)
     {
       const char* val;
       while ((ls_file.length() && (val = ls_file.c_str())) || (val = dir.nextEntry()))
       {
         // this return's a single file or a (filtered) directory list
         XrdOucString entryname = val;
         if (((option.find("a")) == STR_NPOS) && entryname.beginswith("."))
         {
           // skip over . .. and hidden files
           continue;
         }
         if ((filter.length()) && (filter != entryname))
         {
           // apply filter
           continue;
         }
         if ((((option.find("l")) == STR_NPOS)) && ((option.find("F")) == STR_NPOS))
         {
           stdOut += val;
           stdOut += "\n";
         }
         else
         {
           // yeah ... that is actually castor code ;-)
           char t_creat[14];
           char ftype[8];
           unsigned int ftype_v[7];
           char fmode[10];
           int fmode_v[9];
           char modestr[11];
           strcpy(ftype, "pcdb-ls");
           ftype_v[0] = S_IFIFO;
           ftype_v[1] = S_IFCHR;
           ftype_v[2] = S_IFDIR;
           ftype_v[3] = S_IFBLK;
           ftype_v[4] = S_IFREG;
           ftype_v[5] = S_IFLNK;
           ftype_v[6] = S_IFSOCK;
           strcpy(fmode, "rwxrwxrwx");
           fmode_v[0] = S_IRUSR;
           fmode_v[1] = S_IWUSR;
           fmode_v[2] = S_IXUSR;
           fmode_v[3] = S_IRGRP;
           fmode_v[4] = S_IWGRP;
           fmode_v[5] = S_IXGRP;
           fmode_v[6] = S_IROTH;
           fmode_v[7] = S_IWOTH;
           fmode_v[8] = S_IXOTH;
           // return full information
           XrdOucString statpath = spath;
           statpath += "/";
           statpath += val;
           while (statpath.replace("//", "/"))
           {
           }
           struct stat buf;
           if (gOFS->_stat(statpath.c_str(), &buf, *error, *pVid, (const char*) 0))
           {
             stdErr += "error: unable to stat path ";
             stdErr += statpath;
             stdErr += "\n";
             retc = errno;
           }
           else
           {
             int i = 0;
             // TODO: convert virtual IDs back
             XrdOucString suid = "";
             suid += (int) buf.st_uid;
             XrdOucString sgid = "";
             sgid += (int) buf.st_gid;
             XrdOucString sizestring = "";
             struct tm *t_tm;
             struct tm t_tm_local;
             t_tm = localtime_r(&buf.st_ctime, &t_tm_local);

             strcpy(modestr, "----------");
             for (i = 0; i < 6; i++) if (ftype_v[i] == (S_IFMT & buf.st_mode)) break;
             modestr[0] = ftype[i];
             for (i = 0; i < 9; i++) if (fmode_v[i] & buf.st_mode) modestr[i + 1] = fmode[i];
             if (S_ISUID & buf.st_mode) modestr[3] = 's';
             if (S_ISGID & buf.st_mode) modestr[6] = 's';
             if (S_ISVTX & buf.st_mode) modestr[9] = '+';
             if (translateids)
             {
               {
                 // try to translate with password database
                 int terrc = 0;
                 std::string username = "";
                 username = eos::common::Mapping::UidToUserName(buf.st_uid, terrc);
                 if (!terrc)
                 {
                   char uidlimit[16];
                   snprintf(uidlimit, 12, "%s", username.c_str());
                   suid = uidlimit;
                 }
               }

               {
                 // try to translate with password database
                 std::string groupname = "";
                 int terrc = 0;
                 groupname = eos::common::Mapping::GidToGroupName(buf.st_gid, terrc);
                 if (!terrc)
                 {
                   char gidlimit[16];
                   snprintf(gidlimit, 12, "%s", groupname.c_str());
                   sgid = gidlimit;
                 }
               }
             }

             strftime(t_creat, 13, "%b %d %H:%M", t_tm);
             char lsline[4096];
             XrdOucString dirmarker = "";
             if ((option.find("F")) != STR_NPOS)
               dirmarker = "/";
             if (modestr[0] != 'd')
               dirmarker = "";

             if ((option.find("i")) != STR_NPOS)
             {
               // add inode information
               char sinode[16];
               bool isfile = (modestr[0] != 'd');
               snprintf(sinode, 16, "%llu", (unsigned long long) (isfile ? (buf.st_ino >> 28) : buf.st_ino));
               sprintf(lsline, "%-16s", sinode);
               stdOut += lsline;
             }

             sprintf(lsline, "%s %3d %-8.8s %-8.8s %12s %s %s%s\n", modestr, (int) buf.st_nlink,
                     suid.c_str(), sgid.c_str(), eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) buf.st_size), t_creat, val, dirmarker.c_str());
             if ((option.find("l")) != STR_NPOS)
               stdOut += lsline;

             else
             {
               stdOut += val;
               stdOut += dirmarker;
               stdOut += "\n";
             }
           }
         }
         if (ls_file.length())
         {
           // this was a single file to be listed
           break;
         }
       }
       if (!ls_file.length())
       {
         dir.close();
       }
     }
     else
     {
       stdErr += "error: unable to open directory";
       retc = errno;
     }
   }
 }
 return SFS_OK;
}

EOSMGMNAMESPACE_END
