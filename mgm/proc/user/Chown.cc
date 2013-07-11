// ----------------------------------------------------------------------
// File: proc/admin/Chown.cc
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
#include "mgm/Macros.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Chown ()
{
 XrdOucString spath = pOpaque->Get("mgm.path");
 XrdOucString option = pOpaque->Get("mgm.chown.option");
 XrdOucString owner = pOpaque->Get("mgm.chown.owner");

 const char* inpath = spath.c_str();

 NAMESPACEMAP;
 info = 0;
 if (info)info = 0; // for compiler happyness
 PROC_BOUNCE_ILLEGAL_NAMES;
 PROC_BOUNCE_NOT_ALLOWED;

 spath = path;
 if ((!spath.length()) || (!owner.length()))
 {
   stdErr = "error: you have to provide a path and the owner to set!\n";
   retc = EINVAL;
 }
 else
 {
   // find everything to be modified
   std::map<std::string, std::set<std::string> > found;
   std::map<std::string, std::set<std::string> >::const_iterator foundit;
   std::set<std::string>::const_iterator fileit;

   if (option == "r")
   {
     if (gOFS->_find(spath.c_str(), *mError, stdErr, *pVid, found))
     {
       stdErr += "error: unable to search in path";
       retc = errno;
     }
   }
   else
   {
     // the single dir case
     found[spath.c_str()].size();
     //          found_dirs->resize(1);
     //          (*found_dirs)[0].push_back(spath.c_str());
   }

   std::string uid = owner.c_str();
   std::string gid = owner.c_str();
   bool failure = false;

   uid_t uidt;
   gid_t gidt;

   int dpos = 0;

   if ((dpos = owner.find(":")) != STR_NPOS)
   {
     uid.erase(dpos);
     gid.erase(0, dpos + 1);
   }
   else
   {
     gid = "0";
   }

   uidt = (uid_t) atoi(uid.c_str());
   gidt = (gid_t) atoi(gid.c_str());

   if ((uid != "0") && (!uidt))
   {
     int terrc = 0;
     uidt = eos::common::Mapping::UserNameToUid(uid, terrc);

     if (terrc)
     {
       stdErr = "error: I cannot translate your uid string using the pwd database";
       retc = terrc;
       failure = true;
     }
   }

   if ((gid != "0") && (!gidt))
   {
     // try to translate with password database
     int terrc = 0;
     gidt = eos::common::Mapping::GroupNameToGid(gid, terrc);
     if (terrc)
     {
       // cannot translate this name
       stdErr = "error: I cannot translate your gid string using the pwd database";
       retc = terrc;
       failure = true;
     }
   }

   if (pVid->uid && ((!uidt) || (!gidt)))
   {
     stdErr = "error: you are changing to uid/gid=0 but you are not root!";
     retc = EPERM;
     failure = true;
   }

   if (!failure)
   {
     // for directories
     for (foundit = found.begin(); foundit != found.end(); foundit++)
     {
       if (gOFS->_chown(foundit->first.c_str(), uidt, gidt, *mError, *pVid, (char*) 0))
       {
         stdErr += "error: unable to chown of directory ";
         stdErr += foundit->first.c_str();
         stdErr += "\n";
         retc = errno;
       }
       else
       {
         stdOut += "success: owner of directory ";
         stdOut += foundit->first.c_str();
         stdOut += " is now ";
         stdOut += "uid=";
         stdOut += uid.c_str();
         if (!pVid->uid)
         {
           if (gidt)
           {
             stdOut += " gid=";
             stdOut += gid.c_str();
           }
           stdOut += "\n";
         }
       }
     }

     // for files
     for (foundit = found.begin(); foundit != found.end(); foundit++)
     {
       for (fileit = foundit->second.begin(); fileit != foundit->second.end(); fileit++)
       {
         std::string fpath = foundit->first;
         fpath += *fileit;
         if (gOFS->_chown(fpath.c_str(), uidt, gidt, *mError, *pVid, (char*) 0))
         {
           stdErr += "error: unable to chown of file ";
           stdErr += fpath.c_str();
           stdErr += "\n";
           retc = errno;
         }
         else
         {
           stdOut += "success: owner of file ";
           stdOut += fpath.c_str();
           stdOut += " is now ";
           stdOut += "uid=";
           stdOut += uid.c_str();
           if (!pVid->uid)
           {
             if (gidt)
             {
               stdOut += " gid=";
               stdOut += gid.c_str();
             }
             stdOut += "\n";
           }
         }
       }
     }
   }
 }
 return SFS_OK;
}

EOSMGMNAMESPACE_END
