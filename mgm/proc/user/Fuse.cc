// ----------------------------------------------------------------------
// File: proc/user/Fuse.cc
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

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Fuse ()
{
 gOFS->MgmStats.Add("Fuse-Dirlist", pVid->uid, pVid->gid, 1);
 XrdOucString path = opaque->Get("mgm.path");
 resultStream = "inodirlist: retc=";
 if (!path.length())
 {
   resultStream += EINVAL;
 }
 else
 {
   XrdMgmOfsDirectory* inodir = (XrdMgmOfsDirectory*) gOFS->newDir((char*) "");
   if (!inodir)
   {
     resultStream += ENOMEM;
     return SFS_ERROR;
   }

   if ((retc = inodir->open(path.c_str(), *pVid, 0)) != SFS_OK)
   {
     delete inodir;
     retc = -retc;
     resultStream += retc;
     len = resultStream.length();
     offset = 0;
     return SFS_OK;
   }

   const char* entry;

   resultStream += 0;
   resultStream += " ";

   unsigned long long inode = 0;

   char inodestr[256];
   size_t dotend = 0;
   size_t dotstart = resultStream.length();

   while ((entry = inodir->nextEntry()))
   {
     bool isdot = false;
     bool isdotdot = false;

     XrdOucString whitespaceentry = entry;

     if (whitespaceentry == ".")
     {
       isdot = true;
     }
     if (whitespaceentry == "..")
     {
       isdotdot = true;
     }
     whitespaceentry.replace(" ", "%20");
     if ((!isdot) && (!isdotdot))
     {
       resultStream += whitespaceentry;
       resultStream += " ";
     }
     if (isdot)
     {
       // the . and .. has to be streamed as first entries
       resultStream.insert(". ", dotstart);
     }
     if (isdotdot)
     {
       resultStream.insert(".. ", dotend);
     }

     XrdOucString statpath = path;
     statpath += "/";
     statpath += entry;

     eos::common::Path cPath(statpath.c_str());

     // attach MD to get inode number
     eos::FileMD* fmd = 0;
     inode = 0;

     //-------------------------------------------
     {
       eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
       try
       {
         fmd = gOFS->eosView->getFile(cPath.GetPath());
         inode = fmd->getId() << 28;
       }
       catch (eos::MDException &e)
       {
         errno = e.getErrno();
         eos_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
       }
     }
     //-------------------------------------------

     // check if that is a directory in case
     if (!fmd)
     {
       eos::ContainerMD* dir = 0;
       //-------------------------------------------
       eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
       try
       {
         dir = gOFS->eosView->getContainer(cPath.GetPath());
         inode = dir->getId();
       }
       catch (eos::MDException &e)
       {
         dir = 0;
         eos_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
       }
       //-------------------------------------------
     }
     sprintf(inodestr, "%lld", inode);
     if ((!isdot) && (!isdotdot))
     {
       resultStream += inodestr;
       resultStream += " ";
     }
     else
     {
       if (isdot)
       {
         resultStream.insert(inodestr, dotstart + 2);
         resultStream.insert(" ", dotstart + 2 + strlen(inodestr));
         dotend = dotstart + 2 + strlen(inodestr) + 1;
       }
       else
       {
         resultStream.insert(inodestr, dotend + 3);
         resultStream.insert(" ", dotend + strlen(inodestr) + 3);
       }
     }
   }

   inodir->close();
   delete inodir;
   eos_debug("returning resultstream %s", resultStream.c_str());
   len = resultStream.length();
   offset = 0;
 }
 return SFS_OK;
}

EOSMGMNAMESPACE_END
