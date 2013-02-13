// ----------------------------------------------------------------------
// File: proc/admin/Group.cc
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
#include "mgm/FsView.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Group ()
{
 if (mSubCmd == "ls")
 {
   {
     std::string output = "";
     std::string format = "";
     std::string mListFormat = "";
     format = FsView::GetGroupFormat(std::string(mOutFormat.c_str()));
     if ((mOutFormat == "l"))
       mListFormat = FsView::GetFileSystemFormat(std::string(mOutFormat.c_str()));

     if ((mOutFormat == "IO"))
     {
       mListFormat = FsView::GetFileSystemFormat(std::string("io"));
       mOutFormat = "io";
     }

     eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
     FsView::gFsView.PrintGroups(output, format, mListFormat, mSelection);
     stdOut += output.c_str();
   }
 }

 if (mSubCmd == "set")
 {
   if (pVid->uid == 0)
   {
     std::string groupname = (pOpaque->Get("mgm.group")) ? pOpaque->Get("mgm.group") : "";
     std::string status = (pOpaque->Get("mgm.group.state")) ? pOpaque->Get("mgm.group.state") : "";
     std::string key = "status";

     if ((!groupname.length()) || (!status.length()))
     {
       stdErr = "error: illegal parameters";
       retc = EINVAL;
     }
     else
     {
       eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
       if (!FsView::gFsView.mGroupView.count(groupname))
       {
         stdOut = "info: creating group '";
         stdOut += groupname.c_str();
         stdOut += "'";

         if (!FsView::gFsView.RegisterGroup(groupname.c_str()))
         {
           std::string groupconfigname = eos::common::GlobalConfig::gConfig.QueuePrefixName(gOFS->GroupConfigQueuePrefix.c_str(), groupname.c_str());
           retc = EIO;
           stdErr = "error: cannot register group <";
           stdErr += groupname.c_str();
           stdErr += ">";
         }
       }

       if (!retc)
       {
         // set this new group to offline
         if (!FsView::gFsView.mGroupView[groupname]->SetConfigMember(key, status, true, "/eos/*/mgm"))
         {
           stdErr = "error: cannto set config status";
           retc = EIO;
         }
         if (status == "on")
         {
           // recompute the drain status in this group
           FsGroup::const_iterator git;
           bool setactive = false;
           FileSystem* fs = 0;
           if (FsView::gFsView.mGroupView.count(groupname))
           {
             for (git = FsView::gFsView.mGroupView[groupname]->begin(); git != FsView::gFsView.mGroupView[groupname]->end(); git++)
             {
               if (FsView::gFsView.mIdView.count(*git))
               {
                 int drainstatus = (eos::common::FileSystem::GetDrainStatusFromString(FsView::gFsView.mIdView[*git]->GetString("stat.drain").c_str()));
                 if ((drainstatus == eos::common::FileSystem::kDraining) ||
                     (drainstatus == eos::common::FileSystem::kDrainStalling))
                 {
                   // if any group filesystem is draining, all the others have to enable the pull for draining!
                   setactive = true;
                 }
               }
             }
             for (git = FsView::gFsView.mGroupView[groupname]->begin(); git != FsView::gFsView.mGroupView[groupname]->end(); git++)
             {
               fs = FsView::gFsView.mIdView[*git];
               if (fs)
               {
                 if (setactive)
                 {
                   if (fs->GetString("stat.drainer") != "on")
                   {
                     fs->SetString("stat.drainer", "on");
                   }
                 }
                 else
                 {
                   if (fs->GetString("stat.drainer") != "off")
                   {
                     fs->SetString("stat.drainer", "off");
                   }
                 }
               }
             }
           }
         }
         if (status == "off")
         {
           // disable all draining in this group
           // recompute the drain status in this group
           FsGroup::const_iterator git;
           FileSystem* fs = 0;
           if (FsView::gFsView.mGroupView.count(groupname))
           {
             for (git = FsView::gFsView.mGroupView[groupname]->begin(); git != FsView::gFsView.mGroupView[groupname]->end(); git++)
             {
               fs = FsView::gFsView.mIdView[*git];
               if (fs)
               {
                 fs->SetString("stat.drainer", "off");
               }
             }
           }
         }
       }
     }
   }
   else
   {
     retc = EPERM;
     stdErr = "error: you have to take role 'root' to execute this command";
   }
 }

 if (mSubCmd == "rm")
 {
   if (pVid->uid == 0)
   {
     std::string groupname = (pOpaque->Get("mgm.group")) ? pOpaque->Get("mgm.group") : "";
     if ((!groupname.length()))
     {
       stdErr = "error: illegal parameters";
       retc = EINVAL;
     }
     else
     {
       eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);
       if (!FsView::gFsView.mGroupView.count(groupname))
       {
         stdErr = "error: no such group '";
         stdErr += groupname.c_str();
         stdErr += "'";
         retc = ENOENT;
       }
       else
       {
         std::string groupconfigname = eos::common::GlobalConfig::gConfig.QueuePrefixName(FsGroup::sGetConfigQueuePrefix(), groupname.c_str());
         if (!eos::common::GlobalConfig::gConfig.SOM()->DeleteSharedHash(groupconfigname.c_str()))
         {
           stdErr = "error: unable to remove config of group '";
           stdErr += groupname.c_str();
           stdErr += "'";
           retc = EIO;
         }
         else
         {
           if (FsView::gFsView.UnRegisterGroup(groupname.c_str()))
           {
             stdOut = "success: removed group '";
             stdOut += groupname.c_str();
             stdOut += "'";
           }
           else
           {
             stdErr = "error: unable to unregister group '";
             stdErr += groupname.c_str();
             stdErr += "'";
           }
         }
       }
     }
   }
   else
   {
     retc = EPERM;
     stdErr = "error: you have to take role 'root' to execute this command";
   }
 }
 return SFS_OK;
}

EOSMGMNAMESPACE_END
