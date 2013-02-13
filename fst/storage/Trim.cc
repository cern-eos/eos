// ----------------------------------------------------------------------
// File: Trim.cc
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
#include "fst/storage/Storage.hh"
#include "fst/XrdFstOfs.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Storage::Trim ()
{
  // this thread trim's the SQLITE DB every 30 days
 while (1)
 {
   // sleep for a month
   XrdSysTimer sleeper;
   sleeper.Snooze(30 * 86400);
   std::map<eos::common::FileSystem::fsid_t, sqlite3*>::iterator it;

   for (it = gFmdSqliteHandler.GetDB()->begin(); it != gFmdSqliteHandler.GetDB()->end(); ++it)
   {
     eos_static_info("Trimming fsid=%llu ", it->first);
     int fsid = it->first;

     if (!gFmdSqliteHandler.TrimDBFile(fsid))
     {
       eos_static_err("Cannot trim the SQLITE DB file for fsid=%llu ", it->first);
     }
     else
     {
       eos_static_info("Called vaccuum on SQLITE DB file for fsid=%llu ", it->first);
     }
   }
 }
}

EOSFSTNAMESPACE_END


