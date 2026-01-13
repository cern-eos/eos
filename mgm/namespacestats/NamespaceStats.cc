// ----------------------------------------------------------------------
// File: NamespaceStats.cc
// Author: Cedric Caffy - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#include "NamespaceStats.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/stat/Stat.hh"

EOSMGMNAMESPACE_BEGIN

void
NamespaceStats::Add(const char* tag, uid_t uid, gid_t gid, unsigned long val) {
  //No need for lock as the lock is already present in the Add() method of the MgmStats
  gOFS->MgmStats.Add(tag,uid,gid,val);
}

void NamespaceStats::AddExec(const char* tag, float exectime) {
  //No need for lock as the lock is already present in the AddExec() method of the MgmStats
  gOFS->MgmStats.AddExec(tag,exectime);
}

EOSMGMNAMESPACE_END
