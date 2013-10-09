// ----------------------------------------------------------------------
// File: Fmd.cc
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

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "common/FileId.hh"
#include "common/Path.hh"
#include "common/Attr.hh"
#include "fst/FmdDbMap.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "fst/Fmd.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFileSystem.hh"
/*----------------------------------------------------------------------------*/
#include <stdio.h>
#include <sys/mman.h>
#include <fts.h>
#include <iostream>
#include <fstream>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/** 
 * Dump an Fmd record to stderr
 * 
 * @param fmd handle to the Fmd struct
 */

/*----------------------------------------------------------------------------*/
void
FmdHelper::Dump (struct Fmd* fmd)
{
  fprintf(stderr, "%08" PRIx64 " %06" PRIu64 " %04" PRIu32 " %010" PRIu32 " %010" PRIu32 " %010" PRIu32 " %010" PRIu32 " %010" PRIu32 " %010" PRIu32 " %010" PRIu32 " %08" PRIu64 " %08" PRIu64 " %08" PRIu64 " %s %s %s %03" PRIu32 " %05" PRIu32 " %05" PRIu32 "\n",
          fmd->fid(),
          fmd->cid(),
          fmd->fsid(),
          fmd->ctime(),
          fmd->ctime_ns(),
          fmd->mtime(),
          fmd->mtime_ns(),
          fmd->atime(),
          fmd->atime_ns(),
          fmd->checktime(),
          fmd->size(),
          fmd->disksize(),
          fmd->mgmsize(),
          fmd->checksum().c_str(),
          fmd->diskchecksum().c_str(),
          fmd->mgmchecksum().c_str(),
          fmd->lid(),
          fmd->uid(),
          fmd->gid());
}

/*----------------------------------------------------------------------------*/
/** 
 * Convert a Fmd struct into an env representation
 * 
 * 
 * @return env representation
 */

/*----------------------------------------------------------------------------*/
XrdOucEnv*
FmdHelper::FmdToEnv ()
{
  char serialized[1024 * 64];
  sprintf(serialized, "id=%" PRIu64 "&cid=%" PRIu64 "&ctime=%" PRIu32 "&ctime_ns=%" PRIu32 "&mtime=%" PRIu32 "&"
          "mtime_ns=%" PRIu32 "&size=%" PRIu64 "&checksum=%s&lid=%" PRIu32 "&uid=%" PRIu32 "&gid=%" PRIu32 "&",
          fMd.fid(), fMd.cid(), fMd.ctime(), fMd.ctime_ns(), fMd.mtime(), fMd.mtime_ns(), fMd.size(),
          fMd.checksum().c_str(), fMd.lid(), fMd.uid(), fMd.gid());
  return new XrdOucEnv(serialized);
}

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_END



//  LocalWords:  ResyncAllMgm
