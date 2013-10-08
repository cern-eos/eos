// ----------------------------------------------------------------------
// File: FmdSqlite.cc
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
 * Comparison function for modification times
 *
 * @param a pointer to a filestat struct
 * @param b pointer to a filestat struct
 *
 * @return difference between the two modification times within the filestat struct
 */

/*----------------------------------------------------------------------------*/
int
FmdHandler::CompareMtime (const void* a, const void *b)
{

  struct filestat
  {
    struct stat buf;
    char filename[1024];
  };
  return ( (((struct filestat*) b)->buf.st_mtime) - ((struct filestat*) a)->buf.st_mtime);
}



EOSFSTNAMESPACE_END



//  LocalWords:  ResyncAllMgm
