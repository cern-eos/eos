// ----------------------------------------------------------------------
// File: DirCache.hh
// Author: Elvin Sindrilaru/Andreas-Joachim Peters - CERN
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

#ifndef __CACHED_DIR__HH__
#define __CACHED_DIR__HH__

/*--------------------------------------------------------------------------------------------------------*/
#include "xrdposix.hh"
#include <fuse/fuse_lowlevel.h>
/*--------------------------------------------------------------------------------------------------------*/

#ifdef __cplusplus
extern "C" {
#endif

  void cache_init();
  int get_dir_from_cache(fuse_ino_t inode, time_t mtv_sec, char *fullpath, struct dirbuf **b);
  void sync_dir_in_cache(fuse_ino_t dir_inode, char *name, int nentries, time_t mtv_sec, struct dirbuf *b);

  int get_entry_from_dir(fuse_req_t req, fuse_ino_t dir_inode, const char* ifullpath);
  void add_entry_to_dir(fuse_ino_t dir_inode, fuse_ino_t entry_inode, const char *entry_name, struct fuse_entry_param *e);

#ifdef __cplusplus
}
#endif

#endif
