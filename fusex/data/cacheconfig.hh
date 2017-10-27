//------------------------------------------------------------------------------
//! @file cacheconfig.hh
//! @author Andreas-Joachim Peters CERN
//! @brief cacheconfig class
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#ifndef FUSE_CACHECONFIG_HH_
#define FUSE_CACHECONFIG_HH_

enum cache_t
{
  INVALID, MEMORY, DISK
};

struct cacheconfig
{
  cache_t type;
  std::string location;
  uint64_t total_file_cache_size; // total size of the file cache
  uint64_t per_file_cache_max_size; // per file maximum file cache size
  uint64_t total_file_journal_size; // total size of the journal cache
  uint64_t per_file_journal_max_size; // per file maximum journal cache size
  std::string journal;
  bool clean_on_startup; // indicate that the cache is not reusable after restart
};

#endif
