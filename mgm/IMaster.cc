/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#include "mgm/IMaster.hh"
#include "mgm/config/IConfigEngine.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "common/ParseUtils.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Add to master Log
//------------------------------------------------------------------------------
void
IMaster::MasterLog(const char* log)
{
  if (log && strlen(log)) {
    mLog += log;
    mLog += '\n';
  }
}

//------------------------------------------------------------------------------
// Create status file
//------------------------------------------------------------------------------
bool
IMaster:: CreateStatusFile(const char* path)
{
  struct stat buf;

  if (::stat(path, &buf)) {
    int fd = 0;

    if ((fd = ::creat(path, S_IRWXU | S_IRGRP | S_IROTH)) == -1) {
      MasterLog(eos_static_log(LOG_ERR, "msg=\"failed to create %s\" errno=%d", path,
                               errno));
      return false;
    }

    close(fd);
  }

  return true;
}

//------------------------------------------------------------------------------
// Remove status file
//------------------------------------------------------------------------------
bool
IMaster::RemoveStatusFile(const char* path)
{
  struct stat buf;

  if (!::stat(path, &buf)) {
    if (::unlink(path)) {
      MasterLog(eos_static_log(LOG_ERR, "msg=\"failed to unlink %s\" errno=%d",
                               path, errno));
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Populate namespace cache configuration
//------------------------------------------------------------------------------
void IMaster::FillNsCacheConfig(IConfigEngine* configEngine,
                                std::map<std::string, std::string>& namespaceConfig) const
{
  std::string nfilesStr;
  uint64_t nfiles = 40'000'000;

  if (configEngine->Get("ns", "cache-size-nfiles", nfilesStr)) {
    if (!common::ParseUInt64(nfilesStr, nfiles)) {
      eos_static_crit("Could not parse 'cache-size-nfiles' configuration value");
    }
  }

  std::string ndirsStr;
  uint64_t ndirs = 5'000'000;

  if (configEngine->Get("ns", "cache-size-ndirs", ndirsStr)) {
    if (!common::ParseUInt64(ndirsStr, ndirs)) {
      eos_static_crit("Could not parse 'cache-size-ndirs' configuration value");
    }
  }

  namespaceConfig[constants::sMaxNumCacheFiles] = std::to_string(nfiles);
  namespaceConfig[constants::sMaxNumCacheDirs] = std::to_string(ndirs);
}

EOSMGMNAMESPACE_END
