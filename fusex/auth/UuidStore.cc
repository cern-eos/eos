//------------------------------------------------------------------------------
// File: UuidStore.cc
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "UuidStore.hh"
#include "DirectoryIterator.hh"
#include "Utils.hh"
#include "common/SymKeys.hh"
#include "common/Logging.hh"
#include <uuid/uuid.h>
#include <sys/stat.h>


//----------------------------------------------------------------------------
//! Constructor. Provide the repository, that is the directory to use
//! on the physical filesystem.
//----------------------------------------------------------------------------
UuidStore::UuidStore(const std::string& repository_)
  : repository(chopTrailingSlashes(repository_))
{
  struct stat repostat;

  if (::stat(repository.c_str(), &repostat) != 0) {
    THROW("Cannot stat uuid-store repository: " << repository);
  }

  if (!S_ISDIR(repostat.st_mode)) {
    THROW("Repository path is not a directory: " << repository);
  }

  initialCleanup();
}

//------------------------------------------------------------------------------
//! Unlink leftover credential files from previous runs - if eosxd crashes,
//! this can happen. Only unlink files matching our prefix, so that in case
//! of misconfiguration we don't wipe out important files.
//------------------------------------------------------------------------------
void UuidStore::initialCleanup()
{
  DirectoryIterator iterator(repository);
  struct dirent* current = nullptr;

  while ((current = iterator.next())) {
    if (startsWith(current->d_name, "eos-fusex-uuid-store-")) {
      if (unlink(SSTR(repository << "/" << current->d_name).c_str()) != 0) {
        eos_static_crit("UuidStore:: Could not delete %s during initial cleanup, errno %d",
                        current->d_name, errno);
      }
    } else {
      eos_static_crit("Found file in credential store with suspicious filename, should not be there: %s. Not unlinking.",
                      current->d_name);
    }
  }

  if (!iterator.ok()) {
    eos_static_crit("UuidStore:: Cleanup thread encountered an error while iterating over the repository: %s",
                    iterator.err().c_str());
  }
}

//------------------------------------------------------------------------------
//! Store the given contents inside the store. Returns the full filesystem
//! path on which the contents were stored.
//------------------------------------------------------------------------------
std::string UuidStore::put(const std::string& contents)
{
  std::string path = SSTR(repository << "/" << "eos-fusex-uuid-store-"
                          << generateUuid());

  if (!writeFile600(path, contents)) {
    eos_static_crit("UuidStore: Could not write path: %s", path.c_str());
    return "";
  }

  return path;
}

//------------------------------------------------------------------------------
//! Make uuid
//------------------------------------------------------------------------------
std::string UuidStore::generateUuid()
{
  char buffer[64];
  uuid_t uuid;
  uuid_generate_random(uuid);
  uuid_unparse(uuid, buffer);
  return std::string(buffer);
}
