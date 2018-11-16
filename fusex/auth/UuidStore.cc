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

//------------------------------------------------------------------------------
//! Constructor.
//------------------------------------------------------------------------------
UuidStore::UuidStore(const std::string& repository_,
  std::chrono::milliseconds timeoutDuration_)
: repository(chopTrailingSlashes(repository_)),
  timeoutDuration(timeoutDuration_) {

  struct stat repostat;
  if (::stat(repository.c_str(), &repostat) != 0) {
    THROW("Cannot stat uuid-store repository: " << repository);
  }

  if(!S_ISDIR(repostat.st_mode)) {
    THROW("Repository path is not a directory: " << repository);
  }

  cleanupThread.reset(&UuidStore::runCleanupThread, this);
}

//------------------------------------------------------------------------------
//! Cleanup thread loop
//------------------------------------------------------------------------------
void UuidStore::runCleanupThread(ThreadAssistant &assistant) {
  while(!assistant.terminationRequested()) {
    assistant.wait_for(timeoutDuration);
    singleCleanupLoop(assistant);
  }
}

//------------------------------------------------------------------------------
//! Single cleanup loop
//------------------------------------------------------------------------------
void UuidStore::singleCleanupLoop(ThreadAssistant &assistant) {
  DirectoryIterator iterator(repository);

  while(iterator.next()) {
    // TODO LOL
  }

  if(!iterator.ok()) {
    eos_static_crit("UuidStore:: Cleanup thread encountered an error while iterating over the repository: %s", iterator.err().c_str());
  }
}

//------------------------------------------------------------------------------
//! Store the given contents inside the store. Returns the full filesystem
//! path on which the contents were stored.
//------------------------------------------------------------------------------
std::string UuidStore::put(const std::string &contents) {
  std::string path = SSTR(repository << "/" << "eos-fusex-store-"
    << generateUuid());

  if(!writeFile(path, contents)) {
    eos_static_crit("UuidStore: Could not write path: %s", path.c_str());
    return "";
  }

  return path;
}

//------------------------------------------------------------------------------
//! Make uuid
//------------------------------------------------------------------------------
std::string UuidStore::generateUuid() {
  char buffer[64];

  uuid_t uuid;
  uuid_generate_random(uuid);
  uuid_unparse(uuid, buffer);

  return std::string(buffer);
}
