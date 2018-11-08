//------------------------------------------------------------------------------
// File: ContentAddressableStore.cc
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

#include "ContentAddressableStore.hh"
#include "DirectoryIterator.hh"
#include "Utils.hh"
#include "common/SymKeys.hh"
#include "common/Logging.hh"
#include <sys/stat.h>

//------------------------------------------------------------------------------
//! Constructor.
//------------------------------------------------------------------------------
ContentAddressableStore::ContentAddressableStore(const std::string& repository_,
  std::chrono::milliseconds timeoutDuration_, bool fake_)
: repository(chopTrailingSlashes(repository_)),
  timeoutDuration(timeoutDuration_),
  fake(fake_) {

  if(!fake) {
    struct stat repostat;
    if (::stat(repository.c_str(), &repostat) != 0) {
      THROW("Cannot stat content-addressable-store repository: " << repository);
    }

    if(!S_ISDIR(repostat.st_mode)) {
      THROW("Repository path is not a directory: " << repository);
    }

    cleanupThread.reset(&ContentAddressableStore::runCleanupThread, this);
  }
}

//------------------------------------------------------------------------------
//! Cleanup thread loop
//------------------------------------------------------------------------------
void ContentAddressableStore::runCleanupThread(ThreadAssistant &assistant) {
  while(!assistant.terminationRequested()) {
    assistant.wait_for(timeoutDuration);
    singleCleanupLoop(assistant);
  }
}

//------------------------------------------------------------------------------
//! Single cleanup loop
//------------------------------------------------------------------------------
void ContentAddressableStore::singleCleanupLoop(ThreadAssistant &assistant) {
  DirectoryIterator iterator(repository);

  while(iterator.next()) {
    // TODO LOL
  }

  if(!iterator.ok()) {
    eos_static_crit("ContentAddressableStore:: Cleanup thread encountered an error while iterating over the repository: %s", iterator.err().c_str());
  }
}

//------------------------------------------------------------------------------
//! Store the given contents inside the store. Returns the full filesystem
//! path on which the contents were stored.
//------------------------------------------------------------------------------
std::string ContentAddressableStore::put(const std::string &contents) {
  std::string path = formPath(contents);

  if(!fake && !writeFile(path, contents)) {
    eos_static_crit("ContentAddressableStore: Could not write path: %s", path.c_str());
    return "";
  }

  return path;
}

//------------------------------------------------------------------------------
//! Form path for given contents
//------------------------------------------------------------------------------
std::string ContentAddressableStore::formPath(const std::string &contents) {
  return SSTR(
    repository << "/" << "eos-fusex-store-" << eos::common::SymKey::Sha256(contents)
  );
}

