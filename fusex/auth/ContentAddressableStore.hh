//------------------------------------------------------------------------------
// File: ContentAddressableStore.hh
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

#ifndef FUSEX_CONTENT_ADDRESSABLE_STORE_HH
#define FUSEX_CONTENT_ADDRESSABLE_STORE_HH

#include "common/AssistedThread.hh"

#include <string>
#include <chrono>

//------------------------------------------------------------------------------
//! A filesystem-backed content-addressable store, with configurable, automatic
//! file purging.
//------------------------------------------------------------------------------
class ContentAddressableStore {
public:
  //----------------------------------------------------------------------------
  //! Constructor. Provide the repository (directory on the physical
  //! filesystem), as well as the timeout duration for automatic purging.
  //!
  //! "Fake" means nothing is actually stored on the filesystem, we're running
  //! tests.
  //----------------------------------------------------------------------------
  ContentAddressableStore(const std::string& repository,
   std::chrono::milliseconds timeoutDuration,
   bool fake = false);

  //----------------------------------------------------------------------------
  //! Store the given contents inside the store. Returns the full filesystem
  //! path on which the contents were stored.
  //----------------------------------------------------------------------------
  std::string put(const std::string &contents);

private:
  //----------------------------------------------------------------------------
  //! Cleanup thread loop
  //----------------------------------------------------------------------------
  void runCleanupThread(ThreadAssistant &assistant);

  //----------------------------------------------------------------------------
  //! Single cleanup loop
  //----------------------------------------------------------------------------
  void singleCleanupLoop(ThreadAssistant &assistant);

  //----------------------------------------------------------------------------
  //! Form path for given contents
  //----------------------------------------------------------------------------
  std::string formPath(const std::string &contents);

  std::string repository;
  std::chrono::milliseconds timeoutDuration;
  bool fake;
  AssistedThread cleanupThread;
};

#endif

