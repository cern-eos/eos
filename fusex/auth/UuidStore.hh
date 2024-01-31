//------------------------------------------------------------------------------
// File: UuidStore.hh
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

#ifndef FUSEX_UUID_STORE_HH
#define FUSEX_UUID_STORE_HH

#include "common/AssistedThread.hh"

#include <string>
#include <chrono>

//------------------------------------------------------------------------------
//! A filesystem-backed store - every write is assigned to a specific UUID.
//! Contents are not meant to persist after process restart, and in fact will
//! be cleared out explicitly.
//------------------------------------------------------------------------------
class UuidStore
{
public:
  //----------------------------------------------------------------------------
  //! Constructor. Provide the repository, that is the directory to use
  //! on the physical filesystem.
  //----------------------------------------------------------------------------
  UuidStore(const std::string& repository);

  //----------------------------------------------------------------------------
  //! Store the given contents inside the store. Returns the full filesystem
  //! path on which the contents were stored.
  //----------------------------------------------------------------------------
  std::string put(const std::string& contents);

  //----------------------------------------------------------------------------
  //! Unlink leftover credential files from previous runs - if eosxd crashes,
  //! this can happen. Only unlink files matching our prefix, so that in case
  //! of misconfiguration we don't wipe out important files.
  //----------------------------------------------------------------------------
  void initialCleanup();

private:
  //----------------------------------------------------------------------------
  //! Make uuid
  //----------------------------------------------------------------------------
  static std::string generateUuid();

  std::string repository;
};

#endif

