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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief  Inode provider used both for directories and files.
//------------------------------------------------------------------------------

#pragma once
#include "namespace/Namespace.hh"
#include "namespace/ns_quarkdb/persistency/NextInodeProvider.hh"
#include <mutex>
#include <qclient/structures/QHash.hh>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class UnifiedInodeProvider
//------------------------------------------------------------------------------
class UnifiedInodeProvider {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  UnifiedInodeProvider();

  //----------------------------------------------------------------------------
  //! Configure
  //----------------------------------------------------------------------------
  void configure(qclient::QHash& hash);

  //----------------------------------------------------------------------------
  //! Reserve file id
  //----------------------------------------------------------------------------
  int64_t reserveFileId();

  //----------------------------------------------------------------------------
  //! Reserve container id
  //----------------------------------------------------------------------------
  int64_t reserveContainerId();

  //----------------------------------------------------------------------------
  //! Blacklist container ID
  //----------------------------------------------------------------------------
  void blacklistContainerId(int64_t inode);

  //----------------------------------------------------------------------------
  //! Blacklist file ID
  //----------------------------------------------------------------------------
  void blacklistFileId(int64_t inode);

  //----------------------------------------------------------------------------
  //! Get first free file ID
  //----------------------------------------------------------------------------
  int64_t getFirstFreeFileId();

  //----------------------------------------------------------------------------
  //! Get first free container ID
  //----------------------------------------------------------------------------
  int64_t getFirstFreeContainerId();

private:
  bool mSharedInodes = false;
  qclient::QHash *mMetaMap = nullptr;

  std::unique_ptr<NextInodeProvider> mFileIdProvider;
  std::unique_ptr<NextInodeProvider> mContainerIdProvider;
};

EOSNSNAMESPACE_END
