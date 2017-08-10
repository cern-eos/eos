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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Retrieve the next free container / file inode.
//------------------------------------------------------------------------------

#ifndef __EOS_NS_NEXT_INODE_PROVIDER_HH__
#define __EOS_NS_NEXT_INODE_PROVIDER_HH__

#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/ns_quarkdb/LRU.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include <mutex>

EOSNSNAMESPACE_BEGIN

class NextInodeProvider
{
public:
  NextInodeProvider();
  void configure(qclient::QHash &hash, const std::string &field);

  int64_t getFirstFreeId();
  int64_t reserve();
private:
  std::mutex mtx;
  qclient::QHash *pHash; ///< qclient hash - no ownership
  std::string pField;

  int64_t nextId;
  int64_t blockEnd;
  int64_t stepIncrease;
};

EOSNSNAMESPACE_END

#endif
