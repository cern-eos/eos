//------------------------------------------------------------------------------
//! @file InodeGenerator.cc
//! @author Georgios Bitzes CERN
//! @brief inode generator class
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

#include "InodeGenerator.hh"
#include "common/Logging.hh"

std::string InodeGenerator::kInodeKey = "nextinode";

void InodeGenerator::init(kv* st) {
  store = st;

  mNextInode = 1;
  if(store->get(kInodeKey, mNextInode)) {
    // otherwise store it for the first time
    inc();
  }

  eos_static_info("next-inode=%08lx", mNextInode);
}

uint64_t InodeGenerator::inc() {
  std::lock_guard<std::mutex> lock(mtx);
  
  if (0) {
    //sync - works for eosxd shared REDIS backend
    if (!store->inc(kInodeKey, mNextInode)) {
      return mNextInode;
    }
    else {
      throw std::runtime_error("REDIS backend failure - nextinode");
    }
  }
  else {
    //async - works for eosxd exclusive REDIS backend
    uint64_t s_inode = mNextInode + 1;
    store->put(kInodeKey, s_inode);
    return mNextInode++;
  }
}
