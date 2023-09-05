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
//! @brief File list random picker
//------------------------------------------------------------------------------

#include "namespace/interface/IFsView.hh"
#include "FileListRandomPicker.hh"
#include <mutex>
#include "common/utils/RandUtils.hh"

EOSNSNAMESPACE_BEGIN

bool pickRandomFile(const IFsView::FileList &filelist, eos::IFileMD::id_t &retval) {
  if(filelist.empty()) {
    return false;
  }


  while(true) {
    eos::IFileMD::id_t randomPosition = eos::common::getRandom<uint64_t>(0, filelist.bucket_count() - 1);

    // Is there an element at that location on the table?
    auto it = filelist.begin(randomPosition);
    if(it != filelist.end(randomPosition)) {
      retval = *it;
      return true;
    }
  }
}

EOSNSNAMESPACE_END
