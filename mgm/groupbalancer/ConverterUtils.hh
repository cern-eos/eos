//------------------------------------------------------------------------------
// File: ConverterUtils.hh
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#ifndef EOS_CONVERTERUTILS_HH
#define EOS_CONVERTERUTILS_HH

#include <functional>
#include <string_view>

#include "common/FileId.hh"
#include <string>

namespace eos::mgm::group_balancer
{

using SkipFileFn = std::function<bool(std::string_view)>;
inline const SkipFileFn NullFilter = {};

struct PrefixFilter {
    bool operator()(std::string_view path);
    std::string prefix;
    PrefixFilter(std::string_view _prefix): prefix(_prefix) {}
};

//----------------------------------------------------------------------------
//! Produces a file conversion path to be placed in the proc directory taking
//! into account the given group and also returns its size
//!
//! @param fid the file ID
//! @param target_group the group to which the file will be transferred
//! @param size return address for the size of the file
//! @param skip_file_fn function to skip files matching filter
//!          defaults to NullFilter, which means no files will be skipped
//!
//! @return name of the proc transfer file
//----------------------------------------------------------------------------
std::string
getFileProcTransferNameAndSize(eos::common::FileId::fileid_t fid,
                               const std::string& target_group, uint64_t* size,
                               const SkipFileFn& skip_file_fn = NullFilter);
} // eos::mgm::group_balancer
#endif // EOS_CONVERTERUTILS_HH
