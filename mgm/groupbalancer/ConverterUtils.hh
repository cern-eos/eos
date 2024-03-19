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

#include "common/FileId.hh"
#include <string>

namespace eos::mgm::group_balancer
{
//----------------------------------------------------------------------------
//! Produces a file conversion path to be placed in the proc directory taking
//! into account the given group and also returns its size
//!
//! @param fid the file ID
//! @param target_group the group to which the file will be transferred
//! @param size return address for the size of the file
//!
//! @return name of the proc transfer file
//----------------------------------------------------------------------------
std::string
getFileProcTransferNameAndSize(eos::common::FileId::fileid_t fid,
                               const std::string& target_group, uint64_t* size);

} // eos::mgm::group_balancer
#endif // EOS_CONVERTERUTILS_HH
