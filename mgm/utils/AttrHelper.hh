//------------------------------------------------------------------------------
// File: AttrHelper.hh
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

#pragma once
#include "namespace/interface/IContainerMD.hh"
#include "common/VirtualIdentity.hh"

namespace eos::mgm::attr {

/*!
 * check and set directory owner based on "sys.auth.owner" param
 * @param attrmap map of xattrs
 * @param d_uid directory uid
 * @param d_gid directory gid
 * @param vid Vid of the user, may be modified
 * @param path path of directory, only used for logging
 * @return true if sticky permissions, vid will be modified to directory uid/gid
 * if there is an auth.owner match
 */
bool checkStickyDirOwner(const eos::IContainerMD::XAttrMap& attrmap,
                         uid_t d_uid,
                         gid_t d_gid,
                         eos::common::VirtualIdentity& vid,
                         const char* path);


} // eos::mgm::attr
