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
#include <type_traits>

#include "namespace/interface/IContainerMD.hh"
#include "common/VirtualIdentity.hh"
#include "common/StringUtils.hh"

namespace eos::mgm::attr
{

/*!
 * check and set directory owner based on "sys.auth.owner" param
 * @param attrmap map of xattrs
 * @param d_uid directory uid
 * @param d_gid directory gid
 * @param vid Vid of the user, may be modified
 * @param sticky_owner true if directory has "sys.auth.owner=*"
 * @param path path of directory, only used for logging
 * @return true if permissions, vid will be modified to directory uid/gid
 *         only for the non sticky owner case
 *
 * The current usages of sticky_owner are special in comparision to
 * sys.owner.auth, so we don't yet reset the vid in case of sticky_owner
 * if there is an auth.owner match
 */
bool checkDirOwner(const eos::IContainerMD::XAttrMap& attrmap, uid_t d_uid,
                   gid_t d_gid, eos::common::VirtualIdentity& vid,
                   bool& sticky_owner, const char* path);
/*!
 * Check for Atomic Uploads
 * Atomic uploads follow the evaluation order sys.attribute > user.attribute > cgi
 * The CGI is evaluated only if both sys and user attributes are not present
 * @param attrmap map of xattrs
 * @param atomic_cgi cgi from env
 * @return bool status of atomic upload
 */
bool checkAtomicUpload(const eos::IContainerMD::XAttrMap& attrmap,
                       const char* atomic_cgi = nullptr);

/*!
 * Check for versioing attribute
 * Versioning follows the evaluation order cgi > sys.attribute > user.attribute
 * @param attrmap map of xattrs
 * @param versioning_cgi string_view of versioning cgi
 * @return versioning int
 */
int getVersioning(const eos::IContainerMD::XAttrMap& attrmap,
                  std::string_view versioning_cgi = {});

/*!
 * Get string value from xattr map
 * @param attrmap map of xattrs
 * @param key key to search
 * @param out output string
 * @return true if key found and out populated
 */
bool getValue(const eos::IContainerMD::XAttrMap& attrmap,
              const std::string& key, std::string& out);

/*!
 * Get numeric value from xattr map
 * @param attrmap map of xattrs
 * @param key key to search
 * @param out output numeric value
 * @return true if key found and out populated
 */
template <typename T>
auto getValue(const eos::IContainerMD::XAttrMap&  attrmap,
              const std::string& key, T& out)
    -> std::enable_if_t<std::is_arithmetic_v<T>, bool>
{
    auto kv = attrmap.find(key);
    if (kv != attrmap.end()) {
        return common::StringToNumeric(kv->second, out);
    }
    return false;
}

}// eos::mgm::attr
