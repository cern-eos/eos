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
//! @brief  Collection of functions to do permission checking
//------------------------------------------------------------------------------

#pragma once
#include "namespace/Namespace.hh"
#include "namespace/interface/IFileMD.hh"

EOSNSNAMESPACE_BEGIN

#define CANREAD 0x01
#define CANWRITE 0x02
#define CANENTER 0x04

class PermissionHandler
{
public:

  //----------------------------------------------------------------------------
  //! Convert "user" mode_t permission bits to internally-used representation.
  //----------------------------------------------------------------------------
  static char convertModetUser(mode_t mode);

  //----------------------------------------------------------------------------
  //! Convert "group" mode_t permission bits to internally-used representation.
  //----------------------------------------------------------------------------
  static char convertModetGroup(mode_t mode);

  //----------------------------------------------------------------------------
  //! Convert "other" mode_t permission bits to internally-used representation.
  //----------------------------------------------------------------------------
  static char convertModetOther(mode_t mode);

  //----------------------------------------------------------------------------
  //! Check permissions and decide whether to allow or not.
  //----------------------------------------------------------------------------
  static bool checkPerms(char actual, char requested);

  //----------------------------------------------------------------------------
  //! Convert requested permissions to internal representation. Ready to pass
  //! onto checkPerms then.
  //----------------------------------------------------------------------------
  static char convertRequested(mode_t requested);

  //----------------------------------------------------------------------------
  //! Parse octal mask
  //----------------------------------------------------------------------------
  static bool parseOctalMask(const std::string& str, mode_t& out);

  //----------------------------------------------------------------------------
  //! Filter mode based a given mask, passed as string
  //----------------------------------------------------------------------------
  static mode_t filterWithSysMask(const std::string& sysmask, mode_t mode);

  //------------------------------------------------------------------------------
  //! Filter mode based on sys.mask, as given in xattrs.
  //! Template, so we can handle both std::map and protobuf map.
  //------------------------------------------------------------------------------
  template<typename MapType>
  static mode_t filterWithSysMask(const MapType& xattr, mode_t mode)
  {
    auto it = xattr.find("sys.mask");

    if (it == xattr.end()) {
      return mode;
    }

    return filterWithSysMask(it->second, mode);
  }

};

EOSNSNAMESPACE_END
