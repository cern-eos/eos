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
//! @brief Lookup state for asynchronous path lookup
//------------------------------------------------------------------------------

#ifndef EOS_NS_PATH_LOOKUP_STATE_H
#define EOS_NS_PATH_LOOKUP_STATE_H

#include "namespace/Namespace.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IContainerMD.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Lookup state for asynchronous path lookups
//------------------------------------------------------------------------------

struct PathLookupState {
public:
  IContainerMDPtr current;
  size_t symlinkDepth = 0;
};

EOSNSNAMESPACE_END

#endif
