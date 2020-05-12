/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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
//! @brief Helper utilities for extracting attributes out
//!        of file and container MDs
//------------------------------------------------------------------------------

#pragma once
#include "namespace/Namespace.hh"
#include "proto/FileMd.pb.h"

EOSNSNAMESPACE_BEGIN

class AttributeExtraction {
public:
  //----------------------------------------------------------------------------
  //! Extract the given attribute as string. Return true if given attr name is
  //! valid, false otherwise.
  //----------------------------------------------------------------------------
  static bool asString(const eos::ns::FileMdProto &proto,
    const std::string &attr, std::string &out);

};

EOSNSNAMESPACE_END
