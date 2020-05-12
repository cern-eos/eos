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

#include "namespace/ns_quarkdb/inspector/FileMetadataFilter.hh"

EOSNSNAMESPACE_BEGIN

//----------------------------------------------------------------------------
//! Constructor
//----------------------------------------------------------------------------
EqualityFileMetadataFilter::EqualityFileMetadataFilter(const std::string &attr, const std::string &value)
: mAttr(attr), mValue(value) {}

//------------------------------------------------------------------------------
// Is the object valid?
//------------------------------------------------------------------------------
common::Status EqualityFileMetadataFilter::isValid() {
  return common::Status();
}

//------------------------------------------------------------------------------
// Does the given FileMdProto pass through the filter?
//------------------------------------------------------------------------------
bool EqualityFileMetadataFilter::check(const eos::ns::FileMdProto &proto) {
  return true;
}

EOSNSNAMESPACE_END

