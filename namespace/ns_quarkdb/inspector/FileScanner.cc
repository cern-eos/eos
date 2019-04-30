/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "namespace/ns_quarkdb/inspector/FileScanner.hh"
#include "namespace/ns_quarkdb/persistency/Serialization.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileScanner::FileScanner(qclient::QClient &qcl)
: mIterator(&qcl, "eos-file-md") { }

//------------------------------------------------------------------------------
// Is the iterator valid?
//------------------------------------------------------------------------------
bool FileScanner::valid() const {
  if(!mError.empty()) {
    return false;
  }

  return mIterator.valid();
}

//----------------------------------------------------------------------------
// Advance iterator - only call when valid() == true
//----------------------------------------------------------------------------
void FileScanner::next() {
  mIterator.next();
}

//------------------------------------------------------------------------------
// Is there an error?
//------------------------------------------------------------------------------
bool FileScanner::hasError(std::string &err) const {
  if(!mError.empty()) {
    err = mError;
    return true;
  }

  return mIterator.hasError(err);
}

//------------------------------------------------------------------------------
// Get current element
//------------------------------------------------------------------------------
bool FileScanner::getItem(eos::ns::FileMdProto &item) {
  if(!valid()) {
    return false;
  }

  std::string currentValue = mIterator.getValue();
  eos::MDStatus status = Serialization::deserialize(currentValue.c_str(), currentValue.size(), item);

  if(!status.ok()) {
    mError = SSTR("Error while deserializing: " << status.getError());
    return false;
  }

  mScanned++;
  return true;
}

//------------------------------------------------------------------------------
// Get number of elements scanned so far
//------------------------------------------------------------------------------
uint64_t FileScanner::getScannedSoFar() const {
  return mScanned;
}

EOSNSNAMESPACE_END
