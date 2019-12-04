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
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileScannerPrimitive::FileScannerPrimitive(qclient::QClient &qcl)
: mIterator(&qcl, "eos-file-md") { }

//------------------------------------------------------------------------------
// Is the iterator valid?
//------------------------------------------------------------------------------
bool FileScannerPrimitive::valid() const {
  if(!mError.empty()) {
    return false;
  }

  return mIterator.valid();
}

//----------------------------------------------------------------------------
// Advance iterator - only call when valid() == true
//----------------------------------------------------------------------------
void FileScannerPrimitive::next() {
  mIterator.next();
}

//------------------------------------------------------------------------------
// Is there an error?
//------------------------------------------------------------------------------
bool FileScannerPrimitive::hasError(std::string &err) const {
  if(!mError.empty()) {
    err = mError;
    return true;
  }

  return mIterator.hasError(err);
}

//------------------------------------------------------------------------------
// Get current element
//------------------------------------------------------------------------------
bool FileScannerPrimitive::getItem(eos::ns::FileMdProto &item) {
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
uint64_t FileScannerPrimitive::getScannedSoFar() const {
  return mScanned;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileScanner::FileScanner(qclient::QClient &qcl, bool fullPaths)
: mScanner(qcl), mQcl(qcl), mFullPaths(fullPaths) {

  mActive = mFullPaths;

  if(mActive) {
    ensureItemDequeFull();
  }
}

//------------------------------------------------------------------------------
// Is the iterator valid?
//------------------------------------------------------------------------------
bool FileScanner::valid() const {
  if(mActive) {
    return !mItemDeque.empty();
  }
  else {
    return mScanner.valid();
  }
}

//------------------------------------------------------------------------------
// Advance iterator - only call when valid() == true
//------------------------------------------------------------------------------
void FileScanner::next() {
  if(mActive) {
    if(valid()) {
      mItemDeque.pop_front();
      ensureItemDequeFull();
    }
  }
  else {
    return mScanner.next();
  }
}

//------------------------------------------------------------------------------
// Ensure our item deque contains a sufficient number of pending items
//------------------------------------------------------------------------------
void FileScanner::ensureItemDequeFull() {
  if(!mActive) return;

  while(mScanner.valid() && mItemDeque.size() < 500) {
    eos::ns::FileMdProto item;
    if(mScanner.getItem(item)) {

      folly::Future<std::string> fullPath = "";

      if(mFullPaths) {
        fullPath = MetadataFetcher::resolveFullPath(mQcl, ContainerIdentifier(item.cont_id()));
      }

      mItemDeque.emplace_back(
        std::move(item),
        std::move(fullPath)
      );
    }

    mScanner.next();
  }
}

//------------------------------------------------------------------------------
// Is there an error?
//------------------------------------------------------------------------------
bool FileScanner::hasError(std::string &err) const {
  if(mActive) {
    return !mItemDeque.empty() && mScanner.hasError(err);
  }
  else {
    return mScanner.hasError(err);
  }
}

//------------------------------------------------------------------------------
// Get number of elements scanned so far
//------------------------------------------------------------------------------
uint64_t FileScanner::getScannedSoFar() const {
  if(mActive) {
    return mScanned;
  }
  else{
    return mScanner.getScannedSoFar();
  }
}

//------------------------------------------------------------------------------
// Get current element
//------------------------------------------------------------------------------
bool FileScanner::getItem(eos::ns::FileMdProto &proto, FileScanner::Item *item) {
  if(mActive) {
    if(!valid()) {
      return false;
    }

    proto = mItemDeque.front().proto;

    if(item != nullptr) {
      *item = std::move(mItemDeque.front());
    }

    mScanned++;
    return true;
  }
  else {
    return mScanner.getItem(proto);
  }
}




EOSNSNAMESPACE_END
