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

#include "namespace/ns_quarkdb/inspector/ContainerScanner.hh"
#include "namespace/ns_quarkdb/persistency/Serialization.hh"
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ContainerScannerPrimitive::ContainerScannerPrimitive(qclient::QClient &qcl)
: mIterator(&qcl, "eos-container-md") { }

//------------------------------------------------------------------------------
// Is the iterator valid?
//------------------------------------------------------------------------------
bool ContainerScannerPrimitive::valid() const {
  if(!mError.empty()) {
    return false;
  }

  return mIterator.valid();
}

//----------------------------------------------------------------------------
// Advance iterator - only call when valid() == true
//----------------------------------------------------------------------------
void ContainerScannerPrimitive::next() {
  mIterator.next();
}

//------------------------------------------------------------------------------
// Is there an error?
//------------------------------------------------------------------------------
bool ContainerScannerPrimitive::hasError(std::string &err) const {
  if(!mError.empty()) {
    err = mError;
    return true;
  }

  return mIterator.hasError(err);
}

//------------------------------------------------------------------------------
// Get current element
//------------------------------------------------------------------------------
bool ContainerScannerPrimitive::getItem(eos::ns::ContainerMdProto &item, std::string *path) {
  if(!valid()) {
    return false;
  }

  std::string currentValue = mIterator.getValue();
  eos::MDStatus status = Serialization::deserialize(currentValue.c_str(), currentValue.size(), item);

  if(!status.ok()) {
    mError = SSTR("Error while deserializing: " << status.getError());
    return false;
  }

  if(path) {
    *path = item.name();
  }

  mScanned++;
  return true;
}

//------------------------------------------------------------------------------
// Get number of elements scanned so far
//------------------------------------------------------------------------------
uint64_t ContainerScannerPrimitive::getScannedSoFar() const {
  return mScanned;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ContainerScanner::ContainerScanner(qclient::QClient &qcl, bool fullPaths)
: mScanner(qcl), mQcl(qcl), mFullPaths(fullPaths) {
  if(mFullPaths) {
    ensureItemDequeFull();
  }
}

//------------------------------------------------------------------------------
// Is the iterator valid?
//------------------------------------------------------------------------------
bool ContainerScanner::valid() const {
  if(mFullPaths) {
    return !mItemDeque.empty();
  }
  else {
    return mScanner.valid();
  }
}

//------------------------------------------------------------------------------
// Advance iterator - only call when valid() == true
//------------------------------------------------------------------------------
void ContainerScanner::next() {
  if(mFullPaths) {
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
// Ensure our item deque contanis a sufficient number of pending items
//------------------------------------------------------------------------------
void ContainerScanner::ensureItemDequeFull() {
  if(!mFullPaths) return;

  while(mScanner.valid() && mItemDeque.size() < 500) {
    eos::ns::ContainerMdProto item;
    if(mScanner.getItem(item)) {
      mItemDeque.emplace_back(std::move(item),
        MetadataFetcher::resolveFullPath(mQcl, ContainerIdentifier(item.id())));
    }

    mScanner.next();
  }
}

//------------------------------------------------------------------------------
// Is there an error?
//------------------------------------------------------------------------------
bool ContainerScanner::hasError(std::string &err) const {
  if(mFullPaths) {
    return !mItemDeque.empty() && mScanner.hasError(err);
  }
  else {
    return mScanner.hasError(err);
  }
}

//------------------------------------------------------------------------------
// Get number of elements scanned so far
//------------------------------------------------------------------------------
uint64_t ContainerScanner::getScannedSoFar() const {
  if(mFullPaths) {
    return mScanned;
  }
  else{
    return mScanner.getScannedSoFar();
  }
}

//------------------------------------------------------------------------------
// Get current element
//------------------------------------------------------------------------------
bool ContainerScanner::getItem(eos::ns::ContainerMdProto &item, std::string *path) {
  if(mFullPaths) {
    if(!valid()) {
      return false;
    }

    item = mItemDeque.front().proto;

    if(path != nullptr) {
      mItemDeque.front().fullPath.wait();
      if(!mItemDeque.front().fullPath.hasException()) {
        *path = mItemDeque.front().fullPath.get();
      }
      else {
        *path = item.name();
      }
    }

    mScanned++;
    return true;
  }
  else {
    return mScanner.getItem(item, path);
  }
}


EOSNSNAMESPACE_END
