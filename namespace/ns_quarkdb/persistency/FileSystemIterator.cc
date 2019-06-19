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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Class to iterate through all available FileSystems, as found in
//!        the namespace
//------------------------------------------------------------------------------

#include "namespace/ns_quarkdb/persistency/FileSystemIterator.hh"
#include "common/StringTokenizer.hh"
#include "common/Logging.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileSystemIterator::FileSystemIterator(qclient::QClient &qcl)
: mScanner(qcl, "fsview:*:*") {

  while(mScanner.valid() && !parseScannerKey()) {
    mScanner.next();
  }
}

//------------------------------------------------------------------------------
// Get next filesystem ID
//------------------------------------------------------------------------------
IFileMD::location_t FileSystemIterator::getFileSystemID() const {
  return mFilesystemID;
}

//------------------------------------------------------------------------------
// What is the raw redis key this element is referring to?
//------------------------------------------------------------------------------
std::string FileSystemIterator::getRedisKey() const {
  return mRedisKey;
}

//------------------------------------------------------------------------------
// Is this item referring to the unlinked view?
//------------------------------------------------------------------------------
bool FileSystemIterator::isUnlinked() const {
  return mIsUnlinked;
}

//------------------------------------------------------------------------------
// Is the iterator object valid?
//------------------------------------------------------------------------------
bool FileSystemIterator::valid() const {
  return mScanner.valid();
}

//------------------------------------------------------------------------------
// Advance iterator
//------------------------------------------------------------------------------
void FileSystemIterator::next() {
  mScanner.next();
  while(mScanner.valid() && !parseScannerKey()) {
    mScanner.next();
  }
}

//------------------------------------------------------------------------------
// Parse element that mScanner currently points to - return false on parse
// error
//------------------------------------------------------------------------------
bool FileSystemIterator::parseScannerKey() {
  bool ok = rawParseScannerKey();

  if(!ok) {
    eos_static_crit("Could not parse fsview redis key in FileSystemIterator: %s", mRedisKey.c_str());
  }

  return ok;
}

bool FileSystemIterator::rawParseScannerKey() {
  mRedisKey = mScanner.getValue();

  std::vector<std::string> parts =
    eos::common::StringTokenizer::split<std::vector<std::string>>(mRedisKey, ':');

  if (parts.size() != 3) {
    return false;
  }

  if (parts[0] != "fsview") {
    return false;
  }

  mFilesystemID = std::stoull(parts[1]);

  if (parts[2] == "files") {
    mIsUnlinked = false;
  } else if (parts[2] == "unlinked") {
    mIsUnlinked = true;
  } else {
    return false;
  }

  return true;
}

EOSNSNAMESPACE_END
