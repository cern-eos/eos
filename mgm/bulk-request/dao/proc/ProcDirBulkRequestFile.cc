//------------------------------------------------------------------------------
//! @file ProcDirBulkRequestFile.cc
//! @author Cedric Caffy - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "ProcDirBulkRequestFile.hh"

EOSBULKNAMESPACE_BEGIN

ProcDirBulkRequestFile::ProcDirBulkRequestFile() {
}

ProcDirBulkRequestFile::ProcDirBulkRequestFile(const std::string& path):mFullPath(path){
}

void ProcDirBulkRequestFile::setFileId(const eos::common::FileId::fileid_t fileId) {
  mFileId = fileId;
}

const std::optional<eos::common::FileId::fileid_t> ProcDirBulkRequestFile::getFileId() const {
  return mFileId;
}

void ProcDirBulkRequestFile::setError(const std::string& error) {
  mError = error;
}

const std::optional<std::string> ProcDirBulkRequestFile::getError() const {
  return mError;
}

void ProcDirBulkRequestFile::setFullPath(const std::string& fullPath){
  mFullPath = fullPath;
}

const std::string ProcDirBulkRequestFile::getFullPath() const {
  return mFullPath;
}

void ProcDirBulkRequestFile::setNameInBulkRequestDirectory(const std::string& name){
  mNameInBulkRequestDirectory = name;
}

const std::string ProcDirBulkRequestFile::getNameInBulkRequestDirectory() const {
  return mNameInBulkRequestDirectory;
}

bool ProcDirBulkRequestFile::operator<(const ProcDirBulkRequestFile& other) const {
  return mFullPath < other.mFullPath;
}

bool ProcDirBulkRequestFile::operator==(const ProcDirBulkRequestFile& other) const {
  return mFullPath == other.mFullPath;
}

EOSBULKNAMESPACE_END
