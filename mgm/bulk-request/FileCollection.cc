//------------------------------------------------------------------------------
//! @file FileCollection.cc
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

#include "FileCollection.hh"
#include <sstream>
#include "mgm/bulk-request/exception/BulkRequestException.hh"
#include "common/exception/Exception.hh"

EOSBULKNAMESPACE_BEGIN

FileCollection::FileCollection() {
  mFiles.reset(new Files());
}

FileCollection & FileCollection::operator=(const FileCollection& other) {
  if(this != &other){
    this->mFiles = other.mFiles;
  }
  return *this;
}

void FileCollection::addFile(const std::string & path) {
  (*mFiles)[path] = File(path);
}

void FileCollection::addFile(const File & file){
  (*mFiles)[file.getPath()] = file;
}

const std::shared_ptr<FileCollection::Files> FileCollection::getAllFiles() const {
  return this->mFiles;
}

void FileCollection::addError(const std::string & path, const std::string & error) {
  try {
    mFiles->at(path).setError(error);
  } catch(const std::exception & ex){
    std::ostringstream ss;
    ss << "Cannot add the error " << error << " to the path " << path << " because it does not exist in the file collection";
    throw common::Exception(ss.str());
  }
}

const std::shared_ptr<std::set<File>> FileCollection::getAllFilesInError() const {
  std::shared_ptr<std::set<File>> filesInError(new std::set<File>());
  for(const auto & pathFile: *mFiles){
    if(pathFile.second.getError()){
      filesInError->insert(pathFile.second);
    }
  }
  return filesInError;
}

EOSBULKNAMESPACE_END