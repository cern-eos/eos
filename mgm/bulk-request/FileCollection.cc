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

FileCollection::FileCollection()
{
  mFiles.reset(new FilesMap());
  mFilesInsertOrder.reset(new FilesInsertOrder());
}

FileCollection& FileCollection::operator=(const FileCollection& other)
{
  if (this != &other) {
    this->mFiles = other.mFiles;
  }

  return *this;
}

void FileCollection::addFile(std::unique_ptr<File>&& file)
{
  auto insertedElementItor = mFiles->insert(
                               std::pair<std::string, std::unique_ptr<File>>(file->getPath(),
                                   std::move(file)));
  mFilesInsertOrder->emplace_back(insertedElementItor);
}

const std::shared_ptr<FileCollection::Files> FileCollection::getAllFiles()
const
{
  std::shared_ptr<FileCollection::Files> ret =
    std::make_shared<FileCollection::Files>();

  for (auto& itor : *mFilesInsertOrder) {
    ret->emplace_back(itor->second.get());
  }

  return ret;
}

const std::shared_ptr<FileCollection::FilesMap> FileCollection::getFilesMap()
const
{
  return mFiles;
}

const std::shared_ptr<std::set<File>> FileCollection::getAllFilesInError()
                                   const
{
  std::shared_ptr<std::set<File>> filesInError(new std::set<File>());

  for (const auto& pathFile : *mFiles) {
    if (pathFile.second->getError()) {
      filesInError->insert(*pathFile.second);
    }
  }

  return filesInError;
}

EOSBULKNAMESPACE_END
