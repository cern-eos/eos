//------------------------------------------------------------------------------
//! @file FileCollection.hh
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

#ifndef EOS_FILECOLLECTION_HH
#define EOS_FILECOLLECTION_HH

#include "File.hh"
#include "mgm/Namespace.hh"
#include <set>
#include <vector>
#include <map>
#include <memory>

EOSBULKNAMESPACE_BEGIN

/**
 * This class manages a collection of files
 */
class FileCollection
{
public:
  FileCollection() {
    mFiles.reset(new FilesMap());
    mFilesInsertOrder.reset(new FilesInsertOrder());
  }
  FileCollection& operator=(const FileCollection& other) {
    if (this != &other) {
      this->mFiles = other.mFiles;
    }
    return *this;
  }
  /**
   * The collection is a map of <path,File>
   */
  typedef std::vector<File*> Files;
  typedef std::multimap<std::string, std::unique_ptr<File>> FilesMap;
  typedef std::vector<FilesMap::iterator> FilesInsertOrder;
  /**
   * Adds the file passed in parameter to this collection
   * The key of this item will be the path of the file and the value
   * will be the file itself
   * @param file the file to add to this collection
   */
  void addFile(std::unique_ptr<File>&& file) {
    auto insertedElementItor = mFiles->insert(
      std::pair<std::string, std::unique_ptr<File>>(file->getPath(), std::move(file)));
    mFilesInsertOrder->emplace_back(insertedElementItor);
  }
  /**
   * Returns all the files that belongs to this collection
   * @return the pointer of the collection (map) managed by this class
   */
  const std::shared_ptr<FileCollection::Files> getAllFiles() const {
    std::shared_ptr<FileCollection::Files> ret = std::make_shared<FileCollection::Files>();
    for (auto& itor : *mFilesInsertOrder) {
      ret->emplace_back(itor->second.get());
    }
    return ret;
  }

  /**
   * Returns the pointer of the map<path,File> that contains the files of this collection
   * @return the pointer of the map<path,File> that contains the files of this collection
   */
  const std::shared_ptr<FileCollection::FilesMap> getFilesMap() const { return mFiles; }
  /**
   * Returns the files that have an error
   * @return the files that have an error
   */
  const std::shared_ptr<std::set<File>> getAllFilesInError() const {
    std::shared_ptr<std::set<File>> filesInError(new std::set<File>());
    for (const auto& pathFile : *mFiles) {
      if (pathFile.second->getError()) {
        filesInError->insert(*pathFile.second);
      }
    }
    return filesInError;
  }
private:
  std::shared_ptr<FileCollection::FilesMap> mFiles;
  std::shared_ptr<FileCollection::FilesInsertOrder> mFilesInsertOrder;
};

EOSBULKNAMESPACE_END

#endif // EOS_FILECOLLECTION_HH
