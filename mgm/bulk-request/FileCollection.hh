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
#include <map>
#include <memory>

EOSBULKNAMESPACE_BEGIN

/**
 * This class manages a collection of files
 */
class FileCollection {
public:
  FileCollection();
  FileCollection & operator=(const FileCollection & other);
  /**
   * The collection is a map of <path,File>
   */
  typedef std::map<std::string,File> Files;

  /**
   * Creates a new file object associated to the path passed in parameter
   * @param path the path of the file to add in the collection
   */
  void addFile(const std::string & path);
  /**
   * Adds the file passed in parameter to this collection
   * The key of this item will be the path of the file and the value
   * will be the file itself
   * @param file the file to add to this collection
   */
  void addFile(const File & file);
  /**
   * Returns the pointer of the collection (map) managed by this class
   * @return the pointer of the collection (map) managed by this class
   */
  const std::shared_ptr<FileCollection::Files> getAllFiles() const;
  /**
   * Adds an error to the File associated to the path passed in parameter
   * @param path the path of the file to add the error to
   * @param error the error to add to the file
   * @throws an exception if the path does not exist in the collection managed by this instance
   */
  void addError(const std::string &path, const std::string & error);
  /**
   * Returns the files that have an error
   * @return the files that have an error
   */
  const std::shared_ptr<std::set<File>> getAllFilesInError() const;
private:
  std::shared_ptr<FileCollection::Files> mFiles;
};

EOSBULKNAMESPACE_END

#endif // EOS_FILECOLLECTION_HH
