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

class FileCollection {
public:
  FileCollection();
  typedef std::map<std::string,File> Files;

  void addFile(const std::string & path);
  const std::shared_ptr<Files> getAllFiles() const;
  void addError(const std::string &path, const std::string & error);
  const std::shared_ptr<std::set<File>> getAllFilesInError() const;
private:
  std::shared_ptr<Files> mFiles;
};

EOSBULKNAMESPACE_END

#endif // EOS_FILECOLLECTION_HH
