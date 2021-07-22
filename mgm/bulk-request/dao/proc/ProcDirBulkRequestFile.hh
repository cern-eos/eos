//------------------------------------------------------------------------------
//! @file ProcDirBulkRequestFile.hh
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

#ifndef EOS_PROCDIRBULKREQUESTFILE_HH
#define EOS_PROCDIRBULKREQUESTFILE_HH

#include "mgm/Namespace.hh"
#include "common/FileId.hh"
#include <string>
#include <optional>

EOSBULKNAMESPACE_BEGIN

/**
 * Class that will hold information about a file that belongs to a bulk-request
 * in order to be persisted/retrieved in/from the proc directory
 */
class ProcDirBulkRequestFile {
public:
  ProcDirBulkRequestFile();
  ProcDirBulkRequestFile(const std::string & path);
  void setFileId(const eos::common::FileId::fileid_t fileId);
  const std::optional<eos::common::FileId::fileid_t> getFileId() const;
  void setError(const std::string & error);
  const std::optional<std::string> getError() const;
  void setFullPath(const std::string & fullPath);
  const std::string getFullPath() const;
  void setName(const std::string & name);
  const std::string getName() const;
  bool operator<(const ProcDirBulkRequestFile & other) const;
  bool operator==(const ProcDirBulkRequestFile & other) const;
private:
  std::optional<eos::common::FileId::fileid_t> mFileId;
  std::string mName;
  std::optional<std::string> mError;
  std::string mFullPath;
};

EOSBULKNAMESPACE_END

#endif // EOS_PROCDIRBULKREQUESTFILE_HH
