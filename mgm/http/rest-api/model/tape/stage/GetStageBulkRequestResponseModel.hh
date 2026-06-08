// ----------------------------------------------------------------------
// File: GetStageBulkRequestResponseModel.hh
// Author: Cedric Caffy - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#ifndef EOS_GETSTAGEBULKREQUESTRESPONSEMODEL_HH
#define EOS_GETSTAGEBULKREQUESTRESPONSEMODEL_HH

#include "mgm/Namespace.hh"
#include "common/json/Jsonifiable.hh"
#include <ctime>
#include <optional>
#include <string>
#include <vector>

EOSMGMRESTNAMESPACE_BEGIN

/**
 * Response model for GET /api/v1/stage/{id} (WLCG Tape REST API v1).
 */
class GetStageBulkRequestResponseModel : public
  common::Jsonifiable<GetStageBulkRequestResponseModel>
{
public:
  class File
  {
  public:
    std::string mPath;
    std::optional<std::string> mError;
    bool mOnDisk = false;
    bool mShowOnDisk = false;
  };

  GetStageBulkRequestResponseModel() = default;

  inline void addFile(std::unique_ptr<File>&& file)
  {
    mFiles.emplace_back(std::move(file));
  }

  inline const std::vector<std::unique_ptr<File>>& getFiles() const { return mFiles; }
  inline time_t getCreatedAt() const { return mCreatedAt; }
  inline time_t getStartedAt() const { return mStartedAt; }
  inline const std::string& getId() const { return mId; }

  inline void setCreatedAt(const time_t createdAt) { mCreatedAt = createdAt; }
  inline void setStartedAt(const time_t startedAt) { mStartedAt = startedAt; }
  inline void setId(const std::string& id) { mId = id; }

private:
  std::vector<std::unique_ptr<File>> mFiles;
  time_t mCreatedAt = 0;
  time_t mStartedAt = 0;
  std::string mId;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_GETSTAGEBULKREQUESTRESPONSEMODEL_HH
