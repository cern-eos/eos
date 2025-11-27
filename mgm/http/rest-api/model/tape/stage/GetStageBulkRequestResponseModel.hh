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
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"
#include "mgm/bulk-request/BulkRequest.hh"

EOSMGMRESTNAMESPACE_BEGIN

/**
 * This class represents the object that will be returned to the client
 * that wants to track the progression of a previously submitted STAGE bulk-request
 */
class GetStageBulkRequestResponseModel : public
  common::Jsonifiable<GetStageBulkRequestResponseModel>
{
public:
  class File
  {
  public:
    std::string mPath;
    std::string mError;
    bool mOnDisk;
  };
  GetStageBulkRequestResponseModel() {}
  inline void addFile(std::unique_ptr<File>&& file) { mFiles.emplace_back(std::move(file)); }
  inline const std::vector<std::unique_ptr<File>>& getFiles() const { return mFiles; }
  inline time_t getCreationTime() const { return mCreationTime; }
  inline std::string getId() const { return mId; }
  inline void setCreationTime(const time_t& creationTime) { mCreationTime = creationTime; }
  inline void setId(const std::string& id) { mId = id; }
private:
  std::vector<std::unique_ptr<File>> mFiles;
  time_t mCreationTime;
  std::string mId;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_GETSTAGEBULKREQUESTRESPONSEMODEL_HH
