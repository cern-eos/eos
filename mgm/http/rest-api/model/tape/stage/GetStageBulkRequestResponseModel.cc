// ----------------------------------------------------------------------
// File: GetStageBulkRequestResponseModel.cc
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

#include "GetStageBulkRequestResponseModel.hh"

EOSMGMRESTNAMESPACE_BEGIN

void GetStageBulkRequestResponseModel::addFile(std::unique_ptr<File>&& file)
{
  mFiles.emplace_back(std::move(file));
}

const std::vector<std::unique_ptr<GetStageBulkRequestResponseModel::File>>&
    GetStageBulkRequestResponseModel::getFiles() const
{
  return mFiles;
}

time_t GetStageBulkRequestResponseModel::getCreationTime() const
{
  return mCreationTime;
}

std::string GetStageBulkRequestResponseModel::getId() const
{
  return mId;
}

void GetStageBulkRequestResponseModel::setCreationTime(const time_t&
    creationTime)
{
  mCreationTime = creationTime;
}

void GetStageBulkRequestResponseModel::setId(const std::string& id)
{
  mId = id;
}

EOSMGMRESTNAMESPACE_END