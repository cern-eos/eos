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

ProcDirBulkRequestFile::ProcDirBulkRequestFile(const std::string& path): mName(
    path)
{
}

void ProcDirBulkRequestFile::setFileId(const eos::common::FileId::fileid_t
                                       fileId)
{
  mFileId = fileId;
}

const std::optional<eos::common::FileId::fileid_t>
ProcDirBulkRequestFile::getFileId() const
{
  return mFileId;
}

void ProcDirBulkRequestFile::setError(const std::string& error)
{
  mError = error;
}

const std::optional<std::string> ProcDirBulkRequestFile::getError() const
{
  return mError;
}

void ProcDirBulkRequestFile::setName(const std::string& name)
{
  mName = name;
}

const std::string ProcDirBulkRequestFile::getName() const
{
  return mName;
}

bool ProcDirBulkRequestFile::operator<(const ProcDirBulkRequestFile& other)
const
{
  return getName() < other.getName();
}

bool ProcDirBulkRequestFile::operator==(const ProcDirBulkRequestFile& other)
const
{
  return getName() == other.getName();
}

EOSBULKNAMESPACE_END
