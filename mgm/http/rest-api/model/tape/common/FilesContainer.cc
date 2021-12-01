// ----------------------------------------------------------------------
// File: FilesContainer.cc
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

#include "FilesContainer.hh"

EOSMGMRESTNAMESPACE_BEGIN

const std::vector<std::string>& FilesContainer::getPaths() const
{
  return mPaths;
}

const std::vector<std::string>& FilesContainer::getOpaqueInfos() const
{
  return mOpaqueInfos;
}

void FilesContainer::addFile(const std::string& path) {
  mPaths.push_back(path);
  mOpaqueInfos.push_back("");
}

void FilesContainer::addFile(const std::string& path, const std::string& opaqueInfo) {
  mPaths.push_back(path);
  mOpaqueInfos.push_back(opaqueInfo);
}

EOSMGMRESTNAMESPACE_END