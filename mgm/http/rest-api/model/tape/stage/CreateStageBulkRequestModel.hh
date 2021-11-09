// ----------------------------------------------------------------------
// File: CreateStageBulkRequestModel.hh
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
#ifndef EOS_CREATESTAGEBULKREQUESTMODEL_HH
#define EOS_CREATESTAGEBULKREQUESTMODEL_HH

#include "mgm/Namespace.hh"
#include <string>
#include <vector>
#include <map>
#include <any>

EOSMGMRESTNAMESPACE_BEGIN

/**
 * This object represents a client's request
 * to create a stage bulk-request
 */
class CreateStageBulkRequestModel {
public:
  void addPath(const std::string & path);
  void addOpaqueInfo(const std::string & oinfos);
  void addOrModifyMetadata(const std::string & key, const std::any & value);
  const std::vector<std::string> & getPaths() const;
  const std::vector<std::string> & getOpaqueInfos() const;
  inline static const std::string PATHS_KEY_NAME = "paths";
  inline static const std::string METADATA_KEY_NAME = "metadata";
private:
  std::vector<std::string> mPaths;
  std::vector<std::string> mOinfos;
  std::map<std::string,std::any> mMetadata;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_CREATESTAGEBULKREQUESTMODEL_HH
