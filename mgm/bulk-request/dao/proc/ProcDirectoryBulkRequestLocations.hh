//------------------------------------------------------------------------------
//! @file ProcDirectoryBulkRequestLocations.hh
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
#ifndef EOS_PROCDIRECTORYBULKREQUESTLOCATIONS_HH
#define EOS_PROCDIRECTORYBULKREQUESTLOCATIONS_HH

#include "mgm/Namespace.hh"
#include "mgm/bulk-request/BulkRequest.hh"
#include <set>

EOSBULKNAMESPACE_BEGIN

/**
 * This class allows to store the paths where the bulk-requests will be stored
 * according to their types.
 *
 * As the proc directory is created and known when the MGM starts, a pointer to this class
 * is stored in the XrdMgmOfs object so that it can be reused later on.
 */
class ProcDirectoryBulkRequestLocations {
public:
  ProcDirectoryBulkRequestLocations(const std::string & procDirectoryPath);
  /**
   * Returns all the directories where a bulk-request could be persisted in the /proc/ directory
   * @return all the directories where a bulk-request could be persisted in the /proc/ directory
   */
  std::set<std::string> getAllBulkRequestDirectoriesPath();
  std::string getDirectoryPathWhereBulkRequestCouldBeSaved(const BulkRequest::Type & type);
private:
  std::map<BulkRequest::Type, std::string> mBulkRequestTypeToPath;
};

EOSBULKNAMESPACE_END

#endif // EOS_PROCDIRECTORYBULKREQUESTLOCATIONS_HH
