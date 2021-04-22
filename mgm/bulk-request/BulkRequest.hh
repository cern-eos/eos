//------------------------------------------------------------------------------
//! @file BulkRequest.hh
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

#ifndef EOS_BULKREQUEST_HH
#define EOS_BULKREQUEST_HH

#include "mgm/Namespace.hh"
#include <string>
#include <set>

EOSMGMNAMESPACE_BEGIN

/**
 * This class represents a BulkRequest object
 */
class BulkRequest {
public:
  BulkRequest(const std::string & id);
  const std::string getId() const;
private:
  //Id of the bulk request
  std::string mId;
  //Paths of the files contained in the bulk request
  std::set<std::string> mPaths;
};

EOSMGMNAMESPACE_END
#endif // EOS_BULKREQUEST_HH
