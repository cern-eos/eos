//------------------------------------------------------------------------------
//! @file MockTapeRestApiBusiness.hh
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

#ifndef EOS_MOCKTAPERESTAPIBUSINESS_HH
#define EOS_MOCKTAPERESTAPIBUSINESS_HH

#include "mgm/http/rest-api/business/tape/ITapeRestApiBusiness.hh"
#include <gmock/gmock.h>

EOSMGMRESTNAMESPACE_BEGIN

class MockTapeRestApiBusiness : public ITapeRestApiBusiness
{
public:
  MOCK_METHOD(std::shared_ptr<bulk::BulkRequest>, createStageBulkRequest,
              (const CreateStageBulkRequestModel*, const common::VirtualIdentity*),
              (override));
  MOCK_METHOD(void, cancelStageBulkRequest,
              (const std::string&, const PathsModel*, const common::VirtualIdentity*),
              (override));
  MOCK_METHOD(std::shared_ptr<GetStageBulkRequestResponseModel>, getStageBulkRequest,
              (const std::string&, const common::VirtualIdentity*), (override));
  MOCK_METHOD(void, deleteStageBulkRequest,
              (const std::string&, const common::VirtualIdentity*), (override));
  MOCK_METHOD(std::shared_ptr<bulk::QueryPrepareResponse>, getFileInfo,
              (const PathsModel*, const common::VirtualIdentity*), (override));
  MOCK_METHOD(void, releasePaths,
              (const PathsModel*, const common::VirtualIdentity*), (override));
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_MOCKTAPERESTAPIBUSINESS_HH
