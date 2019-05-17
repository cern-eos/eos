//------------------------------------------------------------------------------
// File: Constants.hh
// Author: Jozsef Makai - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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


#pragma once

#include "common/Namespace.hh"

EOSCOMMONNAMESPACE_BEGIN

static constexpr auto TAPE_FS_ID = 65535u;
static constexpr auto RETRIEVE_REQID_ATTR_NAME = "sys.retrieve.req_id";        //!< List of Prepare request IDs for this file
static constexpr auto RETRIEVE_REQTIME_ATTR_NAME = "sys.retrieve.req_time";    //!< Last time the Prepare request was actioned
static constexpr auto RETRIEVE_ERROR_ATTR_NAME = "sys.retrieve.error";         //!< Prepare request failure reason
static constexpr auto ARCHIVE_ERROR_ATTR_NAME = "sys.archive.error";           //!< Archive request failure reason
static constexpr auto RETRIEVE_WRITTEN_WORKFLOW_NAME = "retrieve_written";
static constexpr auto RETRIEVE_FAILED_WORKFLOW_NAME = "sync::retrieve_failed";
static constexpr auto ARCHIVE_FAILED_WORKFLOW_NAME = "sync::archive_failed";
static constexpr auto WF_CUSTOM_ATTRIBUTES_TO_FST_EQUALS = "=";
static constexpr auto WF_CUSTOM_ATTRIBUTES_TO_FST_SEPARATOR = ";;;";

EOSCOMMONNAMESPACE_END
