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

#include "mgm/Namespace.hh"
#include "mgm/XrdMgmOfs.hh"
#include "namespace/interface/IView.hh"

EOSMGMNAMESPACE_BEGIN

static constexpr decltype(gOFS->eosFileService->getFileMD(0)->getLocation(0)) TAPE_FS_ID = 65535;
static constexpr auto RETRIEVES_ATTR_NAME = "sys.retrieves";
static constexpr auto RETRIEVES_ERROR_ATTR_NAME = "sys.retrieve.error";
static constexpr auto ARCHIVE_ERROR_ATTR_NAME = "sys.archive.error";
static constexpr auto RETRIEVE_WRITTEN_WORKFLOW_NAME = "retrieve_written";
static constexpr auto RETRIEVE_FAILED_WORKFLOW_NAME = "retrieve_failed";
static constexpr auto ARCHIVE_FAILED_WORKFLOW_NAME = "archive_failed";

EOSMGMNAMESPACE_END