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
//! List of Prepare request IDs for this file
static constexpr auto RETRIEVE_REQID_ATTR_NAME = "sys.retrieve.req_id";
//! Last time the Prepare request was actioned
static constexpr auto RETRIEVE_REQTIME_ATTR_NAME = "sys.retrieve.req_time";
//! Prepare request failure reason
static constexpr auto RETRIEVE_ERROR_ATTR_NAME = "sys.retrieve.error";
//! Archive request failure reason
static constexpr auto ARCHIVE_ERROR_ATTR_NAME = "sys.archive.error";
//! CTA internal objectstore id for prepare requests
static constexpr auto CTA_OBJECTSTORE_REQ_ID_NAME = "sys.cta.objectstore.id";
// Workflow names
static constexpr auto RETRIEVE_WRITTEN_WORKFLOW_NAME = "retrieve_written";
static constexpr auto RETRIEVE_FAILED_WORKFLOW_NAME = "sync::retrieve_failed";
static constexpr auto ARCHIVE_FAILED_WORKFLOW_NAME = "sync::archive_failed";
static constexpr auto WF_CUSTOM_ATTRIBUTES_TO_FST_EQUALS = "=";
static constexpr auto WF_CUSTOM_ATTRIBUTES_TO_FST_SEPARATOR = ";;;";
//! Max rate in MB/s at which the scanner should run
static constexpr auto SCAN_IO_RATE_NAME = "scanrate";
//! Time interval after which a scanned filed is rescanned
static constexpr auto SCAN_ENTRY_INTERVAL_NAME = "scaninterval";
//! Time interval after which the scanner will rerun
static constexpr auto SCAN_DISK_INTERVAL_NAME = "scan_disk_interval";
//! Maximum ns scan rate when it comes to stat requests done against the
//! local disks on the FSTs
static constexpr auto SCAN_NS_RATE_NAME = "scan_ns_rate";
//! Time interval after which the ns scanner will rerun
static constexpr auto SCAN_NS_INTERVAL_NAME = "scan_ns_interval";
EOSCOMMONNAMESPACE_END
