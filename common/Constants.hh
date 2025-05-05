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
#include <sys/types.h>

EOSCOMMONNAMESPACE_BEGIN

static constexpr auto TAPE_FS_ID = 65535u;
//! List of Prepare request IDs for this file
static constexpr auto RETRIEVE_REQID_ATTR_NAME = "sys.retrieve.req_id";
//! Last time the Prepare request was actioned
static constexpr auto RETRIEVE_REQTIME_ATTR_NAME = "sys.retrieve.req_time";
//! Counter for multiple staging requests on same file
static constexpr auto RETRIEVE_EVICT_COUNTER_NAME =
  "sys.retrieve.evict_counter";
//! Prepare request failure reason
static constexpr auto RETRIEVE_ERROR_ATTR_NAME = "sys.retrieve.error";
//! Archive request failure reason
static constexpr auto ARCHIVE_ERROR_ATTR_NAME = "sys.archive.error";
//! Archive file ID
static constexpr auto ARCHIVE_FILE_ID_ATTR_NAME = "sys.archive.file_id";
//! Archive storage class
static constexpr auto ARCHIVE_STORAGE_CLASS_ATTR_NAME =
  "sys.archive.storage_class";
//! EOS file btime
static constexpr auto EOS_BTIME = "sys.eos.btime";
//! CTA internal objectsore id for archive requests
static constexpr auto CTA_OBJECTSTORE_ARCHIVE_REQ_ID_NAME =
  "sys.cta.archive.objectstore.id";
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
//! Time interval after which a scanned rain filed is rescanned
static constexpr auto SCAN_RAIN_ENTRY_INTERVAL_NAME = "scan_rain_interval";
//! Time interval after which the scanner will rerun
static constexpr auto SCAN_DISK_INTERVAL_NAME = "scan_disk_interval";
//! Maximum ns scan rate when it comes to stat requests done against the
//! local disks on the FSTs
static constexpr auto SCAN_NS_RATE_NAME = "scan_ns_rate";
//! Time interval after which the ns scanner will rerun
static constexpr auto SCAN_NS_INTERVAL_NAME = "scan_ns_interval";
//! Time interval after which fsck inconsistencies are refreshed on each
//! file system.
static constexpr auto FSCK_REFRESH_INTERVAL_NAME = "fsck_refresh_interval";
//! Special EOS scheduling group space
static constexpr auto EOS_SPARE_GROUP = "spare";
//! Application lock attribute
static constexpr auto EOS_APP_LOCK_ATTR = "sys.app.lock";
//! Trace attributes
static constexpr auto EOS_DTRACE_ATTR = "sys.dtrace";
static constexpr auto EOS_VTRACE_ATTR = "sys.vtrace";
static constexpr auto EOS_UTRACE_ATTR = "sys.utrace";
//! FST heartbeat key marker, the "stat." prefix makes it transient
static constexpr auto FST_HEARTBEAT_KEY = "stat.heartbeat";
//! ADM uid and gid
static constexpr uid_t ADM_UID = 3;
static constexpr gid_t ADM_GID = 4;
EOSCOMMONNAMESPACE_END
