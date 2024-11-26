// ----------------------------------------------------------------------
// File: FileInspectorData.hh
// Author: Gianmaria Del Monte - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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

#include <cstdint>
#include <string>
#include <map>
#include <set>
#include <atomic>
#include "mgm/Namespace.hh"
#include <qclient/QClient.hh>

EOSMGMNAMESPACE_BEGIN


#define SCAN_STATS_KEY "scan-stats"
#define FAULTY_FILES_KEY "faulty-files"
#define ACCESS_TIME_FILES_KEY "access-time-files"
#define ACCESS_TIME_VOLUME_KEY "access-time-volume"
#define BIRTH_TIME_FILES_KEY "birth-time-files"
#define BIRTH_TIME_VOLUME_KEY "birth-time-volume"
#define BIRTH_VS_ACCESS_TIME_FILES_KEY "birth-vs-access-time-files"
#define BIRTH_VS_ACCESS_TIME_VOLUME_KEY "birth-vs-access-volume-files"
#define USER_COSTS_KEY "user-costs"
#define GROUP_COSTS_KEY "group-costs"
#define TOTAL_COSTS_KEY "total-costs"
#define USER_BYTES_KEY "user-bytes"
#define GROUP_BYTES_KEY "group-bytes"
#define TOTAL_BYTES_KEY "total-bytes"
#define NUM_FAULTY_FILES_KEY "num-faulty-files"
#define TIME_SCAN_KEY "time-scan"

//------------------------------------------------------------------------------
//! Class holding the stats information
//------------------------------------------------------------------------------
struct FileInspectorStats {

  FileInspectorStats() : TimeScan(0) {}

  ~FileInspectorStats() {};

  FileInspectorStats(const FileInspectorStats& other)
  {
    *this = other;
  }

  FileInspectorStats& operator=(const FileInspectorStats& other);

  FileInspectorStats(FileInspectorStats&& other) noexcept
  {
    *this = std::move(other);
  }

  FileInspectorStats& operator=(FileInspectorStats&& other) noexcept;

  // Counters for the last and current scan by layout id
  std::map<uint64_t, std::map<std::string, uint64_t>> ScanStats;
  //! Map from types of failures to pairs of fid and layoutid
  std::map<std::string, std::map<uint64_t, uint64_t>> FaultyFiles;
  //! Access Time Bins
  std::map<time_t, uint64_t> AccessTimeFiles;
  std::map<time_t, uint64_t> AccessTimeVolume;

  //! Birth Time Bins
  std::map<time_t, uint64_t> BirthTimeFiles;
  std::map<time_t, uint64_t> BirthTimeVolume;

  //! BirthVsAccess Time Bins
  std::map<time_t, std::map<time_t, uint64_t>> BirthVsAccessTimeFiles;
  std::map<time_t, std::map<time_t, uint64_t>> BirthVsAccessTimeVolume;

  //! User Cost Bins
  std::map<uid_t, uint64_t> UserCosts[2];

  //! Group Cost Bins
  std::map<gid_t, uint64_t> GroupCosts[2];

  double TotalCosts[2] = {0};

  //! User Bytes Bins
  std::map<uid_t, uint64_t> UserBytes[2];

  //! Group Bytes Bins
  std::map<gid_t, uint64_t> GroupBytes[2];

  double TotalBytes[2] = {0};

  //! Running count of number of time files have been classed faulty
  uint64_t NumFaultyFiles = 0;

  time_t TimeScan;

private:
  const std::string kFileInspectorStats = "eos-file-inspector-stats";
};


EOSMGMNAMESPACE_END