// ----------------------------------------------------------------------
// File: FileInspectorData.cc
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

#include "mgm/inspector/FileInspectorStats.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/QClient.hh"
#include "common/json/Json.hh"
#include <sstream>

EOSMGMNAMESPACE_BEGIN

template <typename T, size_t N>
void clone(T(&dst)[N], const T(&src)[N])
{
  static_assert(std::is_copy_assignable_v<T>,
                "Type must be copy-assignable");

  for (size_t i = 0; i < N; i++) {
    dst[i] = src[i];
  }
}

FileInspectorStats& FileInspectorStats::operator=(const FileInspectorStats&
    other)
{
  if (this == &other) {
    return *this;
  }

  ScanStats = other.ScanStats;
  FaultyFiles = other.FaultyFiles;
  AccessTimeFiles = other.AccessTimeFiles;
  AccessTimeVolume = other.AccessTimeVolume;
  BirthTimeFiles = other.BirthTimeFiles;
  BirthTimeVolume = other.BirthTimeVolume;
  BirthVsAccessTimeFiles = other.BirthVsAccessTimeFiles;
  BirthVsAccessTimeVolume = other.BirthVsAccessTimeVolume;
  SizeBinsFiles = other.SizeBinsFiles;
  SizeBinsVolume = other.SizeBinsVolume;
  BirthVsSizeFiles = other.BirthVsSizeFiles;
  BirthVsSizeVolume = other.BirthVsSizeVolume;
  clone(UserCosts, other.UserCosts);
  clone(TotalCosts, other.TotalCosts);
  clone(GroupCosts, other.GroupCosts);
  clone(UserBytes, other.UserBytes);
  clone(TotalBytes, other.TotalBytes);
  clone(UserBytes, other.UserBytes);
  NumFaultyFiles = other.NumFaultyFiles;
  TotalFileCount = other.TotalFileCount;
  TotalLogicalBytes = other.TotalLogicalBytes;
  TimeScan = other.TimeScan;
  HardlinkCount = other.HardlinkCount;
  HardlinkVolume = other.HardlinkVolume;
  SymlinkCount = other.SymlinkCount;
  return *this;
}

template <typename T, size_t N>
void move(T(&dst)[N], T(&src)[N]) noexcept
{
  static_assert(std::is_move_assignable_v<T>,
                "Type must be move-assignable");

  for (size_t i = 0; i < N; i++) {
    dst[i] = std::move(src[i]);
    src[i] = T{}; // initialize with zero value
  }
}


FileInspectorStats& FileInspectorStats::operator=(FileInspectorStats&& other)
noexcept
{
  if (this == &other) {
    return *this;
  }

  ScanStats = std::move(other.ScanStats);
  FaultyFiles = std::move(other.FaultyFiles);
  AccessTimeFiles = std::move(other.AccessTimeFiles);
  AccessTimeVolume = std::move(other.AccessTimeVolume);
  BirthTimeFiles = std::move(other.BirthTimeFiles);
  BirthTimeVolume = std::move(other.BirthTimeVolume);
  BirthVsAccessTimeFiles = std::move(other.BirthVsAccessTimeFiles);
  BirthVsAccessTimeVolume = std::move(other.BirthVsAccessTimeVolume);
  SizeBinsFiles = std::move(other.SizeBinsFiles);
  SizeBinsVolume = std::move(other.SizeBinsVolume);
  BirthVsSizeFiles = std::move(other.BirthVsSizeFiles);
  BirthVsSizeVolume = std::move(other.BirthVsSizeVolume);
  move(UserCosts, other.UserCosts);
  move(TotalCosts, other.TotalCosts);
  move(GroupCosts, other.GroupCosts);
  move(UserBytes, other.UserBytes);
  move(TotalBytes, other.TotalBytes);
  move(UserBytes, other.UserBytes);
  NumFaultyFiles = other.NumFaultyFiles;
  TotalFileCount = other.TotalFileCount;
  TotalLogicalBytes = other.TotalLogicalBytes;
  TimeScan = other.TimeScan;
  HardlinkCount = other.HardlinkCount;
  HardlinkVolume = other.HardlinkVolume;
  SymlinkCount = other.SymlinkCount;
  return *this;
}

EOSMGMNAMESPACE_END