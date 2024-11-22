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
  clone(UserCosts, other.UserCosts);
  clone(TotalCosts, other.TotalCosts);
  clone(GroupCosts, other.GroupCosts);
  clone(UserBytes, other.UserBytes);
  clone(TotalBytes, other.TotalBytes);
  clone(UserBytes, other.UserBytes);
  NumFaultyFiles = other.NumFaultyFiles;
  TimeScan = other.TimeScan;
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
  BirthTimeFiles = std::move(BirthTimeFiles);
  BirthTimeVolume = std::move(BirthTimeVolume);
  BirthVsAccessTimeFiles = std::move(other.BirthVsAccessTimeFiles);
  BirthVsAccessTimeVolume = std::move(other.BirthVsAccessTimeVolume);
  move(UserCosts, other.UserCosts);
  move(TotalCosts, other.TotalCosts);
  move(GroupCosts, other.GroupCosts);
  move(UserBytes, other.UserBytes);
  move(TotalBytes, other.TotalBytes);
  move(UserBytes, other.UserBytes);
  NumFaultyFiles = other.NumFaultyFiles;
  TimeScan = other.TimeScan;
  return *this;
}

std::string FileInspectorStatsSerializer::SerializeScanStats()
{
  return Marshal(mFileInspectorStats.ScanStats);
}

std::string FileInspectorStatsSerializer::SerializeFaultyFiles()
{
  return Marshal(mFileInspectorStats.FaultyFiles);
}

std::string FileInspectorStatsSerializer::SerializeAccessTimeFiles()
{
  return Marshal(mFileInspectorStats.AccessTimeFiles);
}

std::string FileInspectorStatsSerializer::SerializeAccessTimeVolume()
{
  return Marshal(mFileInspectorStats.AccessTimeVolume);
}

std::string FileInspectorStatsSerializer::SerializeBirthTimeFiles()
{
  return Marshal(mFileInspectorStats.BirthTimeFiles);
}

std::string FileInspectorStatsSerializer::SerializeBirthTimeVolume()
{
  return Marshal(mFileInspectorStats.BirthTimeVolume);
}

std::string FileInspectorStatsSerializer::SerializeBirthVsAccessTimeFiles()
{
  return Marshal(mFileInspectorStats.BirthVsAccessTimeFiles);
}

std::string FileInspectorStatsSerializer::SerializeBirthVsAccessTimeVolume()
{
  return Marshal(mFileInspectorStats.BirthVsAccessTimeVolume);
}

std::string FileInspectorStatsSerializer::SerializeUserCosts()
{
  return Marshal(mFileInspectorStats.UserCosts);
}

std::string FileInspectorStatsSerializer::SerializeGroupCosts()
{
  return Marshal(mFileInspectorStats.GroupCosts);
}

std::string FileInspectorStatsSerializer::SerializeTotalCosts()
{
  return Marshal(mFileInspectorStats.TotalCosts);
}

std::string FileInspectorStatsSerializer::SerializeUserBytes()
{
  return Marshal(mFileInspectorStats.UserBytes);
}

std::string FileInspectorStatsSerializer::SerializeGroupBytes()
{
  return Marshal(mFileInspectorStats.GroupBytes);
}

std::string FileInspectorStatsSerializer::SerializeTotalBytes()
{
  return Marshal(mFileInspectorStats.TotalBytes);
}

std::string FileInspectorStatsSerializer::SerializeNumFaultyFiles()
{
  return Marshal(mFileInspectorStats.NumFaultyFiles);
}

std::string FileInspectorStatsSerializer::SerializeTimeScan()
{
  return Marshal(mFileInspectorStats.TimeScan);
}

void FileInspectorStatsDeserializer::DeserializeScanStats(
  const std::string& out, FileInspectorStats& stats) const
{
  Unmarshal(out, stats.ScanStats);
}

void FileInspectorStatsDeserializer::DeserializeFaultyFiles(
  const std::string& out, FileInspectorStats& stats) const
{
  Unmarshal(out, stats.FaultyFiles);
}

void FileInspectorStatsDeserializer::DeserializeAccessTimeFiles(
  const std::string& out, FileInspectorStats& stats) const
{
  Unmarshal(out, stats.AccessTimeFiles);
}

void FileInspectorStatsDeserializer::DeserializeAccessTimeVolume(
  const std::string& out, FileInspectorStats& stats) const
{
  Unmarshal(out, stats.AccessTimeVolume);
}

void FileInspectorStatsDeserializer::DeserializeBirthTimeFiles(
  const std::string& out, FileInspectorStats& stats) const
{
  Unmarshal(out, stats.BirthTimeFiles);
}

void FileInspectorStatsDeserializer::DeserializeBirthTimeVolume(
  const std::string& out, FileInspectorStats& stats) const
{
  Unmarshal(out, stats.BirthTimeVolume);
}

void FileInspectorStatsDeserializer::DeserializeBirthVsAccessTimeFiles(
  const std::string& out, FileInspectorStats& stats) const
{
  Unmarshal(out, stats.BirthVsAccessTimeFiles);
}

void FileInspectorStatsDeserializer::DeserializeBirthVsAccessTimeVolume(
  const std::string& out, FileInspectorStats& stats) const
{
  Unmarshal(out, stats.BirthVsAccessTimeVolume);
}

void FileInspectorStatsDeserializer::DeserializeUserCosts(
  const std::string& out, FileInspectorStats& stats) const
{
  Unmarshal(out, stats.UserCosts);
}

void FileInspectorStatsDeserializer::DeserializeGroupCosts(
  const std::string& out, FileInspectorStats& stats) const
{
  Unmarshal(out, stats.GroupCosts);
}

void FileInspectorStatsDeserializer::DeserializeTotalCosts(
  const std::string& out, FileInspectorStats& stats) const
{
  Unmarshal(out, stats.TotalCosts);
}

void FileInspectorStatsDeserializer::DeserializeUserBytes(
  const std::string& out, FileInspectorStats& stats) const
{
  Unmarshal(out, stats.UserBytes);
}

void FileInspectorStatsDeserializer::DeserializeGroupBytes(
  const std::string& out, FileInspectorStats& stats) const
{
  Unmarshal(out, stats.GroupBytes);
}

void FileInspectorStatsDeserializer::DeserializeTotalBytes(
  const std::string& out, FileInspectorStats& stats) const
{
  Unmarshal(out, stats.TotalBytes);
}

void FileInspectorStatsDeserializer::DeserializeNumFaultyFiles(
  const std::string& out, FileInspectorStats& stats) const
{
  Unmarshal(out, stats.NumFaultyFiles);
}

void FileInspectorStatsDeserializer::DeserializeTimeScan(const std::string& out,
    FileInspectorStats& stats) const
{
  Unmarshal(out, stats.TimeScan);
}

EOSMGMNAMESPACE_END