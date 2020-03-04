// ----------------------------------------------------------------------
// File: TapeGc.cc
// Author: Steven Murray - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#include "mgm/tgc/Constants.hh"
#include "mgm/tgc/DummyTapeGcMgm.hh"

EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
DummyTapeGcMgm::DummyTapeGcMgm():
m_nbCallsToGetTapeGcSpaceConfig(0),
m_nbCallsToGetSpaceStats(0),
m_nbCallsToFileInNamespaceAndNotScheduledForDeletion(0),
m_nbCallsToGetFileSizeBytes(0),
m_nbCallsToStagerrmAsRoot(0)
{
}

//----------------------------------------------------------------------------
//! @return The configuration of a tape-aware garbage collector for the
//! specified space.
//! @param spaceName The name of the space
//----------------------------------------------------------------------------
SpaceConfig
DummyTapeGcMgm::getTapeGcSpaceConfig(const std::string &spaceName) {
  const SpaceConfig defaultConfig;

  try {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_nbCallsToGetTapeGcSpaceConfig++;

    auto itor = m_spaceToTapeGcConfig.find(spaceName);
    if(itor == m_spaceToTapeGcConfig.end()) {
      return defaultConfig;
    } else {
      return itor->second;
    }
  } catch(...) {
    return defaultConfig;
  }
}

//----------------------------------------------------------------------------
// Determine if the specified file exists and is not scheduled for deletion
//----------------------------------------------------------------------------
bool
DummyTapeGcMgm::fileInNamespaceAndNotScheduledForDeletion(const IFileMD::id_t /* fid */)
{
  std::lock_guard<std::mutex> lock(m_mutex);
  m_nbCallsToFileInNamespaceAndNotScheduledForDeletion++;
  return true;
}

//----------------------------------------------------------------------------
// Return statistics about the specified space
//----------------------------------------------------------------------------
SpaceStats
DummyTapeGcMgm::getSpaceStats(const std::string &space) const {
  const SpaceStats defaultStats;

  try {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_nbCallsToGetSpaceStats++;

    auto itor = m_spaceToStats.find(space);
    if(itor == m_spaceToStats.end()) {
      return defaultStats;
    } else {
      return itor->second;
    }
  } catch(...) {
    return defaultStats;
  }
}

//----------------------------------------------------------------------------
// Return size of the specified file
//----------------------------------------------------------------------------
uint64_t
DummyTapeGcMgm::getFileSizeBytes(const IFileMD::id_t /* fid */)
{
  std::lock_guard<std::mutex> lock(m_mutex);
  m_nbCallsToGetFileSizeBytes++;
  return 1;
}

//----------------------------------------------------------------------------
// Execute stagerrm as user root
//----------------------------------------------------------------------------
void
DummyTapeGcMgm::stagerrmAsRoot(const IFileMD::id_t /* fid */)
{
  std::lock_guard<std::mutex> lock(m_mutex);
  m_nbCallsToStagerrmAsRoot++;
}

//----------------------------------------------------------------------------
// Return map from file system ID to EOS space name
//----------------------------------------------------------------------------
std::map<common::FileSystem::fsid_t, std::string>
DummyTapeGcMgm::getFsIdToSpaceMap()
{
  return std::map<common::FileSystem::fsid_t, std::string> ();
}

//----------------------------------------------------------------------------
// Return map from EOS space name to disk replicas within that space
//----------------------------------------------------------------------------
std::map<std::string, std::set<ITapeGcMgm::FileIdAndCtime> >
DummyTapeGcMgm::getSpaceToDiskReplicasMap(const std::set<std::string> &spacesToMap, std::atomic<bool> &stop,
  uint64_t &nbfilesScanned)
{
  nbfilesScanned = 0;
  return std::map<std::string, std::set<FileIdAndCtime> >();
}

//----------------------------------------------------------------------------
// Set the configuration of the tape-aware garbage collector
//----------------------------------------------------------------------------
void
DummyTapeGcMgm::setTapeGcSpaceConfig(const std::string &space,
  const SpaceConfig &config) {
  std::lock_guard<std::mutex> lock(m_mutex);

  m_spaceToTapeGcConfig[space] = config;
}

//----------------------------------------------------------------------------
// Set the statistics of the specified EOS space.
//----------------------------------------------------------------------------
void
DummyTapeGcMgm::setSpaceStats(const std::string &space,
  const SpaceStats &spaceStats) {
  std::lock_guard<std::mutex> lock(m_mutex);

  m_spaceToStats[space] = spaceStats;
}

//------------------------------------------------------------------------------
// Return number of times getTapeGcSpaceConfig() has been called
//------------------------------------------------------------------------------
uint64_t
DummyTapeGcMgm::getNbCallsToGetTapeGcSpaceConfig() const {
  std::lock_guard<std::mutex> lock(m_mutex);

  return m_nbCallsToGetTapeGcSpaceConfig;
}

//------------------------------------------------------------------------------
// Return number of times fileInNamespaceAndNotScheduledForDeletion() has been
// called
//------------------------------------------------------------------------------
uint64_t
DummyTapeGcMgm::getNbCallsToFileInNamespaceAndNotScheduledForDeletion() const {
  std::lock_guard<std::mutex> lock(m_mutex);

  return m_nbCallsToFileInNamespaceAndNotScheduledForDeletion;
}

//------------------------------------------------------------------------------
// Return number of times getFileSizeBytes() has been called
//------------------------------------------------------------------------------
uint64_t
DummyTapeGcMgm::getNbCallsToGetFileSizeBytes() const {
  std::lock_guard<std::mutex> lock(m_mutex);

  return m_nbCallsToGetFileSizeBytes;
}

//------------------------------------------------------------------------------
// Return number of times stagerrmAsRoot() has been called
//------------------------------------------------------------------------------
uint64_t
DummyTapeGcMgm::getNbCallsToStagerrmAsRoot() const {
  std::lock_guard<std::mutex> lock(m_mutex);

  return m_nbCallsToStagerrmAsRoot;
}

EOSTGCNAMESPACE_END
