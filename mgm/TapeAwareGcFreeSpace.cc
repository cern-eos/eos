// ----------------------------------------------------------------------
// File: TapeAwareGcFreeSpace.cc
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

#include "mgm/FsView.hh"
#include "mgm/TapeAwareGcFreeSpace.hh"
#include "mgm/TapeAwareGcSpaceNotFound.hh"
#include "mgm/TapeAwareGcUtils.hh"

#include <functional>
#include <sstream>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
TapeAwareGcFreeSpace::TapeAwareGcFreeSpace(const std::string &spaceName,
  const time_t defaultSpaceQueryPeriodSecs):
  m_spaceName(spaceName),
  m_cachedSpaceQueryPeriodSecs(
    defaultSpaceQueryPeriodSecs, // Initial value
    std::bind(TapeAwareGcFreeSpace::getConfSpaceQueryPeriodSecs, spaceName, defaultSpaceQueryPeriodSecs),
    10), // Maximum age of cached value in seconds
  m_freeSpaceBytes(0),
  m_freeSpaceQueryTimestamp(0)
{
}

//------------------------------------------------------------------------------
// Notify this object that a file has been queued for deletion
//------------------------------------------------------------------------------
void
TapeAwareGcFreeSpace::fileQueuedForDeletion(const size_t deletedFileSize) {
  std::lock_guard<std::mutex> lock(m_mutex);

  if(m_freeSpaceBytes < deletedFileSize) {
    m_freeSpaceBytes = 0;
  } else {
    m_freeSpaceBytes -= deletedFileSize;
  }
}

//------------------------------------------------------------------------------
// Return the amount of free space in bytes
//------------------------------------------------------------------------------
uint64_t
TapeAwareGcFreeSpace::getFreeBytes()
{
  std::lock_guard<std::mutex> lock(m_mutex);

  const time_t now = time(nullptr);
  const time_t secsSinceLastQuery = now - m_freeSpaceQueryTimestamp;

  bool spaceQueryPeriodSecsHasChanged = false;
  const auto spaceQueryPeriodSecs = m_cachedSpaceQueryPeriodSecs.get(spaceQueryPeriodSecsHasChanged);
  if(spaceQueryPeriodSecsHasChanged) {
    std::ostringstream msg;
    msg << "msg=\"spaceQueryPeriodSecs has been changed to " << spaceQueryPeriodSecs << "\"";
    eos_static_info(msg.str().c_str());
  }

  if(secsSinceLastQuery >= spaceQueryPeriodSecs) {
    m_freeSpaceQueryTimestamp = now;
    m_freeSpaceBytes = queryMgmForFreeBytes();
    ostringstream msg;
    msg << "TapeAwareGc queried default EOS space for free space: freeSpaceBytes=" << m_freeSpaceBytes;
    eos_static_info(msg.str().c_str());
  }
  return m_freeSpaceBytes;
}

//------------------------------------------------------------------------------
// Query the EOS MGM for the amount of free space in bytes
//------------------------------------------------------------------------------
uint64_t
TapeAwareGcFreeSpace::queryMgmForFreeBytes() {
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

  const auto spaceItor = FsView::gFsView.mSpaceView.find(m_spaceName);

  if(FsView::gFsView.mSpaceView.end() == spaceItor) {
    throw TapeAwareGcSpaceNotFound(std::string(__FUNCTION__) + ": Cannot find space " + m_spaceName);
  }

  if(nullptr == spaceItor->second) {
    throw TapeAwareGcSpaceNotFound(std::string(__FUNCTION__) + ": Cannot find space " + m_spaceName);
  }

  return spaceItor->second->SumLongLong("stat.statfs.freebytes");
}

//------------------------------------------------------------------------------
// Return the configured delay in seconds between free space queries
//------------------------------------------------------------------------------
uint64_t
TapeAwareGcFreeSpace::getConfSpaceQueryPeriodSecs(const std::string spaceName,
  const uint64_t defaultValue) noexcept
{
  try {
    std::string valueStr;

    {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

      const auto spaceItor = FsView::gFsView.mSpaceView.find(spaceName);
      if (FsView::gFsView.mSpaceView.end() != spaceItor && nullptr != spaceItor->second) {
        const auto &space = *(spaceItor->second);
        valueStr = space.GetConfigMember("tapeawaregc.spacequeryperiodsecs");
      }
    }

    return TapeAwareGcUtils::toUint64(valueStr);
  } catch(...) {
    return defaultValue;
  }
}


EOSMGMNAMESPACE_END
