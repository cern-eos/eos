// ----------------------------------------------------------------------
// File: SmartSpaceStats.cc
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

#include "mgm/tgc/SmartSpaceStats.hh"

EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Constructor
//------------------------------------------------------------------------------
SmartSpaceStats::SmartSpaceStats(const std::string &spaceName, ITapeGcMgm &mgm, CachedValue<SpaceConfig> &config):
  m_spaceName(spaceName), m_mgm(mgm), m_queryTimestamp(0), m_config(config)
{
}

//------------------------------------------------------------------------------
// Return statistics about the EOS space being managed
//------------------------------------------------------------------------------
SpaceStats
SmartSpaceStats::get()
{
  const std::time_t now = time(nullptr);

  const auto spaceConfig = m_config.get();

  std::lock_guard<std::mutex> lock(m_mutex);
  const std::time_t secsSinceLastQuery = now - m_queryTimestamp;

  if(secsSinceLastQuery >= spaceConfig.queryPeriodSecs) {
    try {
      m_stats = m_mgm.getSpaceStats(m_spaceName);
    } catch(...) {
      m_stats = SpaceStats();
    }
    m_queryTimestamp = now;
  }

  return m_stats;
}

//----------------------------------------------------------------------------
// Return timestamp at which the last query was made
//----------------------------------------------------------------------------
std::time_t
SmartSpaceStats::getQueryTimestamp()
{
  std::lock_guard<std::mutex> lock(m_mutex);
  return m_queryTimestamp;
}

//------------------------------------------------------------------------------
// Notify this object that a file has been queued for deletion
//------------------------------------------------------------------------------
void
SmartSpaceStats::fileQueuedForDeletion(const size_t deletedFileSizeBytes)
{
  std::lock_guard<std::mutex> lock(m_mutex);

  m_stats.availBytes += deletedFileSizeBytes;
}

EOSTGCNAMESPACE_END
