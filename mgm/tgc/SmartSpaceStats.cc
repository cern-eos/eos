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

#include "common/Logging.hh"
#include "mgm/tgc/SmartSpaceStats.hh"
#include "mgm/tgc/Utils.hh"

#include <sstream>

EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Constructor
//------------------------------------------------------------------------------
SmartSpaceStats::SmartSpaceStats(const std::string &spaceName, ITapeGcMgm &mgm, CachedValue<SpaceConfig> &config):
  m_spaceName(spaceName),
  m_mgm(mgm),
  m_queryMgmTimestamp(0),
  m_freedBytesHistogram(TGC_FREED_BYTES_HISTOGRAM_NB_BINS, TGC_DEFAULT_FREED_BYTES_HISTOGRAM_BIN_WIDTH_SECS, m_clock),
  m_config(config)
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
  const std::time_t secsSinceLastQuery = now - m_queryMgmTimestamp;

  if(secsSinceLastQuery >= spaceConfig.queryPeriodSecs) {
    try {
      m_mgmStats = m_mgm.getSpaceStats(m_spaceName);
    } catch(...) {
      m_mgmStats = SpaceStats();
    }
    m_queryMgmTimestamp = now;
  }

  if (0 == spaceConfig.queryPeriodSecs || TGC_MAX_QRY_PERIOD_SECS < ((std::uint64_t)spaceConfig.queryPeriodSecs)) {
    std::ostringstream msg;
    msg << "spaceName=\"" << m_spaceName << "\" msg=\"Ignoring new value of " << TGC_NAME_QRY_PERIOD_SECS <<
      " : Value must be > 0 and <= " << TGC_MAX_QRY_PERIOD_SECS << ": Value=" << spaceConfig.queryPeriodSecs << "\"";
    eos_static_err(msg.str().c_str());
  } else {
    const std::uint32_t oldBinWidthSecs = m_freedBytesHistogram.getBinWidthSecs();
    const std::uint32_t newBinWidthSecs =
      Utils::divideAndRoundUp(spaceConfig.queryPeriodSecs, m_freedBytesHistogram.getNbBins());
    if (0 == newBinWidthSecs) {
      std::ostringstream msg;
      msg << "spaceName=\"" << m_spaceName << "\" msg=\"The newBinWidthSecs value of 0 will be ignored."
        " Value must be greater than 0.\"";
      eos_static_err(msg.str().c_str());
    } else if (newBinWidthSecs != oldBinWidthSecs) {
      m_freedBytesHistogram.setBinWidthSecs(newBinWidthSecs);
      std::ostringstream msg;
      msg << "spaceName=\"" << m_spaceName << "\" msg=\"Changed bin width of freed bytes histogram:"
        " oldValue=" << oldBinWidthSecs << " newValue=" << newBinWidthSecs << "\"";
      eos_static_info(msg.str().c_str());
    }
  }

  // Space statistics from the MGM are not timestamped and therefore may
  // themselves be out of data by as much as spaceConfig.queryPeriodSecs
  //
  // Add the count of bytes the garbage collector has freed in the last
  // spaceConfig.queryPeriodSecs even if this may cause a temporary double
  // count
  uint64_t nbBytesFreed = 0;
  try {
    nbBytesFreed = m_freedBytesHistogram.getNbBytesFreedInLastNbSecs(spaceConfig.queryPeriodSecs);
  } catch(FreedBytesHistogram::TooFarBackInTime &ex) {
    nbBytesFreed = m_freedBytesHistogram.getTotalBytesFreed();

    std::ostringstream msg;
    msg << "msg=\"" << ex.what() << "\"";
    eos_static_err(msg.str().c_str());
  }
  m_mgmStats.availBytes += nbBytesFreed;

  return m_mgmStats;
}

//----------------------------------------------------------------------------
// Return timestamp at which the last query was made
//----------------------------------------------------------------------------
std::time_t
SmartSpaceStats::getQueryTimestamp()
{
  std::lock_guard<std::mutex> lock(m_mutex);
  return m_queryMgmTimestamp;
}

//------------------------------------------------------------------------------
// Notify this object that a file has been queued for deletion
//------------------------------------------------------------------------------
void
SmartSpaceStats::fileQueuedForDeletion(const size_t deletedFileSizeBytes)
{
  std::lock_guard<std::mutex> lock(m_mutex);

  m_freedBytesHistogram.bytesFreed(deletedFileSizeBytes);
}

EOSTGCNAMESPACE_END
