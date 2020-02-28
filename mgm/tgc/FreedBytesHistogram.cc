// ----------------------------------------------------------------------
// File: FreedBytesHistogram.cc
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
#include "mgm/tgc/FreedBytesHistogram.hh"
#include "mgm/tgc/Utils.hh"

#include <algorithm>
#include <sstream>

EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FreedBytesHistogram::FreedBytesHistogram(
  const std::uint32_t nbBins,
  const std::uint32_t binWidthSecs,
  IClock &clock):
  m_histogram(nbBins, 0),
  m_startIndex(0),
  m_binWidthSecs(binWidthSecs),
  m_clock(clock)
{
  m_lastUpdateTimestamp = m_clock.getTime();

  if (0 == nbBins || TGC_FREED_BYTES_HISTOGRAM_MAX_NB_BINS < nbBins) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " failed: nbBins is invalid. Value must be > 0 and <= " <<
      TGC_FREED_BYTES_HISTOGRAM_MAX_NB_BINS;
    throw InvalidNbBins(msg.str());
  }

  if (0 == binWidthSecs || TGC_FREED_BYTES_HISTOGRAM_MAX_BIN_WIDTH_SECS < binWidthSecs) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " failed: binWidthSecs is invalid. Value must be > 0 and <= " <<
      TGC_FREED_BYTES_HISTOGRAM_MAX_BIN_WIDTH_SECS;
    throw InvalidBinWidth(msg.str());
  }
}

//------------------------------------------------------------------------------
// Notify cache that bytes were freed
//------------------------------------------------------------------------------
void
FreedBytesHistogram::bytesFreed(const uint64_t nbBytes)
{
  std::lock_guard<std::mutex> lock(m_mutex);

  alignHistogramWithNow();

  // Update youngest bin
  m_histogram.at(m_startIndex) += nbBytes;
}

//------------------------------------------------------------------------------
// Return number of bytes freed in the specified last number of seconds
//------------------------------------------------------------------------------
std::uint64_t
FreedBytesHistogram::getNbBytesFreedInLastNbSecs(const std::uint32_t lastNbSecs)
{
  std::lock_guard<std::mutex> lock(m_mutex);

  const std::uint32_t nbBins = m_histogram.size();
  const std::uint32_t historicalDepth = nbBins * m_binWidthSecs;

  if (lastNbSecs > m_histogram.size() * m_binWidthSecs) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " failed: Cannot go back more than " << historicalDepth << " seconds"
     ": requested=" << lastNbSecs << ": Try reducing " << TGC_NAME_QRY_PERIOD_SECS;
    throw TooFarBackInTime(msg.str());
  }
  const size_t nbBinsToTotal = Utils::divideAndRoundUp(lastNbSecs, m_binWidthSecs);
  
  alignHistogramWithNow();

  uint64_t total = 0;
  for (size_t binIndexOffset = 0; binIndexOffset < nbBinsToTotal; binIndexOffset++) {
    const size_t binIndex = (m_startIndex + binIndexOffset) % nbBins;
    total += m_histogram.at(binIndex);
  }

  return total;
}

//------------------------------------------------------------------------------
// Return the total number of freed bytes
//------------------------------------------------------------------------------
std::uint64_t
FreedBytesHistogram::getTotalBytesFreed() {
  std::lock_guard<std::mutex> lock(m_mutex);

  alignHistogramWithNow();

  std::uint64_t total = 0;
  for (size_t i = 0; i < m_histogram.size(); i++) {
    total += m_histogram.at(i);
  }

  return total;
}

//----------------------------------------------------------------------------
//! Return the number of bytes freed in the specified bin
//----------------------------------------------------------------------------
std::uint64_t
FreedBytesHistogram::getFreedBytesInBin(const std::uint32_t binIndex) const {
  std::lock_guard<std::mutex> lock(m_mutex);

  const std::uint32_t maxBinIndex = m_histogram.size() - 1;

  if (binIndex > maxBinIndex) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " failed: binIndex is too large: binIndex=" << binIndex << " maxBinIndex=" << maxBinIndex;
    throw InvalidBinIndex(msg.str());
  }

  const std::uint32_t circularBinIndex = (m_startIndex + binIndex) % m_histogram.size();

  return m_histogram.at(circularBinIndex);
}


//------------------------------------------------------------------------------
// Slide histogram to the right until the first bin is aligned with now
//------------------------------------------------------------------------------
void
FreedBytesHistogram::alignHistogramWithNow()
{
  const time_t now = m_clock.getTime();

  const time_t ageSecs = now - m_lastUpdateTimestamp;
  const size_t rawNbBinsToMove = Utils::divideAndRoundToNearest(ageSecs, m_binWidthSecs);
  const size_t nbBinsToMove = std::min(m_histogram.size(), rawNbBinsToMove);

  // Move start index backwards in order to slide histigram to the the right
  m_startIndex = (m_startIndex + m_histogram.size() - nbBinsToMove) % m_histogram.size();

  // Zero off out-of-date bins
  for (size_t i = 0; i < nbBinsToMove; i++) {
    const size_t binIndex = (m_startIndex + i) % m_histogram.size();
    m_histogram.at(binIndex) = 0;
  }

  // Update histogram timestamp
  m_lastUpdateTimestamp = now;
}

//------------------------------------------------------------------------------
// Set the bin width
//------------------------------------------------------------------------------
void
FreedBytesHistogram::setBinWidthSecs(const std::uint32_t newBinWidthSecs)
{
  if (0 == newBinWidthSecs || TGC_FREED_BYTES_HISTOGRAM_MAX_BIN_WIDTH_SECS < newBinWidthSecs) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " failed: newBinWidthSecs is invalid. Value must be > 0 and <= " <<
      TGC_FREED_BYTES_HISTOGRAM_MAX_BIN_WIDTH_SECS;
    throw InvalidBinWidth(msg.str());
  }

  std::lock_guard<std::mutex> lock(m_mutex);

  const std::uint32_t nbBins = m_histogram.size();

  std::vector tempHistogram(m_histogram.size(), 0);
  const std::uint32_t newHistoricalDepthSecs = nbBins * newBinWidthSecs;

  for (std::uint32_t secsAgo = 1; secsAgo <= newHistoricalDepthSecs; secsAgo++) {
    const std::uint32_t binIndex = (secsAgo - 1) / newBinWidthSecs;
    std::uint64_t bytesFreedPerSec = 0;
    try {
      bytesFreedPerSec = getFreedBytesPerSec(secsAgo);
    } catch(TooFarBackInTime &) {
      break;
    }
    tempHistogram.at(binIndex) += bytesFreedPerSec;
  }

  for (std::uint32_t binIndex = 0; binIndex < nbBins; binIndex++) {
    m_histogram.at(binIndex) = tempHistogram.at(binIndex);
  }
  m_startIndex = 0;
  m_binWidthSecs = newBinWidthSecs;
}

//------------------------------------------------------------------------------
// Return bin width in seconds
//------------------------------------------------------------------------------
std::uint32_t
FreedBytesHistogram::getBinWidthSecs() const {
  std::lock_guard<std::mutex> lock(m_mutex);

  return m_binWidthSecs;
}

//------------------------------------------------------------------------------
// Return number of bins
//------------------------------------------------------------------------------
std::uint32_t
FreedBytesHistogram::getNbBins() const {
  std::lock_guard<std::mutex> lock(m_mutex);

  return m_histogram.size();
}

//------------------------------------------------------------------------------
// Return bytes freed per second during the specified second
//------------------------------------------------------------------------------
std::uint64_t
FreedBytesHistogram::getFreedBytesPerSec(const std::uint32_t secsAgo) const
{
  if (secsAgo > m_histogram.size() * m_binWidthSecs) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " failed: Cannot go back more than " << m_histogram.size() * m_binWidthSecs << " seconds"
     ": requested=" << secsAgo;
    throw TooFarBackInTime(msg.str());
  }

  if (0 == secsAgo) return 0;

  const size_t binIndexOffset = (secsAgo - 1) / m_binWidthSecs;
  const size_t binIndex = (m_startIndex + binIndexOffset) % m_histogram.size();
  const uint64_t freedBytes = m_histogram.at(binIndex);
  const uint64_t bytesPerSec = Utils::divideAndRoundToNearest(freedBytes, m_binWidthSecs);

  return bytesPerSec;
}

EOSTGCNAMESPACE_END
