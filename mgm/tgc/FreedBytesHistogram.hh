// ----------------------------------------------------------------------
// File: FreedBytesHistogram.hh
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

#ifndef __EOSMGMTGCFREEDBYTESHISTOGRAM_HH__
#define __EOSMGMTGCFREEDBYTESHISTOGRAM_HH__

#include "mgm/Namespace.hh"
#include "mgm/tgc/IClock.hh"

#include <cstdint>
#include <ctime>
#include <mutex>
#include <stdexcept>
#include <vector>

/*----------------------------------------------------------------------------*/
/**
 * @file FreedBytesHistogram.hh
 *
 * @brief Histogram of freed bytes over time.
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Histogram of freed bytes over time.
//------------------------------------------------------------------------------
class FreedBytesHistogram {
public:

  //----------------------------------------------------------------------------
  //! Thrown when an invalid number of bins has been specified
  //----------------------------------------------------------------------------
  struct InvalidNbBins: public std::runtime_error {
    InvalidNbBins(const std::string &what): runtime_error(what) {}
  };

  //----------------------------------------------------------------------------
  //! Thrown when an invalid bin width has been specified
  //----------------------------------------------------------------------------
  struct InvalidBinWidth: public std::runtime_error {
    InvalidBinWidth(const std::string &what): runtime_error(what) {}
  };

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param nbBins The number of bins in the histogram
  //! @param binWidthSecs The width of a bin in seconds
  //! @param clock Object responsible for giving the current time
  //! @throw InvalidNbBins If nbBins is invalid
  //! @throw InvalidBinWidth If binWidthSecs is invalid
  //----------------------------------------------------------------------------
  FreedBytesHistogram(std::uint32_t nbBins, std::uint32_t binWidthSecs, IClock &clock);

  //----------------------------------------------------------------------------
  //! Notify cache that bytes were freed
  //----------------------------------------------------------------------------
  void bytesFreed(std::uint64_t nbBytes);

  //----------------------------------------------------------------------------
  //! Thrown when a request for historic data goes too far back in time
  //----------------------------------------------------------------------------
  struct TooFarBackInTime: public std::runtime_error {
    TooFarBackInTime(const std::string &what): runtime_error(what) {}
  };

  //----------------------------------------------------------------------------
  //! @return number of bytes freed in the specified last number of seconds
  //! @param lastNbSecs The last number of seconds.  A value of 0 seconds will
  //! always return a value of 0 freed bytes.
  //! @throw TooFarBackInTime when lastNbSecs goes back in time more than the
  //! finite capacity of the underlying histogram, in other words if more than
  //! nbBins * binWidthSecs
  //----------------------------------------------------------------------------
  std::uint64_t getNbBytesFreedInLastNbSecs(std::uint32_t lastNbSecs);

  //----------------------------------------------------------------------------
  //! @return The total number of bytes freed that the histogram in its finite
  //! capacity knows about
  //----------------------------------------------------------------------------
  std::uint64_t getTotalBytesFreed();

  //----------------------------------------------------------------------------
  //! Thrown when an invalid bin index has been specified
  //----------------------------------------------------------------------------
  struct InvalidBinIndex: public std::runtime_error {
    InvalidBinIndex(const std::string &what): runtime_error(what) {}
  };

  //----------------------------------------------------------------------------
  //! @return Number of bytes freed in the specified histogram bin
  //! @param binIndex Bin index in the range 0 to nbBind - 1 inclusive
  //! @throw InvalidBinIndex If binIndex is invalid
  //----------------------------------------------------------------------------
  std::uint64_t getFreedBytesInBin(std::uint32_t binIndex) const;

  //----------------------------------------------------------------------------
  //! Set the bin width
  //! @param newBinWidthSecs The new bin width in seconds
  //! @throw InvalidBinWidth If newBinWidthSecs is invalid
  //----------------------------------------------------------------------------
  void setBinWidthSecs(std::uint32_t newBinWidthSecs);

  //----------------------------------------------------------------------------
  //! @return Bin width in seconds
  //----------------------------------------------------------------------------
  uint32_t getBinWidthSecs() const;

  //----------------------------------------------------------------------------
  //! @return number of bins
  //----------------------------------------------------------------------------
  uint32_t getNbBins() const;

private:

  //----------------------------------------------------------------------------
  //! Mutex used to protect the contents of this object
  //----------------------------------------------------------------------------
  mutable std::mutex m_mutex;

  //----------------------------------------------------------------------------
  //! Circular histogram of freed bytes over time.  The time/x-axis starts at
  //! 0 seconds since now and goes to nbBins * binWidthSecs seconds since now.
  //----------------------------------------------------------------------------
  std::vector<std::uint64_t> m_histogram;

  //----------------------------------------------------------------------------
  //! Current start index of histogram
  //----------------------------------------------------------------------------
  size_t m_startIndex;

  //----------------------------------------------------------------------------
  //! Width of a histogram bin in seconds
  //----------------------------------------------------------------------------
  std::uint32_t m_binWidthSecs;

  //----------------------------------------------------------------------------
  //! Object responsible for giving the cUrrent time
  //----------------------------------------------------------------------------
  IClock &m_clock;

  //----------------------------------------------------------------------------
  //! Timestamp of last update
  //----------------------------------------------------------------------------
  std::time_t m_lastUpdateTimestamp;

  //----------------------------------------------------------------------------
  //! Slide histogram to the right until the first bin is aligned with now
  //!
  //! Please note that this method assumes a lock has been taken on m_mutex
  //----------------------------------------------------------------------------
  void alignHistogramWithNow();

  //----------------------------------------------------------------------------
  //! @return Number bytes freed per second during the specified second
  //! @param secsAgo The number of seconds ago.  A value of 0 seconds will
  //! always return a value of 0 freed bytes.
  //!
  //! Please note that this method assumes a lock has been taken on m_mutex
  //----------------------------------------------------------------------------
  std::uint64_t getFreedBytesPerSec(std::uint32_t secsAgo) const;
};

EOSTGCNAMESPACE_END

#endif
