// ----------------------------------------------------------------------
// File: SmartSpaceStats.hh
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

#ifndef __EOSMGMSMARTSPACESTATS_HH__
#define __EOSMGMSMARTSPACESTATS_HH__

#include "mgm/Namespace.hh"
#include "mgm/tgc/AsyncUint64ShellCmd.hh"
#include "mgm/tgc/CachedValue.hh"
#include "mgm/tgc/FreedBytesHistogram.hh"
#include "mgm/tgc/ITapeGcMgm.hh"
#include "mgm/tgc/RealClock.hh"
#include "mgm/tgc/SpaceStats.hh"

#include <ctime>
#include <mutex>
#include <string>

/*----------------------------------------------------------------------------*/
/**
 * @file SmartSpaceStats.hh
 *
 * @brief Class encapsulating how the tape-aware GC updates its internal
 * statistics about the EOS space it is managing.
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class encapsulating how the tape-aware GC updates its internal statistics
//! about the EOS space it is managing
//------------------------------------------------------------------------------
class SmartSpaceStats {
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param spaceName Name of the EOS space being managed
  //! @param mgm Interface to the EOS MGM
  //! @param config Configuration of the tape-aware garbage collector
  //----------------------------------------------------------------------------
  SmartSpaceStats(const std::string &spaceName, ITapeGcMgm &mgm, CachedValue<SpaceConfig> &config);

  //----------------------------------------------------------------------------
  //! Notify this object that a disk replica has been queued for deletion
  //!
  //! @param fileSizeBytes File size in bytes
  //----------------------------------------------------------------------------
  void diskReplicaQueuedForDeletion(size_t fileSizeBytes);

  enum class Src {
    NONE,
    INTERNAL_BECAUSE_SCRIPT_PATH_EMPTY,
    INTERNAL_BECAUSE_SCRIPT_PENDING_AND_NO_PREVIOUS_VALUE,
    INTERNAL_BECAUSE_SCRIPT_ERROR,
    SCRIPT_VALUE_BECAUSE_SCRIPT_JUST_FINISHED,
    SCRIPT_PREVIOUS_VALUE_BECAUSE_SCRIPT_PENDING
  };

  static const char *srcToStr(const Src src) {
    switch(src) {
    case Src::NONE: return "NONE";
    case Src::INTERNAL_BECAUSE_SCRIPT_PATH_EMPTY: return "INTERNAL_BECAUSE_SCRIPT_PATH_EMPTY";
    case Src::INTERNAL_BECAUSE_SCRIPT_PENDING_AND_NO_PREVIOUS_VALUE:
      return "INTERNAL_BECAUSE_SCRIPT_PENDING_AND_NO_PREVIOUS_VALUE";
    case Src::INTERNAL_BECAUSE_SCRIPT_ERROR: return "INTERNAL_BECAUSE_SCRIPT_ERROR";
    case Src::SCRIPT_VALUE_BECAUSE_SCRIPT_JUST_FINISHED: return "SCRIPT_VALUE_BECAUSE_SCRIPT_JUST_FINISHED";
    case Src::SCRIPT_PREVIOUS_VALUE_BECAUSE_SCRIPT_PENDING: return "SCRIPT_PREVIOUS_VALUE_BECAUSE_SCRIPT_PENDING";
    default: return "UNKNOWN";
    }
  }

  struct SpaceStatsAndAvailBytesSrc {
    SpaceStats stats;
    Src availBytesSrc = Src::NONE;
  };

  //----------------------------------------------------------------------------
  //! @return statistics about the EOS space being managed
  //----------------------------------------------------------------------------
  SpaceStatsAndAvailBytesSrc get();

  //----------------------------------------------------------------------------
  //! @return timestamp at which the last query was made
  //----------------------------------------------------------------------------
  std::time_t getQueryTimestamp();

private:

  //----------------------------------------------------------------------------
  //! Object used to asynchronously run no more than one tgc.freebytesscript at
  //! a time.
  //!
  //! When the tgc.freebytesscript configuration variable is set there are
  //! effectively two sources from which the total number of free bytes can be
  //! obtained.  The internal filesystem statistics of the MGM that it receives
  //! from its FSTs and the external script whose path is stored in the
  //! tgc.freebytesscript parameter.  There are two cases in which the MGM TGC
  //! will fall back to using its internal statistics.  The MGM will use its
  //! internal statistics until the script has been successfully executed for
  //! the very first time.  The MGM will use its internal statistics if the
  //! script fails where failure can include timing out.  Once the script has
  //! been executed successfully the MGM will cache the returned value until the
  //! next time the script successfully completes.  If at any point the script
  //! fails then the MGM will revert back to using its internal statistics.
  //----------------------------------------------------------------------------
  AsyncUint64ShellCmd m_singleAsyncFreeBytesScript;

  //----------------------------------------------------------------------------
  //! Name of the EOS space being managed
  //----------------------------------------------------------------------------
  std::string m_spaceName;

  //----------------------------------------------------------------------------
  //! Interface to the EOS MGM
  //----------------------------------------------------------------------------
  ITapeGcMgm &m_mgm;

  //----------------------------------------------------------------------------
  //! Mutex to protect the member variables of this object
  //----------------------------------------------------------------------------
  mutable std::mutex m_mutex;

  //----------------------------------------------------------------------------
  //! The timestamp at which the last query to the MGM was made
  //----------------------------------------------------------------------------
  std::time_t m_queryMgmTimestamp;

  //----------------------------------------------------------------------------
  //! MGM statistics about the EOS space being managed
  //----------------------------------------------------------------------------
  SpaceStatsAndAvailBytesSrc m_mgmStats;

  //----------------------------------------------------------------------------
  //! Object responsible for providing the current time
  //!
  //! This member variable MUST be declared before m_freedBytesHistogram
  //----------------------------------------------------------------------------
  RealClock m_clock;

  //----------------------------------------------------------------------------
  //! Histogram of freed bytes over time
  //----------------------------------------------------------------------------
  FreedBytesHistogram m_freedBytesHistogram;

  //----------------------------------------------------------------------------
  //! The configuration of the tape-aware garbage collector
  //----------------------------------------------------------------------------
  CachedValue<SpaceConfig> &m_config;
};

EOSTGCNAMESPACE_END

#endif
