// ----------------------------------------------------------------------
// File: SpaceToTapeGcMap.cc
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

#include "mgm/tgc/MaxLenExceeded.hh"
#include "mgm/tgc/SpaceToTapeGcMap.hh"

#include <memory>
#include <sstream>

/*----------------------------------------------------------------------------*/
/**
 * @file SpaceToTapeAwareGcMap.cc
 *
 * @brief Class implementing a thread safe map from EOS space name to tape
 * ware garbage collector
 *
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//----------------------------------------------------------------------------
//! Constructor
//----------------------------------------------------------------------------
SpaceToTapeGcMap::SpaceToTapeGcMap(ITapeGcMgm &mgm): m_mgm(mgm)
{
}

//----------------------------------------------------------------------------
//! Create a tape aware garbage collector for the specified EOS space.
//----------------------------------------------------------------------------
TapeGc&
SpaceToTapeGcMap::createGc(const std::string &space)
{
  if(space.empty()) {
    std::ostringstream msg;
    msg << "EOS space passed to " << __FUNCTION__ << " is an empty string";
    throw std::runtime_error(msg.str());
  }

  std::lock_guard<std::mutex> lock(m_mutex);

  auto itor = m_gcs.find(space);
  if(m_gcs.end() != itor) {
    std::ostringstream msg;
    msg << "A tape aware garbage collector already exists for EOS space " << space;
    throw GcAlreadyExists(msg.str());
  }

  const auto result = m_gcs.emplace(space, std::make_unique<TapeGc>(m_mgm, space));
  if(!result.second) {
    std::ostringstream msg;
    msg << "Failed to insert new TapeGC for EOS space " << space << " into internal map";
    throw std::runtime_error(msg.str());
  }
  return *(result.first->second);
}

//----------------------------------------------------------------------------
//! Destroys the tape aware garbage collectors for all EOS spaces.
//----------------------------------------------------------------------------
void
SpaceToTapeGcMap::destroyAllGc()
{
  std::lock_guard<std::mutex> lock(m_mutex);
  m_gcs.clear();
}

//----------------------------------------------------------------------------
//! Returns the garbage collector associated with the specified EOS space.
//----------------------------------------------------------------------------
TapeGc
&SpaceToTapeGcMap::getGc(const std::string &space) const
{
  if(space.empty()) {
    std::ostringstream msg;
    msg << "EOS space passed to " << __FUNCTION__ << " is an empty string";
    throw std::runtime_error(msg.str());
  }

  std::lock_guard<std::mutex> lock(m_mutex);

  auto itor = m_gcs.find(space);
  if(m_gcs.end() == itor) {
    std::ostringstream msg;
    msg << "EOS space " << space << " is unknown to " << __FUNCTION__;
    throw UnknownEOSSpace(msg.str());
  }

  auto &gc = itor->second;
  if(!gc) {
    std::stringstream msg;
    msg << "Encountered unexpected nullptr to TapeGc for EOS space " << space;
    throw std::runtime_error(msg.str());
  } 

  return *gc;
}

//----------------------------------------------------------------------------
//! @return map from EOS space name to tape-aware GC statistics
//----------------------------------------------------------------------------
std::map<std::string, TapeGcStats>
SpaceToTapeGcMap::getStats() const
{
  std::map<std::string, TapeGcStats> stats;

  std::lock_guard<std::mutex> lock(m_mutex);

  for(auto &spaceAndTapeGc : m_gcs) {
    if(nullptr != spaceAndTapeGc.second) {
      stats[spaceAndTapeGc.first] = spaceAndTapeGc.second->getStats();
    }
  }

  return stats;
}

//----------------------------------------------------------------------------
// Return the names of the EOS spaces being garbage collected
//----------------------------------------------------------------------------
std::set<std::string>
SpaceToTapeGcMap::getSpaces() const
{
  std::set<std::string> spaces;

  std::lock_guard<std::mutex> lock(m_mutex);

  for (auto &spaceAndTapeGc : m_gcs) {
    if (nullptr != spaceAndTapeGc.second) {
      if (0 != spaces.count(spaceAndTapeGc.first)) {
        std::ostringstream msg;
        msg << __FUNCTION__ << " failed: Detected two garbage collectors working on the same EOS space: space=" <<
          spaceAndTapeGc.first;
        throw std::runtime_error(msg.str());
      }
      spaces.insert(spaceAndTapeGc.first);
    }
  }

  return spaces;
}

//----------------------------------------------------------------------------
// Return A JSON string representation of this map
//----------------------------------------------------------------------------
void
SpaceToTapeGcMap::toJson(std::ostringstream &os, std::uint64_t maxLen) const
{
  os << "{";
  {
    bool isFirstGc = true;
    std::lock_guard<std::mutex> lock(m_mutex);
    for (auto &spaceAndTapeGc : m_gcs) {
      if (isFirstGc) {
        isFirstGc = false;
      } else {
        os << ",";
      }

      if (nullptr != spaceAndTapeGc.second) {
        os << "\"" << spaceAndTapeGc.first << "\":";
        spaceAndTapeGc.second->toJson(os, maxLen);

        const auto osSize = os.tellp();
        if (0 > osSize) throw std::runtime_error(std::string(__FUNCTION__) + ": os.tellp() returned a negative number");
        if (maxLen && maxLen < (std::string::size_type)osSize) {
          std::ostringstream msg;
          msg << __FUNCTION__ << ": maxLen exceeded: maxLen=" << maxLen;
          throw MaxLenExceeded(msg.str());
        }
      }
    }
  }
  os << "}";

  {
    const auto osSize = os.tellp();
    if (0 > osSize) throw std::runtime_error(std::string(__FUNCTION__) + ": os.tellp() returned a negative number");
    if (maxLen && maxLen < (std::string::size_type)osSize) {
      std::ostringstream msg;
      msg << __FUNCTION__ << ": maxLen exceeded: maxLen=" << maxLen;
      throw MaxLenExceeded(msg.str());
    }
  }
}

//--------------------------------------------------------------------------
// Start the worker thread of each garbage collector
//--------------------------------------------------------------------------
void
SpaceToTapeGcMap::startGcWorkerThreads()
{
  std::lock_guard<std::mutex> lock(m_mutex);

  for(auto &spaceAndTapeGc : m_gcs) {
    if(nullptr != spaceAndTapeGc.second) {
      spaceAndTapeGc.second->startWorkerThread();
    }
  }
}

EOSTGCNAMESPACE_END
