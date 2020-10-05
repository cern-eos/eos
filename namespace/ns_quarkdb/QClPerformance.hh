/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @author Elvin Sindrilaru <esindril@cern.ch>
//! @brief Class collecting qclient performance metrics
//------------------------------------------------------------------------------

#pragma once
#include "namespace/Namespace.hh"
#include "qclient/QCallback.hh"
#include <atomic>
#include <map>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class QClPerfMonitor
//------------------------------------------------------------------------------
class QClPerfMonitor: public qclient::QPerfCallback
{
public:
  QClPerfMonitor():
    mMinRtt(std::numeric_limits<unsigned long long>::max()),
    mMaxRtt(0ull), mAvgRtt(0ull)
  {}

  ~QClPerfMonitor() = default;

  //--------------------------------------------------------------------------
  //! Send performance marker
  //!
  //! @param desc string description of the marker
  //! @param value value of the performance marker
  //!
  //! @note: any implementation of this needs to be fast not to block the
  //!        qclient since this gets called from the main event loop
  //--------------------------------------------------------------------------
  virtual void SendPerfMarker(const std::string& name,
                              unsigned long long value);

  //----------------------------------------------------------------------------
  //! Collect performance metrics
  //!
  //! @return get map with performance metrics and their values
  //----------------------------------------------------------------------------
  std::map<std::string, unsigned long long> GetPerfMarkers() const;

private:
  std::atomic<unsigned long long> mMinRtt;
  std::atomic<unsigned long long> mMaxRtt;
  std::atomic<unsigned long long> mAvgRtt;
};

EOSNSNAMESPACE_END
