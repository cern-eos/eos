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

#include "namespace/ns_quarkdb/QClPerformance.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Send performance marker
//------------------------------------------------------------------------------
void
QClPerfMonitor::SendPerfMarker(const std::string& name,
                               unsigned long long value)
{
  if (name == "rtt_us") {
    if (mMinRtt > value) {
      mMinRtt = value;
    }

    if (mMaxRtt < value) {
      mMaxRtt = value;
    }

    if (mMinRtt) {
      mAvgRtt = (mAvgRtt + value) / 2;
    } else {
      mAvgRtt += value;
    }
  }
}


//------------------------------------------------------------------------------
// Collect performance metrics
//------------------------------------------------------------------------------
std::map<std::string, unsigned long long>
QClPerfMonitor::GetPerfMarkers() const
{
  std::map<std::string, unsigned long long> resp;
  resp["rtt_min"] = mMinRtt;
  resp["rtt_max"] = mMaxRtt;
  resp["rtt_avg"] = mAvgRtt;
  return resp;
}

EOSNSNAMESPACE_END
