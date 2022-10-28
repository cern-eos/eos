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
  using namespace std::chrono;

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

    bool found = false;
    unsigned long long current_ts =
      duration_cast<minutes>(system_clock::now().time_since_epoch()).count();
    unsigned long long expire_ts = current_ts - 5;
    std::unique_lock<std::mutex> lock(mMutex);

    for (auto it = mMapTsToRtt.begin(); it != mMapTsToRtt.end(); /*none*/) {
      if (it->first <= expire_ts) {
        it = mMapTsToRtt.erase(it);
      } else if (it->first == current_ts) {
        if (value > it->second) {
          it->second = value;
        }

        found = true;
        break;
      } else {
        ++it;
      }
    }

    if (!found) {
      mMapTsToRtt[current_ts] = value;
    }
  }
}


//------------------------------------------------------------------------------
// Collect performance metrics
//------------------------------------------------------------------------------
std::map<std::string, unsigned long long>
QClPerfMonitor::GetPerfMarkers() const
{
  using namespace std::chrono;
  std::map<std::string, unsigned long long> resp;
  resp["rtt_min"] = mMinRtt;
  resp["rtt_max"] = mMaxRtt;
  resp["rtt_avg"] = mAvgRtt;
  unsigned long long peak_1m {0ull};
  unsigned long long peak_2m {0ull};
  unsigned long long peak_5m {0ull};
  unsigned long long current_ts =
    duration_cast<minutes>(system_clock::now().time_since_epoch()).count();
  {
    std::unique_lock<std::mutex> lock(mMutex);

    for (auto rit = mMapTsToRtt.rbegin(); rit != mMapTsToRtt.rend(); ++rit) {
      if (rit->first == current_ts) {
        peak_1m = rit->second;
      }

      if (rit->first >= current_ts - 1) {
        if (rit->second > peak_2m) {
          peak_2m = rit->second;
        }
      }

      if (rit->first >= current_ts - 4) {
        if (rit->second > peak_5m) {
          peak_5m = rit->second;
        }
      }
    }
  }
  resp["rtt_peak_1m"] = peak_1m;
  resp["rtt_peak_2m"] = peak_2m;
  resp["rtt_peak_5m"] = peak_5m;
  return resp;
}

EOSNSNAMESPACE_END
