// ----------------------------------------------------------------------
// File: InFlightTracker.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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
/*----------------------------------------------------------------------------*/
#include "mgm/InFlightTracker.hh"
#include "mgm/Access.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

std::string
InFlightTracker::PrintOut(bool monitoring)
{
  std::string format_l = !monitoring ? "+l" : "ol";
  std::string format_s = !monitoring ? "s" : "os";
  TableFormatterBase table_all;

  if (!monitoring) {
    table_all.SetHeader({
      std::make_tuple("uid", 8, format_s),
      std::make_tuple("threads", 5, format_l),
      std::make_tuple("sessions", 5, format_l),
      std::make_tuple("limit", 5, format_l),
      std::make_tuple("stalls", 5, format_l),
      std::make_tuple("stalltime", 5, format_l),
      std::make_tuple("status", 16, format_s)
    });
  } else {
    table_all.SetHeader({
      std::make_tuple("uid", 0, format_s),
      std::make_tuple("threads", 0, format_l),
      std::make_tuple("sessions", 0, format_l),
      std::make_tuple("limit", 0, format_l),
      std::make_tuple("stalls", 0, format_l),
      std::make_tuple("stalltime", 00, format_l),
      std::make_tuple("status", 0, format_s)
    });
  }

  std::map<uid_t, size_t> vids = getInFlightUids();

  for (auto it : vids) {
    TableData table_data;
    size_t limit = Access::ThreadLimit(it.first);
    size_t global_limit = Access::ThreadLimit();
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::to_string((long long)it.first),
                                          format_l));
    table_data.back().push_back(TableCell((long long)it.second, format_l));
    table_data.back().push_back(TableCell((long long)
                                          eos::common::Mapping::ActiveSessions(it.first), format_l));
    table_data.back().push_back(TableCell((long long)limit, format_l));
    table_data.back().push_back(TableCell((long long)getStalls(it.first),
                                          format_l));
    table_data.back().push_back(TableCell((long long)getStallTime(it.first, limit),
                                          format_l));

    if (GetInFlight() > (int64_t) global_limit) {
      table_data.back().push_back(TableCell("pool-OL", format_s));
    }  else if (it.second >= limit) {
      table_data.back().push_back(TableCell("user-OL", format_s));
    } else {
      if (it.second >= (0.9 * limit)) {
        table_data.back().push_back(TableCell("user-LIMIT", format_s));
      } else {
        table_data.back().push_back(TableCell("user-OK", format_s));
      }
    }

    table_all.AddRows(table_data);
  }

  return table_all.GenerateTable(HEADER);
}


size_t
InFlightTracker::getStallTime(uid_t uid, size_t& limit)
{
  size_t sessions = (uid == 0) ? eos::common::Mapping::ActiveSessions() :
                    eos::common::Mapping::ActiveSessions(uid);
  size_t stalltime = limit ? ((size_t)(2.0 * sessions / limit)) : 0;

  if (stalltime > 60) {
    stalltime = 60;
  } else if (stalltime < 1) {
    stalltime = 1;
  }

  int random_stall = rand() % stalltime;
  stalltime /= 2;
  stalltime += random_stall;

  if (stalltime < 1) {
    stalltime = 1;
  }

  return stalltime;
}

size_t
InFlightTracker::ShouldStall(uid_t uid)
{
  size_t limit = Access::ThreadLimit(uid);
  size_t global_limit = Access::ThreadLimit();
  //size_t global_sessions = eos::common::Mapping::ActiveSessions();

  // user limit
  if (limit > 1) {
    if (getInFlight(uid) > limit) {
      incStalls(uid);
      // estimate a stall time
      return getStallTime(uid, limit);
    }
  }

  // global limit
  if (GetInFlight() > (int64_t) global_limit) {
    incStalls(uid);
    return getStallTime(0, global_limit);
  }

  return 0;
}

EOSMGMNAMESPACE_END
