//------------------------------------------------------------------------------
//! @file ResponseCollector.cc
//! @author Elvin-Alin Sindrilaru - CERN
//------------------------------------------------------------------------------

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

#include "fst/io/xrd/ResponseCollector.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Collect future object
//------------------------------------------------------------------------------
void
ResponseCollector::CollectFuture(std::future<XrdCl::XRootDStatus> fut)
{
  std::unique_lock<std::mutex> lock(mMutex);
  mResponses.push_back(std::move(fut));
}

//------------------------------------------------------------------------------
// Check the status of the responses
//------------------------------------------------------------------------------
bool
ResponseCollector::CheckResponses(bool wait_all)
{
  bool ok = true;
  std::unique_lock<std::mutex> lock(mMutex);

  while (!mResponses.empty()) {
    auto& fut = mResponses.front();

    if (!fut.valid()) {
      ok = false;
      mResponses.pop_front();
      continue;
    }

    if (wait_all) {
      fut.wait();
    } else {
      if (fut.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
        break;
      }
    }

    XrdCl::XRootDStatus status = fut.get();

    if (!status.IsOK()) {
      ok = false;
    }

    mResponses.pop_front();
  }

  return ok;
}

EOSFSTNAMESPACE_END
