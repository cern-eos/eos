// ----------------------------------------------------------------------
// File: ErrorReportListener.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "mq/ErrorReportListener.hh"

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ErrorReportListener::ErrorReportListener(const std::string& serveruri,
    const std::string& hostname)
{
  XrdOucString broker = serveruri.c_str();

  if (!broker.endswith("//")) {
    if (!broker.endswith("/")) {
      broker += ":1097//";
    } else {
      broker.erase(broker.length() - 2);
      broker += ":1097//";
    }
  } else {
    broker.erase(broker.length() - 3);
    broker += ":1097//";
  }

  broker += "eos/";
  broker += hostname.c_str();
  broker += ":";
  broker += (int) getpid();
  broker += ":";
  broker += (int) getppid();
  broker += "/errorreport";

  if (!mClient.AddBroker(broker.c_str())) {
    eos_static_err("failed to add broker %s", broker.c_str());
  } else {
    mClient.Subscribe();
  }
}

//----------------------------------------------------------------------------
// Fetch error report
//----------------------------------------------------------------------------
bool ErrorReportListener::fetch(std::string& out, ThreadAssistant* assistant)
{
  std::unique_ptr<XrdMqMessage> message = std::unique_ptr<XrdMqMessage>
                                          (mClient.RecvMessage(assistant));

  if (message) {
    out = message->GetBody();
    return true;
  }

  return false;
}

EOSMQNAMESPACE_END
