// ----------------------------------------------------------------------
// File: ReportListener.cc
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

#include "mq/ReportListener.hh"

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ReportListener::ReportListener(const std::string& broker,
                               const std::string& hostname,
                               bool use_qdb_listener,
                               eos::QdbContactDetails& qdb_details,
                               const std::string& channel)
{
  if (use_qdb_listener) {
    mQdbListener.reset(new QdbListener(qdb_details, channel));
  } else {
    XrdOucString queue = broker.c_str();
    queue += hostname.c_str();
    queue += "/report";
    queue.replace("root://", "root://daemon@");

    if (!mClient.AddBroker(queue.c_str())) {
      eos_static_err("msg=\"failed to add broker\" queue=%s", queue.c_str());
    } else {
      mClient.Subscribe();
    }
  }
}

//------------------------------------------------------------------------------
// Fetch report
//------------------------------------------------------------------------------
bool
ReportListener::fetch(std::string& out, ThreadAssistant* assistant)
{
  if (mQdbListener) {
    return mQdbListener->fetch(out, assistant);
  } else {
    std::unique_ptr<XrdMqMessage> message = std::unique_ptr<XrdMqMessage>
                                            (mClient.RecvMessage(assistant));

    if (message) {
      out = message->GetBody();
      return true;
    }

    return false;
  }
}

EOSMQNAMESPACE_END
