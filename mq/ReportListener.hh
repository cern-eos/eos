// ----------------------------------------------------------------------
// File: ReportListener.hh
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

#pragma once
#include "mq/Namespace.hh"
#include "mq/XrdMqClient.hh"
#include "mq/QdbListener.hh"

//! Forward declarations
class ThreadAssistant;

namespace eos
{
class QdbContactDetails;
}

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Helper class for listening to and processing IoStat report messages.
//------------------------------------------------------------------------------
class ReportListener
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param broker MQ broker URL
  //! @param hostname current hostname
  //! @param use_qdb_listener if true then use QdbListener otherwise use old
  //!        MQ client implementation
  //! @param qdb_details QDB connection details
  //! @param channel subscription channel for reports
  //----------------------------------------------------------------------------
  ReportListener(const std::string& broker, const std::string& hostname,
                 bool use_qdb_listener, eos::QdbContactDetails& qdb_details,
                 const std::string& channel);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ReportListener() = default;

  //----------------------------------------------------------------------------
  //! Fetch report
  //----------------------------------------------------------------------------
  bool fetch(std::string& out, ThreadAssistant* assistant = nullptr);

private:
  XrdMqClient mClient;
  std::unique_ptr<QdbListener> mQdbListener {nullptr};
};

EOSMQNAMESPACE_END
