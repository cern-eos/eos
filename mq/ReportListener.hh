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

#ifndef EOS_MQ_REPORT_LISTENER_HH
#define EOS_MQ_REPORT_LISTENER_HH

#include "mq/Namespace.hh"
#include "mq/XrdMqClient.hh"

class ThreadAssistant;

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Helper class for listening to and processing IoStat report messages.
//------------------------------------------------------------------------------
class ReportListener
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ReportListener(const std::string& broker, const std::string& hostname);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ReportListener() = default;

  //----------------------------------------------------------------------------
  //! Fetch report
  //----------------------------------------------------------------------------
  bool fetch(std::string& out, ThreadAssistant& assistant);

private:
  XrdMqClient mClient;
};

EOSMQNAMESPACE_END

#endif
