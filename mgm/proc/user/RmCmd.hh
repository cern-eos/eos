//------------------------------------------------------------------------------
// File: RmCmd.hh
// Author: Jozsef Makai - CERN
//------------------------------------------------------------------------------

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

#include "mgm/Namespace.hh"
#include "mgm/proc/IProcCommand.hh"
#include "proto/ConsoleRequest.pb.h"

EOSMGMNAMESPACE_BEGIN

class RmCmd : public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  RmCmd(eos::console::RequestProto&& req,
        eos::common::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, true) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RmCmd() override = default;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behaviour of the command executed by the
  //! asynchronous thread
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ProcessRequest() noexcept override;

private:

  //----------------------------------------------------------------------------
  //! Attempts to remove file.
  //!
  //! @param path the file path
  //! @param force bypass recycling policies
  //!
  //! @return 0 on success, errno otherwise
  //----------------------------------------------------------------------------
  int RemoveFile(const std::string path, bool force);

  //----------------------------------------------------------------------------
  //! Attempts to remove directory.
  //!
  //! @param path the directory path
  //! @param force bypass recycling policies
  //! @param outMsg output message string
  //! @param errMsg error message string
  //!
  //! @return 0 on success, errno otherwise
  //----------------------------------------------------------------------------
  int RemoveDirectory(const std::string path, bool force,
                      std::string& outMsg,
                      std::string& errMsg);


  //----------------------------------------------------------------------------
  //! Attempts to remove files matching a given filter.
  //! Note: directories will not be removed
  //!
  //! @param path directory path where filter matching is applied
  //! @param filter the filter to match against
  //! @param force bypass recycling policies
  //! @param errMsg error message string
  //!
  //! @return 0 on success, errno otherwise
  //----------------------------------------------------------------------------
  int RemoveFilterMatch(const std::string path,
                        const std::string filter,
                        bool force,
                        std::string& errMsg);
};

EOSMGMNAMESPACE_END
