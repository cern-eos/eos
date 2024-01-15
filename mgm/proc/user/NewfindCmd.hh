//------------------------------------------------------------------------------
// File: NewfindCmd.hh
// Author: Jozsef Makai - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
#include "mgm/proc/IProcCommand.hh"
#include "mgm/Namespace.hh"
#include "proto/ConsoleRequest.pb.h"

#ifdef EOS_GRPC
#include "proto/EosWnc.grpc.pb.h"
#endif

namespace eos
{
class IFileMD;
}

EOSMGMNAMESPACE_BEGIN

class FindResult;

class NewfindCmd : public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit NewfindCmd(eos::console::RequestProto&& req,
                      eos::common::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, true)
  {}
  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~NewfindCmd() override = default;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behaviour of the command executed by the
  //! asynchronous thread
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ProcessRequest() noexcept override;

#ifdef EOS_GRPC
  void ProcessRequest(grpc::ServerWriter<eos::console::ReplyProto>* writer);
#endif

private:
  //----------------------------------------------------------------------------
  //! Print fileinfo data in monitoring format to the given output stream
  //!
  //! @param ss output stream
  //! @param find_obj file/container obj
  //! @param errInfo error info object
  //----------------------------------------------------------------------------
  void PrintFileInfoMinusM(std::ostream& ss, const FindResult& find_obj,
                           XrdOucErrInfo& errInfo);

  //----------------------------------------------------------------------------
  //! Print fileinfo data in monitoring format to default output stream
  //! @note uses the above implementation
  //----------------------------------------------------------------------------
  void PrintFileInfoMinusM(const FindResult& find_obj, XrdOucErrInfo& errInfo)
  {
    PrintFileInfoMinusM(mOfsOutStream, find_obj, errInfo);
  }

  //----------------------------------------------------------------------------
  //! Trigger a file layout command to modify the number of stripes
  //!
  //! @param ss output stream
  //! @param req find request object
  //! @param fspath file identifier
  //----------------------------------------------------------------------------
  void ModifyLayoutStripes(std::ostream& ss,
                           const eos::console::FindProto& req,
                           const std::string& fspath);

  //----------------------------------------------------------------------------
  //! Trigger a file layout command to modify the number of stripes
  //! @note uses the above implementation
  //----------------------------------------------------------------------------
  void ModifyLayoutStripes(const eos::console::FindProto& req,
                           const std::string& fspath)
  {
    ModifyLayoutStripes(mOfsOutStream, req, fspath);
  }

  template<typename S>   // std::ofstream or std::stringstream
  void ProcessAtomicFilePurge(S& ss, const std::string& fspath,
                              eos::IFileMD& fmd);

  template<typename S>   // std::ofstream or std::stringstream
  void PurgeVersions(S& ss, int64_t maxVersion, const std::string& dirpath);

};

EOSMGMNAMESPACE_END
