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
class ProcCommand;

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
  //! Set once the opening bracket of the JSON array of entries was written
  bool mJsonArrayOpen{false};

  //! Set while serving a gRPC request, which never gets the JSON output.
  //! A gRPC reply carries stdout, stderr and a return code as separate fields,
  //! and the traversal reports its diagnostics - the truncation warning above
  //! all - with a return code of 0, so the client only ever displays them if
  //! they sit in stdout. They cannot go into a JSON array of entries, and
  //! moving them out of stdout would hide them, so gRPC keeps the monitoring
  //! format it has always produced.
  bool mGrpcRequest{false};

  //----------------------------------------------------------------------------
  //! Close the JSON array of fileinfo entries, if one was opened
  //!
  //! Only a JSON '--fileinfo' request ever opens the array, and it is opened
  //! lazily by its first entry, so that a request reporting no entry at all -
  //! '--count' and friends - keeps its plain output. Anything else gets
  //! nothing back, which is why the traversals can call this unconditionally.
  //!
  //! @return the closing bracket, or an empty string if no entry was written
  //----------------------------------------------------------------------------
  std::string
  CloseJsonArrayIfOpen()
  {
    if (!mJsonArrayOpen) {
      return "";
    }

    mJsonArrayOpen = false;
    return "]\n";
  }

  //----------------------------------------------------------------------------
  //! Print fileinfo data about an entry to the given output stream, in
  //! monitoring format or, if the client asked for it, as a JSON array element
  //!
  //! @param ss output stream
  //! @param find_obj file/container obj
  //! @param errInfo error info object
  //----------------------------------------------------------------------------
  void PrintFileInfo(std::ostream& ss, const FindResult& find_obj,
                     XrdOucErrInfo& errInfo);

  //----------------------------------------------------------------------------
  //! Resolve the namespace id of an entry reported by the traversal
  //!
  //! @note the entry is always designated to the fileinfo collection by its id
  //! rather than by its path. A path is user controlled, and concatenating it
  //! into an opaque string lets a '&' in a file name inject further CGI keys.
  //!
  //! @param find_obj file/container obj
  //! @param errInfo error info object
  //!
  //! @return the container or file id, 0 if the entry could not be resolved
  //----------------------------------------------------------------------------
  uint64_t ResolveEntryId(const FindResult& find_obj, XrdOucErrInfo& errInfo);

  //----------------------------------------------------------------------------
  //! Append fileinfo data about an entry to the given output stream, as an
  //! element of the JSON array of entries
  //!
  //! @note the fileinfo command is deliberately not reached through its CGI
  //! interface here. Its opaque result machinery seals '&' into '#and#', and
  //! there is no lossless way back: a name legitimately holding '#and#' cannot
  //! be told apart from a sealed '&'. The JSON is collected directly instead,
  //! which also keeps the path of an entry out of an opaque string.
  //!
  //! @param ss output stream
  //! @param cmd proc command collecting the fileinfo
  //! @param find_obj file/container obj
  //! @param errInfo error info object
  //----------------------------------------------------------------------------
  void PrintFileInfoJson(std::ostream& ss, ProcCommand& cmd, const FindResult& find_obj,
                         XrdOucErrInfo& errInfo);

  //----------------------------------------------------------------------------
  //! Print fileinfo data about an entry to the default output stream
  //! @note uses the above implementation
  //----------------------------------------------------------------------------
  void
  PrintFileInfo(const FindResult& find_obj, XrdOucErrInfo& errInfo)
  {
    PrintFileInfo(mOfsOutStream, find_obj, errInfo);
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
