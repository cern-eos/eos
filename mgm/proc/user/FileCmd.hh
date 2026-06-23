//------------------------------------------------------------------------------
// @file: FileCmd.hh
// @author: Octavian-Mihai Matei - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
#include "mgm/proc/ProcCommand.hh"
#include "proto/File.pb.h"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class FileCmd - class handling 'file' commands issued by a regular user
//! (path-based, per-path permission checks are enforced by the individual
//! subcommand handlers, mirroring the legacy ProcCommand::File() behaviour).
//------------------------------------------------------------------------------
class FileCmd : public IProcCommand {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit FileCmd(eos::console::RequestProto&& req, eos::common::VirtualIdentity& vid)
      : IProcCommand(std::move(req), vid, false)
  {
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~FileCmd() override = default;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behaviour of the command executed by the
  //! asynchronous thread
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ProcessRequest() noexcept override;

private:
  //----------------------------------------------------------------------------
  //! Resolve the target path of the 'file' command from the Metadata field
  //! (path, fid or fxid:<hex>/fid:<dec> encoded path). Mirrors the resolution
  //! logic at the top of the legacy ProcCommand::File().
  //!
  //! @param md metadata proto object carrying path and/or numeric id
  //! @param spath resolved path (output)
  //! @param fid resolved numeric file id, 0 if not given via id (output)
  //! @param reply reply proto object, filled with an error if resolution fails
  //!
  //! @return true if resolution succeeded (or is a no-op for 'drop'), false
  //!         if an error was already stored in reply and processing must stop
  //----------------------------------------------------------------------------
  bool ResolvePath(const eos::console::Metadata& md, std::string& spath,
                   unsigned long long& fid, eos::console::ReplyProto& reply,
                   bool allow_empty_path);

  //----------------------------------------------------------------------------
  //! Execute drop subcommand
  //----------------------------------------------------------------------------
  void DropSubcmd(const eos::console::FileDropProto& drop, const std::string& spath,
                  unsigned long long fid, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute layout subcommand
  //----------------------------------------------------------------------------
  void LayoutSubcmd(const eos::console::FileLayoutProto& layout, const std::string& spath,
                    eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute verify subcommand
  //----------------------------------------------------------------------------
  void VerifySubcmd(const eos::console::FileVerifyProto& verify, const std::string& spath,
                    eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute move subcommand
  //----------------------------------------------------------------------------
  void MoveSubcmd(const eos::console::FileMoveProto& move, const std::string& spath,
                  eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute replicate subcommand
  //----------------------------------------------------------------------------
  void ReplicateSubcmd(const eos::console::FileReplicateProto& replicate,
                       const std::string& spath, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute share subcommand
  //----------------------------------------------------------------------------
  void ShareSubcmd(const eos::console::FileShareProto& share, const std::string& spath,
                   eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute rename subcommand
  //----------------------------------------------------------------------------
  void RenameSubcmd(const eos::console::FileRenameProto& rename, const std::string& spath,
                    eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute rename_with_symlink subcommand
  //----------------------------------------------------------------------------
  void RenameWithSymlinkSubcmd(const eos::console::FileRenameWithSymlinkProto& rename,
                               const std::string& spath, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute symlink subcommand
  //----------------------------------------------------------------------------
  void SymlinkSubcmd(const eos::console::FileSymlinkProto& symlink,
                     const std::string& spath, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute workflow subcommand
  //----------------------------------------------------------------------------
  void WorkflowSubcmd(const eos::console::FileWorkflowProto& workflow,
                      const std::string& spath, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute tag subcommand
  //----------------------------------------------------------------------------
  void TagSubcmd(const eos::console::FileTagProto& tag, const std::string& spath,
                 unsigned long long fid, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute copy subcommand
  //----------------------------------------------------------------------------
  void CopySubcmd(const eos::console::FileCopyProto& copy, const std::string& spath,
                  eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute convert subcommand
  //----------------------------------------------------------------------------
  void ConvertSubcmd(const eos::console::FileConvertProto& convert,
                     const std::string& spath, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute touch subcommand
  //----------------------------------------------------------------------------
  void TouchSubcmd(const eos::console::TouchProto& touch, const std::string& spath,
                   const std::string& spathid, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute adjustreplica subcommand
  //----------------------------------------------------------------------------
  void AdjustReplicaSubcmd(const eos::console::FileAdjustreplicaProto& adjust,
                           const std::string& spath, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute purge subcommand
  //----------------------------------------------------------------------------
  void PurgeSubcmd(const eos::console::FilePurgeProto& purge, const std::string& spath,
                   eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute version subcommand
  //----------------------------------------------------------------------------
  void VersionSubcmd(const eos::console::FileVersionProto& version,
                     const std::string& spath, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute versions subcommand
  //----------------------------------------------------------------------------
  void VersionsSubcmd(const eos::console::FileVersionsProto& versions,
                      const std::string& spath, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute fileinfo subcommand ('file info' / 'eos fileinfo'). Bridges onto
  //! the existing legacy ProcCommand::Fileinfo() implementation by building
  //! the equivalent 'mgm.cmd=fileinfo&...' opaque request, the same approach
  //! already used by GrpcRestGwInterface::FileinfoCall() to drive the legacy
  //! handler from a FileinfoProto - avoids duplicating the ~1200 line
  //! mgm/proc/user/Fileinfo.cc implementation.
  //----------------------------------------------------------------------------
  void FileinfoSubcmd(const eos::console::FileinfoProto& info,
                      eos::console::ReplyProto& reply);
};

EOSMGMNAMESPACE_END
