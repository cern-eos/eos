//------------------------------------------------------------------------------
//! @file FileCmd.hh
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
#include "mgm/Namespace.hh"
#include "mgm/proc/ProcCommand.hh"
#include "mgm/FsView.hh"
#include "proto/File.pb.h"
#include <list>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class FileCmd - class handling file command from a client
//------------------------------------------------------------------------------
class FileCmd: public IProcCommand
{
public:
//----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit FileCmd(eos::console::RequestProto&& req,
                   eos::common::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, true)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FileCmd() = default;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behavior of the command executed by the
  //! asynchronous thread
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ProcessRequest() noexcept override;
private:
//--------------------------------------------------------------------------
  //! Generate JSON output for file metadata
  //!
  //! @param fmd file metadata object
  //! @param path file path
  //! @param json output JSON object
  //--------------------------------------------------------------------------
  void GenerateFileJSON(std::shared_ptr<eos::IFileMD> fmd,
                        const std::string& path,
                        Json::Value& json) noexcept;

  //--------------------------------------------------------------------------
  //! Generate JSON output for directory metadata
  //!
  //! @param cmd container metadata object
  //! @param path directory path
  //! @param json output JSON object
  //--------------------------------------------------------------------------
  void GenerateDirectoryJSON(std::shared_ptr<eos::IContainerMD> cmd,
                             const std::string& path,
                             Json::Value& json) noexcept;

  //--------------------------------------------------------------------------
  //! Convert file metadata to status string
  //!
  //! @param fmd file metadata object
  //! @return status string
  //--------------------------------------------------------------------------
  std::string FileMDToStatus(std::shared_ptr<eos::IFileMD> fmd) noexcept;

  //--------------------------------------------------------------------------
  //! Generate JSON output for metadata location information
  //!
  //! @param fmd file metadata object
  //! @param ns_path namespace path
  //! @param json output JSON object
  //--------------------------------------------------------------------------
  void GenerateMdLocationJSON(std::shared_ptr<eos::IFileMD> fmd,
                              const std::string& ns_path,
                              Json::Value& json) noexcept;

//------------------------------------------------------------------------------
// Fileinfo subcommand - returns file metadata information
//------------------------------------------------------------------------------
  eos::console::ReplyProto
  TouchSubcmd(const eos::console::FileProto& file) noexcept;

//------------------------------------------------------------------------------
// Fileinfo subcommand - returns file metadata information
//------------------------------------------------------------------------------
  eos::console::ReplyProto FileinfoSubcmd(const eos::console::FileProto&
                                          file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle check subcommand - retrieves stat from physical replicas
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto GetMdLocationSubcmd(
    const eos::console::FileProto& file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle adjustreplica subcommand - brings replica layouts to nominal level
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto AdjustReplicaSubcmd(
    const eos::console::FileProto& file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle convert subcommand - converts file layout
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ConvertSubcmd(
    const eos::console::FileProto& file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle copy subcommand - synchronous third-party copy
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto CopySubcmd(
    const eos::console::FileProto& file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle drop subcommand - drops file from filesystem
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto DropSubcmd(
    const eos::console::FileProto& file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle layout subcommand - changes file layout properties
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto LayoutSubcmd(
    const eos::console::FileProto& file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle move subcommand - moves file replica between filesystems
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto MoveSubcmd(
    const eos::console::FileProto& file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle purge subcommand - purges atomic upload leftovers
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto PurgeSubcmd(
    const eos::console::FileProto& file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle rename subcommand - renames file or directory
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto RenameSubcmd(
    const eos::console::FileProto& file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle rename_with_symlink subcommand - atomically renames with symlink
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto RenameWithSymlinkSubcmd(
    const eos::console::FileProto& file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle replicate subcommand - replicates file to target filesystem
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ReplicateSubcmd(
    const eos::console::FileProto& file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle share subcommand - creates sharing URL for file
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ShareSubcmd(
    const eos::console::FileProto& file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle symlink subcommand - creates symbolic link
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto SymlinkSubcmd(
    const eos::console::FileProto& file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle tag subcommand - manages file locations via tagging
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto TagSubcmd(
    const eos::console::FileProto& file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle verify subcommand - verifies file against disk images
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto VerifySubcmd(
    const eos::console::FileProto& file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle version subcommand - creates new file version
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto VersionSubcmd(
    const eos::console::FileProto& file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle versions subcommand - lists or grabs file versions
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto VersionsSubcmd(
    const eos::console::FileProto& file) noexcept;

  //----------------------------------------------------------------------------
  //! Handle workflow subcommand - triggers workflow with event on path
  //!
  //! @param file FileProto message containing command parameters
  //!
  //! @return ReplyProto with command output
  //----------------------------------------------------------------------------
  eos::console::ReplyProto WorkflowSubcmd(
    const eos::console::FileProto& file) noexcept;
};

EOSMGMNAMESPACE_END