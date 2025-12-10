//------------------------------------------------------------------------------
//! @file FileHelper.hh
//! @author Octavian Matei - CERN
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
#include "console/commands/ICmdHelper.hh"
#include "common/StringTokenizer.hh"
#include "common/Fmd.hh"

//------------------------------------------------------------------------------
//! Class FileHelper
//------------------------------------------------------------------------------
class FileHelper: public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  FileHelper(const GlobalOptions& opts):
    ICmdHelper(opts)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~FileHelper() = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg);

  //----------------------------------------------------------------------------
  //! Check if this is a check command
  //!
  //! @return true if the request contains a check command, otherwise false
  //----------------------------------------------------------------------------
  bool IsCheckCommand();

  //----------------------------------------------------------------------------
  //! Override Execute to handle special formatting for check command
  //----------------------------------------------------------------------------
  int Execute(bool print_err = true, bool add_route = false);



private:

  //------------------------------------------------------------------------------
  //! Return Fmd from a remote filesystem
  //!
  //! @param manager host:port of the server to contact
  //! @param shexfid hex string of the file id
  //! @param sfsid string of filesystem id
  //! @param fmd reference to the Fmd struct to store Fmd
  //------------------------------------------------------------------------------
  int GetRemoteFmdFromLocalDb(const char* manager, const char* shexfid,
                              const char* sfsid, eos::common::FmdHelper& fmd);

  //----------------------------------------------------------------------------
  //! Format check command output from server response
  //!
  //! @param response server response string in key-value format
  //! @param options check command options
  //----------------------------------------------------------------------------
  void FormatCheckOutput(const std::string& response, const std::string& options);

  //----------------------------------------------------------------------------
  //! Parse info subcommand
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseInfo(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse touch subcommand
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseTouch(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Set path in the proto message, handling special prefixes and relative paths
  //!
  //! @param in_path input path string
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetPath(const std::string& in_path);

  //----------------------------------------------------------------------------
  //! Parse adjustreplica subcommand - brings replica layouts to nominal level
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseAdjustreplica(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse check subcommand - retrieves stat from physical replicas
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCheck(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse convert subcommand - converts file layout
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseConvert(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse copy subcommand - synchronous third-party copy
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCopy(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse drop subcommand - drops file from filesystem
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseDrop(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse layout subcommand - changes file layout properties
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseLayout(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse move subcommand - moves file replica between filesystems
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseMove(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse purge subcommand - purges atomic upload leftovers
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParsePurge(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse replicate subcommand - replicates file to target filesystem
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseReplicate(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse share subcommand - creates sharing URL for file
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseShare(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse symlink subcommand - creates symbolic link
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseSymlink(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse tag subcommand - manages file extended attributes/tags
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseTag(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse verify subcommand - verifies file against disk images
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseVerify(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse version subcommand - creates new file version
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseVersion(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse versions subcommand - lists or grabs file versions
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseVersions(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse workflow subcommand - triggers workflow with event on path
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseWorkflow(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse rename subcommand - legacy command, needs reimplementation
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseRename(eos::common::StringTokenizer& tokenizer);

  //----------------------------------------------------------------------------
  //! Parse rename_with_symlink subcommand - legacy command, needs reimplementation
  //!
  //! @param tokenizer string tokenizer with remaining tokens
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseRenameWithSymlink(eos::common::StringTokenizer& tokenizer);
};
