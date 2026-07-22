//------------------------------------------------------------------------------
//! @file FileHelper.hh
//! @author Octavian-Mihai Matei - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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
#include "console/commands/helpers/ICmdHelper.hh"
#include <functional>
#include <string>

//------------------------------------------------------------------------------
//! Class FileHelper - builds the FileProto request of the 'eos file' command
//! from its command line. Shared between the console command and the gRPC
//! client (eos-grpc-ns)
//------------------------------------------------------------------------------
class FileHelper : public ICmdHelper {
public:
  //----------------------------------------------------------------------------
  //! Callback turning a user supplied path into an absolute one. The console
  //! passes abspath(), which resolves against the current 'eos' working
  //! directory; callers with no such notion (e.g. eos-grpc-ns) leave the
  //! default, which passes the path through unchanged.
  //----------------------------------------------------------------------------
  using PathResolver = std::function<std::string(const char*)>;

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //! @param resolver path resolver, identity mapping if not given
  //----------------------------------------------------------------------------
  FileHelper(const GlobalOptions& opts, PathResolver resolver = nullptr)
      : ICmdHelper(opts)
      , mPathResolver(std::move(resolver))
  {
    mIsAdmin = false;
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~FileHelper() override = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg) override;

private:
  PathResolver mPathResolver; ///< Path resolver, identity if unset

  //----------------------------------------------------------------------------
  //! Apply the path resolver, if any
  //----------------------------------------------------------------------------
  std::string AbsPath(const char* in) const;

  //----------------------------------------------------------------------------
  //! Detect whether token is a path/fid/fxid-style specifier (as opposed to a
  //! flag like "-f")
  //----------------------------------------------------------------------------
  static bool IsPathOrId(const std::string& s);

  //----------------------------------------------------------------------------
  //! Populate the shared eos::console::Metadata target (path or numeric file
  //! id) from a raw token that may be a plain path, "fid:<dec>" or
  //! "fxid:<hex>"
  //----------------------------------------------------------------------------
  void SetPathOrId(eos::console::Metadata* md, const std::string& raw) const;

  //----------------------------------------------------------------------------
  //! Like SetPathOrId, but also understands the container/inode specifiers
  //! accepted by 'file info': pid:<dec>, pxid:<hex> (container id) and
  //! inode:<dec> (fuse inode)
  //----------------------------------------------------------------------------
  void SetInfoPathOrId(eos::console::Metadata* md, const std::string& raw) const;
};
