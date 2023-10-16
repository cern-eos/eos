//------------------------------------------------------------------------------
//! @file FmdAttr.hh
//! @author Abhishek Lekshmanan - CERN
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
#include "fst/Namespace.hh"
#include "fst/filemd/FmdHandler.hh"

EOSFSTNAMESPACE_BEGIN

static constexpr auto gFmdAttrName = "user.eos.fmd";

//! Forward declarations
class FSPathHandler;
class FileIo;

//------------------------------------------------------------------------------
//! Class FmdAttrHandler
//----------------------------------------------------------------------------
class FmdAttrHandler final: public FmdHandler
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FmdAttrHandler(std::unique_ptr<FSPathHandler>&& _FSPathHandler);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~FmdAttrHandler() = default;

  void LocalDeleteFmd(eos::common::FileId::fileid_t fid,
                      eos::common::FileSystem::fsid_t fsid,
                      bool drop_file = false) override;

  bool Commit(eos::common::FmdHelper* fmd, bool lockit = false,
              std::string* path = nullptr) override;

  std::unique_ptr<eos::common::FmdHelper>
  LocalGetFmd(eos::common::FileId::fileid_t fid,
              eos::common::FileSystem::fsid_t fsid,
              bool force_retrieve = false, bool do_create = false,
              uid_t uid = 0, gid_t gid = 0,
              eos::common::LayoutId::layoutid_t layoutid = 0) override;

  std::pair<bool, eos::common::FmdHelper>
  LocalRetrieveFmd(eos::common::FileId::fileid_t fid,
                   eos::common::FileSystem::fsid_t fsid,
                   const std::string& path = "") override;

  std::pair<bool, eos::common::FmdHelper>
  LocalRetrieveFmd(const std::string& path);

private:
  std::unique_ptr<FSPathHandler> mFSPathHandler;

  //----------------------------------------------------------------------------
  //! Attach Fmd metadata info to the current file identifier
  //!
  //! @param fmd file metadata info protobuf object
  //! @param fid file identifier
  //! @param fsid file system identifier
  //! @param path local file absolute path
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool LocalPutFmd(const eos::common::FmdHelper& fmd,
                   eos::common::FileId::fileid_t fid,
                   eos::common::FileSystem::fsid_t fsid,
                   const std::string& path = "") override;

  bool LocalPutFmd(const eos::common::FmdHelper& fmd,
                   const std::string& path);

  void LocalDeleteFmd(const std::string& path, bool drop_file);

  bool ResetDiskInformation(eos::common::FileSystem::fsid_t fsid) override;

  bool ResetMgmInformation(eos::common::FileSystem::fsid_t fsid) override;

  void SetSyncStatus(eos::common::FileSystem::fsid_t, bool) override {}
};


EOSFSTNAMESPACE_END
