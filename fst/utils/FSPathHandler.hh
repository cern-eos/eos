/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                           *
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
 ************************************************************************
 */

#pragma once
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include <string_view>
#include <string>

namespace eos::fst
{


struct FSPathHandler {
  virtual std::string GetFSPath(eos::common::FileSystem::fsid_t fsid) = 0;
  virtual std::string GetPath(eos::common::FileId::fileid_t fid,
                              eos::common::FileSystem::fsid_t fsid);
  static eos::common::FileSystem::fsid_t GetFsid(std::string_view path,
      bool at_root = false);
  virtual ~FSPathHandler() = default;
};

class FixedFSPathHandler : public FSPathHandler
{
public:
  FixedFSPathHandler(std::string_view _fs_path)
    : FSPathHandler(), fs_path(_fs_path) {}

  std::string GetFSPath(eos::common::FileSystem::fsid_t fsid) override;


private:
  std::string fs_path;
};

inline std::unique_ptr<FSPathHandler>
makeFSPathHandler(std::string_view path)
{
  return std::make_unique<FixedFSPathHandler>(path);
}

} // namespace eos::fst
