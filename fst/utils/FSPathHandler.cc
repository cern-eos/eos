// /************************************************************************
//  * EOS - the CERN Disk Storage System                                   *
//  * Copyright (C) 2022 CERN/Switzerland                           *
//  *                                                                      *
//  * This program is free software: you can redistribute it and/or modify *
//  * it under the terms of the GNU General Public License as published by *
//  * the Free Software Foundation, either version 3 of the License, or    *
//  * (at your option) any later version.                                  *
//  *                                                                      *
//  * This program is distributed in the hope that it will be useful,      *
//  * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
//  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
//  * GNU General Public License for more details.                         *
//  *                                                                      *
//  * You should have received a copy of the GNU General Public License    *
//  * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
//  ************************************************************************
//

#include "fst/utils/FSPathHandler.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "common/StringSplit.hh"
#include "common/StringUtils.hh"

namespace eos::fst
{

eos::common::FileSystem::fsid_t
FSPathHandler::GetFsid(std::string_view path, bool at_root)
{
  eos::common::FileSystem::fsid_t fsid;
  std::string err_msg;
  std::string fsidpath = at_root ? eos::common::GetRootPath(path) : std::string(
                           path);
  fsidpath += "/.eosfsid";
  std::string sfsid;
  eos::common::StringConversion::LoadFileIntoString(fsidpath.c_str(), sfsid);

  if (!eos::common::StringToNumeric(sfsid, fsid, (uint32_t)0, &err_msg)) {
    eos_static_crit("msg=\"Unable to obtain FSID from path=\"%s", path.data());
    // TODO: this is exceptional, throw an error!
  }

  return fsid;
}

std::string
FSPathHandler::GetPath(eos::common::FileId::fileid_t fid,
                       eos::common::FileSystem::fsid_t fsid)
{
  return eos::common::FileId::FidPrefix2FullPath(eos::common::FileId::Fid2Hex(
           fid).c_str(),
         GetFSPath(fsid).c_str());
}

std::string
FixedFSPathHandler::GetFSPath(eos::common::FileSystem::fsid_t fsid)
{
  return fs_path;
}

} // namespace eos::fst
