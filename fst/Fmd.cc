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

#include "fst/Fmd.hh"
#include "common/StringConversion.hh"
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Compute layout error
//------------------------------------------------------------------------------
int
FmdHelper::LayoutError(const Fmd& fmd, eos::common::FileSystem::fsid_t fsid)
{
  uint32_t lid = fmd.lid();

  if (lid == 0) {
    // An orphan has no lid at the MGM e.g. lid=0
    return eos::common::LayoutId::kOrphan;
  }

  size_t valid_replicas = 0;
  auto location_set = GetLocations(fmd, valid_replicas);
  size_t nstripes = eos::common::LayoutId::GetStripeNumber(lid) + 1;
  int lerror = 0;

  if (nstripes != valid_replicas) {
    lerror |= eos::common::LayoutId::kReplicaWrong;
  }

  if (!location_set.count(fsid)) {
    lerror |= eos::common::LayoutId::kUnregistered;
  }

  return lerror;
}

//---------------------------------------------------------------------------
// Reset file meta data object
//---------------------------------------------------------------------------
void
FmdHelper::Reset(Fmd& fmd)
{
  fmd.set_fid(0);
  fmd.set_cid(0);
  fmd.set_ctime(0);
  fmd.set_ctime_ns(0);
  fmd.set_mtime(0);
  fmd.set_mtime_ns(0);
  fmd.set_atime(0);
  fmd.set_atime_ns(0);
  fmd.set_checktime(0);
  fmd.set_size(0xfffffff1ULL);
  fmd.set_disksize(0xfffffff1ULL);
  fmd.set_mgmsize(0xfffffff1ULL);
  fmd.set_checksum("");
  fmd.set_diskchecksum("");
  fmd.set_mgmchecksum("");
  fmd.set_lid(0);
  fmd.set_uid(0);
  fmd.set_gid(0);
  fmd.set_filecxerror(0);
  fmd.set_blockcxerror(0);
  fmd.set_layouterror(0);
  fmd.set_locations("");
}

//---------------------------------------------------------------------------
// Get the set of all file system id locations of the current file
//---------------------------------------------------------------------------
std::set<eos::common::FileSystem::fsid_t>
FmdHelper::GetLocations(const Fmd& fmd, size_t& valid_replicas)
{
  valid_replicas = 0;
  std::vector<std::string> location_vector;
  eos::common::StringConversion::Tokenize(fmd.locations(), location_vector, ",");
  std::set<eos::common::FileSystem::fsid_t> location_set;

  for (size_t i = 0; i < location_vector.size(); i++) {
    if (location_vector[i].length()) {
      // Unlinked locations have a '!' in front of the fsid
      if (location_vector[i][0] == '!') {
        location_set.insert(strtoul(location_vector[i].c_str() + 1, 0, 10));
      } else {
        location_set.insert(strtoul(location_vector[i].c_str(), 0, 10));
        ++valid_replicas;
      }
    }
  }

  return location_set;
}

//-------------------------------------------------------------------------------
// Convert fmd object to env representation
//-------------------------------------------------------------------------------
std::unique_ptr<XrdOucEnv>
FmdHelper::FmdToEnv()
{
  std::ostringstream serializedStream;
  serializedStream << "id=" << mProtoFmd.fid() << "&cid=" << mProtoFmd.cid() << "&ctime=" << mProtoFmd.ctime();
  serializedStream << "&ctime_ns=" << mProtoFmd.ctime_ns() << "&mtime=" << mProtoFmd.mtime() << "&mtime_ns=" << mProtoFmd.mtime_ns();
  serializedStream << "&size=" << mProtoFmd.size() << "&checksum=" << mProtoFmd.checksum() << "&lid=" << mProtoFmd.lid();
  serializedStream << "&uid=" << mProtoFmd.uid() << "&gid=" << mProtoFmd.gid() << '&';

  return std::unique_ptr<XrdOucEnv>(new XrdOucEnv(serializedStream.str().c_str()));
}

EOSFSTNAMESPACE_END
