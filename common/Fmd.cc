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

#include "common/Fmd.hh"
#include "common/StringConversion.hh"
#include "common/LayoutId.hh"

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Convert an FST env representation to an Fmd struct
//------------------------------------------------------------------------------
bool EnvToFstFmd(XrdOucEnv& env, FmdHelper& fmd)
{
  // Check that all tags are present
  std::set<std::string> tags {"id", "cid",  "ctime", "ctime_ns", "mtime",
                              "mtime_ns", "size", "lid", "uid", "gid"};
  //      "fsid",  "disksize", "filecxerror",
  //      "blockcxerror", "layouterror", "locations"};

  for (const auto& tag : tags) {
    if (env.Get(tag.c_str()) == nullptr) {
      int envlen = 0;
      eos_static_crit("msg=\"missing fields in fmd encoding\" field=%s "
                      "encoding=\"%s\"", tag.c_str(), env.Env(envlen));
      return false;
    }
  }

  fmd.mProtoFmd.set_fid(strtoull(env.Get("id"), 0, 10));
  fmd.mProtoFmd.set_cid(strtoull(env.Get("cid"), 0, 10));
  fmd.mProtoFmd.set_ctime(strtoul(env.Get("ctime"), 0, 10));
  fmd.mProtoFmd.set_ctime_ns(strtoul(env.Get("ctime_ns"), 0, 10));
  fmd.mProtoFmd.set_mtime(strtoul(env.Get("mtime"), 0, 10));
  fmd.mProtoFmd.set_mtime_ns(strtoul(env.Get("mtime_ns"), 0, 10));
  fmd.mProtoFmd.set_size(strtoull(env.Get("size"), 0, 10));
  fmd.mProtoFmd.set_lid(strtoul(env.Get("lid"), 0, 16));
  fmd.mProtoFmd.set_uid((uid_t) strtoul(env.Get("uid"), 0, 10));
  fmd.mProtoFmd.set_gid((gid_t) strtoul(env.Get("gid"), 0, 10));

  if (env.Get("fsid")) {
    fmd.mProtoFmd.set_fsid(strtoull(env.Get("fsid"), 0, 10));
  }

  if (env.Get("disksize")) {
    fmd.mProtoFmd.set_disksize(strtoull(env.Get("disksize"), 0, 10));
  }

  if (env.Get("checksum")) {
    fmd.mProtoFmd.set_checksum(env.Get("checksum"));
  }

  if (env.Get("filecxerror")) {
    fmd.mProtoFmd.set_filecxerror(strtoul(env.Get("filecxerror"), 0, 16));
  }

  if (env.Get("blockcxerror")) {
    fmd.mProtoFmd.set_blockcxerror(strtoul(env.Get("blockcxerror"), 0, 16));
  }

  if (env.Get("layouterror")) {
    fmd.mProtoFmd.set_layouterror(strtoul(env.Get("layouterror"), 0, 16));
  }

  if (fmd.mProtoFmd.checksum() == "none") {
    fmd.mProtoFmd.set_checksum("");
  }

  if (env.Get("diskchecksum")) {
    fmd.mProtoFmd.set_diskchecksum(env.Get("diskchecksum"));
  }

  if (fmd.mProtoFmd.diskchecksum() == "none") {
    fmd.mProtoFmd.set_diskchecksum("");
  }

  if (env.Get("mgmchecksum")) {
    fmd.mProtoFmd.set_mgmchecksum(env.Get("mgmchecksum"));
  }

  if (fmd.mProtoFmd.mgmchecksum() == "none") {
    fmd.mProtoFmd.set_mgmchecksum("");
  }

  if (env.Get("locations")) {
    fmd.mProtoFmd.set_locations(env.Get("locations"));
  }

  if (fmd.mProtoFmd.locations() == "none") {
    fmd.mProtoFmd.set_locations("");
  }

  return true;
}

//------------------------------------------------------------------------------
// Compute layout error
//------------------------------------------------------------------------------
int
FmdHelper::LayoutError(eos::common::FileSystem::fsid_t fsid)
{
  uint32_t lid = mProtoFmd.lid();

  if (lid == 0) {
    // An orphan has no lid at the MGM e.g. lid=0
    return eos::common::LayoutId::kOrphan;
  }

  auto location_set = GetLocations();
  size_t nstripes = eos::common::LayoutId::GetStripeNumber(lid) + 1;
  int lerror = 0;

  if (nstripes != location_set.size()) {
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
FmdHelper::Reset()
{
  mProtoFmd.set_fid(0);
  mProtoFmd.set_cid(0);
  mProtoFmd.set_ctime(0);
  mProtoFmd.set_ctime_ns(0);
  mProtoFmd.set_mtime(0);
  mProtoFmd.set_mtime_ns(0);
  mProtoFmd.set_atime(0);
  mProtoFmd.set_atime_ns(0);
  mProtoFmd.set_checktime(0);
  mProtoFmd.set_size(UNDEF);
  mProtoFmd.set_disksize(UNDEF);
  mProtoFmd.set_mgmsize(UNDEF);
  mProtoFmd.set_checksum("");
  mProtoFmd.set_diskchecksum("");
  mProtoFmd.set_mgmchecksum("");
  mProtoFmd.set_lid(0);
  mProtoFmd.set_uid(0);
  mProtoFmd.set_gid(0);
  mProtoFmd.set_filecxerror(0);
  mProtoFmd.set_blockcxerror(0);
  mProtoFmd.set_layouterror(0);
  mProtoFmd.set_locations("");
}

//------------------------------------------------------------------------------
// Get set of valid (not unlinked) locations for the given fmd
//------------------------------------------------------------------------------
std::set<eos::common::FileSystem::fsid_t>
FmdHelper::GetLocations() const
{
  std::vector<std::string> location_vector;
  eos::common::StringConversion::Tokenize(mProtoFmd.locations(), location_vector,
                                          ",");
  std::set<eos::common::FileSystem::fsid_t> location_set;

  for (size_t i = 0; i < location_vector.size(); i++) {
    if (location_vector[i].length()) {
      // Exclude unlinked locations i.e. they have a ! in front
      if (location_vector[i][0] != '!') {
        location_set.insert(strtoul(location_vector[i].c_str(), 0, 10));
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
  std::ostringstream oss;
  oss << "id=" << mProtoFmd.fid()
      << "&cid=" << mProtoFmd.cid()
      << "&fsid=" << mProtoFmd.fsid()
      << "&ctime=" << mProtoFmd.ctime()
      << "&ctime_ns=" << mProtoFmd.ctime_ns()
      << "&mtime=" << mProtoFmd.mtime()
      << "&mtime_ns=" << mProtoFmd.mtime_ns()
      << "&atime=" << mProtoFmd.atime()
      << "&atime_ns=" << mProtoFmd.atime_ns()
      << "&size=" << mProtoFmd.size()
      << "&disksize=" << mProtoFmd.disksize()
      << "&mgmsize=" << mProtoFmd.mgmsize()
      << "&lid=0x" << std::hex << mProtoFmd.lid() << std::dec
      << "&uid=" << mProtoFmd.uid()
      << "&gid=" << mProtoFmd.gid()
      << "&filecxerror=0x" << std::hex << mProtoFmd.filecxerror()
      << "&blockcxerror=0x" << mProtoFmd.blockcxerror()
      << "&layouterror=0x" << mProtoFmd.layouterror();

  // Take care at string fields since XrdOucEnv does not deal well with empty
  // values
  if (mProtoFmd.checksum().empty()) {
    oss << "&checksum=none";
  } else {
    oss << "&checksum=" << mProtoFmd.checksum();
  }

  if (mProtoFmd.diskchecksum().empty()) {
    oss << "&diskchecksum=none";
  } else {
    oss << "&diskchecksum=" << mProtoFmd.diskchecksum();
  }

  if (mProtoFmd.mgmchecksum().empty()) {
    oss << "&mgmchecksum=none";
  } else {
    oss << "&mgmchecksum=" << mProtoFmd.mgmchecksum();
  }

  if (mProtoFmd.locations().empty()) {
    oss << "&locations=none";
  } else {
    oss << "&locations=" << std::dec << mProtoFmd.locations();
  }

  oss << '&';
  return std::unique_ptr<XrdOucEnv>
         (new XrdOucEnv(oss.str().c_str()));
}

EOSCOMMONNAMESPACE_END
