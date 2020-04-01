/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Namespace etag utilities
//------------------------------------------------------------------------------

#include "namespace/utils/Etag.hh"
#include "namespace/interface/IFileMD.hh"
#include "common/FileId.hh"
#include "common/Fmd.hh"
#include "namespace/utils/Checksum.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Compatibility hack: Use old inodes below 34B, and new above.
//
// The old inode scheme breaks down once it reaches 34B files.
// Yes this is kinda awful.
//------------------------------------------------------------------------------
static uint64_t findInode(uint64_t fid) {
  uint64_t threshold = 34'000'000'000ull;

  if(fid < threshold) {
    return common::FileId::LegacyFidToInode(fid);
  }
  else {
    return common::FileId::NewFidToInode(fid);
  }
}

//------------------------------------------------------------------------------
// Calculate etag - supply flag to indicate whether to use checksum or not.
//------------------------------------------------------------------------------
void calculateEtag(bool useChecksum, const fst::FmdBase &fmdBase, std::string &out) {
  if(useChecksum) {
    return calculateEtagInodeAndChecksum(fmdBase, out);
  }
  else {
    return calculateEtagInodeAndMtime(fmdBase.fid(), fmdBase.mtime(), out);
  }
}

//------------------------------------------------------------------------------
// Calculate etag based on checksum type + fst fmdproto.
// TODO(gbitzes): Maybe checksumType is not needed? Maybe we can derive
// checksum type from layout id of fmdproto?
//------------------------------------------------------------------------------
void calculateEtagInodeAndChecksum(const fst::FmdBase &fmdBase, std::string &out) {
  if(eos::common::LayoutId::GetChecksum(fmdBase.lid()) != eos::common::LayoutId::kMD5) {
    // use inode + checksum
    char setag[256];
    snprintf(setag, sizeof(setag) - 1, "\"%llu:%s\"",
              (unsigned long long) findInode(fmdBase.fid()),
              fmdBase.checksum().c_str());
    out = setag;
  } else {
    // use checksum, S3 wants the pure MD5
    char setag[256];
    snprintf(setag, sizeof(setag) - 1, "\"%s\"",
             fmdBase.checksum().c_str());
    out = setag;
  }
}

//------------------------------------------------------------------------------
// Calculate etag based on inode + mtime.
//------------------------------------------------------------------------------
void calculateEtagInodeAndMtime(uint64_t fid, uint64_t mtimeSec, std::string &out) {
  char setag[256];
  snprintf(setag, sizeof(setag) - 1, "\"%llu:%llu\"",
          (unsigned long long) findInode(fid),
          (unsigned long long) mtimeSec);
  out = setag;
}

//------------------------------------------------------------------------------
// Calculate etag for the given FileMD.
//------------------------------------------------------------------------------
void calculateEtag(const eos::ns::FileMdProto &proto, std::string &out) {
  //----------------------------------------------------------------------------
  // Forced etag?
  //----------------------------------------------------------------------------
  constexpr char tmpEtag[] = "sys.tmp.etag";
  if(proto.xattrs().count(tmpEtag)) {
    out = proto.xattrs().at(tmpEtag);
    return;
  }

  //----------------------------------------------------------------------------
  // Nope. Is there a checksum?
  //----------------------------------------------------------------------------
  size_t cxlen = eos::common::LayoutId::GetChecksumLen(proto.layout_id());

  if(cxlen > 0) {
    //--------------------------------------------------------------------------
    // Yes, use inode + checksum for the etag.
    // If MD5 checksums are used we omit the inode number, S3 wants that
    //--------------------------------------------------------------------------
    if (eos::common::LayoutId::GetChecksum(proto.layout_id()) == eos::common::LayoutId::kMD5) {
      out = "\"";
      eos::appendChecksumOnStringProtobuf(proto, out);
      out += "\"";
    }
    else {
      char setag[256];
      snprintf(setag, sizeof(setag) - 1, "\"%llu:", (unsigned long long) findInode(proto.id()));
      out = setag;
      eos::appendChecksumOnStringProtobuf(proto, out);
      out += "\"";
    }

    return;
  }

  //----------------------------------------------------------------------------
  // Nope, fallback to inode + mtime.
  //----------------------------------------------------------------------------
  eos::IFileMD::ctime_t mtime;
  (void) memcpy(&mtime, proto.mtime().data(), sizeof(eos::IFileMD::ctime_t));
  calculateEtagInodeAndMtime(proto.id(), mtime.tv_sec, out);

  return;
}

//------------------------------------------------------------------------------
// Calculate etag for the given FileMD.
//------------------------------------------------------------------------------
void calculateEtag(const IFileMD *const fmd, std::string &out) {
  //----------------------------------------------------------------------------
  // Forced etag?
  //----------------------------------------------------------------------------
  constexpr char tmpEtag[] = "sys.tmp.etag";
  if(fmd->hasAttribute(tmpEtag)) {
    out = fmd->getAttribute(tmpEtag);
    return;
  }

  //----------------------------------------------------------------------------
  // Nope. Is there a checksum?
  //----------------------------------------------------------------------------
  size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId());

  if(cxlen > 0) {
    //--------------------------------------------------------------------------
    // Yes, use inode + checksum for the etag.
    // If MD5 checksums are used we omit the inode number, S3 wants that
    //--------------------------------------------------------------------------
    if (eos::common::LayoutId::GetChecksum(fmd->getLayoutId()) == eos::common::LayoutId::kMD5) {
      out = "\"";
      eos::appendChecksumOnStringAsHex(fmd, out);
      out += "\"";
    }
    else {
      char setag[256];
      snprintf(setag, sizeof(setag) - 1, "\"%llu:", (unsigned long long) findInode(fmd->getId()));
      out = setag;
      eos::appendChecksumOnStringAsHex(fmd, out);
      out += "\"";
    }

    return;
  }

  //----------------------------------------------------------------------------
  // Nope, fallback to inode + mtime.
  //----------------------------------------------------------------------------
  eos::IFileMD::ctime_t mtime;
  fmd->getCTime(mtime);
  calculateEtagInodeAndMtime(fmd->getId(), mtime.tv_sec, out);
  return;
}

//----------------------------------------------------------------------------
// Calculate etag for the given ContainerMD.
//----------------------------------------------------------------------------
void calculateEtag(IContainerMD *cmd, std::string &out) {
  //----------------------------------------------------------------------------
  // Forced etag?
  //----------------------------------------------------------------------------
  constexpr char tmpEtag[] = "sys.tmp.etag";
  if(cmd->hasAttribute(tmpEtag)) {
    out = cmd->getAttribute(tmpEtag);
    return;
  }

  //----------------------------------------------------------------------------
  // Use inode + tmtime
  //----------------------------------------------------------------------------
  eos::IFileMD::ctime_t mtime;
  cmd->getTMTime(mtime);

  char setag[256];
  snprintf(setag, sizeof(setag) - 1, "%llx:%llu.%03lu",
          (unsigned long long) cmd->getId(),
          (unsigned long long) mtime.tv_sec,
          (unsigned long) mtime.tv_nsec / 1000000);

  out = setag;
  return;
}

EOSNSNAMESPACE_END
