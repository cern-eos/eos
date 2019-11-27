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
#include "namespace/utils/Checksum.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Calculate etag for the given FileMD.
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
  unsigned long long inodeNumber = eos::common::FileId::etagInode(proto.id());

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
      snprintf(setag, sizeof(setag) - 1, "\"%llu:", (unsigned long long) inodeNumber);
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

  char setag[256];
  snprintf(setag, sizeof(setag) - 1, "\"%llu:%llu\"",
          (unsigned long long) inodeNumber,
          (unsigned long long) mtime.tv_sec);
  out = setag;
  return;
}

//------------------------------------------------------------------------------
//! Calculate etag for the given FileMD.
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
  unsigned long long inodeNumber = eos::common::FileId::etagInode(fmd->getId());

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
      snprintf(setag, sizeof(setag) - 1, "\"%llu:", (unsigned long long) inodeNumber);
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

  char setag[256];
  snprintf(setag, sizeof(setag) - 1, "\"%llu:%llu\"",
          (unsigned long long) inodeNumber,
          (unsigned long long) mtime.tv_sec);
  out = setag;
  return;
}

//----------------------------------------------------------------------------
//! Calculate etag for the given ContainerMD.
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
