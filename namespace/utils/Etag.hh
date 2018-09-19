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
// author: Georgios Bitzes <georgios.bitzes@cern.ch>
// desc:   Namespace etag utilities
//------------------------------------------------------------------------------

#include "namespace/interface/IFileMD.hh"
#include "common/FileId.hh"

#ifndef EOS_NS_ETAG_HH
#define EOS_NS_ETAG_HH

namespace eos
{

  //----------------------------------------------------------------------------
  //! Calculate etag for the given FileMD.
  //----------------------------------------------------------------------------
  void calculateEtag(IFileMD *fmd, std::string &out) {
    //----------------------------------------------------------------------------
    // Forced etag?
    //----------------------------------------------------------------------------
    constexpr char tmpEtag[] = "sys.tmp.etag";
    if(fmd->hasAttribute(tmpEtag)) {
      out = fmd->getAttribute(tmpEtag);
      return;
    }

    //--------------------------------------------------------------------------
    // Nope. Is there a checksum?
    //--------------------------------------------------------------------------
    size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId());
    unsigned long long inodeNumber = eos::common::FileId::FidToInode(fmd->getId());

    if(cxlen > 0) {
      //------------------------------------------------------------------------
      // Yes, use inode + checksum for the etag.
      // If MD5 checksums are used we omit the inode number, S3 wants that
      //------------------------------------------------------------------------
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

    //--------------------------------------------------------------------------
    // Nope, fallback to inode + mtime.
    //--------------------------------------------------------------------------
    eos::IFileMD::ctime_t mtime;
    fmd->getCTime(mtime);

    char setag[256];
    snprintf(setag, sizeof(setag) - 1, "\"%llu:%llu\"",
            (unsigned long long) inodeNumber,
            (unsigned long long) mtime.tv_sec);
    out = setag;
    return;
  }

}

#endif
