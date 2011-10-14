// ----------------------------------------------------------------------
// File: ChecksumPlugins.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#ifndef __EOSFST_CHECKSUMPLUGIN_HH__
#define __EOSFST_CHECKSUMPLUGIN_HH__

/*----------------------------------------------------------------------------*/
#include "common/LayoutId.hh"
#include "fst/Namespace.hh"
#include "fst/checksum/CheckSum.hh"
#include "fst/checksum/Adler.hh"
#include "fst/checksum/CRC32.hh"
#include "fst/checksum/CRC32C.hh"
#include "fst/checksum/MD5.hh"
#include "fst/checksum/SHA1.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class ChecksumPlugins {
public:
  ChecksumPlugins() {};
  ~ChecksumPlugins() {};

  static CheckSum* GetChecksumObject(unsigned int layoutid, bool blockchecksum=false) {
    if (blockchecksum) {
      if (eos::common::LayoutId::GetBlockChecksum(layoutid) == eos::common::LayoutId::kAdler) {
        return (CheckSum*)new Adler;
      }
      if (eos::common::LayoutId::GetBlockChecksum(layoutid) == eos::common::LayoutId::kCRC32) {
        return (CheckSum*)new CRC32;
      }
      if (eos::common::LayoutId::GetBlockChecksum(layoutid) == eos::common::LayoutId::kCRC32C) {
        return (CheckSum*)new CRC32C;
      }
      if (eos::common::LayoutId::GetBlockChecksum(layoutid) == eos::common::LayoutId::kMD5) {
        return (CheckSum*)new MD5;
      }
      if (eos::common::LayoutId::GetBlockChecksum(layoutid) == eos::common::LayoutId::kSHA1) {
        return (CheckSum*)new SHA1;
      }
    } else {
      if (eos::common::LayoutId::GetChecksum(layoutid) == eos::common::LayoutId::kAdler) {
        return (CheckSum*)new Adler;
      }
      if (eos::common::LayoutId::GetChecksum(layoutid) == eos::common::LayoutId::kCRC32) {
        return (CheckSum*)new CRC32;
      }
      if (eos::common::LayoutId::GetChecksum(layoutid) == eos::common::LayoutId::kCRC32C) {
        return (CheckSum*)new CRC32C;
      }
      if (eos::common::LayoutId::GetChecksum(layoutid) == eos::common::LayoutId::kMD5) {
        return (CheckSum*)new MD5;
      }
      if (eos::common::LayoutId::GetChecksum(layoutid) == eos::common::LayoutId::kSHA1) {
        return (CheckSum*)new SHA1;
      }
    }

    return 0;
  }
};

EOSFSTNAMESPACE_END

#endif
