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

#pragma once
#include "fst/Namespace.hh"
#include "common/LayoutId.hh"
#include "fst/checksum/CheckSum.hh"
#include "fst/checksum/Adler.hh"
#include "fst/checksum/BLAKE3.hh"
#include "fst/checksum/CRC32.hh"
#include "fst/checksum/CRC32C.hh"
#include "fst/checksum/MD5.hh"
#include "fst/checksum/SHA1.hh"
#include "fst/checksum/CRC64.hh"
#include "fst/checksum/SHA256.hh"

#ifdef XXHASH_FOUND
#include "fst/checksum/XXHASH64.hh"
#endif

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class ChecksumPluging
//------------------------------------------------------------------------------
class ChecksumPlugins
{
public:
  //----------------------------------------------------------------------------
  //! Get checksum object depending on the given type
  //!
  //! @param xs_type checksum type given usigned long
  //!
  //! @return checksum object
  //----------------------------------------------------------------------------
  static CheckSum*
  GetXsObj(unsigned long xs_type)
  {
    if (xs_type == eos::common::LayoutId::kAdler) {
      return static_cast<CheckSum*>(new Adler());
    } else if (xs_type == eos::common::LayoutId::kBLAKE3) {
      return static_cast<CheckSum*>(new BLAKE3());
    } else if (xs_type == eos::common::LayoutId::kCRC32) {
      return static_cast<CheckSum*>(new CRC32());
    } else if (xs_type == eos::common::LayoutId::kCRC32C) {
      return static_cast<CheckSum*>(new CRC32C());
    } else if (xs_type == eos::common::LayoutId::kMD5) {
      return static_cast<CheckSum*>(new MD5());
    } else if (xs_type == eos::common::LayoutId::kSHA1) {
      return static_cast<CheckSum*>(new SHA1());
    } else if (xs_type == eos::common::LayoutId::kCRC64) {
      return static_cast<CheckSum*>(new CRC64());
    } else if (xs_type == eos::common::LayoutId::kSHA256) {
      return static_cast<CheckSum*>(new SHA256());
#ifdef XXHASH_FOUND
    } else if (xs_type == eos::common::LayoutId::kXXHASH64) {
      return static_cast<CheckSum*>(new XXHASH64());
#endif
    }

    return nullptr;
  }

  //----------------------------------------------------------------------------
  //! Get checksum object depending on the given type
  //!
  //! @param xs_type checksum type given as string
  //!
  //! @return checksum object
  //----------------------------------------------------------------------------
  static std::unique_ptr<CheckSum>
  GetXsObj(const std::string& xs_type)
  {
    return std::unique_ptr<CheckSum>
           (GetXsObj(eos::common::LayoutId::GetChecksumFromString(xs_type)));
  }

  //----------------------------------------------------------------------------
  //! Get checksum object given the layoutid
  //!
  //! @param layoutid layout id endcoding see eos::common::LayoutId
  //! @param blockchecksum if true then return the checksum object for the
  //!        block checksum part encoded in the layout id
  //!
  //! @return checksum object
  //----------------------------------------------------------------------------
  static std::unique_ptr<CheckSum>
  GetChecksumObject(unsigned long layoutid, bool blockchecksum = false)
  {
    unsigned int xs_type = (blockchecksum ?
                            eos::common::LayoutId::GetBlockChecksum(layoutid) :
                            eos::common::LayoutId::GetChecksum(layoutid));
    return std::unique_ptr<CheckSum>(GetXsObj(xs_type));
  }
};

EOSFSTNAMESPACE_END
