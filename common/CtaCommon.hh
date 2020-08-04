//-----------------------------------------------------------------------
//! @file CtaCommon.hh
//! @author Michael Davis - CERN
//! @brief CTA to EOS conversion functions shared by MGM and FST
//-----------------------------------------------------------------------

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

#include "LayoutId.hh"
#include "cta_frontend.pb.h"

EOSCOMMONNAMESPACE_BEGIN

class CtaCommon {
public:

  /*!
   * Convert EOS checksum to CTA checksum blob protobuf
   */
  static void SetChecksum(cta::common::ChecksumBlob::Checksum *cs, int type, const std::string &value) {
    // Convert LayoutId enum to CTA+EOS enum
    switch(LayoutId::GetChecksum(type)) {
      case LayoutId::kNone:   cs->set_type(cta::common::ChecksumBlob::Checksum::NONE);    break;
      case LayoutId::kAdler:  cs->set_type(cta::common::ChecksumBlob::Checksum::ADLER32); break;
      case LayoutId::kCRC32:  cs->set_type(cta::common::ChecksumBlob::Checksum::CRC32);   break;
      case LayoutId::kMD5:    cs->set_type(cta::common::ChecksumBlob::Checksum::MD5);     break;
      case LayoutId::kSHA1:   cs->set_type(cta::common::ChecksumBlob::Checksum::SHA1);    break;
      case LayoutId::kCRC32C: cs->set_type(cta::common::ChecksumBlob::Checksum::CRC32C);  break;
      // Follows the behaviour of LayoutId::GetChecksumString():
      // unknown enum values set checksum type to None rather than throwing an exception
      default:                cs->set_type(cta::common::ChecksumBlob::Checksum::NONE);
    };

    // Validate the length of the supplied hex string
    auto byteArrayLen = LayoutId::GetChecksumLen(type);
    if(byteArrayLen == 0) {
      cs->set_value("");
      return;
    } else if((value.length()%2 != 0) || (value.length() > byteArrayLen*2)) {
      cs->set_value("INVALID CHECKSUM LENGTH=" + std::to_string(value.length()));
      return;
    }

    // Convert string hex representation into a little-endian byte array
    std::string byteArray;
    for(unsigned int i = 0; i < value.length(); i += 2) {
      char byte = static_cast<char>(strtol(value.substr(i, 2).c_str(), NULL, 16));
      byteArray.insert(byteArray.begin(), byte);
    }
    cs->set_value(byteArray);
  }

  /*!
   * Convert CTA ResponseType to std::string
   */
  static std::string ctaResponseCodeToString(cta::xrd::Response::ResponseType rt)
  {
    switch(rt) {
      case cta::xrd::Response::RSP_ERR_CTA:      return "RSP_ERR_CTA";
      case cta::xrd::Response::RSP_ERR_USER:     return "RSP_ERR_USER";
      case cta::xrd::Response::RSP_ERR_PROTOBUF: return "RSP_ERR_PROTOBUF";
      case cta::xrd::Response::RSP_INVALID:      
      default:                                   return "RSP_INVALID";
    }
  }
};

EOSCOMMONNAMESPACE_END
